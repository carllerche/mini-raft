use crate::*;

use std::io;
use tokio::time::Instant;

pub(crate) struct Candidate<T: Driver> {
    // True if this is a pre-election
    pre_election: bool,

    /// How long to wait for votes
    restart_campaign_at: Instant,

    /// Tracks votes received from peers.
    votes: Votes<T>,

    /// When we last received a message from a known leader.
    last_leader_msg_at: Instant,
}

impl<T: Driver> Candidate<T> {
    #[tracing::instrument(level = "debug", skip(node))]
    pub(crate) fn transition_from_follower(
        node: &mut Node<T>,
        last_leader_msg_at: Instant,
    ) -> Candidate<T> {
        // Transitioning to pre-candidiate only happens when there is no known
        // group leader.
        node.group.leader = None;

        Candidate::new(node, true, last_leader_msg_at)
    }

    #[tracing::instrument(level = "debug", skip(node))]
    pub(crate) fn transition_from_pre_candidate(
        node: &mut Node<T>,
        last_leader_msg_at: Instant,
    ) -> Candidate<T> {
        assert!(node.group.leader.is_none());

        // When transitioning to candidate, the term is incremented.
        node.increment_term();
        node.group
            .transition_to_candidate(&node.config.id, &node.log);

        Candidate::new(node, false, last_leader_msg_at)
    }

    pub(crate) fn transition_from_timed_out_campaign(
        node: &mut Node<T>,
        last_leader_msg_at: Instant,
    ) -> Candidate<T> {
        // There should **still** be no leader
        assert!(node.group.leader.is_none());

        // Now back at the pre-campaign stage
        Candidate::new(node, true, last_leader_msg_at)
    }

    fn new(node: &mut Node<T>, pre_election: bool, last_leader_msg_at: Instant) -> Candidate<T> {
        // There should be no known leader
        assert!(node.group.leader.is_none());

        // Reset the vote tracker & vote for ourself (because we are cool).
        let mut votes = Votes::new();

        // Vote for ourselves
        votes.record(&node.group, &node.config.id, true);

        let last_log_pos = node.log.last_appended();

        for voter_id in node.group.voter_ids() {
            // Don't send a message to ourself
            if node.config.id == *voter_id {
                continue;
            }

            node.driver.dispatch(
                voter_id.clone(),
                Message {
                    origin: message::Origin {
                        id: node.config.id.clone(),
                        term: if pre_election {
                            // A pre-vote message uses the *next* term that the node would
                            // use when announcing its candidacy.
                            node.term + 1
                        } else {
                            node.term
                        },
                    },
                    action: if pre_election {
                        message::Action::PreVote(message::Vote { last_log_pos })
                    } else {
                        message::Action::Vote(message::Vote { last_log_pos })
                    },
                },
            );
        }

        Candidate {
            pre_election,
            restart_campaign_at: node.campaign_at(),
            votes,
            last_leader_msg_at,
        }
    }

    pub(crate) async fn receive(
        &mut self,
        node: &mut Node<T>,
        message: Message<T::Id>,
    ) -> io::Result<Option<Stage<T>>> {
        use message::Action::*;

        match message.action {
            PreVoteResponse(message::VoteResponse { granted }) => {
                // If the pre-vote response is no, then the term is the peer's
                // **actual** term.
                if !granted && message.origin.term > node.term {
                    self.transition_to_follower(node, message).await
                } else if !self.pre_election {
                    // Not currently a pre-election, so ignore the messages
                    Ok(None)
                } else if message.origin.term < node.term {
                    // Outdated message
                    Ok(None)
                } else if granted && message.origin.term != node.term + 1 {
                    // Outdated message
                    Ok(None)
                } else {
                    self.votes.record(&node.group, &message.origin.id, granted);
                    // Counting votes happens in `tick`
                    Ok(None)
                }
            }
            VoteResponse(message::VoteResponse { granted }) => {
                // A vote response with a higher term always results in updating the term.
                if message.origin.term > node.term {
                    self.transition_to_follower(node, message).await
                } else if message.origin.term == node.term {
                    if !self.pre_election {
                        self.votes.record(&node.group, &message.origin.id, granted);
                        Ok(None)
                    } else {
                        // The message is out of date, ignore it.
                        Ok(None)
                    }
                } else {
                    Ok(None)
                }
            }
            PreVote(vote) => {
                // We must respond to pre-votes even if the term is lower. It is
                // possible to have a peer with a lower term but more log. Not
                // responding would result in the group deadlocking.
                let last_appended = node.log.last_appended();

                // For pre-votes, we only vote "yes" if the message term is greater than ours.
                let granted = message.origin.term > node.term && vote.last_log_pos >= last_appended;

                // Never update our own term or state as part of a pre-vote
                // request. Because we are a candidate, we have not yet issued
                // any votes that we need to be held to. The only determining
                // factor for whether or not we *may* vote for the peer
                // candidate is if the term is greater than our current term and
                // the candidate's log is at least as current as ours.
                node.driver.dispatch(
                    message.origin.id.clone(),
                    Message {
                        origin: message::Origin {
                            id: node.config.id.clone(),
                            // See `Follower` for the reason we are using
                            // the origin term here.
                            term: message.origin.term,
                        },
                        action: message::Action::PreVoteResponse(message::VoteResponse { granted }),
                    },
                );

                Ok(None)
            }
            Vote(..) => {
                if message.origin.term > node.term {
                    // We received a vote request with a term greater than ours.
                    // We must transition back to follower. If the candidate has
                    // a log at least as up to date as ours, then we can vote
                    // for the candidate.
                    self.transition_to_follower(node, message).await
                } else {
                    // As a candidate, we already voted for ourselves this term.
                    node.driver.dispatch(
                        message.origin.id.clone(),
                        Message {
                            origin: message::Origin {
                                id: node.config.id.clone(),
                                term: node.term,
                            },
                            action: message::Action::PreVoteResponse(message::VoteResponse {
                                granted: false,
                            }),
                        },
                    );

                    Ok(None)
                }
            }
            AppendEntries(..) => {
                if message.origin.term < node.term {
                    // We have received messages from a leader at a lower term.
                    // It is possible that these messages were simply delayed in
                    // the network, but this could also mean that this node has
                    // advanced its term number during a network partition, and
                    // it is now unable to either win an election or to rejoin
                    // the majority on the old term. If checkQuorum is false,
                    // this will be handled by incrementing term numbers in
                    // response to MsgVote with a higher term, but if
                    // checkQuorum is true we may not advance the term on
                    // MsgVote and must generate other messages to advance the
                    // term. The net result of these two features is to minimize
                    // the disruption caused by nodes that have been removed
                    // from the cluster's configuration: a removed node will
                    // send MsgVotes which will be ignored, but it will not
                    // receive MsgApp or MsgHeartbeat, so it will not create
                    // disruptive term increases, by notifying leader of this
                    // node's activeness. The above comments also true for
                    // Pre-Vote
                    //
                    // When follower gets isolated, it soon starts an election
                    // ending up with a higher term than leader, although it
                    // won't receive enough votes to win the election. When it
                    // regains connectivity, this response with "pb.MsgAppResp"
                    // of higher term would force leader to step down. However,
                    // this disruption is inevitable to free this stuck node
                    // with fresh election. This can be prevented with Pre-Vote
                    // phase.
                    //
                    // (Copied from raft-rs)
                    node.driver.dispatch(
                        message.origin.id.clone(),
                        Message {
                            origin: message::Origin {
                                id: node.config.id.clone(),
                                term: node.term,
                            },
                            action: message::Action::AppendEntriesResponse(
                                message::AppendEntriesResponse::Reject,
                            ),
                        },
                    );
                    Ok(None)
                } else {
                    // Switch back to follower, maybe incrementing our term.
                    self.transition_to_follower(node, message).await
                }
            }
            AppendEntriesResponse(_) => {
                // Does not apply to candidates. The node most likely used to be
                // a leader and is receiving an outdated message. However,
                // **if** the origin term is greater than the node's term, this
                // indicates a peer got partitioned, incremented its term, and
                // is now trying to rejoin the group.
                if message.origin.term > node.term {
                    self.transition_to_follower(node, message).await
                } else {
                    Ok(None)
                }
            }
        }
    }

    pub(crate) async fn tick(&mut self, node: &mut Node<T>) -> io::Result<Step<Stage<T>>> {
        use votes::Tally::*;

        // First, tally votes.
        Ok(match self.votes.tally(&node.group) {
            Win => {
                if self.pre_election {
                    Candidate::transition_from_pre_candidate(node, self.last_leader_msg_at).into()
                } else {
                    Leader::transition_from_candidate(node).await?.into()
                }
            }
            Lose => {
                // We lost! Oh no... well, transition back to follower.
                Follower::transition_from_lost_election(node, self.last_leader_msg_at).into()
            }
            Pending => {
                // At this point, just wait for the timeout to expire. If it is
                // hit, this means that no further messages are received and the
                // pre-electiololn failed.
                if node.before(self.restart_campaign_at) {
                    return Ok(self.restart_campaign_at.into());
                }

                // Transition back to pre-candidate and restart the election
                Candidate::transition_from_timed_out_campaign(node, self.last_leader_msg_at).into()
            }
        })
    }

    // Process a message that transitioned the node back to follower
    async fn transition_to_follower(
        &mut self,
        node: &mut Node<T>,
        message: Message<T::Id>,
    ) -> io::Result<Option<Stage<T>>> {
        // If the term is **not** being incremented and this is **not** a
        // pre-election, then we already voted for ourselves this term and will
        // ignore all other votes.
        let voted_for = if !self.pre_election && message.origin.term == node.term {
            Some(node.config.id.clone())
        } else {
            None
        };

        // We received a message that is forcing us back to follower.
        let mut follower = Follower::transition_from_candidate(
            node,
            message.origin.term,
            None,
            voted_for,
            self.last_leader_msg_at,
        );

        // Process the message as a follower
        transition!(follower.receive(node, message,).await?, follower)
    }
}

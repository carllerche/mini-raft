use crate::*;

use std::io;
use tokio::time::Instant;

pub(crate) struct Follower<T: Driver> {
    observer: Observer<T>,
    /// If the node is currently eligible to vote in elections and become a
    /// leader. Note, that even when this is `Observer`, the node must still
    /// respond to vote requests as it may have been promoted to be a voter but
    /// has not been informed yet.
    role: Role,

    /// The instant at which we **last** heard from the leader. This is used to
    /// determine if we respond to vote requests.
    last_leader_msg_at: Instant,

    /// Who did we vote for **this** term
    voted_for: Option<T::Id>,
}

/// Whether or not the node is eligible for elections.
#[derive(Debug)]
enum Role {
    /// The node is just an observer and is not eligible for election. Even so,
    /// **if** we receive vote requests, we must still respond.
    Observer,
    /// The node is elligible to be elected a leader.
    Candidate {
        /// When the follower should start campaining. This instant is pushed
        /// forward as long as the follower receives signal that the leader is
        /// healthy.
        campaign_at: Instant,
    },
}

impl<T: Driver> Follower<T> {
    pub(crate) fn new_observer(node: &mut Node<T>) -> Follower<T> {
        Follower {
            observer: Observer::new(),
            role: Role::Observer,
            // Set `last_leader_msg_at` in the past to be able to immediately
            // respond to vote requests.
            last_leader_msg_at: node.now - node.config.max_election_interval,
            voted_for: None,
        }
    }

    pub(crate) fn transition_from_observer(node: &mut Node<T>) -> Follower<T> {
        Follower {
            observer: Observer::new(),
            role: Role::Candidate {
                campaign_at: node.campaign_at(),
            },
            last_leader_msg_at: node.now,
            voted_for: None,
        }
    }

    pub(crate) fn transition_from_leader(
        node: &mut Node<T>,
        term: Term,
        leader: Option<T::Id>,
    ) -> Follower<T> {
        node.set_term(term, leader);

        // We are being **downgraded** from a leader to a follower because a
        // newer leader sent us a message. Thus, we have received a message from
        // the leader!
        Follower {
            observer: Observer::new(),
            role: Role::Candidate {
                campaign_at: node.campaign_at(),
            },
            last_leader_msg_at: node.now,
            voted_for: None,
        }
    }

    /// The node transitioned from candidate to follower.
    ///
    /// This can happens when the candidate receives a message that forces it
    /// back to the leader stage.
    pub(crate) fn transition_from_candidate(
        node: &mut Node<T>,
        term: Term,
        leader: Option<T::Id>,
        voted_for: Option<T::Id>,
        last_leader_msg_at: Instant,
    ) -> Follower<T> {
        if term > node.term {
            node.set_term(term, leader);
        } else if leader.is_some() {
            assert!(node.group.leader.is_none());
            node.group.leader = leader;
        }

        Follower {
            observer: Observer::new(),
            role: Role::Candidate {
                campaign_at: node.campaign_at(),
            },
            last_leader_msg_at,
            voted_for,
        }
    }

    pub(crate) fn transition_from_lost_election(
        node: &mut Node<T>,
        last_leader_msg_at: Instant,
    ) -> Follower<T> {
        Follower {
            observer: Observer::new(),
            role: Role::Candidate {
                campaign_at: node.campaign_at(),
            },
            last_leader_msg_at,
            voted_for: Some(node.config.id.clone()),
        }
    }

    /// The leader lost connectivity with a quorum of voters.
    pub(crate) fn transition_from_leader_lost_quorum(node: &mut Node<T>) -> Follower<T> {
        // We should be transitioning from being a leader, so at this point, we
        // should believe that *we* are the leader.
        assert_eq!(node.group.leader.as_ref(), Some(&node.config.id));

        // Since we *just* were the leader, this means we voted for ourselves
        // this term. This should remain, we cannot vote for any other peer in
        // the current term.
        let voted_for = Some(node.config.id.clone());

        // We were just a leader, but we stepped down. So, we will set
        // `last_leader_msg_at` in the past to enable ourselves to vote for new
        // leaders as soon as our term is incremented.
        let last_leader_msg_at = node.now - node.config.max_election_interval;

        Follower {
            observer: Observer::new(),
            role: Role::Candidate {
                campaign_at: node.campaign_at(),
            },
            last_leader_msg_at,
            voted_for,
        }
    }

    /// Returns true if the node is an observer
    pub(crate) fn is_observer(&self) -> bool {
        matches!(self.role, Role::Observer)
    }

    pub(crate) async fn receive(
        &mut self,
        node: &mut Node<T>,
        message: Message<T::Id>,
    ) -> io::Result<Option<Stage<T>>> {
        use message::Action::*;

        match message.action {
            AppendEntries(append_entries) => {
                use observer::ReceiveAppendEntries::*;

                // Term mismatch is handled in the observer's
                // receive_append_entries method.
                let did_append_config = match self
                    .observer
                    .receive_append_entries(node, &message.origin, append_entries)
                    .await?
                {
                    Discard => return Ok(None),
                    Appended => false,
                    AppendedConfig => true,
                };

                // Track that we received a message from the leader
                self.last_leader_msg_at = node.now;

                // TODO: this should not be called if the message is rejected
                if let Role::Candidate { campaign_at } = &mut self.role {
                    *campaign_at = node.campaign_at();
                }

                if did_append_config {
                    self.process_membership_change(node);
                }

                Ok(None)
            }
            PreVote(message::Vote { last_log_pos }) => {
                self.process_vote_request(node, &message.origin, last_log_pos, true);
                Ok(None)
            }
            Vote(message::Vote { last_log_pos }) => {
                self.process_vote_request(node, &message.origin, last_log_pos, false);
                Ok(None)
            }
            // Never update a term when receiving a granted pre-vote response
            PreVoteResponse(message::VoteResponse { granted: true }) => Ok(None),
            AppendEntriesResponse(_) | PreVoteResponse(_) | VoteResponse(_) => {
                if message.origin.term > node.term {
                    self.process_term_inc(node, message.origin.term, None);
                }
                // Not expected to receive these messages in this state.
                Ok(None)
            }
        }
    }

    pub(crate) fn tick(&mut self, node: &mut Node<T>) -> Step<Stage<T>> {
        match self.role {
            Role::Candidate { campaign_at } => {
                // Wait until the the campaign timeout is reached
                if node.before(campaign_at) {
                    return campaign_at.into();
                }

                // At this point, no messages have been received, so maybe
                // the leader is no more?
                Candidate::transition_from_follower(node, self.last_leader_msg_at).into()
            }
            Role::Observer => Step::Wait(None),
        }
    }

    // We received a (valid) message with a higher term, so lets update
    // ourselves to the next term.
    fn process_term_inc(&mut self, node: &mut Node<T>, term: Term, leader: Option<T::Id>) {
        assert!(term > node.term);

        node.set_term(term, leader);

        self.voted_for = None;

        if let Role::Candidate { campaign_at } = &mut self.role {
            *campaign_at = node.campaign_at();
        }
    }

    fn process_vote_request(
        &mut self,
        node: &mut Node<T>,
        origin: &message::Origin<T::Id>,
        last_log_pos: Option<Pos>,
        pre_vote: bool,
    ) {
        // If the leader is still sending us messages, then we will not
        // participate in any elections.
        let next_vote_at = self.last_leader_msg_at + node.config.min_election_interval;

        if origin.term < node.term {
            self.respond_vote_no(node, origin, pre_vote);
        } else if node.now < next_vote_at {
            // As far as we know, the leader is still active. Explicitly reject
            // the vote if this is a pre-election, otherwise just ignore the
            // message.
            if pre_vote {
                self.respond_vote_no(node, origin, pre_vote);
            }
        } else if origin.term > node.term {
            if !pre_vote {
                self.process_term_inc(node, origin.term, None);
            }

            self.respond_vote(node, origin, last_log_pos, pre_vote);
        } else if let Some(_) = &node.group.leader {
            self.respond_vote_no(node, origin, pre_vote);
        } else {
            // If we already voted for a different node, then we reject.
            // Otherwise, accept since it just means we received the vote
            // request twice.
            match &self.voted_for {
                Some(id) if *id != origin.id => {
                    self.respond_vote_no(node, origin, pre_vote);
                }
                _ => {
                    self.respond_vote(node, origin, last_log_pos, pre_vote);
                }
            }
        }
    }

    fn respond_vote(
        &mut self,
        node: &mut Node<T>,
        origin: &message::Origin<T::Id>,
        last_log_pos: Option<Pos>,
        pre_vote: bool,
    ) {
        let our_last_log_pos = node.log.last_appended();

        if last_log_pos < our_last_log_pos {
            // Candidate is not as up-to-date as we are.
            self.respond_vote_no(node, origin, pre_vote);
        } else {
            self.respond_vote_yes(node, origin, pre_vote);
        }
    }

    fn process_membership_change(&mut self, node: &mut Node<T>) {
        let peer = node
            .group
            .peers
            .get(&node.config.id)
            .expect("self missing in raft group");

        if peer.is_voter() {
            if self.is_observer() {
                *self = Follower::transition_from_observer(node);
            }
        } else {
            if !self.is_observer() {
                todo!("unexpected downgrade to observer");
            }
        }
    }

    /// Vote for the candidate.
    ///
    /// Respond with the term included in the original message.
    fn respond_vote_yes(
        &mut self,
        node: &mut Node<T>,
        origin: &message::Origin<T::Id>,
        pre_vote: bool,
    ) {
        if pre_vote {
            assert!(node.term <= origin.term);
        } else {
            assert!(node.term == origin.term);
            assert!(self.voted_for.is_none() || self.voted_for == Some(origin.id.clone()));

            self.voted_for = Some(origin.id.clone());
        }

        let response = message::VoteResponse { granted: true };
        node.driver.dispatch(
            origin.id.clone(),
            Message {
                origin: message::Origin {
                    id: node.config.id.clone(),
                    // When responding to pre-vote messages, the origin term is
                    // set to the original vote request term, not the current
                    // term. The current node may still be in a past term.
                    // Because we don't update our own term when receiving a
                    // pre-vote message, if we include an out-of-date term, then
                    // the candidate sending the vote request will discard the
                    // message. If this is a regular vote (not pre), then the
                    // node's current term is the same as the origin term. See
                    // the above assertion that checks for this.
                    term: origin.term,
                },
                action: if pre_vote {
                    message::Action::PreVoteResponse(response)
                } else {
                    message::Action::VoteResponse(response)
                },
            },
        );
    }

    fn respond_vote_no(&self, node: &mut Node<T>, origin: &message::Origin<T::Id>, pre_vote: bool) {
        let response = message::VoteResponse { granted: false };
        node.driver.dispatch(
            origin.id.clone(),
            Message {
                origin: message::Origin {
                    id: node.config.id.clone(),
                    // A "no" vote always responds with the current node's term (as opposed to a "yes" pre-vote response).
                    term: node.term,
                },
                action: if pre_vote {
                    message::Action::PreVoteResponse(response)
                } else {
                    message::Action::VoteResponse(response)
                },
            },
        )
    }
}

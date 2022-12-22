use crate::*;

use std::cmp;
use std::io;
use std::marker::PhantomData;
use tokio::time::Instant;

pub(crate) struct Leader<T: Driver> {
    /// When to send out the next heartbeat
    heartbeat_at: Instant,

    /// Figures out the highest committed index.
    commitments: Commitments<T>,

    /// Used to check if the leader still holds quorum. If the leader gets
    /// partitioned from a majority of peers, this process will force it to step
    /// down eventually.
    check_quorum: Votes<T>,

    /// When to perform the next group quorum test.
    check_quorum_at: Instant,
}

/// Figures out what log indices are committed, factoring in joint-consensus
pub(crate) struct Commitments<T: Driver> {
    /// Whn the group is **not** in joint-consensus mode, commitments are
    /// tracked here. When the group **is** in joint-consensus mode, peers in
    /// the **incoming** group are tracked here.
    primary: Vec<Index>,

    /// When the group is **not** in joint-consensus mode, this vec is not used.
    /// When the group **is** in joint-consensus mode, peers in the **outgoing**
    /// group are tracked here.
    secondary: Vec<Index>,

    _p: PhantomData<T>,
}

impl<T: Driver> Leader<T> {
    pub(crate) async fn transition_from_candidate(node: &mut Node<T>) -> io::Result<Leader<T>> {
        // Reset group state to reflect we are becoming the leader.
        node.group
            .transition_to_leader(&node.config.id, &node.log, node.now);

        let mut leader = Leader::new(node);

        // Propose a new, empty, entry to puncutate the start of the new term.
        //
        // Skip this proposal if we are already at the max uncommitted entries
        // limit. This value will be proposed once committing catches up. This
        // is needed to avoid runaway proposals on new election when the network
        // is flaky. If this is enabled, it is possible for uncommitted entries
        // to grow unbounded.
        // if leader.commitments.committed_index(&node.group)
        let quorum_replicated = leader.commitments.quorum_replicated(&node.group);
        if leader.can_propose_new_term(node, quorum_replicated) {
            leader.propose(node, message::Value::new_term()).await?;
        }

        // Initialize the heartbeat interval
        leader.heartbeat_at = heartbeat_at(node);

        Ok(leader)
    }

    pub(crate) fn new(node: &Node<T>) -> Leader<T> {
        Leader {
            heartbeat_at: heartbeat_at(node),
            commitments: Commitments {
                primary: vec![],
                secondary: vec![],
                _p: PhantomData,
            },
            check_quorum: Votes::new(),
            check_quorum_at: check_quorum_at(node),
        }
    }

    pub(crate) async fn receive(
        &mut self,
        node: &mut Node<T>,
        message: Message<T::Id>,
    ) -> io::Result<Option<Stage<T>>> {
        use message::Action::*;

        match message.action {
            AppendEntriesResponse(response) => {
                // TODO: verify origin term
                self.receive_append_entries_response(node, &message.origin, response)
                    .await
            }
            AppendEntries(append_entries) => {
                self.receive_append_entries(node, &message.origin, append_entries)
                    .await
            }
            // We believe we are the leader, so we shouldn't vote for
            // candidates.
            //
            // If we are partitioned from a quorum of voters, then next
            // `check_quorum` we will step down and become a follower.
            PreVote(_) => {
                self.respond_vote_no(node, &message.origin, true);
                Ok(None)
            }
            Vote(_) => {
                self.respond_vote_no(node, &message.origin, false);
                Ok(None)
            }
            PreVoteResponse(message::VoteResponse { granted: true }) => {
                // Never increase our term on a granted `PreVoteResponse`
                Ok(None)
            }
            VoteResponse(_) | PreVoteResponse(_) => {
                // If the term is higher, we need to transition back to a follower.
                if message.origin.term > node.term {
                    Ok(Follower::transition_from_leader(node, message.origin.term, None).into())
                } else {
                    Ok(None)
                }
            }
        }
    }

    pub(crate) async fn propose(
        &mut self,
        node: &mut Node<T>,
        value: message::Value<T::Id>,
    ) -> io::Result<Pos> {
        // At this point, we should already have ensured the node currently
        // believes itself to be the leader.
        assert!(node.is_leader());

        // Compute the log position this new value will be appended at.
        let pos = node.next_pos();

        if value.is_new_term() {
            debug_assert_ne!(node.log.last_appended.map(|pos| pos.term), Some(node.term));
        }

        // Append the entries to the log. If this is the only node in the raft
        // group, it will also commit the entries.
        let did_configure_self = node
            .append_entries(&[message::Entry {
                pos,
                value: value.clone(),
            }])
            .did_configure_self;

        // Update our own `matched`
        let peer = node
            .group
            .peers
            .get_mut(&node.config.id)
            .expect("missing our own peer state");

        // It should not be possible for our own peer to have matched logs in the future
        assert!(peer.matched < Some(pos.index));
        peer.matched = Some(pos.index);

        // This proposal **may** have resulted in the current node being removed
        // from the voter set. If this is the case, then we need to step down.
        if did_configure_self {
            if !peer.is_voter() {
                todo!("handle stepping down as leader");
            }
        }

        let quorum_replicated = self.commitments.quorum_replicated(&node.group);

        // Maybe updated the committed index.
        self.update_committed(node, quorum_replicated).await?;

        // Broadcast the new entry to all peers.
        self.broadcast_entries(node).await?;

        Ok(pos)
    }

    pub(crate) async fn tick(&mut self, node: &mut Node<T>) -> io::Result<Step<Stage<T>>> {
        loop {
            // When is there work to do?
            let next_action_at = cmp::min(self.heartbeat_at, self.check_quorum_at);

            // Wait until that instant arrives
            if node.now < next_action_at {
                return Ok(next_action_at.into());
            }

            if self.check_quorum_at <= node.now {
                if self.check_quorum(node) {
                    self.check_quorum_at = check_quorum_at(node);
                    // Reset check_quorum_at
                } else {
                    // We are no longer able to reach a quorum of peers. This
                    // most likely means we have become partitioned. That said,
                    // we may still be able to reach **some** peers. In order to
                    // avoid disrupting the group, we should step down and
                    // become a follower at the **current** term.
                    return Ok(Follower::transition_from_leader_lost_quorum(node).into());
                }
            }

            if self.heartbeat_at <= node.now {
                self.broadcast_heartbeat(node).await?
            }
        }
    }

    /// Broadcast newly appended entries to peers
    ///
    /// This is called when new entries are appended to the leader's log.
    async fn broadcast_entries(&mut self, node: &mut Node<T>) -> io::Result<()> {
        for peer in node.group.peers_mut(&node.config.id) {
            // If we are currently probing the peer's log to synchronize
            // replication, then we should not send new entries as they would
            // just be rejected. Once we have synced up with the peer, then we
            // can send entries again.
            if !peer.replication.is_probing() {
                peer.send_append_entries(
                    &mut node.driver,
                    &node.log,
                    false,
                    message::Origin {
                        id: node.config.id.clone(),
                        term: node.term,
                    },
                )
                .await?;
            }
        }

        Ok(())
    }

    // Right now, broadcasting a heartbeat is the same as broadcasting entries
    async fn broadcast_heartbeat(&mut self, node: &mut Node<T>) -> io::Result<()> {
        for peer in node.group.peers_mut(&node.config.id) {
            peer.send_append_entries(
                &mut node.driver,
                &node.log,
                true,
                message::Origin {
                    id: node.config.id.clone(),
                    term: node.term,
                },
            )
            .await?;
        }

        // Update the next heartbeat time
        self.heartbeat_at = heartbeat_at(node);
        Ok(())
    }

    async fn receive_append_entries_response(
        &mut self,
        node: &mut Node<T>,
        origin: &message::Origin<T::Id>,
        message: message::AppendEntriesResponse,
    ) -> io::Result<Option<Stage<T>>> {
        use message::AppendEntriesResponse::*;

        if origin.term > node.term {
            // The term increased
            return Ok(Follower::transition_from_leader(node, origin.term, None).into());
        }
        if origin.term < node.term {
            // If the message term is behind the curernt node's term, then we
            // just drop the message. In theory, we could use it to track peer
            // activity, but if the peer is alive, we should get a response to a
            // heartbeat message.
            return Ok(None);
        }

        match message {
            Success { last_log_pos } => {
                // The message term **should** be less than the leader's current term.
                if origin.term > node.term {
                    todo!();
                }

                self.track_matched_index(node, &origin.id, last_log_pos.index)
                    .await?;

                Ok(None)
            }
            Conflict { rejected, hint } => {
                if let Some(peer) = node.group.peers.get_mut(&origin.id) {
                    peer.receive_append_entries_conflict(
                        &mut node.driver,
                        &node.log,
                        node.now,
                        rejected,
                        hint,
                        message::Origin {
                            id: node.config.id.clone(),
                            term: node.term,
                        },
                    )
                    .await?;
                }

                Ok(None)
            }
            Reject => {
                if origin.term > node.term {
                    // The term increased
                    Ok(Follower::transition_from_leader(node, origin.term, None).into())
                } else {
                    // Message is out of date, discard
                    Ok(None)
                }
            }
        }
    }

    /// Another node believes it is the leader
    async fn receive_append_entries(
        &mut self,
        node: &mut Node<T>,
        origin: &message::Origin<T::Id>,
        append_entries: message::AppendEntries<T::Id>,
    ) -> io::Result<Option<Stage<T>>> {
        // We believe that we are currently the leader and should not be
        // receiving `AppendEntries` messages.
        if origin.term < node.term {
            node.driver.dispatch(
                origin.id.clone(),
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
        } else if origin.term == node.term {
            // This should not be possible and either indicates a configuration
            // problem or a bug.
            tracing::warn!(
                "received `AppendEntries` at same term while currently leader; message={:?}",
                append_entries.to_message(origin.id.clone(), origin.term)
            );

            Ok(None)
        } else {
            let mut follower =
                Follower::transition_from_leader(node, origin.term, Some(origin.id.clone()));
            // Process the message as a follower
            transition!(
                follower
                    .receive(
                        node,
                        append_entries.to_message(origin.id.clone(), origin.term)
                    )
                    .await?,
                follower
            )
        }
    }

    /// Track that a peer successfully replicated our log up to the given index
    async fn track_matched_index(
        &mut self,
        node: &mut Node<T>,
        id: &T::Id,
        index: Index,
    ) -> io::Result<()> {
        // This should not be called on ourselves
        assert_ne!(node.config.id, *id);

        let peer = match node.group.peers.get_mut(id) {
            Some(peer) => {
                // Track that we've seen the node recently.
                peer.last_seen_at = node.now;

                // If `index` has already been matched by the peer, then there is
                // nothing else to do. This can happen if `AppendEntriesResponse`
                // messages are received out of order.
                if let Some(matched) = peer.matched {
                    // If we are currently probing, it is possible to get an ACK
                    // that repeates a previously matched index. This happens if
                    // the `AppendEntries` message sending **more** entries is
                    // lost.
                    //
                    // TODO: is this correct?
                    if peer.replication.is_probing() {
                        if index < matched {
                            return Ok(());
                        }
                    } else {
                        if index <= matched {
                            return Ok(());
                        }
                    }
                }

                peer
            }
            None => {
                // Received an out of date message
                return Ok(());
            }
        };

        peer.matched = Some(index);
        peer.synced();

        if peer.is_voter() {
            let quorum_replicated = self.commitments.quorum_replicated(&node.group);

            // Get the most recently committed entry's term. If this term is less
            // than the leader's term, then the `NewTerm` entry has not been
            // proposed yet. If peers have sufficiently replicated the log, then we
            // should propose it now.
            let last_appended_term = node.log.last_appended().map(|pos| pos.term);

            if last_appended_term < Some(node.term) {
                if self.can_propose_new_term(node, quorum_replicated) {
                    self.propose(node, message::Value::new_term()).await?;
                }
            }

            // Since we received acks from potential followers, this may result in
            // more log entries becoming committed.
            if let Some(committed) = self.update_committed(node, quorum_replicated).await? {
                self.maybe_propose_upgrade_phase_2(node, committed).await?;

                // TODO: If the committed change should result in a peer node
                // dropping, then an `AppendEntries` should be eagerly sent to
                // complete the configuration process.
            }
        } else if let peer::Role::Observer {
            // TODO: clean up this condition
            auto_upgrade: Some(pos),
        } = peer.role
        {
            // If the peer is an observer that is configured to auto upgrade, check
            // if it has acknowledged the position.
            if pos.index <= index {
                // Upgrade the peer to follower
                self.propose(node, message::Value::upgrade_node_phase_one(id.clone()))
                    .await?;
            }
        }

        let peer = node.group.peers.get_mut(id).expect("peer missing");
        assert!(peer.replication.is_replicating());

        // The peer accepted a previous `AppendEntries` message. It
        // might be ready to accept more.
        peer.send_append_entries(
            &mut node.driver,
            &node.log,
            false,
            message::Origin {
                id: node.config.id.clone(),
                term: node.term,
            },
        )
        .await?;

        Ok(())
    }

    /// Maybe update the committed index.
    ///
    /// Returns the new committed position
    async fn update_committed(
        &mut self,
        node: &mut Node<T>,
        quorum_replicated: Option<Index>,
    ) -> io::Result<Option<Pos>> {
        if let Some(committed) = quorum_replicated {
            // As the leader, we can only commit entries from **our** term.
            // Also, note that somtimes the computed committed index can go
            // back. This happens when the group enters joint consensus with a
            // node being removed. In this case, it is possible for one of the
            // groups to have a lower committed index quorum. This is only
            // temporary as the group cannot exit joint consensus until both
            // sets of peers have committed the config entry that removes the
            // peer.
            node.log
                .maybe_commit(&mut node.driver, committed, node.term)
                .await
        } else {
            Ok(None)
        }
    }

    async fn maybe_propose_upgrade_phase_2(
        &mut self,
        node: &mut Node<T>,
        committed: Pos,
    ) -> io::Result<()> {
        // If any peers have reached their "phase-two" transition point,
        // issue the second phase of the configuration change.
        //
        // The `propose` step may result in a peer being removed from the
        // group, so we need to factor that into the loop.
        let mut idx = 0;
        let mut len = node.group.peers.len();

        while idx < len {
            let peer = &mut node.group.peers[idx];

            if let Some(msg) = peer.maybe_propose_upgrade_phase_2(committed) {
                // The peer transitioned to a full voter. Commit the log
                // entry to finalize the group configuration change.
                self.propose(node, msg).await?;
            }

            // Jump through some hoops to figure out if we need to increment
            // our index.
            let new_len = node.group.peers.len();

            if new_len >= len {
                idx += 1;
            }

            len = new_len;
        }

        Ok(())
    }

    fn respond_vote_no(&self, node: &mut Node<T>, origin: &message::Origin<T::Id>, pre_vote: bool) {
        // If the received message has a higher term, just ignore it.
        if node.term >= origin.term {
            let response = message::VoteResponse { granted: false };
            node.driver.dispatch(
                origin.id.clone(),
                Message {
                    origin: message::Origin {
                        id: node.config.id.clone(),
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

    fn check_quorum(&mut self, node: &mut Node<T>) -> bool {
        use votes::Tally::*;

        let active_cutoff = node.now - node.config.min_election_interval;
        self.check_quorum.clear();

        for peer in node.group.peers.values() {
            let grant = peer.id == node.config.id || peer.last_seen_at > active_cutoff;
            self.check_quorum.record(&node.group, &peer.id, grant);
        }

        match self.check_quorum.tally(&node.group) {
            Win => true,
            Lose => false,
            Pending => panic!("expected CheckQuorum vote to complete, but it is still pending."),
        }
    }

    /// `true` when the leader can propose a `NewTerm` entry without violating
    /// the `max_uncommitted_entries` configuration option.
    ///
    /// To check for this, we can't *just* look at the `leader_committed_index`
    /// because updating that may be blocked on committing the `NewTerm` entry.
    /// Instead, we check the highest replicated index, even if it is an older
    /// term.
    fn can_propose_new_term(&self, node: &Node<T>, quorum_replicated: Option<Index>) -> bool {
        match node.config.max_uncommitted_entries {
            Some(max_uncommitted_entries) => {
                let quorum_replicated = quorum_replicated
                    .map(|index| index.0 + 1)
                    .unwrap_or_default();

                let last_committed = node
                    .log
                    .last_committed_index()
                    .map(|index| index.0 + 1)
                    .unwrap_or_default();

                let last_appended = node
                    .log
                    .last_appended_index()
                    .map(|index| index.0 + 1)
                    .unwrap_or_default();

                let delta = last_appended
                    .checked_sub(cmp::max(quorum_replicated, last_committed))
                    .unwrap_or_default();

                delta < max_uncommitted_entries
            }
            None => true,
        }
    }
}

impl<T: Driver> Commitments<T> {
    fn quorum_replicated(&mut self, group: &Group<T>) -> Option<Index> {
        if group.is_joint_consensus() {
            // Compute the committed index for both the incoming and outgoing groups
            let incoming = Self::compute(&mut self.primary, group.incoming_voters());
            let outgoing = Self::compute(&mut self.secondary, group.outgoing_voters());

            // Return the min between both groups, this is the committed index.
            cmp::min(incoming, outgoing)
        } else {
            Self::compute(&mut self.primary, group.voters())
        }
    }

    fn compute<'a>(
        indices: &mut Vec<Index>,
        peers: impl Iterator<Item = &'a Peer<T>>,
    ) -> Option<Index> {
        indices.clear();
        let mut num_voters = 0;

        for peer in peers {
            num_voters += 1;

            if let Some(matched) = peer.matched {
                indices.push(matched);
            }
        }

        // The +1 for quorum isn't needed since we are indexing (starting w/
        // zero)
        let quorum_idx = num_voters / 2;

        // Sort indices in *descending* order
        indices.sort_by(|a, b| b.cmp(a));

        // Get the highest committed log index
        indices.get(quorum_idx).copied()
    }
}

fn heartbeat_at<T: Driver>(node: &Node<T>) -> Instant {
    node.now + node.config.leader_heartbeat_interval
}

fn check_quorum_at<T: Driver>(node: &Node<T>) -> Instant {
    node.now + node.config.min_election_interval
}

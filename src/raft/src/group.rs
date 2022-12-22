use crate::*;

use indexmap::IndexMap;
use tokio::time::Instant;

pub(crate) struct Group<T: Driver> {
    /// Which peer is known to be the leader
    pub(crate) leader: Option<T::Id>,

    /// True when the Raft group is in "joint consensus" mode
    is_joint_consensus: bool,

    /// Set of all known peer nodes. This includes the leader, voters, and
    /// observers.
    pub(crate) peers: IndexMap<T::Id, Peer<T>>,
}

impl<T: Driver> Group<T> {
    pub(crate) fn new() -> Group<T> {
        Group {
            leader: None,
            is_joint_consensus: false,
            peers: IndexMap::new(),
        }
    }

    /// Called on the first node of a Raft group to bootstrap consensus.
    pub(crate) fn bootstrap(&mut self, leader: T::Id, now: Instant) {
        let mut peer = Peer::new(leader.clone(), peer::Role::Voter, Index(0), now);

        // Update `next_idx` and `matched` to reflect the `InitRaft` log entry
        // that is appended during bootstrap.
        peer.matched = Some(peer.next_idx);
        peer.next_idx += 1;

        self.peers.insert(leader.clone(), peer);

        self.leader = Some(leader);
    }

    pub(crate) fn peers_mut<'a>(
        &'a mut self,
        self_id: &'a T::Id,
    ) -> impl Iterator<Item = &mut Peer<T>> + 'a {
        self.peers.iter_mut().filter_map(
            move |(id, peer)| {
                if id == self_id {
                    None
                } else {
                    Some(peer)
                }
            },
        )
    }

    // The current node is transitioning to the candidate stage
    pub(crate) fn transition_to_candidate(&mut self, id: &T::Id, log: &Log<T>) {
        let last_appended = log.last_appended_index();

        for peer in self.peers.values_mut() {
            peer.replication = peer::ReplicationState::Init;

            if peer.id == *id {
                peer.replication = peer::ReplicationState::Replicate;
                peer.matched = last_appended;
            } else {
                peer.matched = None;
            }
        }
    }

    // The current node is transitioning to the leader stage
    pub(crate) fn transition_to_leader(&mut self, id: &T::Id, log: &Log<T>, now: Instant) {
        let next_idx = log
            .last_appended_index()
            .map(|index| index + 1)
            .unwrap_or_default();

        for peer in self.peers.values_mut() {
            peer.last_seen_at = now;

            if peer.id == *id {
                assert!(peer.replication.is_replicating());
            } else if peer.replication.is_init() {
                assert!(peer.matched.is_none());
                peer.next_idx = next_idx;
            } else {
                todo!("handle early syncing with VoteResponse");
            }
        }

        self.leader = Some(id.clone());
    }

    pub(crate) fn is_valid_config_change(
        &self,
        pos: Pos,
        config_change: &message::ConfigChange<T::Id>,
    ) -> bool {
        use message::ConfigChange::*;

        match config_change {
            InitGroup { .. } => {
                if pos.index != 0 || pos.term != 0 {
                    false
                } else {
                    true
                }
            }
            AddNode { id, .. } => {
                if self.is_joint_consensus() {
                    false
                } else {
                    // When adding a node, the node must not already be part of the group
                    !self.peers.contains_key(id)
                }
            }
            UpgradeNode {
                id,
                phase: message::Phase::One,
            } => {
                if self.is_joint_consensus() {
                    false
                } else {
                    // To be a valid configuration transition, the peer being
                    // upgraded must currently be an observer.
                    match self.peers.get(id) {
                        Some(peer) if peer.is_observer() => true,
                        _ => false,
                    }
                }
            }
            UpgradeNode {
                id,
                phase: message::Phase::Two,
            } => {
                if !self.is_joint_consensus() {
                    false
                } else {
                    // There must be an entry for the peer being upgraded and the
                    // peer must be in phase one.
                    match self.peers.get(id) {
                        Some(peer) => matches!(peer.role, peer::Role::VoterIncoming { .. }),
                        None => false,
                    }
                }
            }
            RemoveNode {
                id,
                phase: message::Phase::One,
            } => {
                if self.is_joint_consensus() {
                    false
                } else {
                    // There must be an entry for the peer being removed and the
                    // peer must be fully added.
                    match self.peers.get(id) {
                        Some(peer) => peer.is_removable(),
                        None => false,
                    }
                }
            }
            RemoveNode {
                id,
                phase: message::Phase::Two,
            } => {
                if !self.is_joint_consensus() {
                    false
                } else {
                    // There must be an entry for the peer being removed and the
                    // peer must be fully added.
                    match self.peers.get(id) {
                        Some(peer) => matches!(peer.role, peer::Role::VoterOutgoing { .. }),
                        None => false,
                    }
                }
            }
        }
    }

    pub(crate) fn apply_config_change(
        &mut self,
        log: &Log<T>,
        pos: Pos,
        config_change: &message::ConfigChange<T::Id>,
        now: Instant,
    ) -> Result<(), ()> {
        use message::ConfigChange::*;
        use std::cmp;

        // The configuration must be valid. This should have been checked at an
        // earlier point, we are just double checking here. It also is possible
        // that a peer sent an invalid entry. If so, we don't want to apply it.
        if !self.is_valid_config_change(pos, config_change) {
            return Err(());
        }

        match config_change {
            InitGroup { id } => {
                let next_idx = cmp::max(pos.index, log.last_appended_index().unwrap_or_default());

                let prev = self.peers.insert(
                    id.clone(),
                    Peer::new(id.clone(), peer::Role::Voter, next_idx, now),
                );

                assert!(prev.is_none());
                assert!(!self.is_joint_consensus());
            }
            AddNode { id, auto_upgrade } => {
                let next_idx = cmp::max(pos.index, log.last_appended_index().unwrap_or_default());

                let prev = self.peers.insert(
                    id.clone(),
                    Peer::new(
                        id.clone(),
                        peer::Role::Observer {
                            // If auto-upgrade is requested, the node should be
                            // upgraded to follower once it has synchronized the
                            // log.
                            auto_upgrade: if *auto_upgrade { Some(pos) } else { None },
                        },
                        next_idx,
                        now,
                    ),
                );

                // Once again, make sure there was no previous entry
                assert!(prev.is_none());

                // And, since we are adding an observer, make sure we are not in
                // joint consensus mode.
                assert!(!self.is_joint_consensus());
            }
            UpgradeNode {
                id,
                phase: message::Phase::One,
            } => {
                // Only one peer can be upgraded at a time. This means we cannot
                // be in joint consensus mode when starting to upgrade a node.
                assert!(!self.is_joint_consensus());

                self.peers
                    .get_mut(id)
                    .expect("peer missing")
                    .upgrade_to_follower(pos);

                // Track that we are in joint-consensus mode
                self.set_joint_consensus(true);
            }
            UpgradeNode {
                id,
                phase: message::Phase::Two,
            } => {
                // This step moves the group out of joint consensus
                assert!(self.is_joint_consensus());

                self.peers
                    .get_mut(id)
                    .expect("peer missing")
                    .upgrade_to_follower_phase_2();

                self.set_joint_consensus(false);
            }
            RemoveNode {
                id,
                phase: message::Phase::One,
            } => {
                // Only one configuration change can be applied at a time. This
                // means we cannot be in joint consensus when starting to
                // upgrade a node.
                assert!(!self.is_joint_consensus());

                let peer = self.peers.get_mut(id).expect("peer missing");

                if peer.is_voter() {
                    peer.remove_phase_one(pos);

                    // Track that we are in joint-consensus mode
                    self.set_joint_consensus(true);

                    // Number of voters stays the same
                } else {
                    assert!(peer.is_observer());

                    // Observers can just be removed
                    self.peers.remove(id);
                }
            }
            RemoveNode {
                id,
                phase: message::Phase::Two,
            } => {
                // This step moves the group out of joint consensus
                assert!(self.is_joint_consensus());

                // Remove the peer
                let peer = self.peers.remove(id).expect("peer missing");

                // The peer should be an outgoing voter
                assert!(matches!(peer.role, peer::Role::VoterOutgoing { .. }));

                self.set_joint_consensus(false);
            }
        }

        Ok(())
    }

    /// Iterate all voters in the Raft group
    pub(crate) fn voters(&self) -> impl Iterator<Item = &Peer<T>> {
        self.peers.values().filter(|peer| peer.is_voter())
    }

    /// Iterate all IDs for voters in the Raft group
    pub(crate) fn voter_ids(&self) -> impl Iterator<Item = &T::Id> {
        self.voters().map(|peer| &peer.id)
    }

    /// Iterate all incoming voters in the Raft group.
    ///
    /// During joint-consensus, these are the peers that are either full voters
    /// or in the "incoming" set.
    pub(crate) fn incoming_voters(&self) -> impl Iterator<Item = &Peer<T>> {
        self.peers.values().filter(|peer| peer.is_incoming_voter())
    }

    /// During joint-consensus, these are the voter IDs from the "incoming" set.
    pub(crate) fn incoming_voter_ids(&self) -> impl Iterator<Item = &T::Id> {
        self.incoming_voters().map(|peer| &peer.id)
    }

    pub(crate) fn outgoing_voters(&self) -> impl Iterator<Item = &Peer<T>> {
        self.peers.values().filter(|peer| peer.is_outgoing_voter())
    }

    /// During joint-consensus, these are the voter IDs from the "outgoing" set.
    pub(crate) fn outgoing_voter_ids(&self) -> impl Iterator<Item = &T::Id> {
        self.outgoing_voters().map(|peer| &peer.id)
    }

    pub(crate) fn is_joint_consensus(&self) -> bool {
        use peer::Role::*;

        debug_assert_eq!(
            self.is_joint_consensus,
            self.peers.values().any(|peer| match peer.role {
                Observer { .. } | Voter => false,
                VoterIncoming { .. } | VoterOutgoing { .. } => true,
            })
        );

        self.is_joint_consensus
    }

    fn set_joint_consensus(&mut self, value: bool) {
        self.is_joint_consensus = value;

        // Call the accessor to perform a consistency check.
        self.is_joint_consensus();
    }
}

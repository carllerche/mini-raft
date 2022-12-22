use crate::*;

use std::{fmt, io};
use tokio::time::Instant;

pub(crate) struct Peer<T: Driver> {
    /// Node identifier
    pub(crate) id: T::Id,

    /// What role the peer is playing in the raft group
    pub(crate) role: Role,

    /// Index of the *next* log entry to send to this peer
    pub(crate) next_idx: Index,

    /// Highest log entry known to be replicated with this peer.
    pub(crate) matched: Option<Index>,

    /// `Instant` representing the when we **last** heard from the peer.
    ///
    /// This is used by the leader to detect if it has become separated from the
    /// group. A connected leader should receive `AppendEntriesResponse`
    /// messages from all other group members at an interval. If those
    /// `AppendEntriesResponse` **stop** arriving, the leader will step down.
    pub(crate) last_seen_at: Instant,

    /// Tracks the current replication state with the peer.
    pub(crate) replication: ReplicationState,
}

#[derive(Debug)]
pub(crate) enum Role {
    /// The node is only observing the Raft group. It receives new log entries
    /// but does not participate in voting.
    Observer {
        /// When set, the node should automatically be upgraded to follower when
        /// it has synchronized the log up to the given point.
        auto_upgrade: Option<Pos>,
    },

    /// The node is a voter. If the Raft group is in "joint consensus" mode,
    /// this node exists in both the "incoming" and the "outgoing"
    /// configuration.
    Voter,

    /// The Raft group is in "joint consensus" mode and the current node is
    /// being **added** to the group.
    VoterIncoming {
        /// Adding a voter is a two-phased approach. The first phase needs to be
        /// committed before starting the second phase. The `phase_two_at` is
        /// the log position that must be committed before initiating phase two.
        phase_two_at: Pos,
    },

    /// The Raft group is in "joint consensus" mode and the curent node is being
    /// **removed** from the group.
    VoterOutgoing { phase_two_at: Pos },
}

#[derive(Debug)]
pub(crate) enum ReplicationState {
    /// The peer has just been initialized and is neither probing or
    /// replicating. This usually means we know nothing about the peer and will
    /// start by sending the last entry.
    Init,

    /// We are not synced with the peer and are in the process of finding the
    /// most recent log entry that we have in common. This is done by sending
    /// empty `AppendEntry` responses until receiving a successful response.
    Probe,

    /// We are currently synced with the peer and are sending log entries.
    Replicate,
}

impl<T: Driver> Peer<T> {
    pub(crate) fn new(id: T::Id, role: peer::Role, next_idx: Index, now: Instant) -> Peer<T> {
        Peer {
            id,
            role,
            next_idx,
            matched: None,
            last_seen_at: now,
            /// Always start by probing as we do not know the current state of
            /// the peer's log.
            replication: ReplicationState::Init,
        }
    }

    /// Returns true if the peer can vote.
    pub(crate) fn is_voter(&self) -> bool {
        matches!(
            self.role,
            Role::Voter | Role::VoterIncoming { .. } | Role::VoterOutgoing { .. }
        )
    }

    /// Returns true if the peer is in the incoming set during joint-consensus.
    ///
    /// This method will always return `true` if the group is in joint-consensus
    /// mode.
    pub(crate) fn is_incoming_voter(&self) -> bool {
        matches!(self.role, Role::Voter | Role::VoterIncoming { .. })
    }

    pub(crate) fn is_outgoing_voter(&self) -> bool {
        matches!(self.role, Role::Voter | Role::VoterOutgoing { .. })
    }

    /// Returns true if the peer is an observer
    pub(crate) fn is_observer(&self) -> bool {
        matches!(self.role, Role::Observer { .. })
    }

    /// Returns true if the peer can be removed in its current state.
    pub(crate) fn is_removable(&self) -> bool {
        // A node can be removed if it is a voter **or** it is an observer that
        // is not going to auto upgrade.
        matches!(
            self.role,
            Role::Voter | Role::Observer { auto_upgrade: None }
        )
    }

    /// Called by the leader when it has synced replication with the peer.
    pub(crate) fn synced(&mut self) {
        if !self.replication.is_replicating() {
            self.replication = ReplicationState::Replicate;
            self.next_idx = self.matched.map(|index| index + 1).unwrap_or_default();
        }
    }

    /// Send an `AppendEntries` message to the peer
    pub(crate) async fn send_append_entries(
        &mut self,
        driver: &mut T,
        log: &Log<T>,
        heartbeat: bool,
        origin: message::Origin<T::Id>,
    ) -> io::Result<()> {
        let last_appended = log.last_appended();
        let mut entries = vec![];

        let prev_log_pos = if let Some(last_appended) = last_appended {
            // Log entries are only included when we are synced with the peer
            // and we are not sending a heartbeat.
            if !self.replication.is_probing() && !heartbeat {
                // Copy entries within the range
                // Sometimes, `next_idx` is set past last appended
                //
                // TODO: test this comparison
                if self.next_idx <= last_appended.index {
                    log.copy_range_to(driver, self.next_idx, last_appended.index, &mut entries)
                        .await?;
                }

                if entries.is_empty() {
                    return Ok(());
                }
            }

            // If `next_idx` is zero, then this is the first entry being sent.
            // In this case, `prev_log_pos` is `None`.
            if self.next_idx == 0 {
                None
            } else {
                log.pos_for(driver, self.next_idx - 1).await?
            }
        } else {
            if self.replication.is_replicating() && !heartbeat {
                // Nothing to send here
                return Ok(());
            }

            None
        };

        // Only optimistically update `next_idx` when in the replicating state
        if self.replication.is_replicating() {
            // Track which index should be sent to the peer next.
            self.next_idx += entries.len();
        }

        // Get the last committed index visible to the leader
        let leader_committed_index = log.last_committed().map(|pos| pos.index);

        driver.dispatch(
            self.id.clone(),
            Message {
                origin,
                action: message::Action::AppendEntries(message::AppendEntries {
                    prev_log_pos,
                    entries,
                    leader_committed_index,
                }),
            },
        );

        // If in the "initial" state, we have no idea what the peer's log is
        // currently at. We send a message and transition to "probing". This
        // only happens when a leader is newly promoted.
        if self.replication.is_init() {
            self.replication = ReplicationState::Probe;
        }

        Ok(())
    }

    pub(crate) async fn receive_append_entries_conflict(
        &mut self,
        driver: &mut T,
        log: &Log<T>,
        now: Instant,
        rejected: Index,
        hint: Option<Pos>,
        origin: message::Origin<T::Id>,
    ) -> io::Result<()> {
        // Track that we have recently seen the node
        self.last_seen_at = now;

        // If the follower has an uncommitted log tail, we would end up
        // probing one by one until we hit the common prefix.
        //
        // For example, if the leader has:
        //
        //   idx        1 2 3 4 5 6 7 8 9
        //              -----------------
        //   term (L)   1 3 3 3 5 5 5 5 5
        //   term (F)   1 1 1 1 2 2
        //
        // Then, after sending an append anchored at (idx=9,term=5) we
        // would receive a RejectHint of 6 and LogTerm of 2. Without the
        // code below, we would try an append at index 6, which would
        // fail again.
        //
        // However, looking only at what the leader knows about its own
        // log and the rejection hint, it is clear that a probe at index
        // 6, 5, 4, 3, and 2 must fail as well:
        //
        // For all of these indexes, the leader's log term is larger
        // than the rejection's log term. If a probe at one of these
        // indexes succeeded, its log term at that index would match the
        // leader's, i.e. 3 or 5 in this example. But the follower
        // already told the leader that it is still at term 2 at index
        // 9, and since the log term only ever goes up (within a log),
        // this is a contradiction.
        //
        // At index 1, however, the leader can draw no such conclusion,
        // as its term 1 is not larger than the term 2 from the
        // follower's rejection. We thus probe at 1, which will succeed
        // in this example. In general, with this approach we probe at
        // most once per term found in the leader's log.
        //
        // There is a similar mechanism on the follower (implemented in
        // handleAppendEntries via a call to findConflictByTerm) that is
        // useful if the follower has a large divergent uncommitted log
        // tail[1], as in this example:
        //
        //   idx        1 2 3 4 5 6 7 8 9
        //              -----------------
        //   term (L)   1 3 3 3 3 3 3 3 7
        //   term (F)   1 3 3 4 4 5 5 5 6
        //
        // Naively, the leader would probe at idx=9, receive a rejection
        // revealing the log term of 6 at the follower. Since the
        // leader's term at the previous index is already smaller than
        // 6, the leader- side optimization discussed above is
        // ineffective. The leader thus probes at index 8 and, naively,
        // receives a rejection for the same index and log term 5.
        // Again, the leader optimization does not improve over linear
        // probing as term 5 is above the leader's term 3 for that and
        // many preceding indexes; the leader would have to probe
        // linearly until it would finally hit index 3, where the probe
        // would succeed.
        //
        // Instead, we apply a similar optimization on the follower.
        // When the follower receives the probe at index 8 (log term 3),
        // it concludes that all of the leader's log preceding that
        // index has log terms of 3 or below. The largest index in the
        // follower's log with a log term of 3 or below is index 3. The
        // follower will thus return a rejection for index=3, log term=3
        // instead. The leader's next probe will then succeed at that
        // index.
        //
        // [1]: more precisely, if the log terms in the large
        // uncommitted tail on the follower are larger than the
        // leader's. At first, it may seem unintuitive that a follower
        // could even have such a large tail, but it can happen:
        //
        // 1. Leader appends (but does not commit) entries 2 and 3,
        //    crashes.
        //   idx        1 2 3 4 5 6 7 8 9
        //              -----------------
        //   term (L)   1 2 2     [crashes]
        //   term (F)   1
        //   term (F)   1
        //
        // 2. a follower becomes leader and appends entries at term 3.
        //              -----------------
        //   term (x)   1 2 2     [down]
        //   term (F)   1 3 3 3 3
        //   term (F)   1
        //
        // 3. term 3 leader goes down, term 2 leader returns as term 4
        //    leader. It commits the log & entries at term 4.
        //
        //              -----------------
        //   term (L)   1 2 2 2
        //   term (x)   1 3 3 3 3 [down]
        //   term (F)   1
        //              -----------------
        //   term (L)   1 2 2 2 4 4 4
        //   term (F)   1 3 3 3 3 [gets probed]
        //   term (F)   1 2 2 2 4 4 4
        //
        // 4. the leader will now probe the returning follower at index
        //    7, the rejection points it at the end of the follower's
        //    log which is at a higher log term than the actually
        //    committed log.
        //
        // (comment from raft-rs)
        let next_probe = match hint {
            Some(pos) if pos.term > 0 => log
                .find_conflict(driver, pos.index, pos.term)
                .await?
                .map(|pos| pos.index + 1)
                .unwrap_or_default(),
            Some(pos) => pos.index + 1,
            None => Index(0),
        };

        if self.decr_next_index(rejected, next_probe) {
            self.send_append_entries(driver, log, false, origin).await?;
        }

        Ok(())
    }

    /// Upgrade the peer from an observer to a follower.
    pub(crate) fn upgrade_to_follower(&mut self, phase_two_at: Pos) {
        // This should have already been checked
        assert!(self.is_observer());

        // A node isn't immediately upgraded to a follower. Upgrading is
        // a two phased process.
        self.role = peer::Role::VoterIncoming { phase_two_at };
    }

    pub(crate) fn maybe_propose_upgrade_phase_2(
        &mut self,
        pos: Pos,
    ) -> Option<message::Value<T::Id>> {
        use Role::*;

        match self.role {
            VoterIncoming { phase_two_at } if phase_two_at <= pos => {
                Some(message::Value::upgrade_node_phase_two(self.id.clone()))
            }
            VoterOutgoing { phase_two_at } if phase_two_at <= pos => {
                Some(message::Value::remove_node_phase_two(self.id.clone()))
            }
            _ => None,
        }
    }

    pub(crate) fn upgrade_to_follower_phase_2(&mut self) {
        assert!(matches!(self.role, Role::VoterIncoming { .. }));
        self.role = Role::Voter;
    }

    pub(crate) fn remove_phase_one(&mut self, phase_two_at: Pos) {
        // This should already have been checked
        assert!(matches!(self.role, Role::Voter));

        // A node isn't immediately removed when it is a voter.
        self.role = peer::Role::VoterOutgoing { phase_two_at };
    }

    /// Decrement the index of the next message to send, returning `true` if the
    /// index was actually decremented.
    fn decr_next_index(&mut self, rejected: Index, hint: Index) -> bool {
        use std::cmp;

        if self.replication.is_replicating() {
            if Some(rejected) <= self.matched {
                // The peer has already matched the rejected index, it must be
                // stale.
                return false;
            }
        } else {
            // The rejected index is set to the `prev_log_pos` from the
            // `AppendEntries` message received by the peer. This is also equal
            // to `next_idx - 1`. If `rejected` does not match `next_idx - 1`
            // then the message is stale.
            if Some(rejected) != self.next_idx.checked_sub(1) {
                return false;
            }
        }

        // TODO: handle out-of-order messages better
        self.next_idx = cmp::min(rejected, hint);

        if self.next_idx == 0 {
            // We have reached the beginning of the log. Since we cannot probe
            // back further, we have synchronized.
            self.replication = ReplicationState::Replicate;
        } else if self.replication.is_replicating() {
            // We are no longer synchronized with the peer, transition to probing
            self.replication = ReplicationState::Probe;
        }

        true
    }
}

impl<T: Driver> fmt::Debug for Peer<T> {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.debug_struct("Peer")
            .field("id", &self.id)
            .field("role", &self.role)
            .field("next_idx", &self.next_idx)
            .field("matched", &self.matched)
            .field("last_seen_at", &self.last_seen_at)
            .field("replication", &self.replication)
            .finish()
    }
}

impl ReplicationState {
    pub(crate) fn is_init(&self) -> bool {
        matches!(self, ReplicationState::Init)
    }

    pub(crate) fn is_replicating(&self) -> bool {
        matches!(self, ReplicationState::Replicate)
    }

    pub(crate) fn is_probing(&self) -> bool {
        matches!(self, ReplicationState::Probe)
    }
}

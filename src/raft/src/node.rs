use crate::*;

use std::io;
use tokio::time::{Duration, Instant};

/// State and methods common to all nodes in Raft group.
pub(crate) struct Node<T: Driver> {
    /// Per-node configuration.
    pub(crate) config: Config<T::Id>,

    /// Handle to the driver, used to issue outbound requests, persist data to
    /// disk, ...
    pub(crate) driver: T,

    /// The current election term.
    pub(crate) term: Term,

    /// Tracks nodes in the raft group
    pub(crate) group: Group<T>,

    /// Log of entries received by this node.
    pub(crate) log: Log<T>,

    /// Index of the last log entry applied to the local state machine
    pub(crate) last_applied: Option<Pos>,

    /// Last known time
    pub(crate) now: Instant,
}

/// Result of `append_entries` operation.
#[derive(Debug)]
pub(crate) struct AppendEntries {
    /// True if an appended log entry is a group configuration change that
    /// applies to the current node.
    pub(crate) did_configure_self: bool,

    /// The number of entries appended. This can be lower than the number of
    /// provided entries if an an invalid entry is encountered.
    pub(crate) num_appended: usize,
}

impl<T: Driver> Node<T> {
    pub(crate) fn new(driver: T, config: Config<T::Id>, now: Instant) -> Node<T> {
        Node {
            config,
            driver,
            term: Term::default(),
            group: Group::new(),
            log: Log::new(),
            last_applied: None,
            now,
        }
    }

    /// Returns true if the current node believes itself to be the leader
    pub(crate) fn is_leader(&self) -> bool {
        Some(&self.config.id) == self.group.leader.as_ref()
    }

    /// Returns the position of the next appended entry in the current term.
    pub(crate) fn next_pos(&self) -> Pos {
        let index = self
            .log
            .last_appended_index()
            .map(|index| index + 1)
            .unwrap_or_default();

        Pos {
            term: self.term,
            index,
        }
    }

    /// Returns `true` if the node has reached the `uncommitted_entries` limit.
    pub(crate) fn is_max_uncommitted_entries(&self) -> bool {
        match self.config.max_uncommitted_entries {
            Some(max_uncommitted) => {
                let num_appended = self
                    .log
                    .last_appended_index()
                    .map(|index| index.0 + 1)
                    .unwrap_or_default();

                let num_committed = self
                    .log
                    .last_committed_index()
                    .map(|index| index.0 + 1)
                    .unwrap_or_default();

                let num_uncommitted = num_appended.checked_sub(num_committed).unwrap_or_default();

                num_uncommitted >= max_uncommitted
            }
            None => false,
        }
    }

    /// Returns `true` if any of the entries are configuration changes that
    /// apply to the **current** node.
    pub(crate) fn append_entries(
        &mut self,
        mut entries: &[message::Entry<T::Id>],
    ) -> AppendEntries {
        let mut ret = AppendEntries {
            did_configure_self: false,
            num_appended: entries.len(),
        };

        // Immediately apply group configuration changes. This happens *before*
        // the entry is committed, which means it is possible that the change
        // will have to be reversed if the leader crashes before the entry is
        // committed.
        for (i, entry) in entries.iter().enumerate() {
            if let message::Value::Config(config_change) = &entry.value {
                if self
                    .group
                    .apply_config_change(&self.log, entry.pos, config_change, self.now)
                    .is_ok()
                {
                    if *config_change.id() == self.config.id {
                        ret.did_configure_self |= true;
                    }
                } else {
                    // The configuration change is invalid. Let's **not** apply
                    // this entry
                    entries = &entries[..i];
                    ret.num_appended = i;

                    break;
                }
            }
        }

        if !entries.is_empty() {
            self.log.append_entries(&mut self.driver, entries);
        }

        ret
    }

    /// Copies committed entries to the
    pub async fn copy_committed_entries_to(
        &mut self,
        dst: &mut Vec<message::Entry<T::Id>>,
    ) -> io::Result<()> {
        let start = self
            .last_applied
            .map(|pos| pos.index + 1)
            .unwrap_or_default();

        let end = match self.log.last_committed {
            Some(pos) if pos.index >= start => pos.index,
            _ => return Ok(()),
        };

        self.log
            .copy_range_to(&mut self.driver, start, end, dst)
            .await
    }

    pub(crate) fn applied_to(&mut self, pos: Pos) {
        let pos = Some(pos);

        assert!(pos <= self.log.last_committed);

        if pos <= self.last_applied {
            return;
        }

        self.last_applied = pos;
    }

    pub(crate) fn increment_term(&mut self) {
        self.set_term(self.term + 1, None);
    }

    pub(crate) fn set_term(&mut self, term: Term, leader: Option<T::Id>) {
        assert!(term > self.term);

        self.group.leader = leader;
        self.term = term;
    }

    /// Calculates when the next election campaign should start
    pub(crate) fn campaign_at(&mut self) -> Instant {
        let min = self.config.min_election_interval;
        let max = self.config.max_election_interval;

        let delay = Duration::from_millis(self.driver.rand_election_timeout(
            min.as_millis().try_into().expect("duration too big"),
            max.as_millis().try_into().expect("duration too big"),
        ));

        assert!(delay >= self.config.min_election_interval);
        assert!(delay <= self.config.max_election_interval);

        self.now + delay
    }

    pub(crate) fn before(&self, when: Instant) -> bool {
        self.now < when
    }
}

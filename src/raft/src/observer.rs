use crate::*;

use std::marker::PhantomData;
use std::{cmp, io};

/// All Raft nodes that are not bootstrapping the group start as an observer.
/// This lets them catch up with the current log.
pub(crate) struct Observer<T: Driver> {
    _p: PhantomData<T>,
}

/// Returned by `receive_append_entries`
pub(crate) enum ReceiveAppendEntries {
    /// The message is outdated or has been discarded for some other reason.
    Discard,

    /// The entries have been appended
    Appended,

    /// The entries have been appended and include a configuration change that
    /// applies to the current node.
    AppendedConfig,
}

impl<T: Driver> Observer<T> {
    /// Initialize the observer state.
    ///
    /// The `leader` identifies the Raft group leader. This is where the request
    /// to join the group is sent to.
    pub(crate) fn new() -> Observer<T> {
        Observer { _p: PhantomData }
    }

    /// Returns `true` if any entries contain configuration changes that apply
    /// to the current node.
    pub(crate) async fn receive_append_entries(
        &mut self,
        node: &mut Node<T>,
        origin: &message::Origin<T::Id>,
        append_entries: message::AppendEntries<T::Id>,
    ) -> io::Result<ReceiveAppendEntries> {
        use message::AppendEntriesResponse::*;
        use ReceiveAppendEntries::*;

        if origin.term > node.term {
            // We need to increment our term. The leader field will be set if we
            // accept the message.
            node.set_term(origin.term, None);
        } else if origin.term < node.term {
            // Let the peer know it has fallen out of date.
            node.driver.dispatch(
                origin.id.clone(),
                Reject.to_message(node.config.id.clone(), node.term),
            );

            // The message is discarded
            return Ok(Discard);
        }

        // First, perform some basic validation of the message.
        match append_entries.prev_log_pos {
            Some(pos) => {
                // The origin term should be greater than the prev_log_pos term.
                // Reject messsages for which this isn't true.
                if pos.term > origin.term {
                    return Ok(Discard);
                } else if let Some(entry) = append_entries.entries.first() {
                    if entry.pos <= pos {
                        return Ok(Discard);
                    }
                }
            }
            None => {
                if let Some(entry) = append_entries.entries.first() {
                    if entry.pos.index > 0 {
                        return Ok(Discard);
                    }
                }
            }
        }

        // Check that the message will not truncate committed entries
        if append_entries.prev_log_index() < node.log.last_committed_index() {
            return Ok(Discard);
        }

        // Check the first entry only as we will apply any that are valid
        if let Some(entry) = append_entries.entries.first() {
            if let message::Value::Config(config_change) = &entry.value {
                if !node.group.is_valid_config_change(entry.pos, config_change) {
                    return Ok(Discard);
                }
            }
        }

        if let Some(leader) = &node.group.leader {
            if origin.id != *leader {
                return Ok(Discard);
            }
        } else {
            // The message origin matches the current term, but this node has
            // not yet observed a leader.
            node.group.leader = Some(origin.id.clone());
        }

        // At this point, the message origin should be the currently known leader.
        assert_eq!(Some(&origin.id), node.group.leader.as_ref());

        // Sanity check: the entry terms should be less than or equal to the
        // message term.
        if let Some(entry) = append_entries.entries.last() {
            if entry.pos.term > origin.term {
                // Something is wrong, reject it
                return Ok(Discard);
            }
        }

        // We may already have logged some entries included in this
        // AppendEntries message. If this is the case, we will skip appending
        // these messages.
        let offset;

        // Check that the received entries are sequenced immediately after the
        // last entry currently appended to our log.
        // if let Some(prev_log_pos) = append_entries.prev_log_pos {
        let last_appended = node.log.last_appended();

        match append_entries.prev_log_pos {
            // If the message's `prev_log_pos` matches our last appended entry
            // position, then the received entries are sequenced at the end of
            // our log.
            pos if pos == last_appended => {
                offset = 0;
            }
            // Our log **does not** contain the entry immediately preceeding the
            // received entries. This means our log has divered from the
            // leader's. We need to re-synchronized before resuming replication.
            Some(prev_log_pos)
                if !node
                    .log
                    .contains_pos(&mut node.driver, prev_log_pos)
                    .await? =>
            {
                // Compute a hint for the leader to speed up synchronizing
                // replication
                let hint = if let Some(last_appended) = last_appended {
                    let hint = node
                        .log
                        .find_conflict(
                            &mut node.driver,
                            cmp::min(prev_log_pos.index, last_appended.index),
                            prev_log_pos.term,
                        )
                        .await?;

                    // The `last_committed_index` value cannot be used as we are
                    // not synchronized with the leader's log.

                    assert!(hint.is_some());
                    hint
                } else {
                    None
                };

                node.driver.dispatch(
                    origin.id.clone(),
                    Conflict {
                        rejected: prev_log_pos.index,
                        hint,
                    }
                    .to_message(node.config.id.clone(), node.term),
                );

                // Even though we didn't actually append any entries, we still
                // accepted the message as from a valid leader.
                return Ok(Appended);
            }
            // While our log contains the entry **immediately** preceeding the
            // received entries, it also has entries after that one. This means
            // we either received duplicate entries **or** our log has diverged
            // from the leader. We need to figure out which one it is.
            _ => {
                // If the loop completes, then we already have all entries and
                // should discard them all.
                let mut off = append_entries.entries.len();

                for (i, entry) in append_entries.entries.iter().enumerate() {
                    // Lookup the term for the entry at the same index in our log.
                    let our_term = node.log.term_for(&mut node.driver, entry.pos.index).await?;

                    if Some(entry.pos.term) == our_term {
                        // If the terms match, then we already have the entry in our log.
                        //
                        // When debugging, lets check that the entries are actually the same
                        debug_assert_eq!(
                            Some(entry),
                            node.log
                                .get(&mut node.driver, entry.pos.index)
                                .await?
                                .as_ref()
                        );
                    } else {
                        // We found a conflict
                        node.log.truncate(&mut node.driver, entry.pos.index).await?;
                        off = i;
                        break;
                    }
                }

                offset = off;
            }
        }

        let mut ret = Appended;

        // Grab the entries that are new to us.
        let entries = &append_entries.entries[offset..];

        // If there are no entries, this is heart beat message.
        let last_synced = if !entries.is_empty() {
            // Append the entries. This also applies any received Raft group
            // configuration changes. Raft group configuration changes are applied
            // **before** they are committed.
            let append_entries = node.append_entries(entries);

            if append_entries.did_configure_self {
                ret = AppendedConfig;
            }

            if append_entries.num_appended == 0 {
                // Let the peer know it has fallen out of date.
                return Ok(Discard);
            }

            entries.last().map(|entry| entry.pos)
        } else {
            append_entries.prev_log_pos
        };

        // Cannot commit past the last log entry that has been synchronized.
        let last_committed_index = cmp::min(
            last_synced.map(|pos| pos.index),
            append_entries.leader_committed_index,
        );

        // Update the committed index to the index specified in the message
        if let Some(leader_committed) = last_committed_index {
            // If we have not yet received log entries, then there is nothing for us to do.
            if let Some(last_appended) = node.log.last_appended() {
                // Because we just ensured that this node's log is synchronized
                // with the leader., if `leader_committed_index` is past our
                // log's last appended index, it is safe to commit to
                // `last_appended`.
                if leader_committed >= last_appended.index {
                    node.log.commit(last_appended);
                } else {
                    // Need to get the term if the message to commit it
                    let maybe_pos = node.log.pos_for(&mut node.driver, leader_committed).await?;

                    if let Some(pos) = maybe_pos {
                        node.log.commit(pos);
                    }
                }
            }
        }

        let last_log_pos = cmp::min(last_synced, node.log.last_appended()).unwrap_or_default();

        // Send ACK
        node.driver.dispatch(
            origin.id.clone(),
            Success { last_log_pos }.to_message(node.config.id.clone(), node.term),
        );

        Ok(ret)
    }
}

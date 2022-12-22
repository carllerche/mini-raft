use isolated::*;
use mini_raft::message::Entry;
use mini_raft::*;
use pretty_assertions::assert_eq;

/// Send some entries to a new node
#[test]
fn basic_replication() {
    let mut raft = Instance::builder().build_follower("0", &["1"], &[]);

    let entries = vec![
        entry!(7:0, b"one"),
        entry!(8:0, b"two"),
        entry!(9:0, b"three"),
        entry!(10:0, "bfour"),
    ];

    raft.recv_append_entries(
        "0",
        Term(0),
        message::AppendEntries {
            prev_log_pos: Some(pos!(6:0)),
            entries: entries[..1].to_vec(),
            leader_committed_index: Some(Index(0)),
        },
    );

    assert_term!(raft, 0);
    assert_eq!(&raft.log()[7..], &entries[..1]);

    raft.assert_sent(
        "0",
        message::AppendEntriesResponse::Success {
            last_log_pos: pos!(7:0),
        }
        .to_message("raft", Term(0)),
    )
    .assert_idle();

    // Send a heartbeat
    raft.recv_append_entries(
        "0",
        Term(0),
        message::AppendEntries {
            prev_log_pos: Some(pos!(7:0)),
            entries: vec![],
            leader_committed_index: Some(Index(0)),
        },
    );

    assert_term!(raft, 0);
    assert_eq!(&raft.log()[7..], &entries[..1]);

    raft.assert_sent(
        "0",
        message::AppendEntriesResponse::Success {
            last_log_pos: pos!(7:0),
        }
        .to_message("raft", Term(0)),
    )
    .assert_idle();

    raft.recv_append_entries(
        "0",
        Term(0),
        message::AppendEntries {
            prev_log_pos: Some(pos!(7:0)),
            entries: entries[1..3].to_vec(),
            leader_committed_index: Some(Index(0)),
        },
    );

    assert_term!(raft, 0);
    assert_eq!(&raft.log()[7..], &entries[..3]);

    raft.assert_sent(
        "0",
        message::AppendEntriesResponse::Success {
            last_log_pos: pos!(9:0),
        }
        .to_message("raft", Term(0)),
    )
    .assert_idle();

    raft.recv_append_entries(
        "0",
        Term(0),
        message::AppendEntries {
            prev_log_pos: Some(pos!(9:0)),
            entries: entries[3..].to_vec(),
            leader_committed_index: Some(Index(8)),
        },
    );

    assert_term!(raft, 0);
    assert_committed!(raft, 8);
    assert_eq!(&raft.log()[7..], &entries[..]);

    raft.assert_sent(
        "0",
        message::AppendEntriesResponse::Success {
            last_log_pos: pos!(10:0),
        }
        .to_message("raft", Term(0)),
    )
    .assert_idle();
}

/// When a node first starts replicating, it is possible for AppendEntries
/// messages to come from a leader that is different than the one that
/// initialized the raft group. If that happens, then the term should be greater
/// than 0.
#[test]
#[ignore] // TODO: Is this worth checking?
fn leader_different_than_init_group_when_first_replicating_invalid() {
    let mut raft = Instance::new();

    raft.recv_append_entries(
        "1",
        Term(0),
        message::AppendEntries {
            prev_log_pos: None,
            entries: vec![entry!(0:0, init_group("0"))],
            leader_committed_index: Some(Index(0)),
        },
    );

    assert_observer!(raft);
    assert_term!(raft, 0);
    assert!(raft.log().is_empty());

    raft.assert_idle();
}

/// When a node first starts replicating, it is possible for AppendEntries
/// messages to come from a leader that is different than the one that
/// initialized the raft group. If that happens, then the term should be greater
/// than 0.
#[test]
fn leader_different_than_init_group_when_first_replicating_valid() {
    let mut raft = Instance::new();

    let entries = vec![Entry {
        pos: pos!(0:0),
        value: message::Value::init_group("0"),
    }];

    raft.recv_append_entries(
        "1",
        Term(1),
        message::AppendEntries {
            prev_log_pos: None,
            entries: entries.clone(),
            leader_committed_index: Some(Index(0)),
        },
    );

    assert_observer!(raft);
    assert_term!(raft, 1);
    assert_eq!(&entries, raft.log());
    assert_eq!(raft.leader(), Some("1"));
    assert_peers!(raft, ["0"]);

    raft.assert_sent(
        "1",
        message::AppendEntriesResponse::Success {
            last_log_pos: pos!(0:0),
        }
        .to_message("raft", Term(1)),
    )
    .assert_idle();
}

/// When a node receives an `AppendEntries` message from a leader that is far in
/// the future, the leader's committed index may be past the last entry the
/// receiver has. In this case, the node should track the committed index as the
/// last entry it has stored.
#[test]
fn receive_append_entries_with_commit_index_in_future() {
    let mut raft = Instance::new();

    let entries = vec![Entry {
        pos: pos!(0:0),
        value: message::Value::init_group("0"),
    }];

    raft.recv_append_entries(
        "0",
        Term(0),
        message::AppendEntries {
            prev_log_pos: None,
            entries: entries.clone(),
            leader_committed_index: Some(Index(10)),
        },
    );

    assert_observer!(raft);
    assert_term!(raft, 0);
    assert_committed!(raft, 0);
    assert_eq!(&entries[..1], raft.log());
    assert_eq!(raft.leader(), Some("0"));
    assert_peers!(raft, ["0"]);

    raft.assert_sent(
        "0",
        message::AppendEntriesResponse::Success {
            last_log_pos: pos!(0:0),
        }
        .to_message("raft", Term(0)),
    )
    .assert_idle();
}

/// If `AppendEntries::prev_log_pos` references an entry that is *not* the
/// follower's last appended entry, then the follower checks that it does
/// contain such an entry and truncates its log.
#[test]
fn if_prev_log_pos_is_not_last_appended_check_if_entry_is_in_log_uncommitted() {
    let mut raft = Instance::builder().build_follower("0", &["1"], &[]);

    let entries = vec![
        entry!(7:0, b"foo"),
        entry!(8:0, b"bar"),
        entry!(9:0, b"hello"),
        entry!(10:0, b"world"),
    ];

    // Sync some log entries
    raft.recv_append_entries(
        "0",
        Term(0),
        message::AppendEntries {
            prev_log_pos: Some(pos!(6:0)),
            entries: entries.clone(),
            leader_committed_index: Some(Index(6)),
        },
    );

    raft.assert_sent(
        "0",
        message::AppendEntriesResponse::Success {
            last_log_pos: pos!(10:0),
        }
        .to_message("raft", Term(0)),
    )
    .assert_idle();

    let log = raft.log().to_vec();

    assert_eq!(&log[7..], &entries);

    let entries2 = vec![entry!(9:1, b"a"), entry!(10:1, b"b")];

    // Now, receive an AppendEntries from a different leader that is in the
    // middle of the entries just synced.
    raft.recv_append_entries(
        "1",
        Term(1),
        message::AppendEntries {
            prev_log_pos: Some(pos!(8:0)),
            entries: entries2.clone(),
            leader_committed_index: Some(Index(6)),
        },
    );

    raft.assert_sent(
        "1",
        message::AppendEntriesResponse::Success {
            last_log_pos: pos!(10:1),
        }
        .to_message("raft", Term(1)),
    )
    .assert_idle();

    assert_term!(raft, 1);
    assert_eq!(&raft.log()[..9], &log[..9]);
    assert_eq!(&raft.log()[9..], &entries2[..])
}

#[test]
fn if_prev_log_pos_is_not_last_appended_check_if_entry_is_in_log_committed() {
    let mut raft = Instance::builder().build_follower("0", &["1"], &[]);

    let entries = vec![
        entry!(7:0, b"foo"),
        entry!(8:0, b"bar"),
        entry!(9:0, b"hello"),
        entry!(10:0, b"world"),
    ];

    // Sync some log entries
    raft.recv_append_entries(
        "0",
        Term(0),
        message::AppendEntries {
            prev_log_pos: Some(pos!(6:0)),
            entries: entries.clone(),
            leader_committed_index: Some(Index(6)),
        },
    );

    raft.assert_sent(
        "0",
        message::AppendEntriesResponse::Success {
            last_log_pos: pos!(10:0),
        }
        .to_message("raft", Term(0)),
    )
    .assert_idle();

    let log = raft.log().to_vec();

    assert_eq!(&log[7..], &entries);

    let entries2 = vec![entry!(7:1, b"a"), entry!(8:1, b"b")];

    // Now, receive an AppendEntries from a different leader that is in the
    // middle of the entries just synced.
    raft.recv_append_entries(
        "1",
        Term(1),
        message::AppendEntries {
            prev_log_pos: Some(pos!(6:0)),
            entries: entries2.clone(),
            leader_committed_index: Some(Index(6)),
        },
    );

    raft.assert_sent(
        "1",
        message::AppendEntriesResponse::Success {
            last_log_pos: pos!(8:1),
        }
        .to_message("raft", Term(1)),
    )
    .assert_idle();

    assert_term!(raft, 1);
    assert_eq!(&raft.log()[..7], &log[..7]);
    assert_eq!(&raft.log()[7..], &entries2[..])
}

#[test]
fn reject_entries_that_would_require_truncating_committed_entries_1() {
    let mut raft = Instance::builder().build_follower("0", &["1"], &[]);

    let entries = vec![
        entry!(7:0, b"foo"),
        entry!(8:0, b"bar"),
        entry!(9:0, b"hello"),
        entry!(10:0, b"world"),
    ];

    // Sync some log entries
    raft.recv_append_entries(
        "0",
        Term(0),
        message::AppendEntries {
            prev_log_pos: Some(pos!(6:0)),
            entries: entries.clone(),
            leader_committed_index: Some(Index(9)),
        },
    );

    assert_committed!(raft, 9);
    raft.assert_sent(
        "0",
        message::AppendEntriesResponse::Success {
            last_log_pos: pos!(10:0),
        }
        .to_message("raft", Term(0)),
    )
    .assert_idle();

    let log = raft.log().to_vec();
    assert_eq!(&log[7..], &entries);

    // This should be rejected
    raft.recv_append_entries(
        "1",
        Term(1),
        message::AppendEntries {
            prev_log_pos: Some(pos!(6:0)),
            entries: vec![entry!(7:1, b"a"), entry!(8:1, b"b")],
            leader_committed_index: Some(Index(6)),
        },
    );

    raft.assert_idle();

    assert_term!(raft, 1);
    assert_committed!(raft, 9);
    assert_eq!(raft.log(), log);
}

#[test]
fn reject_entries_that_would_require_truncating_committed_entries_2() {
    let mut raft = Instance::builder().build_follower("0", &["1"], &[]);

    let entries = vec![
        entry!(7:0, b"foo"),
        entry!(8:0, b"bar"),
        entry!(9:0, b"hello"),
        entry!(10:0, b"world"),
    ];

    // Sync some log entries
    raft.recv_append_entries(
        "0",
        Term(0),
        message::AppendEntries {
            prev_log_pos: Some(pos!(6:0)),
            entries: entries.clone(),
            leader_committed_index: Some(Index(9)),
        },
    );

    assert_committed!(raft, 9);
    raft.assert_sent(
        "0",
        message::AppendEntriesResponse::Success {
            last_log_pos: pos!(10:0),
        }
        .to_message("raft", Term(0)),
    )
    .assert_idle();

    let log = raft.log().to_vec();
    assert_eq!(&log[7..], &entries);

    // This should be rejected
    raft.recv_append_entries(
        "1",
        Term(1),
        message::AppendEntries {
            prev_log_pos: None,
            entries: vec![entry!(0:1, init_group("1"))],
            leader_committed_index: Some(Index(6)),
        },
    );

    raft.assert_idle();

    assert_term!(raft, 1);
    assert_committed!(raft, 9);
    assert_eq!(raft.log(), log);
}

/// The node's log contains a conflicting entry at the index of the new leader's
/// committed index. If the message is rejected, the committed index should not
/// be updated.
#[test]
fn receive_append_entries_with_commit_index_diverged_conflict() {
    let mut raft = Instance::new();

    let entries = vec![
        entry!(0:0, init_group("0")),
        entry!(1:0, b"one"),
        entry!(2:0, b"two"),
        entry!(3:0, b"three"),
    ];

    // Send some entries to append
    raft.recv_append_entries(
        "0",
        Term(0),
        message::AppendEntries {
            prev_log_pos: None,
            entries: entries.clone(),
            leader_committed_index: Some(Index(0)),
        },
    );

    assert_committed!(raft, 0);

    raft.assert_sent(
        "0",
        message::AppendEntriesResponse::Success {
            last_log_pos: pos!(3:0),
        }
        .to_message("raft", Term(0)),
    )
    .assert_idle();

    // Send some entries to append
    raft.recv_append_entries(
        "0",
        Term(1),
        message::AppendEntries {
            prev_log_pos: Some(pos!(2:1)),
            entries: vec![entry!(3:1, b"three.one")],
            // The replicated message is also committed.
            leader_committed_index: Some(Index(3)),
        },
    );

    // Our committed index does not change since we cannot accept the log
    // entries.
    assert_committed!(raft, 0);
    assert_eq!(&entries[..], raft.log());

    raft.assert_sent(
        "0",
        message::AppendEntriesResponse::Conflict {
            rejected: Index(2),
            hint: Some(pos!(2:0)),
        }
        .to_message("raft", Term(1)),
    )
    .assert_idle();
}

/// The node's log diverged from the leader. The leader sends an AppendEntries
/// heartbeat that **matches** our log but with a greater
/// `leader_committed_index` that references an index that has diverged.
///
/// In this case, we should only commit **up to** the index that we know has
/// matched.
#[test]
fn receive_append_entries_with_commit_index_diverged_non_conflict() {
    let mut raft = Instance::new();

    let entries = vec![
        entry!(0:0, init_group("0")),
        entry!(1:0, b"one"),
        entry!(2:0, b"two"),
        entry!(3:0, b"three"),
    ];

    // Send some entries to append
    raft.recv_append_entries(
        "0",
        Term(0),
        message::AppendEntries {
            prev_log_pos: None,
            entries: entries.clone(),
            leader_committed_index: Some(Index(0)),
        },
    );

    assert_committed!(raft, 0);

    raft.assert_sent(
        "0",
        message::AppendEntriesResponse::Success {
            last_log_pos: pos!(3:0),
        }
        .to_message("raft", Term(0)),
    )
    .assert_idle();

    // Send some entries to append
    raft.recv_append_entries(
        "0",
        Term(1),
        message::AppendEntries {
            prev_log_pos: Some(pos!(2:0)),
            entries: vec![],
            // The replicated message is also committed.
            leader_committed_index: Some(Index(3)),
        },
    );

    // Our committed index does not change since we cannot accept the log
    // entries.
    assert_committed!(raft, 2);
    assert_eq!(&entries[..], raft.log());

    raft.assert_sent(
        "0",
        message::AppendEntriesResponse::Success {
            last_log_pos: pos!(2:0),
        }
        .to_message("raft", Term(1)),
    )
    .assert_idle();
}

/// When a follower receives an `AppendEntries` with a `prev_log_pos` value that
/// indicates a significant divergence, it walks back its own log to find the
/// first entry that it could have in common with the leader and sends that as
/// the hint.
///
/// This test is based on one of the cases in the comment in leader.rs
#[test]
fn rejects_append_entries_with_significant_divergence() {
    let mut raft = Instance::new();

    // Prime the follower
    let entries = vec![
        entry!(0:0, init_group("0")),
        entry!(1:0, add_node("1")),
        entry!(2:0, upgrade_node_phase_one("1")),
        entry!(3:0, upgrade_node_phase_two("1")),
        entry!(4:0),
        entry!(5:3),
        entry!(6:3),
        entry!(7:4),
        entry!(8:4),
        entry!(9:5),
        entry!(10:5),
        entry!(11:5),
        entry!(12:6),
    ];

    // Send some entries to append
    raft.recv_append_entries(
        "0",
        Term(6),
        message::AppendEntries {
            prev_log_pos: None,
            entries: entries.clone(),
            leader_committed_index: Some(Index(3)),
        },
    );

    assert_committed!(raft, 3);
    assert_term!(raft, 6);

    raft.assert_sent(
        "0",
        message::AppendEntriesResponse::Success {
            last_log_pos: pos!(12:6),
        }
        .to_message("raft", Term(6)),
    )
    .assert_idle();

    // Node "0" is no longer the leader. "1" has taken over but its log has
    // diverged.
    raft.recv_append_entries(
        "1",
        Term(7),
        message::AppendEntries {
            prev_log_pos: Some(pos!(11:3)),
            entries: vec![entry!(12:7)],
            leader_committed_index: Some(Index(3)),
        },
    );

    // The committed index stays the same
    assert_committed!(raft, 3);
    // The term is incremented
    assert_term!(raft, 7);
    // The log is not touched
    assert_eq!(raft.log(), entries);

    raft.assert_sent(
        "1",
        message::AppendEntriesResponse::Conflict {
            rejected: Index(11),
            hint: Some(pos!(6:3)),
        }
        .to_message("raft", Term(7)),
    )
    .assert_idle();
}

/// If the leader sends corrupted log entries, they are not appended.
#[test]
fn rejects_invalid_log_entries() {
    let mut raft = Instance::new();

    raft.recv_append_entries(
        "0",
        Term(0),
        message::AppendEntries {
            prev_log_pos: None,
            entries: vec![entry!(0:0, upgrade_node_phase_two("1"))],
            leader_committed_index: Some(Index(0)),
        },
    );

    assert_observer!(raft);
    assert_term!(raft, 0);
    assert!(raft.log().is_empty());

    raft.assert_idle();
}

/// If some of the leader's log entries are valid but one in the middle is
/// corrupt, the follower applies all valid log entries.
#[test]
fn append_entries_part_valid_drops_invalid() {
    let mut raft = Instance::new();

    // Prime the follower
    let entries = vec![
        entry!(0:0, init_group("0")),
        entry!(1:0, add_node("1")),
        entry!(2:0, upgrade_node_phase_one("1")),
        entry!(3:0, upgrade_node_phase_two("2")),
    ];

    // Send some entries to append
    raft.recv_append_entries(
        "0",
        Term(0),
        message::AppendEntries {
            prev_log_pos: None,
            entries: entries.clone(),
            leader_committed_index: Some(Index(3)),
        },
    );

    assert_committed!(raft, 2);
    assert_eq!(raft.log(), &entries[..3]);

    raft.assert_sent(
        "0",
        message::AppendEntriesResponse::Success {
            last_log_pos: pos!(2:0),
        }
        .to_message("raft", Term(0)),
    )
    .assert_idle();
}

/// The `prev_log_pos` field cannot be for a term higher than the origin term
/// and it cannot be for an index **greater** than the entries.
#[test]
fn rejects_invalid_prev_log_pos_with_greater_term_than_origin() {
    let mut raft = Instance::new();

    raft.recv_append_entries(
        "0",
        Term(1),
        message::AppendEntries {
            prev_log_pos: Some(pos!(10:2)),
            entries: vec![],
            leader_committed_index: Some(Index(0)),
        },
    );

    raft.assert_idle();
    assert_term!(raft, 1);
    assert!(raft.log().is_empty());
}

#[test]
fn rejects_invalid_prev_log_pos_none_first_entry_not_0_0() {
    let mut raft = Instance::new();

    raft.recv_append_entries(
        "0",
        Term(2),
        message::AppendEntries {
            prev_log_pos: None,
            entries: vec![entry!(1:0, b"lol")],
            leader_committed_index: Some(Index(0)),
        },
    );

    raft.assert_idle();
    assert_term!(raft, 2);
    assert!(raft.log().is_empty());
}

#[test]
fn rejects_invalid_prev_log_pos_index_greater_than_first_entry() {
    let mut raft = Instance::new();

    raft.recv_append_entries(
        "0",
        Term(2),
        message::AppendEntries {
            prev_log_pos: Some(pos!(10:2)),
            entries: vec![entry!(9:2, b"lol")],
            leader_committed_index: Some(Index(0)),
        },
    );

    raft.assert_idle();
    assert_term!(raft, 2);
    assert!(raft.log().is_empty());
}

#[test]
fn rejects_invalid_prev_log_pos_term_greater_than_first_entry() {
    let mut raft = Instance::new();

    raft.recv_append_entries(
        "0",
        Term(2),
        message::AppendEntries {
            prev_log_pos: Some(pos!(10:2)),
            entries: vec![entry!(11:1, b"lol")],
            leader_committed_index: Some(Index(0)),
        },
    );

    raft.assert_idle();
    assert_term!(raft, 2);
    assert!(raft.log().is_empty());
}

/// The leader term should always be equal or greater to the highest term in the
/// `AppendEntries` message. The follower should reject messages where this does
/// not hold.
#[test]
fn rejects_invalid_append_entries_term() {
    let mut raft = Instance::new();

    let entries = vec![
        entry!(0:0, init_group("0")),
        entry!(1:0, add_node("1")),
        entry!(2:0, add_node("raft")),
        entry!(3:0, upgrade_node_phase_one("1")),
        entry!(4:0, upgrade_node_phase_two("1")),
        entry!(5:0, upgrade_node_phase_one("raft")),
        entry!(6:1, upgrade_node_phase_two("raft")),
    ];

    raft.recv_append_entries(
        "0",
        Term(0),
        message::AppendEntries {
            prev_log_pos: None,
            entries: entries.clone(),
            leader_committed_index: Some(Index(0)),
        },
    );

    raft.assert_idle();
    assert_term!(raft, 0);
    assert!(raft.log().is_empty());
}

/// Rejected AppendEntries do not reset the election timeout
#[test]
fn rejected_last_log_pos_does_not_reset_election_timeout() {
    let mut raft = Instance::builder().build_follower("0", &[], &[]);

    for _ in 0..3 {
        assert_follower!(raft);

        raft.recv_append_entries(
            "0",
            Term(1),
            message::AppendEntries {
                prev_log_pos: Some(pos!(10:2)),
                entries: vec![],
                leader_committed_index: Some(Index(0)),
            },
        );

        assert_term!(raft, 1);
        raft.sleep_for(50);
    }

    assert_candidate!(raft);

    raft.assert_sent(
        "0",
        message::Vote {
            last_log_pos: Some(pos!(3:0)),
        }
        .to_prevote_message("raft", Term(2)),
    )
    .assert_idle();
}

#[test]
fn rejected_config_does_not_reset_election_timeout() {
    let mut raft = Instance::builder().build_follower("0", &[], &[]);

    for _ in 0..3 {
        assert_follower!(raft);

        raft.recv_append_entries(
            "0",
            Term(1),
            message::AppendEntries {
                prev_log_pos: Some(pos!(3:0)),
                entries: vec![entry!(4:0, init_group("1"))],
                leader_committed_index: Some(Index(0)),
            },
        );

        assert_term!(raft, 1);
        raft.sleep_for(50);
    }

    assert_candidate!(raft);

    raft.assert_sent(
        "0",
        message::Vote {
            last_log_pos: Some(pos!(3:0)),
        }
        .to_prevote_message("raft", Term(2)),
    )
    .assert_idle();
}

/// Followers can vote for only one peer per term. Each follower tracks the peer
/// they voted for. When the term is incremented, the voted_for slot should be
/// cleared.
///
/// This tests that incrementing the term due to an `AppendEntries` message
/// clears the `voted_for` slot.
#[test]
#[ignore]
fn term_inc_on_append_entries_clears_voted_for() {
    todo!();
}

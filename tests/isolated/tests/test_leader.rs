use isolated::*;
use mini_raft::*;

/// When a leader receives an `AppendEntries` message from a peer with a higher
/// term, it will step down as this means another peer has been promoted to
/// leader.
#[test]
fn leader_receives_empty_append_entries_with_higher_term() {
    let mut raft = Instance::new_leader(&["0", "1"], &[]);
    let term = raft.term();
    let pos = raft.last_appended();
    let log = raft.log().to_vec();

    raft.recv_append_entries(
        "0",
        term + 1,
        message::AppendEntries {
            prev_log_pos: Some(pos),
            entries: vec![],
            leader_committed_index: Some(pos.index),
        },
    );

    assert_follower!(raft);
    assert_term!(raft, term + 1);

    // Sends out a response
    raft.assert_sent(
        "0",
        message::AppendEntriesResponse::Success { last_log_pos: pos }.to_message("raft", term + 1),
    )
    .assert_idle()
    .assert_sleep_for(150);

    assert_eq!(raft.log(), log);
}

// Same as above, but the message includes entries
#[test]
fn leader_receives_append_entries_with_entries_and_higher_term() {
    let mut raft = Instance::new_leader(&["0", "1"], &[]);
    let term = raft.term();
    let pos = raft.last_appended();
    let mut log = raft.log().to_vec();

    let entry = message::Entry {
        pos: Pos {
            term: term + 1,
            index: pos.index + 1,
        },
        value: val!(b"hello world"),
    };

    raft.recv_append_entries(
        "0",
        term + 1,
        message::AppendEntries {
            prev_log_pos: Some(pos),
            entries: vec![entry.clone()],
            leader_committed_index: Some(pos.index),
        },
    );

    assert_follower!(raft);
    assert_term!(raft, term + 1);

    // Sends out a response
    raft.assert_sent(
        "0",
        message::AppendEntriesResponse::Success {
            last_log_pos: entry.pos,
        }
        .to_message("raft", term + 1),
    )
    .assert_idle()
    .assert_sleep_for(150);

    log.push(entry);
    assert_eq!(raft.log(), log);
}

/// If a leader receives an `AppendEntries` of the same term, it ignores it.
/// This is probably a bug.
#[test]
fn leader_receives_append_entries_with_same_term() {
    let mut raft = Instance::new_leader(&["0", "1"], &[]);
    let term = raft.term();
    let pos = raft.last_appended();
    let log = raft.log().to_vec();

    raft.recv_append_entries(
        "0",
        term,
        message::AppendEntries {
            prev_log_pos: Some(pos),
            entries: vec![],
            leader_committed_index: Some(pos.index),
        },
    );

    assert_leader!(raft);
    assert_term!(raft, term);

    // Sends out a response
    raft.assert_idle();

    assert_eq!(raft.log(), log);
}

/// If a leader receives an `AppendEntries` with a **lower** term, it ignores it
/// as this is an outdated message.
#[test]
fn leader_receives_append_entries_with_lower_term() {
    let entries = vec![
        entry!(0:0, init_group("0")),
        entry!(1:1, add_node("raft")),
        entry!(2:3, add_node("1")),
        entry!(3:3, upgrade_node_phase_one("raft")),
        entry!(4:3, upgrade_node_phase_two("raft")),
        entry!(5:5),
        entry!(6:5),
        entry!(7:5),
        entry!(8:5),
        entry!(9:5),
    ];

    let mut raft = Instance::builder().build_promoted_leader(Index(0), &entries[..]);
    assert_leader!(raft);
    assert_term!(raft, 6);

    let pos = raft.last_appended();
    let log = raft.log().to_vec();

    raft.recv_append_entries(
        "0",
        Term(5),
        message::AppendEntries {
            prev_log_pos: Some(pos),
            entries: vec![],
            leader_committed_index: Some(pos.index),
        },
    );

    assert_leader!(raft);
    assert_term!(raft, 6);

    // Sends out a response
    raft.assert_sent(
        "0",
        message::AppendEntriesResponse::Reject.to_message("raft", Term(6)),
    )
    .assert_idle();

    assert_eq!(raft.log(), log);
}

/// Leaders cannot commit messages from previous terms. Instead, leaders must
/// commit a new message from their current term, which commits all prior
/// entries by induction.
///
/// While we could *probably* use these outdated messages for tracking if the
/// peer is still alive, it is also safe to just ignore them.
#[test]
fn leader_ignores_append_entries_response_with_lower_term() {
    let entries = vec![
        entry!(0:0, init_group("0")),
        entry!(1:1, add_node("raft")),
        entry!(2:3, add_node("1")),
        entry!(3:3, upgrade_node_phase_one("raft")),
        entry!(4:3, upgrade_node_phase_two("raft")),
        entry!(5:3, upgrade_node_phase_one("1")),
        entry!(6:3, upgrade_node_phase_two("1")),
    ];

    let mut raft = Instance::builder().build_promoted_leader(Index(0), &entries[..]);
    assert_leader!(raft);
    assert_term!(raft, 4);

    let log = raft.log().to_vec();
    assert_eq!(log.last().unwrap().pos, pos!(7:4));

    raft.receive_append_entries_response(
        "0",
        Term(3),
        message::AppendEntriesResponse::Success {
            // This is invalid, but lets try to make this fail!
            last_log_pos: pos!(6:3),
        },
    );

    // TODO: we should check the matched value for the peer.

    assert_leader!(raft);
    assert_term!(raft, 4);
    // The committed index does not move
    assert_committed!(raft, 0);

    // Sends out a response
    raft.assert_idle().assert_peer_matched("0", None);
}

#[test]
fn only_commit_entries_from_current_term() {
    let entries = vec![
        entry!(0:0, init_group("0")),
        entry!(1:1, add_node("raft")),
        entry!(2:3, add_node("1")),
        entry!(3:3, upgrade_node_phase_one("raft")),
        entry!(4:3, upgrade_node_phase_two("raft")),
        entry!(5:3, upgrade_node_phase_one("1")),
        entry!(6:3, upgrade_node_phase_two("1")),
    ];

    let mut raft = Instance::builder().build_promoted_leader(Index(0), &entries[..]);
    assert_leader!(raft);
    assert_term!(raft, 4);

    let log = raft.log().to_vec();
    assert_eq!(log.last().unwrap().pos, pos!(7:4));

    raft.receive_append_entries_response(
        "0",
        Term(4),
        message::AppendEntriesResponse::Success {
            last_log_pos: pos!(6:3),
        },
    );

    assert_leader!(raft);
    assert_term!(raft, 4);
    // The committed index does not move
    assert_committed!(raft, 0);

    // Sends out the rest of the log
    raft.assert_sent(
        "0",
        message::AppendEntries {
            prev_log_pos: Some(pos!(6:3)),
            entries: vec![entry!(7:4, new_term)],
            leader_committed_index: Some(Index(0)),
        }
        .to_message("raft", Term(4)),
    )
    .assert_idle()
    .assert_peer_matched("0", Some(Index(6)));
}

/// (Pre)VoteResponse messages are either late or invalid. They should always
/// have an identical term or lower. These should just be discarded.
#[test]
fn leader_receives_vote_response() {
    fn new() -> Instance {
        Instance::builder().build_promoted_leader(
            Index(0),
            &[
                entry!(0:0, init_group("0")),
                entry!(1:0, add_node("1")),
                entry!(2:0, add_node("raft")),
                entry!(3:0, upgrade_node_phase_one("1")),
                entry!(4:0, upgrade_node_phase_two("1")),
                entry!(5:0, upgrade_node_phase_one("raft")),
                entry!(6:0, upgrade_node_phase_two("raft")),
            ],
        )
    }

    for msg in &[
        message::VoteResponse { granted: false }.to_prevote_message("0", Term(2)),
        message::VoteResponse { granted: true }.to_message("0", Term(2)),
        message::VoteResponse { granted: false }.to_message("0", Term(2)),
    ] {
        let mut raft = new();
        raft.assert_idle();
        assert_leader!(raft);
        assert_term!(raft, 1);

        // Receiving a **successful** pre-vote response at any term does nothing
        for term in &[0, 1, 2] {
            raft.receive_pre_vote_response(
                "0",
                Term(*term),
                message::VoteResponse { granted: true },
            );

            assert_leader!(raft);
            assert_term!(raft, 1);
            raft.assert_idle();
        }

        // Receiving a **successful** vote request increments the term
        raft.recv(msg.clone());

        assert_follower!(raft);
        assert_term!(raft, 2);
        raft.assert_idle();
    }
}

/// When a leader is initialized, there are no other nodes in the group so
/// messages are immediately committed.
///
/// When a leader is initialized for a new group, it starts with a committed
/// entry initializing group membership. All proposed entries are automatically
/// committed (only node in the group) **until** a new follower is added.
#[test]
fn leader_commits_entries_when_alone_until_follower_added() {
    let mut raft = Instance::new_group();
    raft.tick();

    let entries = vec![
        entry!(0:0, init_group("raft")),
        entry!(1:0, b"one"),
        entry!(2:0, add_node_auto_upgrade("0")),
        entry!(3:0, upgrade_node_phase_one("0")),
        entry!(4:0, upgrade_node_phase_two("0")),
    ];

    assert_leader!(raft);
    assert_tick_at!(raft, 10); // Next heartbeat
    assert_committed!(raft, 0);
    assert_eq!(raft.log(), &entries[..1],);

    raft.assert_idle();

    // Propose a new value, it should be immediately committed
    let pos = raft.propose(entries[1].value.clone()).unwrap();
    assert_eq!(pos, pos!(1:0));

    assert_committed!(raft, 1);
    assert_eq!(raft.log(), &entries[..2]);

    // Add a node, this adds it as an observer and starts replication
    let pos = raft.propose(entries[2].value.clone()).unwrap();
    assert_eq!(pos, pos!(2:0));

    assert_committed!(raft, 2);
    assert_eq!(raft.log(), &entries[..3]);

    raft.assert_sent(
        "0",
        message::AppendEntries {
            prev_log_pos: Some(pos!(1:0)),
            entries: entries[2..3].to_vec(),
            leader_committed_index: Some(Index(2)),
        }
        .to_message("raft", Term(0)),
    )
    .assert_idle();

    // Sync full logs
    raft.receive_append_entries_response(
        "0",
        Term(0),
        message::AppendEntriesResponse::Conflict {
            rejected: Index(1),
            hint: None,
        },
    );

    // The leader sends log entries to the new observer
    raft.assert_sent(
        "0",
        message::AppendEntries {
            prev_log_pos: None,
            entries: entries[0..3].to_vec(),
            leader_committed_index: Some(Index(2)),
        }
        .to_message("raft", Term(0)),
    )
    .assert_idle();

    // ACK the append entries, this automatically upgrades the peer to a
    // follower. The new log entry is not committed until the peer ACKs.
    raft.receive_append_entries_response(
        "0",
        Term(0),
        message::AppendEntriesResponse::Success {
            last_log_pos: pos!(2:0),
        },
    );

    assert_committed!(raft, 2);
    assert_eq!(raft.log(), &entries[..4]);

    raft.assert_sent(
        "0",
        message::AppendEntries {
            prev_log_pos: Some(pos!(2:0)),
            entries: entries[3..4].to_vec(),
            leader_committed_index: Some(Index(2)),
        }
        .to_message("raft", Term(0)),
    )
    .assert_idle();

    // Once the peer ACKs this message, it becomes committed
    raft.receive_append_entries_response(
        "0",
        Term(0),
        message::AppendEntriesResponse::Success {
            last_log_pos: pos!(3:0),
        },
    );

    assert_committed!(raft, 3);
    assert_eq!(raft.log(), &entries[..5]);

    raft.assert_sent(
        "0",
        message::AppendEntries {
            prev_log_pos: Some(pos!(3:0)),
            entries: entries[4..5].to_vec(),
            leader_committed_index: Some(Index(3)),
        }
        .to_message("raft", Term(0)),
    )
    .assert_idle();

    // Ack that last message to commit
    raft.receive_append_entries_response(
        "0",
        Term(0),
        message::AppendEntriesResponse::Success {
            last_log_pos: pos!(4:0),
        },
    );

    // Entry is committed but nothing is broadcast yet.
    assert_committed!(raft, 4);
    raft.assert_idle().assert_sleep_for(10);

    // Ticking without advancing time does nothing
    raft.tick();
    raft.assert_idle().assert_sleep_for(10);

    // The new committed index is broadcast with the heartbeat.
    raft.sleep();
    raft.assert_sent(
        "0",
        message::AppendEntries {
            prev_log_pos: Some(pos!(4:0)),
            entries: vec![],
            leader_committed_index: Some(Index(4)),
        }
        .to_message("raft", Term(0)),
    )
    .assert_idle();
}

/// test_leader_start_replication tests that when receiving client proposals,
/// the leader appends the proposal to its log as a new entry, then issues
/// AppendEntries RPCs in parallel to each of the other servers to replicate the
/// entry. Also, when sending an AppendEntries RPC, the leader includes the
/// index and term of the entry in its log that immediately precedes the new
/// entries. Also, it writes the new entry into stable storage.
///
/// Reference: section 5.3
#[test]
fn leader_sends_append_entries_after_proposal() {
    let mut raft = Instance::builder().build_follower("0", &["1"], &["2"]);
    let pos = raft.log().last().unwrap().pos;

    // Run election
    raft.sleep();
    raft.assert_sent(
        "0",
        message::Vote {
            last_log_pos: Some(pos),
        }
        .to_prevote_message("raft", Term(1)),
    )
    .assert_sent(
        "1",
        message::Vote {
            last_log_pos: Some(pos),
        }
        .to_prevote_message("raft", Term(1)),
    )
    .assert_idle();

    raft.receive_pre_vote_response("1", Term(1), message::VoteResponse { granted: true });

    raft.assert_sent(
        "0",
        message::Vote {
            last_log_pos: Some(pos),
        }
        .to_message("raft", Term(1)),
    )
    .assert_sent(
        "1",
        message::Vote {
            last_log_pos: Some(pos),
        }
        .to_message("raft", Term(1)),
    )
    .assert_idle();

    raft.receive_vote_response("1", Term(1), message::VoteResponse { granted: true });

    assert_leader!(raft);

    for peer in &["0", "1", "2"] {
        raft.assert_sent(
            peer,
            message::AppendEntries {
                prev_log_pos: Some(pos),
                entries: vec![message::Entry {
                    pos: Pos {
                        index: pos.index + 1,
                        term: Term(1),
                    },
                    value: val!(new_term),
                }],
                leader_committed_index: Some(Index(0)),
            }
            .to_message("raft", Term(1)),
        );
    }
}

/// Leaders send heartbeat messages to all peers in the group, at an interval.
#[test]
fn leader_sends_heartbeats_to_followers_and_observers() {
    let mut raft = Instance::new_leader(&["0", "1"], &["2"]);
    let last_pos = raft.log().last().unwrap().pos;

    // The leader indefinitely sends hearbeats
    for _ in 0..10 {
        raft.assert_sleep_for(10).assert_idle();

        // Tick to the heartbeat
        raft.sleep();

        for peer in &["0", "1", "2"] {
            raft.assert_sent(
                peer,
                message::AppendEntries {
                    prev_log_pos: Some(last_pos),
                    entries: vec![],
                    leader_committed_index: Some(last_pos.index),
                }
                .to_message("raft", Term(0)),
            );
        }

        raft.assert_sleep_for(10).assert_idle();
    }
}

#[test]
fn leader_sends_heartbeat_at_interval_even_when_broadcasting_proposal() {
    let mut raft = Instance::new_leader(&["0", "1"], &["2"]);

    raft.assert_sleep_for(10).assert_idle();

    // Sleep for only 1ms, then propos an entry
    raft.sleep_for(1);

    raft.propose(val!(b"hello")).unwrap();

    for peer in &["0", "1", "2"] {
        raft.assert_sent(
            peer,
            message::AppendEntries {
                prev_log_pos: Some(pos!(7:0)),
                entries: vec![entry!(8:0, b"hello")],
                leader_committed_index: Some(Index(7)),
            }
            .to_message("raft", Term(0)),
        );
    }

    raft.assert_sleep_for(9).assert_idle();

    // Tick to the heartbeat
    raft.sleep();

    for peer in &["0", "1", "2"] {
        raft.assert_sent(
            peer,
            message::AppendEntries {
                prev_log_pos: Some(pos!(8:0)),
                entries: vec![],
                leader_committed_index: Some(Index(7)),
            }
            .to_message("raft", Term(0)),
        );
    }
}

#[test]
fn leader_handles_conflict_hint() {
    let mut raft = Instance::builder().build_promoted_leader(
        Index(0),
        &[
            entry!(0:0, init_group("0")),
            entry!(1:1, add_node("raft")),
            entry!(2:3, add_node("1")),
            entry!(3:3, upgrade_node_phase_one("raft")),
            entry!(4:3, upgrade_node_phase_two("raft")),
            entry!(5:5),
            entry!(6:5),
            entry!(7:5),
            entry!(8:5),
            entry!(9:5),
        ],
    );

    let entries = raft.log().to_vec();

    raft.assert_sleep_for(10);
    raft.sleep();

    for peer in &["0", "1"] {
        raft.assert_sent(
            peer,
            message::AppendEntries {
                prev_log_pos: Some(pos!(9:5)),
                entries: vec![],
                leader_committed_index: Some(Index(0)),
            }
            .to_message("raft", Term(6)),
        );
    }
    raft.assert_idle();

    // Now, get back a conflict from "1" and make sure the appropriate response
    // is sent back out.
    raft.receive_append_entries_response(
        "1",
        Term(6),
        message::AppendEntriesResponse::Conflict {
            rejected: Index(9),
            hint: Some(pos!(6:2)),
        },
    );

    raft.assert_sent(
        "1",
        message::AppendEntries {
            prev_log_pos: Some(pos!(1:1)),
            // TODO: This should be done via probing and not send the full log
            // at this point.
            entries: vec![],
            leader_committed_index: Some(Index(0)),
        }
        .to_message("raft", Term(6)),
    )
    .assert_idle();

    // Synchronize
    raft.receive_append_entries_response(
        "1",
        Term(6),
        message::AppendEntriesResponse::Success {
            last_log_pos: pos!(1:1),
        },
    );

    // Send log entries
    raft.assert_sent(
        "1",
        message::AppendEntries {
            prev_log_pos: Some(pos!(1:1)),
            // TODO: This should be done via probing and not send the full log
            // at this point.
            entries: entries[2..].to_vec(),
            leader_committed_index: Some(Index(0)),
        }
        .to_message("raft", Term(6)),
    )
    .assert_idle();
}

/// When the leader receives a conflict that rejects Index 0, this means the
/// peer has no log and needs to replicate from the beginning.
#[test]
#[ignore]
fn leader_handles_conflict_index_0() {
    let mut raft = Instance::builder().build_group();

    raft.propose(val!(add_node("0"))).unwrap();

    // Sends out a message to the new peer
    raft.assert_sent(
        "0",
        message::AppendEntries {
            prev_log_pos: Some(pos!(0:0)),
            entries: vec![entry!(1:0, add_node("0"))],
            leader_committed_index: Some(Index(1)),
        }
        .to_message("raft", Term(0)),
    );

    // Receive a conflict
    raft.receive_append_entries_response(
        "0",
        Term(0),
        message::AppendEntriesResponse::Conflict {
            rejected: Index(0),
            hint: None,
        },
    );

    raft.assert_idle(); // not correct

    // TODO: finish, this wasn't actually a bug, but we can always add the test.
}

#[test]
fn leader_starts_replicating_when_receives_ack_with_already_matched_index() {
    let mut raft = Instance::builder().build_group();

    raft.propose(val!(add_node("0"))).unwrap();

    // Sends out a message to the new peer
    raft.assert_sent(
        "0",
        message::AppendEntries {
            prev_log_pos: Some(pos!(0:0)),
            entries: vec![entry!(1:0, add_node("0"))],
            leader_committed_index: Some(Index(1)),
        }
        .to_message("raft", Term(0)),
    );

    raft.receive_append_entries_response(
        "0",
        Term(0),
        message::AppendEntriesResponse::Success {
            last_log_pos: pos!(1:0),
        },
    );

    raft.assert_idle();

    // Propose a new value
    raft.propose(val!(b"hello")).unwrap();

    raft.assert_sent(
        "0",
        message::AppendEntries {
            prev_log_pos: Some(pos!(1:0)),
            entries: vec![entry!(2:0, b"hello")],
            leader_committed_index: Some(Index(2)),
        }
        .to_message("raft", Term(0)),
    )
    .assert_idle();

    // Now, receive a conflict suggesting to go back to index 1
    raft.receive_append_entries_response(
        "0",
        Term(0),
        message::AppendEntriesResponse::Conflict {
            rejected: Index(2),
            hint: Some(pos!(1:0)),
        },
    );

    // Leader sends out a probe
    raft.assert_sent(
        "0",
        message::AppendEntries {
            prev_log_pos: Some(pos!(1:0)),
            entries: vec![],
            leader_committed_index: Some(Index(2)),
        }
        .to_message("raft", Term(0)),
    )
    .assert_idle();

    // Receive a conflict
    raft.receive_append_entries_response(
        "0",
        Term(0),
        message::AppendEntriesResponse::Success {
            last_log_pos: pos!(1:0),
        },
    );

    raft.assert_sent(
        "0",
        message::AppendEntries {
            prev_log_pos: Some(pos!(1:0)),
            entries: vec![entry!(2:0, b"hello")],
            leader_committed_index: Some(Index(2)),
        }
        .to_message("raft", Term(0)),
    )
    .assert_idle();
}

/// When a follower responds with a conflict with a hint of None, the leader
/// should not translate that to `Index(0)`. This test is to catch an off-by-one
/// error.
#[test]
fn leader_handles_no_conflict_hint() {
    let mut raft = Instance::builder().build_promoted_leader(
        Index(0),
        &[
            entry!(0:0, init_group("0")),
            entry!(1:0, add_node("raft")),
            entry!(2:0, add_node("1")),
            entry!(3:0, upgrade_node_phase_one("raft")),
            entry!(4:0, upgrade_node_phase_two("raft")),
        ],
    );

    let entries = raft.log().to_vec();

    // At this point, the leader should send out heartbeats.
    raft.assert_sleep_for(10);
    raft.sleep();

    for peer in &["0", "1"] {
        raft.assert_sent(
            peer,
            message::AppendEntries {
                prev_log_pos: Some(pos!(4:0)),
                entries: vec![],
                leader_committed_index: Some(Index(0)),
            }
            .to_message("raft", Term(1)),
        );
    }
    raft.assert_idle();

    // Now, get back a conflict from "1" and make sure the appropriate response
    // is sent back out.
    raft.receive_append_entries_response(
        "1",
        Term(1),
        message::AppendEntriesResponse::Conflict {
            rejected: Index(4),
            hint: None,
        },
    );

    raft.assert_sent(
        "1",
        message::AppendEntries {
            prev_log_pos: None,
            // TODO: This should be done via probing and not send the full log
            // at this point.
            entries: entries.clone(),
            leader_committed_index: Some(Index(0)),
        }
        .to_message("raft", Term(1)),
    )
    .assert_idle();
}

/// On an interval, the leader checks it still has connectivity with a quroum of
/// voters. This ensures that, when the group is partially partitioned, and the
/// leader can no longer reach a quroum of voters, a peer election is not
/// disrupted by the old, partitioned, leader.
#[test]
fn leader_steps_down_when_partitioned_from_all_voters() {
    let mut raft = Instance::new_leader(&["0", "1"], &[]);
    let term = raft.term();
    let prev_log_pos = raft.log().last().unwrap().pos;

    // Send out the heartbeat, but only one peer will respond.
    for _ in 0..14 {
        raft.assert_sleep_for(10);
        raft.sleep();

        assert_leader!(raft);

        for peer in &["0", "1"] {
            raft.assert_sent(
                peer,
                message::AppendEntries {
                    prev_log_pos: Some(prev_log_pos),
                    entries: vec![],
                    leader_committed_index: Some(prev_log_pos.index),
                }
                .to_message("raft", term),
            );
        }
    }

    // This is the tick that the leader will step down.
    raft.assert_sleep_for(10);
    raft.sleep();

    assert_follower!(raft);
}

// Same as above, but 1 node responds.
#[test]
fn leader_steps_down_when_partitioned_from_quroum_voters() {
    let mut raft = Instance::new_leader(&["0", "1", "2"], &[]);
    let term = raft.term();
    let prev_log_pos = raft.log().last().unwrap().pos;

    // Send out the heartbeat, but only one peer will respond.
    for _ in 0..14 {
        raft.assert_sleep_for(10);
        raft.sleep();

        assert_leader!(raft);

        for peer in &["0", "1", "2"] {
            raft.assert_sent(
                peer,
                message::AppendEntries {
                    prev_log_pos: Some(prev_log_pos),
                    entries: vec![],
                    leader_committed_index: Some(prev_log_pos.index),
                }
                .to_message("raft", term),
            );
        }

        raft.receive_append_entries_response(
            "0",
            term,
            message::AppendEntriesResponse::Success {
                last_log_pos: prev_log_pos,
            },
        );
    }

    // This is the tick that the leader will step down.
    raft.assert_sleep_for(10);
    raft.sleep();

    assert_follower!(raft);
}

/// This time, a quorum (but not all) voters will respond to the leader, but the
/// leader **will not** step down.
#[test]
fn leader_does_not_step_down_when_quorum_voters_present() {
    let mut raft = Instance::new_leader(&["0", "1", "2"], &[]);
    let term = raft.term();
    let prev_log_pos = raft.log().last().unwrap().pos;

    // Send out the heartbeat, but only one peer will respond.
    for _ in 0..100 {
        raft.assert_sleep_for(10);
        raft.sleep();

        assert_leader!(raft);

        for peer in &["0", "1", "2"] {
            raft.assert_sent(
                peer,
                message::AppendEntries {
                    prev_log_pos: Some(prev_log_pos),
                    entries: vec![],
                    leader_committed_index: Some(prev_log_pos.index),
                }
                .to_message("raft", term),
            );
        }

        // Two respond
        for peer in &["0", "1"] {
            raft.receive_append_entries_response(
                peer,
                term,
                message::AppendEntriesResponse::Success {
                    last_log_pos: prev_log_pos,
                },
            );
        }
    }
}

#[test]
fn accepts_many_uncommitted_proposals_by_default() {
    let mut raft = Instance::builder().build_leader(&["0", "1"], &[]);

    for _ in 0..10_000 {
        assert!(raft.propose(val!("hello")).is_ok());
    }
}

/// If there are too many non-committed entries in the log, the leader should
/// stop accepting new requests.
#[test]
fn reject_proposal_when_max_uncommitted_entries_reached() {
    let mut raft = Instance::builder()
        .max_uncommitted_entries(10)
        .build_leader(&["0", "1"], &[]);

    for _ in 0..10 {
        assert!(raft.propose(val!("hello")).is_ok());
    }

    let err = raft.propose(val!("nope")).unwrap_err();
    assert!(matches!(err, ProposeError::TooManyUncommitted));
}

/// Always accept configuration changes
#[test]
fn accept_proposed_config_change_when_max_uncommitted_entries_reached() {
    let mut raft = Instance::builder()
        .max_uncommitted_entries(5)
        .build_leader(&["0", "1"], &[]);

    for id in &[
        "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m",
    ] {
        assert!(raft.propose(val!(add_node(id))).is_ok());
    }
}

/// Reject invalid configuration changes
#[test]
fn reject_invalid_configuration_changes() {
    let mut raft = Instance::new_group();

    let err = raft
        .propose(val!(upgrade_node_phase_one("lol")))
        .unwrap_err();
    assert!(matches!(err, ProposeError::InvalidConfig));
}

// When a new log index has been fully committed, all AppendEntries **after**
// that one include the new committed index.
#[test]
fn include_most_recent_committed_index_in_append_entries_message() {
    let mut raft = Instance::builder().build_leader(&["0", "1"], &[]);

    raft.assert_idle();

    // Propose a new entry
    assert!(raft.propose(val!(b"hello")).is_ok());

    for peer in &["0", "1"] {
        raft.assert_sent(
            peer,
            message::AppendEntries {
                prev_log_pos: Some(pos!(6:0)),
                entries: vec![entry!(7:0, b"hello")],
                leader_committed_index: Some(Index(6)),
            }
            .to_message("raft", Term(0)),
        );
    }

    raft.assert_idle();

    // Sleep and it sends out a heartbeat w/ the same index
    raft.sleep();

    for peer in &["0", "1"] {
        raft.assert_sent(
            peer,
            message::AppendEntries {
                prev_log_pos: Some(pos!(7:0)),
                entries: vec![],
                leader_committed_index: Some(Index(6)),
            }
            .to_message("raft", Term(0)),
        );
    }

    raft.assert_idle();

    // Receive an ACK from one peer commits the entry
    raft.receive_append_entries_response(
        "0",
        Term(0),
        message::AppendEntriesResponse::Success {
            last_log_pos: pos!(7:0),
        },
    );

    // No messages yet
    raft.assert_idle().sleep();

    for peer in &["0", "1"] {
        raft.assert_sent(
            peer,
            message::AppendEntries {
                prev_log_pos: Some(pos!(7:0)),
                entries: vec![],
                leader_committed_index: Some(Index(7)),
            }
            .to_message("raft", Term(0)),
        );
    }

    // Proposing another value includes the most recent commit index
    assert!(raft.propose(val!(b"world")).is_ok());

    for peer in &["0", "1"] {
        raft.assert_sent(
            peer,
            message::AppendEntries {
                prev_log_pos: Some(pos!(7:0)),
                entries: vec![entry!(8:0, b"world")],
                leader_committed_index: Some(Index(7)),
            }
            .to_message("raft", Term(0)),
        );
    }
}

/// Test that a leader considers an entry to be committed when it has been
/// replicated to a quorum of peers.
#[test]
fn leader_considers_entry_committed_once_replicated_to_quorum() {
    let observers = &["a", "b", "c"];

    let tests: &[(&[Id], &[Id])] = &[
        (&["0"], &["0"]),
        (&["0", "1"], &["0"]),
        (&["0", "1"], &["1"]),
        (&["0", "1", "2"], &["0", "1"]),
        (&["0", "1", "2"], &["2", "1"]),
        (&["0", "1", "2", "3"], &["0", "1"]),
        (&["0", "1", "2", "3"], &["3", "0"]),
        // Acks from observers are ignored when it comes to determining
        // committed indices.
        (&["0"], &["a", "b", "0"]),
        (&["0", "1"], &["a", "b", "1"]),
        (&["0", "1", "2"], &["0", "a", "b", "1"]),
    ];

    for (voters, acks) in tests {
        let mut raft = Instance::builder().build_leader(voters, observers);
        let pos = raft.log().last().unwrap().pos;

        raft.propose(val!(b"hello")).unwrap();

        for voter in voters.iter().chain(observers.iter()) {
            raft.assert_sent(
                voter,
                message::AppendEntries {
                    prev_log_pos: Some(pos),
                    entries: vec![message::Entry {
                        pos: Pos {
                            index: pos.index + 1,
                            term: pos.term,
                        },
                        value: val!(b"hello"),
                    }],
                    leader_committed_index: Some(pos.index),
                }
                .to_message("raft", Term(0)),
            );
        }

        raft.assert_idle();

        // Receive acks
        for ack in acks.iter() {
            assert_committed!(raft, pos.index);
            raft.receive_append_entries_response(
                ack,
                Term(0),
                message::AppendEntriesResponse::Success {
                    last_log_pos: Pos {
                        index: pos.index + 1,
                        term: pos.term,
                    },
                },
            );
        }

        assert_committed!(raft, pos.index + 1);
    }
}

#[test]
#[ignore]
fn test_commit_quorum_during_joint_consensus() {
    todo!();
}

#[test]
#[ignore]
fn test_accept_proposal_during_joint_consensus() {
    todo!()
}

#[test]
fn leader_commits_all_preceeding_entries_when_committing_an_entry() {
    let mut raft = Instance::builder().build_leader(&["0", "1"], &[]);
    let pos = raft.log().last().unwrap().pos;

    for i in 0..3 {
        raft.propose(val!(b"value")).unwrap();

        for peer in &["0", "1"] {
            raft.assert_sent(
                peer,
                message::AppendEntries {
                    prev_log_pos: Some(Pos {
                        index: pos.index + i,
                        term: pos.term,
                    }),
                    entries: vec![message::Entry {
                        pos: Pos {
                            index: pos.index + i + 1,
                            term: pos.term,
                        },
                        value: val!(b"value"),
                    }],
                    leader_committed_index: Some(pos.index),
                }
                .to_message("raft", Term(0)),
            );
        }

        raft.assert_idle();
    }

    let index = pos.index + 3;

    raft.receive_append_entries_response(
        "0",
        Term(0),
        message::AppendEntriesResponse::Success {
            last_log_pos: Pos {
                index,
                term: Term(0),
            },
        },
    );

    assert_committed!(raft, index);
    raft.assert_idle().sleep();

    for peer in &["0", "1"] {
        raft.assert_sent(
            peer,
            message::AppendEntries {
                prev_log_pos: Some(Pos {
                    index,
                    term: Term(0),
                }),
                entries: vec![],
                leader_committed_index: Some(index),
            }
            .to_message("raft", Term(0)),
        );
    }
}

/// Because a node becomes a voter as soon as the configuration change is
/// proposed, it is possible that it will become a leader **before** the
/// configuration entry commits.
#[test]
fn leader_commits_its_own_phase_one_upgrade() {
    let mut raft = Instance::new();

    // Sync the leader as well as phase one of adding ourselves
    raft.recv_append_entries(
        "id",
        Term(0),
        message::AppendEntries {
            prev_log_pos: None,
            entries: vec![
                entry!(0:0, init_group("0")),
                entry!(1:0, add_node("raft")),
                entry!(2:0, upgrade_node_phase_one("raft")),
            ],
            leader_committed_index: Some(Index(1)),
        },
    );

    // Immediately become a voter
    raft.drain_sent();
    assert_follower!(raft);
    raft.assert_sleep_for(150);

    // Become a candidate
    raft.sleep();
    assert_candidate!(raft);

    // Pre-vote sent out
    raft.assert_sent(
        "0",
        message::Vote {
            last_log_pos: Some(pos!(2:0)),
        }
        .to_prevote_message("raft", Term(1)),
    );

    // Receive +1
    raft.receive_pre_vote_response("0", Term(1), message::VoteResponse { granted: true });

    assert_candidate!(raft);
    assert_term!(raft, 1);

    // Pre-vote sent out
    raft.assert_sent(
        "0",
        message::Vote {
            last_log_pos: Some(pos!(2:0)),
        }
        .to_message("raft", Term(1)),
    );

    // Receive +1
    raft.receive_vote_response("0", Term(1), message::VoteResponse { granted: true });

    assert_leader!(raft);
    let next = raft.log().len();

    // Send out AppendEntries
    raft.assert_sent(
        "0",
        message::AppendEntries {
            prev_log_pos: Some(pos!(2:0)),
            entries: vec![entry!(3:1, new_term)],
            leader_committed_index: Some(Index(1)),
        }
        .to_message("raft", Term(1)),
    );

    // Receive the ACK
    raft.receive_append_entries_response(
        "0",
        Term(1),
        message::AppendEntriesResponse::Success {
            last_log_pos: pos!(3:1),
        },
    );

    let entries = vec![entry!(4:1, upgrade_node_phase_two("raft"))];

    assert_eq!(&raft.log()[next..], &entries[..]);

    // Send out AppendEntries
    raft.assert_sent(
        "0",
        message::AppendEntries {
            prev_log_pos: Some(pos!(3:1)),
            entries: entries.clone(),
            leader_committed_index: Some(Index(3)),
        }
        .to_message("raft", Term(1)),
    );

    raft.assert_idle();

    raft.receive_append_entries_response(
        "0",
        Term(1),
        message::AppendEntriesResponse::Success {
            last_log_pos: pos!(4:1),
        },
    );

    assert_committed!(raft, 4);
}

#[test]
fn leader_receives_stale_append_entries_response() {
    let mut raft = Instance::builder().build_leader(&["1"], &[]);

    // Propose some data
    let pos = raft.propose(val!(b"hello world")).unwrap();

    raft.assert_sent(
        "1",
        message::AppendEntries {
            prev_log_pos: Some(pos!(3:0)),
            entries: vec![message::Entry {
                pos,
                value: val!(b"hello world"),
            }],
            leader_committed_index: Some(Index(3)),
        }
        .to_message("raft", Term(0)),
    );

    // Have the peer ACK
    raft.receive_append_entries_response(
        "1",
        Term(0),
        message::AppendEntriesResponse::Success { last_log_pos: pos },
    );

    // Now reject the same entry
    raft.receive_append_entries_response(
        "1",
        Term(0),
        message::AppendEntriesResponse::Conflict {
            rejected: pos.index,
            hint: None,
        },
    );
}

#[test]
fn leader_receives_append_entries_response_from_unknown_peer() {
    let mut raft = Instance::builder().build_leader(&["1"], &[]);

    // Now reject the same entry
    raft.receive_append_entries_response(
        "a",
        Term(0),
        message::AppendEntriesResponse::Conflict {
            rejected: Index(2),
            hint: None,
        },
    );
}

/// If the origin term on `AppendEntriesResponse::Conflict` is equal to the
/// leader's term, then the message is out of date and can be discarded.
#[test]
fn leader_ignores_append_entries_response_reject_with_equal_term() {
    let mut raft = Instance::builder().build_promoted_leader(
        Index(3),
        &[
            entry!(0:0, init_group("0")),
            entry!(1:0, add_node("raft")),
            entry!(2:0, upgrade_node_phase_one("raft")),
            entry!(3:0, upgrade_node_phase_two("raft")),
        ],
    );
    assert_term!(raft, 1);

    raft.receive_append_entries_response("0", Term(1), message::AppendEntriesResponse::Reject);

    assert_leader!(raft);
    raft.assert_idle();
}

/// If the origin term on `AppendEntriesResponse::Conflict` is equal to the
/// leader's term, then the message is out of date and can be discarded.
#[test]
fn leader_ignores_append_entries_response_reject_with_lower_term() {
    let mut raft = Instance::builder().build_promoted_leader(
        Index(3),
        &[
            entry!(0:0, init_group("0")),
            entry!(1:0, add_node("raft")),
            entry!(2:0, upgrade_node_phase_one("raft")),
            entry!(3:0, upgrade_node_phase_two("raft")),
        ],
    );
    assert_term!(raft, 1);

    raft.receive_append_entries_response("0", Term(0), message::AppendEntriesResponse::Reject);

    assert_leader!(raft);
    raft.assert_idle();
}

#[test]
fn leader_receives_append_entries_response() {
    let entries = &[
        entry!(0:0, init_group("0")),
        entry!(1:0, add_node("raft")),
        entry!(2:0, upgrade_node_phase_one("raft")),
        entry!(3:0, upgrade_node_phase_two("raft")),
    ];

    for msg in &[
        message::AppendEntriesResponse::Success {
            last_log_pos: pos!(0:0),
        },
        message::AppendEntriesResponse::Conflict {
            rejected: Index(0),
            hint: None,
        },
        message::AppendEntriesResponse::Reject,
    ] {
        let mut raft = Instance::builder().build_promoted_leader(Index(3), entries);
        assert_term!(raft, 1);
        assert_leader!(raft);

        // When term is greater, it transition back to follower
        raft.receive_append_entries_response("0", Term(2), msg.clone());
        assert_follower!(raft);
        assert_term!(raft, 2);

        let mut raft = Instance::builder().build_promoted_leader(Index(3), entries);
        assert_term!(raft, 1);

        // When term is equal to node, it ignores
        raft.receive_append_entries_response("0", Term(0), msg.clone());
        assert_leader!(raft);
        assert_term!(raft, 1);
    }
}

/// If something goes wrong and a leader receives an AppendEntriesResponse with
/// InvalidEntry, it is possible that the leader's log is corrupted. At first,
/// do nothing because it may be the sender that is wrong. However, if quorum
/// peers also stop accepting entries due to an invalid entry, then the leader
/// should step down.
#[test]
#[ignore]
fn leader_receives_append_entries_result_invalid_entry() {
    todo!();
}

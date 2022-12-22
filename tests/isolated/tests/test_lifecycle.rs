use isolated::*;
use mini_raft::*;

#[test]
fn node_starts_as_observer() {
    let mut raft = Instance::new();

    assert_observer!(raft);
    assert_none_committed!(raft);
    assert!(raft.leader().is_none());
    assert!(raft.peers().is_empty());

    // No outbound messages
    raft.assert_idle();
}

#[test]
fn node_becomes_follower_one_append() {
    let mut raft = Instance::new();

    let entries = vec![
        message::Entry {
            pos: pos!(0:0),
            value: message::Value::init_group("0"),
        },
        message::Entry {
            pos: pos!(1:0),
            value: message::Value::add_node("raft"),
        },
        message::Entry {
            pos: pos!(2:0),
            value: message::Value::upgrade_node_phase_one("raft"),
        },
    ];

    raft.recv_append_entries(
        "0",
        Term(0),
        message::AppendEntries {
            prev_log_pos: None,
            entries: entries.clone(),
            leader_committed_index: None,
        },
    );

    assert_follower!(raft);
    assert_term!(raft, 0);
    assert_none_committed!(raft);
    assert_tick_at!(raft, 150);
    assert_eq!(entries, raft.log());
    assert_eq!(raft.leader(), Some("0"));
    assert_peers!(raft, ["raft", "0"]);

    raft.assert_sent(
        "0",
        message::AppendEntriesResponse::Success {
            last_log_pos: pos!(2:0),
        }
        .to_message("raft", Term(0)),
    )
    .assert_idle();
}

#[test]
fn node_becomes_follower_multi_append() {
    let mut raft = Instance::new();

    let entries = vec![
        message::Entry {
            pos: pos!(0:0),
            value: message::Value::init_group("0"),
        },
        message::Entry {
            pos: pos!(1:0),
            value: message::Value::add_node("raft"),
        },
        message::Entry {
            pos: pos!(2:0),
            value: message::Value::upgrade_node_phase_one("raft"),
        },
    ];

    raft.recv_append_entries(
        "0",
        Term(0),
        message::AppendEntries {
            prev_log_pos: None,
            entries: vec![entries[0].clone()],
            leader_committed_index: None,
        },
    );

    assert_observer!(raft);
    assert_term!(raft, 0);
    assert_none_committed!(raft);
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

    raft.recv_append_entries(
        "0",
        Term(0),
        message::AppendEntries {
            prev_log_pos: Some(pos!(0:0)),
            entries: vec![entries[1].clone()],
            leader_committed_index: Some(Index(0)),
        },
    );

    assert_observer!(raft);
    assert_term!(raft, 0);
    assert_committed!(raft, 0);
    assert_eq!(&entries[..2], raft.log());
    assert_eq!(raft.leader(), Some("0"));
    assert_peers!(raft, ["raft", "0"]);

    raft.assert_sent(
        "0",
        message::AppendEntriesResponse::Success {
            last_log_pos: pos!(1:0),
        }
        .to_message("raft", Term(0)),
    )
    .assert_idle();

    raft.recv_append_entries(
        "0",
        Term(0),
        message::AppendEntries {
            prev_log_pos: Some(pos!(1:0)),
            entries: vec![entries[2].clone()],
            leader_committed_index: Some(Index(0)),
        },
    );

    assert_follower!(raft);
    assert_term!(raft, 0);
    assert_tick_at!(raft, 150);
    assert_eq!(entries, raft.log());
    assert_eq!(raft.leader(), Some("0"));
    assert_peers!(raft, ["raft", "0"]);

    raft.assert_sent(
        "0",
        message::AppendEntriesResponse::Success {
            last_log_pos: pos!(2:0),
        }
        .to_message("raft", Term(0)),
    )
    .assert_idle();
}

use isolated::*;
use mini_raft::*;
use pretty_assertions::assert_eq;

/// Send some entries to a new node
#[test]
fn basic_replication() {
    let mut raft = Instance::new();

    let entries = vec![
        entry!(0:0, init_group("0")),
        entry!(1:0, b"one"),
        entry!(2:0, b"two"),
        entry!(3:0, b"three"),
    ];

    raft.recv_append_entries(
        "0",
        Term(0),
        message::AppendEntries {
            prev_log_pos: None,
            entries: entries[..1].to_vec(),
            leader_committed_index: Some(Index(0)),
        },
    );

    assert_term!(raft, 0);
    assert_eq!(raft.log(), &entries[..1]);

    raft.assert_sent(
        "0",
        message::AppendEntriesResponse::Success {
            last_log_pos: pos!(0:0),
        }
        .to_message("raft", Term(0)),
    )
    .assert_idle();

    // Send a heartbeat
    raft.recv_append_entries(
        "0",
        Term(0),
        message::AppendEntries {
            prev_log_pos: Some(pos!(0:0)),
            entries: vec![],
            leader_committed_index: Some(Index(0)),
        },
    );

    assert_term!(raft, 0);
    assert_eq!(raft.log(), &entries[..1]);

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
            entries: entries[1..3].to_vec(),
            leader_committed_index: Some(Index(0)),
        },
    );

    assert_term!(raft, 0);
    assert_eq!(raft.log(), &entries[..3]);

    raft.assert_sent(
        "0",
        message::AppendEntriesResponse::Success {
            last_log_pos: pos!(2:0),
        }
        .to_message("raft", Term(0)),
    )
    .assert_idle();

    raft.recv_append_entries(
        "0",
        Term(0),
        message::AppendEntries {
            prev_log_pos: Some(pos!(2:0)),
            entries: entries[3..].to_vec(),
            leader_committed_index: Some(Index(2)),
        },
    );

    assert_term!(raft, 0);
    assert_eq!(raft.log(), &entries[..]);

    raft.assert_sent(
        "0",
        message::AppendEntriesResponse::Success {
            last_log_pos: pos!(3:0),
        }
        .to_message("raft", Term(0)),
    )
    .assert_idle();
}

#[test]
fn ignore_append_entries_response() {
    let mut raft = Instance::new();

    raft.receive_append_entries_response(
        "1",
        Term(1),
        message::AppendEntriesResponse::Success {
            last_log_pos: pos!(10:0),
        },
    );

    raft.assert_idle();
    assert_observer!(raft);
    assert_term!(raft, 1);
}

#[test]
fn responds_to_pre_vote() {
    let mut raft = Instance::new();

    raft.receive_pre_vote_request(
        "1",
        Term(1),
        message::Vote {
            last_log_pos: Some(pos!(10:0)),
        },
    );

    raft.assert_sent(
        "1",
        message::VoteResponse { granted: true }.to_prevote_message("raft", Term(1)),
    )
    .assert_idle();

    assert_observer!(raft);
    assert_term!(raft, 0);
}

#[test]
fn ignore_pre_vote_response() {
    let mut raft = Instance::new();

    raft.receive_pre_vote_response("1", Term(1), message::VoteResponse { granted: true });

    raft.assert_idle();
    assert_observer!(raft);
    assert_term!(raft, 0);
}

#[test]
fn responds_to_vote() {
    let mut raft = Instance::new();

    raft.receive_vote_request(
        "1",
        Term(1),
        message::Vote {
            last_log_pos: Some(pos!(10:0)),
        },
    );

    raft.assert_sent(
        "1",
        message::VoteResponse { granted: true }.to_message("raft", Term(1)),
    )
    .assert_idle();

    assert_observer!(raft);
    assert_term!(raft, 1);
}

#[test]
fn ignore_vote_response() {
    let mut raft = Instance::new();

    raft.receive_vote_response("1", Term(1), message::VoteResponse { granted: true });

    raft.assert_idle();
    assert_observer!(raft);
    assert_term!(raft, 1);
}

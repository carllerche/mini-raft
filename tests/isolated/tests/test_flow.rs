use isolated::*;
use mini_raft::*;

#[test]
fn leader_sends_empty_heartbeat_even_if_peer_is_not_synced() {
    let mut raft = Instance::new_leader(&["0", "1"], &[]);

    raft.propose(message::Value::data("hello")).unwrap();

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

    raft.assert_idle().assert_sleep_for(10).sleep();

    // Send heartbeats
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
}

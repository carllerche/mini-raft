use mini_raft::{message, ProposeError};
use simulated::*;
use std::time::Duration;

#[test]
fn single_node_group() {
    TestGroup::builder(1).fuzz(|group| {
        group.test(async {
            // The first node should *start* as a leader
            assert!(group.info("raft0").stage.is_leader());

            group.send("raft0", Message::propose_data(b"hello world"));

            let message = group.recv_from("raft0").await;
            assert_eq!(message.to_propose_result().unwrap().index, 1);
        });
    });
}

#[test]
fn single_leader_add_observer() {
    TestGroup::builder(2)
        .simulation_duration(Duration::from_millis(600))
        .seed([
            196, 161, 95, 121, 61, 10, 141, 232, 156, 23, 115, 29, 133, 247, 37, 129, 48, 255, 49,
            88, 112, 73, 226, 10, 185, 124, 253, 186, 49, 180, 36, 254,
        ])
        .fuzz(|group| {
            group.test(async {
                // The first node should *start* as a leader
                assert!(group.info("raft0").stage.is_leader());
                assert!(group.info("raft1").stage.is_observer());

                let propose = Message::add_observer(group.lookup("raft1"));

                // Add the second node to the group
                group.send("raft0", propose.clone());

                // Wait for the config change to be added to the log
                let pos = group.recv_from("raft0").await.to_propose_result().unwrap();

                // Read from the observer at the same log position
                group.send("raft1", Message::read(pos));

                let message = group.recv_from("raft1").await.to_read_value();
                assert_eq!(message, propose.to_propose_value());

                // Propose a second value
                let value = message::Value::Data(b"hello world".to_vec());
                group.send(
                    "raft0",
                    Message::Propose {
                        value: value.clone(),
                    },
                );

                // Wait for the message to commit
                let message = group.recv_from("raft0").await;

                // Read from the observer
                group.send("raft1", Message::read(message.to_propose_result().unwrap()));

                let message = group.recv_from("raft1").await.to_read_value();
                assert_eq!(message, value);
            });
        });
}

#[test]
fn single_leader_add_follower() {
    TestGroup::builder(2).fuzz(|group| {
        group.test(async {
            let raft1 = group.lookup("raft1");

            // The first node should *start* as a leader
            assert!(group.info("raft0").stage.is_leader());
            assert!(group.info("raft1").stage.is_observer());

            let propose = Message::add_follower(raft1);

            // Add the second node to the group
            group.send("raft0", propose.clone());

            // Wait for the config change to be added to the log
            let mut pos = group.recv_from("raft0").await.to_propose_result().unwrap();

            // Read from the observer at the same log position
            group.send("raft1", Message::read(pos));

            let message = group.recv_from("raft1").await.to_read_value();
            assert_eq!(message, propose.to_propose_value());

            // Read the second value, which should be the *upgrade* request
            pos.index += 1;
            group.send("raft1", Message::read(pos));

            let message = group.recv_from("raft1").await.to_read_value();
            assert_eq!(message, message::Value::upgrade_node_phase_one(raft1));

            // Read the *third* value, which is the phase-two *upgrade* message
            pos.index += 1;
            group.send("raft1", Message::read(pos));

            let message = group.recv_from("raft1").await.to_read_value();
            assert_eq!(message, message::Value::upgrade_node_phase_two(raft1));

            // Check that the second node has become a voter.
            assert!(group.info("raft0").group[raft1].is_stable_voter());

            // Check that the second node sees itself as a voter
            assert!(group.info("raft1").group[raft1].is_stable_voter());
        });
    });
}

#[test]
fn single_leader_add_2_followers() {
    TestGroup::builder(3).fuzz(|group| {
        group.test(async move {
            add_two_followers(&group).await;
        });
    });
}

#[test]
fn add_2_followers_remove_one() {
    TestGroup::builder(3).fuzz(|group| {
        group.test(async move {
            add_two_followers(&group).await;

            // Remove "raft1"
            let addr = group.lookup("raft1");
            let mut pos = group
                .request("raft0", Message::remove_follower(addr))
                .await
                .to_propose_result()
                .unwrap();

            // The next entry should be the second phase in the removal process.
            pos.index += 1;

            let phase2 = group
                .request("raft0", Message::read(pos))
                .await
                .to_read_value();
            assert_eq!(phase2, message::Value::remove_node_phase_two(addr));
        });
    });
}

#[test]
#[ignore]
fn repeated_failure_1() {
    // This test assumes that links fail
    TestGroup::builder(3)
        .max_message_latency(Duration::from_millis(1000))
        .simulation_duration(Duration::from_secs(200))
        .tick_duration(Duration::from_millis(10))
        // .log("repeated_failure.log")
        .seed([
            67, 153, 15, 136, 147, 29, 83, 97, 147, 240, 121, 113, 130, 72, 2, 176, 198, 17, 187,
            27, 100, 133, 166, 82, 208, 74, 63, 51, 141, 13, 70, 52,
        ])
        .fuzz(|group| {
            group.test(async move {
                add_two_followers(&group).await;

                let mut term = group.info("raft0").term;

                // Propose a message every few elections
                for _ in 0..3 {
                    let mut leader = group
                        .request("raft0", Message::wait_leader(term + 3))
                        .await
                        .to_info()
                        .group
                        .leader
                        .unwrap();

                    term = group.info("raft0").term;

                    // Propose some data
                    let data = format!("data {:?}", term);

                    let pos = loop {
                        let message = group
                            .request(leader, Message::propose_data(data.as_bytes()))
                            .await;

                        match message.to_propose_result() {
                            Ok(pos) => break pos,
                            Err(ProposeError::NotLeader(Some(new_leader))) => {
                                leader = new_leader;
                            }
                            Err(ProposeError::NotLeader(None)) => {
                                let not_leader_term = group.info(leader).term;
                                leader = group
                                    .request(leader, Message::wait_leader(not_leader_term))
                                    .await
                                    .to_info()
                                    .group
                                    .leader
                                    .unwrap();
                            }
                            Err(_) => todo!(),
                        }
                    };

                    // Wait until synced on all hosts
                    for host in ["raft0", "raft1", "raft2"] {
                        let resp = group.request(host, Message::read(pos)).await;
                        assert_eq!(
                            message::Value::data(data.as_bytes()),
                            resp.to_read_result().value
                        );
                    }
                }
            });
        });
}

#[test]
fn repeated_failure_2() {
    // This test assumes that links fail
    TestGroup::builder(3)
        .duration(Duration::from_secs(100))
        .max_message_latency(Duration::from_millis(750))
        .simulation_duration(Duration::from_secs(50_000))
        .tick_duration(Duration::from_millis(10))
        .fuzz(|group| {
            group.test(async move {
                add_two_followers(&group).await;

                let mut term = group.info("raft0").term;

                // Propose a message every few elections
                for _ in 0..3 {
                    let mut leader = group
                        .request("raft0", Message::wait_leader(term + 3))
                        .await
                        .to_info()
                        .group
                        .leader
                        .unwrap();

                    term = group.info("raft0").term;

                    // Propose some data
                    let data = format!("data {:?}", term);

                    let pos = loop {
                        let message = group
                            .request(leader, Message::propose_data(data.as_bytes()))
                            .await;

                        match message.to_propose_result() {
                            Ok(pos) => break pos,
                            Err(ProposeError::NotLeader(Some(new_leader))) => {
                                leader = new_leader;
                            }
                            Err(ProposeError::NotLeader(None)) => {
                                let not_leader_term = group.info(leader).term;
                                leader = group
                                    .request(leader, Message::wait_leader(not_leader_term))
                                    .await
                                    .to_info()
                                    .group
                                    .leader
                                    .unwrap();
                            }
                            Err(ProposeError::FailedToCommit) => {}
                            Err(_) => todo!(),
                        }
                    };

                    // Wait until synced on all hosts
                    for host in ["raft0", "raft1", "raft2"] {
                        let resp = group.request(host, Message::read(pos)).await;
                        assert_eq!(
                            message::Value::data(data.as_bytes()),
                            resp.to_read_result().value
                        );
                    }
                }
            });
        });
}

#[test]
fn leader_disconnect() {
    TestGroup::builder(3).fuzz(|group| {
        group.test(async move {
            add_two_followers(&group).await;

            // Read the latest log position
            let last_applied = group.info("raft0").last_applied.unwrap();

            group.partition("raft0");

            // Wait for a leader to be elected in the next term
            group.send("raft1", Message::wait_leader(last_applied.term + 1));

            let message = group.recv_from("raft1").await.to_info();
            let leader = message.group.leader.unwrap();

            // Propose some data
            let message = group
                .request(leader, Message::propose_data(b"hello world"))
                .await;
            let pos = message.to_propose_result().unwrap();

            // The value should be committed
            for host in ["raft1", "raft2"] {
                let resp = group.request(host, Message::read(pos)).await;
                assert_eq!(
                    message::Value::data(&b"hello world"[..]),
                    resp.to_read_result().value
                );
            }

            // Bring "raft0" back. It should become a follower and catch up

            group.repair("raft0");

            let resp = group.request("raft0", Message::read(pos)).await;
            assert_eq!(
                message::Value::data(&b"hello world"[..]),
                resp.to_read_result().value
            );
        });
    })
}

async fn add_two_followers(group: &TestGroup) {
    // The first node should *start* as a leader
    assert!(group.info("raft0").stage.is_leader());

    let mut leader = group.lookup("raft0");

    for host in ["raft1", "raft2"] {
        let addr = group.lookup(host);
        let propose = Message::add_follower(addr);

        assert!(group.info(host).stage.is_observer());

        // Wait for the config change to be added to the log
        let pos = loop {
            group.send(leader, propose.clone());
            match group.recv_from(leader).await.to_propose_result() {
                Ok(pos) => break pos,
                Err(ProposeError::FailedToCommit) => {
                    continue;
                }
                Err(ProposeError::NotLeader(l)) => {
                    leader = l.unwrap();
                }
                Err(e) => panic!("error={:?}", e),
            }
        };

        // Read from the observer at the same log position
        group.send(host, Message::read(pos));

        let message = group.recv_from(host).await.to_read_value();
        assert_eq!(message, propose.to_propose_value());

        // Read the second value, which should be the *upgrade* request
        // let index = pos.index + 1;
        let message = read_next_config(group, host, pos.index + 1)
            .await
            .to_read_result();

        assert_eq!(message.value, message::Value::upgrade_node_phase_one(addr));

        // Read the *third* value, which is the phase-two *upgrade* message
        let message = read_next_config(group, host, message.pos.index + 1)
            .await
            .to_read_result();
        assert_eq!(message.value, message::Value::upgrade_node_phase_two(addr));

        // Check that the second node has become a voter.
        assert!(group.info(host).group[addr].is_stable_voter());

        let info = group.info(host);

        // Check that the second node sees itself as a voter
        assert!(info.group[addr].is_stable_voter());

        // Check that the second node includes the leader in its group
        assert!(info.group[leader].is_stable_voter());
    }
}

async fn read_next_config(group: &TestGroup, host: &str, mut index: mini_raft::Index) -> Message {
    loop {
        group.send(host, Message::read_idx(index));

        let message = group.recv_from(host).await;

        match &message {
            Message::ReadResult {
                value: message::Value::Config(..),
                ..
            } => {
                return message;
            }
            _ => {
                index += 1;
            }
        }
    }
}

use crate::{ElectionTimeout, Id, Instance, MockDriver, Raft};

use mini_raft::*;
use validated::Validated;

use pretty_assertions::assert_eq;
use tokio::time::Instant;

pub struct Builder {
    driver: MockDriver,
    config: Config<&'static str>,
}

impl Builder {
    pub(crate) fn new() -> Builder {
        Builder {
            driver: MockDriver::new(),
            config: Config::new("raft"),
        }
    }

    pub fn election_timeout(&mut self, election_timeout: ElectionTimeout) -> &mut Self {
        self.driver.election_timeout = election_timeout;
        self
    }

    pub fn max_uncommitted_entries(&mut self, max: u64) -> &mut Self {
        self.config.max_uncommitted_entries = Some(max);
        self
    }

    pub fn build_observer(&mut self) -> Instance {
        let now = Instant::now();
        let raft = Raft::new_observer(self.driver.clone(), self.config.clone(), now);

        Instance {
            raft: Validated::new(raft),
            epoch: now,
            now,
            tick_at: None,
        }
    }

    pub fn build_group(&mut self) -> Instance {
        let now = Instant::now();
        let raft = Raft::new_group(self.driver.clone(), self.config.clone(), now);

        Instance {
            raft: Validated::new(raft),
            epoch: now,
            now,
            tick_at: None,
        }
    }

    pub fn build_follower(&mut self, leader: Id, followers: &[Id], observers: &[Id]) -> Instance {
        let mut entries = vec![];
        let mut next_index = Index(1);

        // Initialize the group with the leader
        entries.push(entry!(0:0, init_group(leader)));

        // Add all the other nodes
        for peer in ["raft"]
            .iter()
            .chain(followers.iter())
            .chain(observers.iter())
        {
            entries.push(message::Entry {
                pos: Pos {
                    term: Term(0),
                    index: next_index,
                },
                value: val!(add_node(peer)),
            });

            next_index += 1;
        }

        // Now, upgrade followers
        for peer in ["raft"].iter().chain(followers.iter()) {
            entries.push(message::Entry {
                pos: Pos {
                    term: Term(0),
                    index: next_index,
                },
                value: val!(upgrade_node_phase_one(peer)),
            });

            entries.push(message::Entry {
                pos: Pos {
                    term: Term(0),
                    index: next_index + 1,
                },
                value: val!(upgrade_node_phase_two(peer)),
            });

            next_index += 2;
        }

        let pos = entries.last().unwrap().pos;

        let mut raft = self.build_observer();

        // Sync the log
        raft.recv_append_entries(
            "0",
            Term(0),
            message::AppendEntries {
                prev_log_pos: None,
                entries,
                leader_committed_index: Some(Index(0)),
            },
        );

        raft.assert_sent(
            "0",
            message::AppendEntriesResponse::Success { last_log_pos: pos }
                .to_message("raft", Term(0)),
        );

        raft
    }

    pub fn build_leader(&mut self, followers: &[Id], observers: &[Id]) -> Instance {
        let mut raft = self.build_group();

        let mut peers = followers.to_vec();
        peers.extend_from_slice(observers);

        // First add all the nodes as observers
        for (i, new) in peers.iter().enumerate() {
            let pos = raft.propose(message::Value::add_node(new)).unwrap();

            for peer in &peers[..i] {
                let entries = vec![raft.log().last().unwrap().clone()];

                // Send the initial `AppendEntries` to replicate the log
                raft.assert_sent(
                    peer,
                    message::AppendEntries {
                        prev_log_pos: Some(Pos {
                            index: entries[0].pos.index - 1,
                            term: Term(0),
                        }),
                        entries,
                        // Everything is committed because we haven't added any voters yet.
                        leader_committed_index: Some(pos.index),
                    }
                    .to_message("raft", Term(0)),
                );
            }

            // The full log is sent to the new node
            let entries = raft.log().to_vec();
            let prev_log_pos = entries[entries.len() - 2].pos;

            // Send the initial `AppendEntries` to sync the log
            raft.assert_sent(
                new,
                message::AppendEntries {
                    prev_log_pos: Some(prev_log_pos),
                    entries: vec![entries.last().cloned().unwrap()],
                    // Everything is committed because we haven't added any voters yet.
                    leader_committed_index: Some(pos.index),
                }
                .to_message("raft", Term(0)),
            )
            .assert_idle();

            // Receive the response that indicates we nothing is replicated
            raft.receive_append_entries_response(
                new,
                Term(0),
                message::AppendEntriesResponse::Conflict {
                    rejected: prev_log_pos.index,
                    hint: None,
                },
            );

            // Because `hint: None`, the leader assumes the peer has no log
            // entries and starts sending everything.
            raft.assert_sent(
                new,
                message::AppendEntries {
                    prev_log_pos: None,
                    entries: entries.clone(),
                    leader_committed_index: Some(entries.last().unwrap().pos.index),
                }
                .to_message("raft", Term(0)),
            )
            .assert_idle();
        }

        raft.assert_idle();

        let last_log_pos = raft.log().last().unwrap().pos;

        // ACK from all peers
        for peer in &peers {
            raft.receive_append_entries_response(
                peer,
                Term(0),
                message::AppendEntriesResponse::Success { last_log_pos },
            );
        }

        raft.assert_idle();

        // Now we have to upgrade each follower. This is a multi-step process.
        for follower in followers {
            raft.propose(message::Value::upgrade_node_phase_one(follower))
                .unwrap();
            let entry = raft.log().last().unwrap().clone();
            let prev_log_pos = Pos {
                term: Term(0),
                index: entry.pos.index - 1,
            };

            // Message is sent to peer
            for peer in &peers {
                raft.assert_sent(
                    peer,
                    message::AppendEntries {
                        prev_log_pos: Some(prev_log_pos),
                        entries: vec![entry.clone()],
                        leader_committed_index: Some(prev_log_pos.index),
                    }
                    .to_message("raft", Term(0)),
                );
            }

            raft.assert_idle();

            // All peers ACK message
            for peer in &peers {
                raft.receive_append_entries_response(
                    peer,
                    Term(0),
                    message::AppendEntriesResponse::Success {
                        last_log_pos: entry.pos,
                    },
                );
            }

            let entry = raft.log().last().unwrap().clone();
            assert_eq!(
                entry.value,
                message::Value::upgrade_node_phase_two(*follower)
            );
            let prev_log_pos = Pos {
                term: Term(0),
                index: entry.pos.index - 1,
            };

            // Phase 2
            for peer in &peers {
                raft.assert_sent(
                    peer,
                    message::AppendEntries {
                        prev_log_pos: Some(prev_log_pos),
                        entries: vec![entry.clone()],
                        leader_committed_index: Some(prev_log_pos.index),
                    }
                    .to_message("raft", Term(0)),
                );
            }

            raft.assert_idle();

            for peer in &peers {
                raft.receive_append_entries_response(
                    peer,
                    Term(0),
                    message::AppendEntriesResponse::Success {
                        last_log_pos: entry.pos,
                    },
                );
            }
        }

        // First tick
        raft.tick();

        assert_leader!(raft);

        raft
    }

    pub fn build_promoted_leader(
        &mut self,
        committed: Index,
        entries: &[message::Entry<Id>],
    ) -> Instance {
        let mut raft = self.build_observer();
        let last_log_pos = entries.last().unwrap().pos;

        raft.recv_append_entries(
            "0",
            last_log_pos.term,
            message::AppendEntries {
                prev_log_pos: None,
                entries: entries.to_vec(),
                leader_committed_index: Some(committed),
            },
        );

        raft.assert_sent(
            "0",
            message::AppendEntriesResponse::Success { last_log_pos }
                .to_message("raft", last_log_pos.term),
        );

        // Sleep until the election campaign starts
        raft.assert_sleep_for(150);
        raft.sleep();

        let mut pre_vote_responses = vec![];

        for (outbound, dst) in raft.raft.driver_mut().outbound.drain(..) {
            match outbound.action {
                message::Action::PreVote(_) => pre_vote_responses.push(
                    message::VoteResponse { granted: true }
                        .to_prevote_message(dst, outbound.origin.term),
                ),
                _ => panic!("unexpected outbound message; {:#?}", outbound),
            }
        }

        for response in pre_vote_responses.drain(..) {
            raft.recv(response);
        }

        let mut vote_responses = vec![];

        for (outbound, dst) in raft.raft.driver_mut().outbound.drain(..) {
            match outbound.action {
                message::Action::Vote(_) => vote_responses.push(
                    message::VoteResponse { granted: true }.to_message(dst, outbound.origin.term),
                ),
                _ => panic!("unexpected outbound message; {:#?}", outbound),
            }
        }

        for response in vote_responses.drain(..) {
            raft.recv(response);
        }

        let entries = raft.log().to_vec();
        let new_term = entries[entries.len() - 1].clone();

        for (outbound, _) in raft.raft.driver_mut().outbound.drain(..) {
            assert_eq!(
                outbound,
                message::AppendEntries {
                    prev_log_pos: Some(entries[entries.len() - 2].pos),
                    entries: vec![new_term.clone()],
                    leader_committed_index: Some(committed),
                }
                .to_message("raft", new_term.pos.term)
            );
        }

        raft.assert_idle();

        assert_leader!(raft);
        assert_term!(raft, last_log_pos.term + 1);
        assert_committed!(raft, committed);

        raft
    }
}

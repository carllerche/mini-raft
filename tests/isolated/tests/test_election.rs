use isolated::*;
use mini_raft::*;

#[test]
fn basic_election() {
    let mut raft = Instance::builder().build_follower("0", &["1"], &[]);
    let mut entries = raft.log().to_vec();

    // Now, wait for the election
    raft.assert_sleep_for(150);
    raft.sleep();

    raft.assert_sent(
        "0",
        message::Vote {
            last_log_pos: Some(pos!(6:0)),
        }
        .to_prevote_message("raft", Term(1)),
    )
    .assert_sent(
        "1",
        message::Vote {
            last_log_pos: Some(pos!(6:0)),
        }
        .to_prevote_message("raft", Term(1)),
    )
    .assert_idle();

    assert_candidate!(raft);
    // The pre-vote does not increment the term
    assert_term!(raft, 0);

    // Receive one pre-vote approval, which is sufficient to continue
    raft.receive_pre_vote_response("0", Term(1), message::VoteResponse { granted: true });

    raft.assert_sent(
        "0",
        message::Vote {
            last_log_pos: Some(pos!(6:0)),
        }
        .to_message("raft", Term(1)),
    )
    .assert_sent(
        "1",
        message::Vote {
            last_log_pos: Some(pos!(6:0)),
        }
        .to_message("raft", Term(1)),
    )
    .assert_idle();

    assert_candidate!(raft);
    assert_term!(raft, 1);

    // Receive one vote approval, which is sufficient to continue
    raft.receive_vote_response("0", Term(1), message::VoteResponse { granted: true });

    assert_leader!(raft);
    assert_term!(raft, 1);

    entries.push(entry!(7:1, new_term));

    // Sends `AppendEntries` to all nodes
    raft.assert_sent(
        "0",
        message::AppendEntries {
            prev_log_pos: Some(pos!(6:0)),
            entries: entries[7..].to_vec(),
            leader_committed_index: Some(Index(0)),
        }
        .to_message("raft", Term(1)),
    )
    .assert_sent(
        "1",
        message::AppendEntries {
            prev_log_pos: Some(pos!(6:0)),
            entries: entries[7..].to_vec(),
            leader_committed_index: Some(Index(0)),
        }
        .to_message("raft", Term(1)),
    )
    .assert_idle();
}

/// When the group is bigger, the vote does not conclude until a *quorum* of
/// voters respond (including the node that initiated the election).
#[test]
fn election_concludes_when_quorum_respond() {
    macro_rules! rounds {
        ($(
            $win:expr => ( $( $peer:expr => $granted:expr ),* );
        )*) => {{
            vec![$(
                ($win, vec![$( ($peer, $granted) ),*])
            ),*]
        }};
    }

    let rounds = rounds! {
        // Basic "all vote yes"
        true => ( "0" => true, "1" => true );
        // Basic "all vote no"
        false => ( "0" => false, "1" => false, "2" => false );
        // No at first, but then yes
        true => ( "0" => false, "1" => false, "2" => true, "3" => true );
        // Yes at first, but then no
        false => ( "0" => true, "1" => false, "2" => false, "3" => false );

        // Ignores observers voting yes for "yes" vote
        true => ( "4" => true, "5" => true, "6" => true, "0" => true, "1" => true );
        // Ignores observers voting no for "yes" vote
        true => ( "4" => false, "5" => false, "6" => false, "0" => true, "1" => true );
        // Ignores observers voting yes for "no" vote
        false => ( "4" => true, "5" => true, "6" => true, "0" => false, "1" => false, "2" => false );
        // Ignores observers voting no for "no" vote
        false => ( "4" => false, "5" => false, "6" => false, "0" => false, "1" => false, "2" => false );

        // Ignores unknown peers voting yes for "yes" vote
        true => ( "10" => true, "0" => true, "1" => true );
        // Ignores unknown peers voting no for "yes" vote
        true => ( "10" => false, "0" => true, "1" => true );
        // Ignores unknown peers voting "yes" for "no" vote
        false => ( "10" => true, "0" => false, "1" => false, "2" => false );
        // Ignores unknown peers voting "no" for "no" vote
        false => ( "10" => false, "0" => false, "1" => false, "2" => false );
    };

    let voters = &["0", "1", "2", "3"];

    for (win, votes) in rounds {
        let mut raft = Instance::builder().build_follower("0", &["1", "2", "3"], &["4", "5", "6"]);
        let pos = raft.log().last().unwrap().pos;

        // Sleep until an election starts
        raft.sleep();

        assert_candidate!(raft);
        assert_term!(raft, 0);

        // A vote request is sent to all voters.
        for peer in voters {
            raft.assert_sent(
                peer,
                message::Vote {
                    last_log_pos: Some(pos),
                }
                .to_prevote_message("raft", Term(1)),
            );
        }

        for (peer, granted) in votes.clone() {
            assert_candidate!(raft);
            assert_term!(raft, 0);

            raft.assert_idle();

            raft.receive_pre_vote_response(
                peer,
                if granted { Term(1) } else { Term(0) },
                message::VoteResponse { granted },
            );
        }

        if !win {
            assert_follower!(raft);
            assert_term!(raft, 0);
            raft.assert_idle();

            // Just for fun, send all the pre-votes again, it shouldn't change things
            for (peer, granted) in votes {
                raft.receive_pre_vote_response(
                    peer,
                    if granted { Term(1) } else { Term(0) },
                    message::VoteResponse { granted },
                );

                assert_follower!(raft);
                assert_term!(raft, 0);
                raft.assert_idle();
            }
        } else {
            // The node won the pre-vote, now it does a vote before becoming leader.
            for peer in voters {
                raft.assert_sent(
                    peer,
                    message::Vote {
                        last_log_pos: Some(pos),
                    }
                    .to_message("raft", Term(1)),
                );
            }

            for (peer, granted) in votes.clone() {
                assert_candidate!(raft);
                assert_term!(raft, 1);

                raft.assert_idle();

                raft.receive_vote_response(peer, Term(1), message::VoteResponse { granted });
            }

            assert_leader!(raft);

            // Notify peers of new leader
            for peer in &["0", "1", "2", "3", "4", "5", "6"] {
                raft.assert_sent(
                    peer,
                    message::AppendEntries {
                        prev_log_pos: Some(pos),
                        entries: vec![entry!(16:1, new_term)],
                        leader_committed_index: Some(Index(0)),
                    }
                    .to_message("raft", Term(1)),
                );
            }

            raft.assert_idle();

            // Just for fun, send all the pre-votes again, it shouldn't change things
            for (peer, granted) in votes.clone() {
                raft.receive_pre_vote_response(
                    peer,
                    if granted { Term(1) } else { Term(0) },
                    message::VoteResponse { granted },
                );

                assert_leader!(raft);
                assert_term!(raft, 1);
                raft.assert_idle();
            }

            // And now as votes
            for (peer, granted) in votes {
                raft.receive_vote_response(peer, Term(1), message::VoteResponse { granted });

                assert_leader!(raft);
                assert_term!(raft, 1);
                raft.assert_idle();
            }
        }
    }
}

/// Tests that a follower will vote for at most one candidate in a given term,
/// on a first-come-first-served basis.
#[test]
fn follower_votes_for_at_most_one_peer_per_term() {
    let peers = &["0", "1", "2"];

    for peer in peers {
        let mut raft = Instance::builder()
            .election_timeout(isolated::ElectionTimeout::Offset(10))
            .build_follower("0", &["1"], &["2"]);

        let pos = raft.log().last().unwrap().pos;

        // Need to delay when we start an election
        raft.sleep_for(150);

        assert_term!(raft, 0);

        raft.receive_vote_request(
            peer,
            Term(1),
            message::Vote {
                last_log_pos: Some(pos),
            },
        );

        raft.assert_sent(
            peer,
            message::VoteResponse { granted: true }.to_message("raft", Term(1)),
        );

        for other_peer in peers {
            assert_follower!(raft);

            raft.receive_vote_request(
                other_peer,
                Term(1),
                message::Vote {
                    last_log_pos: Some(pos),
                },
            );

            assert_term!(raft, 1);

            if other_peer == peer {
                // Can repeat vote
                raft.assert_sent(
                    other_peer,
                    message::VoteResponse { granted: true }.to_message("raft", Term(1)),
                )
                .assert_idle();
            } else {
                raft.assert_sent(
                    other_peer,
                    message::VoteResponse { granted: false }.to_message("raft", Term(1)),
                )
                .assert_idle();
            }

            // Pre-votes are handled the same
            raft.receive_pre_vote_request(
                other_peer,
                Term(1),
                message::Vote {
                    last_log_pos: Some(pos),
                },
            );

            assert_term!(raft, 1);

            if other_peer == peer {
                // Can repeat vote
                raft.assert_sent(
                    other_peer,
                    message::VoteResponse { granted: true }.to_prevote_message("raft", Term(1)),
                )
                .assert_idle();
            } else {
                raft.assert_sent(
                    other_peer,
                    message::VoteResponse { granted: false }.to_prevote_message("raft", Term(1)),
                )
                .assert_idle();
            }
        }
    }
}

/// While a follower only votes for a single peer per term, it can vote for any
/// number of pre-votes.
#[test]
fn follower_pre_votes_for_any_number_of_peers_per_term() {
    let peers = &["0", "1", "2"];

    for peer in peers {
        let mut raft = Instance::builder()
            .election_timeout(isolated::ElectionTimeout::Offset(10))
            .build_follower("0", &["1"], &["2"]);

        let pos = raft.log().last().unwrap().pos;

        // Need to delay when we start an election
        raft.sleep_for(150);

        assert_term!(raft, 0);

        raft.receive_pre_vote_request(
            peer,
            Term(1),
            message::Vote {
                last_log_pos: Some(pos),
            },
        );

        raft.assert_sent(
            peer,
            message::VoteResponse { granted: true }.to_prevote_message("raft", Term(1)),
        );

        for other_peer in peers {
            assert_follower!(raft);

            raft.receive_pre_vote_request(
                other_peer,
                Term(1),
                message::Vote {
                    last_log_pos: Some(pos),
                },
            );

            assert_term!(raft, 0);

            // Always yes
            raft.assert_sent(
                other_peer,
                message::VoteResponse { granted: true }.to_prevote_message("raft", Term(1)),
            )
            .assert_idle();
        }
    }
}

#[test]
fn pre_candidate_will_become_follower_on_valid_append_entries() {
    let peers = ["0", "1", "2", "3"];
    let voters = ["0", "1"];
    for peer in &peers {
        let mut raft = Instance::builder().build_follower("0", &["1"], &["2"]);
        let pos = raft.log().last().unwrap().pos;

        // Sleep until an election starts
        raft.sleep();

        for dst in &voters {
            raft.assert_sent(
                dst,
                message::Vote {
                    last_log_pos: Some(pos),
                }
                .to_prevote_message("raft", Term(1)),
            );
        }

        raft.assert_idle();

        raft.recv_append_entries(
            peer,
            Term(1),
            message::AppendEntries {
                prev_log_pos: Some(pos),
                entries: vec![],
                leader_committed_index: Some(Index(0)),
            },
        );

        assert_follower!(raft);
        assert_term!(raft, 1);

        // Sends an ACK
        raft.assert_sent(
            peer,
            message::AppendEntriesResponse::Success { last_log_pos: pos }
                .to_message("raft", Term(1)),
        );

        // Receive vote acks does nothing
        for voter in &voters {
            raft.receive_pre_vote_response(voter, Term(1), message::VoteResponse { granted: true });
            assert_follower!(raft);
            assert_term!(raft, 1);
        }
    }
}

#[test]
fn candidate_will_become_follower_on_valid_append_entries() {
    let peers = ["0", "1", "2", "3"];
    let voters = ["0", "1"];
    for peer in &peers {
        let mut raft = Instance::builder().build_follower("0", &["1"], &["2"]);
        let pos = raft.log().last().unwrap().pos;

        // Sleep until an election starts
        raft.sleep();

        for dst in &voters {
            raft.assert_sent(
                dst,
                message::Vote {
                    last_log_pos: Some(pos),
                }
                .to_prevote_message("raft", Term(1)),
            );
        }

        raft.assert_idle();

        // Receive granted to move to vote
        raft.receive_pre_vote_response("0", Term(1), message::VoteResponse { granted: true });

        for dst in &voters {
            raft.assert_sent(
                dst,
                message::Vote {
                    last_log_pos: Some(pos),
                }
                .to_message("raft", Term(1)),
            );
        }

        raft.recv_append_entries(
            peer,
            Term(1),
            message::AppendEntries {
                prev_log_pos: Some(pos),
                entries: vec![],
                leader_committed_index: Some(Index(0)),
            },
        );

        assert_follower!(raft);
        assert_term!(raft, 1);

        // Sends an ACK
        raft.assert_sent(
            peer,
            message::AppendEntriesResponse::Success { last_log_pos: pos }
                .to_message("raft", Term(1)),
        );

        // Receive vote acks does nothing
        for voter in &voters {
            raft.receive_vote_response(voter, Term(1), message::VoteResponse { granted: true });
            assert_follower!(raft);
            assert_term!(raft, 1);
        }
    }
}

/// A candidate will timeout the election and restart a new one on a randomized
/// interval.
#[test]
fn candidate_election_timeout_randomized() {
    let mut raft = Instance::builder()
        .election_timeout(isolated::ElectionTimeout::Multi(vec![
            1, 2, 3, 4, 5, 6, 7, 8, 9,
        ]))
        .build_follower("0", &["1"], &[]);
    let pos = raft.log().last().unwrap().pos;

    raft.assert_sleep_for(151);
    raft.sleep();

    assert_candidate!(raft);

    for offset in &[2, 3, 4, 5, 6] {
        for peer in &["0", "1"] {
            raft.assert_sent(
                peer,
                message::Vote {
                    last_log_pos: Some(pos),
                }
                .to_prevote_message("raft", Term(1)),
            );
        }

        raft.assert_sleep_for(150 + offset);
        raft.sleep();

        assert_candidate!(raft);
        assert_term!(raft, 0);
    }
}

/// A leader will always vote no as it believes it is currently the leader. This
/// is true even if the vote request is at a higher term. A
/// leader only steps down if it receives an `AppendEntries` message with a
/// higher term **or** `CheckQuorum` fails.
///
/// This is included in the 3rd concern for Joint Consensus, where if another
/// peer is removed from the cluster it may try to hold elections and disrupt
/// stability.
#[test]
fn leader_votes_no_equal_term() {
    let mut raft = Instance::new_leader(&["0", "1"], &[]);
    let term = raft.info().term;
    let last_log_pos = Some(raft.log().last().unwrap().pos);

    raft.receive_vote_request("0", term, message::Vote { last_log_pos });

    raft.assert_sent(
        "0",
        message::VoteResponse { granted: false }.to_message("raft", term),
    )
    .assert_idle();
}

#[test]
fn leader_votes_no_higher_term() {
    let mut raft = Instance::new_leader(&["0", "1"], &[]);
    let term = raft.info().term;
    let last_log_pos = Some(raft.log().last().unwrap().pos);

    raft.receive_vote_request("0", term + 1, message::Vote { last_log_pos });

    raft.assert_idle();
}

/// Followers will vote no on election as long as they believe a leader is still
/// active. The follower tracks this by measuring how long has elapsed since the
/// last `AppendEntries` message.
#[test]
fn follower_votes_no_if_still_receiving_append_entries_from_leader() {
    let mut raft = Instance::builder().build_follower("0", &["1"], &[]);

    // Now, wait for the election
    raft.assert_sleep_for(150);

    // Receive a pre-vote request from a peer within the election timeout
    raft.receive_pre_vote_request(
        "1",
        Term(1),
        message::Vote {
            last_log_pos: Some(pos!(6:0)),
        },
    );

    // Reject
    raft.assert_sent(
        "1",
        message::VoteResponse { granted: false }.to_prevote_message("raft", Term(0)),
    );

    // It did not increment its term
    assert_term!(raft, 0);

    // Still waiting for the next election timeout:
    raft.assert_sleep_for(150);

    // Now, receive a vote request and still reject it without incrementing our term.
    // Receive a pre-vote request from a peer within the election timeout
    raft.receive_vote_request(
        "1",
        Term(1),
        message::Vote {
            last_log_pos: Some(pos!(6:0)),
        },
    );

    // In the case of a **vote** we are just going to ignore the response. This
    // is mostly to make the logic of whether or not one should increment a term
    // on VoteResponse receipt simpler.
    raft.assert_idle();

    // It did not increment its term
    assert_term!(raft, 0);
}

#[test]
fn follower_votes_no_if_its_log_is_more_up_to_date() {
    let mut raft = Instance::builder()
        .election_timeout(isolated::ElectionTimeout::Offset(50))
        .build_follower("0", &["1"], &[]);

    raft.recv_append_entries(
        "0",
        Term(0),
        message::AppendEntries {
            prev_log_pos: Some(pos!(6:0)),
            entries: vec![entry!(7:0, b"a"), entry!(8:0, b"b"), entry!(9:0, b"c")],
            leader_committed_index: Some(Index(6)),
        },
    );
    raft.drain_sent();
    assert_term!(raft, 0);

    // Lets accept votes
    raft.sleep_for(150);

    raft.receive_pre_vote_request(
        "1",
        Term(1),
        message::Vote {
            last_log_pos: Some(pos!(8:0)),
        },
    );

    raft.assert_sent(
        "1",
        message::VoteResponse { granted: false }.to_prevote_message("raft", Term(0)),
    );

    assert_follower!(raft);
    assert_term!(raft, 0);

    // Same w/ a real vote, but the term is incremented

    raft.receive_vote_request(
        "1",
        Term(1),
        message::Vote {
            last_log_pos: Some(pos!(8:0)),
        },
    );

    raft.assert_sent(
        "1",
        message::VoteResponse { granted: false }.to_message("raft", Term(1)),
    );

    assert_follower!(raft);
    assert_term!(raft, 1);
}

#[test]
#[ignore]
fn ignores_vote_request_from_unknown_peer() {
    todo!();
}

fn run_election(raft: &mut Instance, peers: &[Id]) {
    raft.assert_idle().assert_sleep_for(150);
    raft.sleep();

    let pos = raft.log().last().unwrap().pos;
    let term = raft.info().term + 1;

    for peer in peers {
        raft.assert_sent(
            peer,
            message::Vote {
                last_log_pos: Some(pos),
            }
            .to_prevote_message("raft", term),
        );
    }

    raft.assert_idle();

    // Receive ACK
    raft.receive_pre_vote_response("0", term, message::VoteResponse { granted: true });

    for peer in peers {
        raft.assert_sent(
            peer,
            message::Vote {
                last_log_pos: Some(pos),
            }
            .to_message("raft", term),
        );
    }

    raft.receive_vote_response("0", term, message::VoteResponse { granted: true });

    assert_leader!(raft);
}

fn churn_terms(n: usize, raft: &mut Instance, peers: &[Id]) {
    let mut pos = raft.log().last().unwrap().pos;
    let committed = pos.index;

    raft.recv_append_entries(
        "0",
        pos.term,
        message::AppendEntries {
            prev_log_pos: Some(pos),
            entries: vec![],
            leader_committed_index: Some(committed),
        },
    );

    // Ignore the response
    raft.drain_sent();

    assert_follower!(raft);
    assert_term!(raft, Term(0));
    assert_committed!(raft, committed);

    for _ in 0..n {
        let term = raft.info().term + 1;
        run_election(raft, peers);

        for peer in peers {
            raft.assert_sent(
                peer,
                message::AppendEntries {
                    prev_log_pos: Some(pos),
                    entries: vec![message::Entry {
                        pos: Pos {
                            index: pos.index + 1,
                            term,
                        },
                        value: val!(new_term),
                    }],
                    leader_committed_index: Some(committed),
                }
                .to_message("raft", term),
            );
        }

        for _ in 0..14 {
            raft.assert_sleep_for(10).sleep();

            for peer in peers {
                raft.assert_sent(
                    peer,
                    message::AppendEntries {
                        prev_log_pos: Some(pos),
                        entries: vec![],
                        leader_committed_index: Some(committed),
                    }
                    .to_message("raft", term),
                );
            }
        }

        raft.assert_sleep_for(10).sleep();
        assert_follower!(raft);

        raft.assert_idle();

        pos.index += 1;
        pos.term += 1;
    }
}

#[test]
fn proposes_new_term_entry_when_no_limit_is_set() {
    let mut raft = Instance::builder().build_follower("0", &["1"], &[]);
    let peers = &["0", "1"];

    churn_terms(100, &mut raft, peers);

    let pos = raft.log().last().unwrap().pos;
    assert!(pos.index > 100)
}

#[test]
fn proposes_new_term_entry_when_limit_is_set() {
    let mut raft = Instance::builder()
        .max_uncommitted_entries(5)
        .build_follower("0", &["1"], &[]);
    let peers = &["0", "1"];

    churn_terms(5, &mut raft, peers);

    let pos = raft.log().last().unwrap().pos;
    assert!(pos.index > 5);

    run_election(&mut raft, peers);
    raft.assert_idle();
}

fn build_candidate(peers: &[Id]) -> Instance {
    let mut raft = Instance::builder().build_follower(peers[0], &peers[1..], &[]);
    let last_log_pos = raft.log().last().unwrap().pos;

    raft.assert_sleep_for(150).sleep();

    assert_candidate!(raft);

    for peer in peers {
        raft.assert_sent(
            peer,
            message::Vote {
                last_log_pos: Some(last_log_pos),
            }
            .to_prevote_message("raft", Term(1)),
        );
    }
    raft.assert_idle();
    raft
}

#[test]
fn candidate_transitions_to_follower_on_vote_response_higher_term_excpet_pre_granted() {
    for msg in &[
        message::VoteResponse { granted: true }.to_message("0", Term(2)),
        message::VoteResponse { granted: false }.to_message("0", Term(2)),
        message::VoteResponse { granted: false }.to_prevote_message("0", Term(2)),
    ] {
        let mut raft = build_candidate(&["0", "1", "2"]);

        raft.recv(msg.clone());

        assert_follower!(raft);
        assert_term!(raft, 2);
    }
}

#[test]
fn ignore_outdated_pre_vote_responses() {
    let mut raft = build_candidate(&["0"]);
    raft.receive_pre_vote_response("0", Term(0), message::VoteResponse { granted: true });

    assert_candidate!(raft);
    raft.assert_idle();

    raft.receive_pre_vote_response("0", Term(1), message::VoteResponse { granted: true });
    raft.assert_sent(
        "0",
        message::Vote {
            last_log_pos: Some(pos!(3:0)),
        }
        .to_message("raft", Term(1)),
    );
}

#[test]
fn ignore_outdated_vote_responses() {
    let mut raft = build_candidate(&["0"]);

    raft.receive_pre_vote_response("0", Term(1), message::VoteResponse { granted: true });
    raft.assert_sent(
        "0",
        message::Vote {
            last_log_pos: Some(pos!(3:0)),
        }
        .to_message("raft", Term(1)),
    );

    raft.receive_vote_response("0", Term(0), message::VoteResponse { granted: true });
    raft.assert_idle();
    assert_candidate!(raft);

    raft.receive_vote_response("0", Term(1), message::VoteResponse { granted: true });
    assert_leader!(raft);
}

#[test]
fn candidate_receives_append_entries_response() {
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
        let mut raft = build_candidate(&["0"]);
        assert_term!(raft, 0);

        // When term is greater, it transition back to follower
        raft.receive_append_entries_response("0", Term(1), msg.clone());
        assert_follower!(raft);
        assert_term!(raft, 1);

        let mut raft = build_candidate(&["0"]);
        assert_term!(raft, 0);

        // When term is equal to node, it ignores
        raft.receive_append_entries_response("0", Term(0), msg.clone());
        assert_candidate!(raft);
        assert_term!(raft, 0);
    }
}

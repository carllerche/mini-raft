use crate::*;
use mini_raft::{Index, Pos, ProposeError, Term};
use validated::Validated;

use futures::executor::block_on;
use std::cell::RefCell;
use std::rc::Rc;
use tokio::time::Duration;
use turmoil::Io;

/// Simulated Raft node
pub(crate) struct Node {
    /// Handle to the Raft state machine for this node
    raft: RaftRef,

    /// This node's host name
    host: String,

    /// Handle for receiving and sending messages
    io: Io<Message>,

    /// Last applied position
    last_applied: Option<Pos>,

    /// In-flight requests
    requests: Vec<Request>,
}

enum Request {
    Propose { pos: Pos, sender: SocketAddr },
    Read { pos: Pos, sender: SocketAddr },
    ReadIdx { idx: Index, sender: SocketAddr },
    WaitLeader { term: Term, sender: SocketAddr },
}

type RaftRef = Rc<RefCell<Validated<Driver>>>;

impl Node {
    pub(crate) fn new(raft: RaftRef, host: String, io: Io<Message>) -> Node {
        Node {
            raft,
            host,
            io,
            last_applied: None,
            requests: vec![],
        }
    }

    pub(crate) async fn run(&mut self) {
        use futures::FutureExt;

        // Tick once to prime the pump
        let tick = {
            let mut raft = self.raft.borrow_mut();
            raft.set_now(Instant::now());
            block_on(raft.tick()).unwrap()
        };

        let when = tick
            .tick_at
            .unwrap_or(Instant::now() + Duration::from_secs(10));
        let mut sleep = Box::pin(time::sleep_until(when));
        let mut entries = vec![];

        loop {
            tracing::trace!(?self.host, "Sim::tick;");

            futures::select_biased! {
                (message, sender) = self.io.recv().fuse() => {
                    self.raft.borrow_mut().set_now(Instant::now());
                    self.recv(message, sender);
                }
                _ = (&mut sleep).fuse() => {
                    self.raft.borrow_mut().set_now(Instant::now());
                }
            }

            // Tick raft
            let tick = block_on(self.raft.borrow_mut().tick()).unwrap();
            let when = tick
                .tick_at
                .unwrap_or(Instant::now() + Duration::from_secs(10));
            sleep.as_mut().reset(when);

            // Find newly comitted entries
            entries.clear();
            block_on(
                self.raft
                    .borrow_mut()
                    .copy_committed_entries_to(&mut entries),
            )
            .unwrap();

            // Track entries as applied
            for entry in &entries {
                assert!(
                    Some(entry.pos) > self.last_applied,
                    "re-applying commits; last-applied={:?}; entry={:?}",
                    self.last_applied,
                    entry
                );
                self.raft.borrow_mut().applied_to(entry.pos);
                self.last_applied = Some(entry.pos);
            }

            // Try completing requests
            self.process_requests();
        }
    }

    fn process_requests(&mut self) {
        use Request::*;

        self.requests.retain_mut(|request| match request {
            Propose { pos, sender } => {
                if Some(*pos) <= self.last_applied {
                    let maybe_entry = block_on(self.raft.borrow_mut().get(*pos)).unwrap();

                    if let Some(entry) = maybe_entry {
                        self.io.send(
                            *sender,
                            Message::ProposeResult {
                                result: message::ProposeResult::Ok(entry.pos),
                            },
                        );
                    } else {
                        self.io.send(
                            *sender,
                            Message::ProposeResult {
                                result: message::ProposeResult::Failed,
                            },
                        );
                    }
                    false
                } else {
                    true
                }
            }
            Read { pos, sender } => {
                if Some(*pos) <= self.last_applied {
                    let entry = match block_on(self.raft.borrow_mut().get(*pos)).unwrap() {
                        Some(entry) => entry,
                        None => {
                            panic!(
                                "expected to read pos {:?}; host={:?}; last_applied={:?}",
                                pos, self.host, self.last_applied
                            );
                        }
                    };
                    self.io.send(
                        *sender,
                        Message::ReadResult {
                            pos: *pos,
                            value: entry.value,
                        },
                    );
                    false
                } else {
                    true
                }
            }
            ReadIdx { idx, sender } => {
                let last_applied_index = self.last_applied.map(|pos| pos.index);

                if Some(*idx) <= last_applied_index {
                    let entry = block_on(self.raft.borrow_mut().get_index(*idx))
                        .unwrap()
                        .unwrap();
                    self.io.send(
                        *sender,
                        Message::ReadResult {
                            pos: entry.pos,
                            value: entry.value,
                        },
                    );
                    false
                } else {
                    true
                }
            }
            WaitLeader { term, sender } => {
                let info = self.raft.borrow().info();

                if info.term >= *term && info.group.leader.is_some() {
                    self.io.send(*sender, Message::WaitLeaderResponse { info });
                    false
                } else {
                    true
                }
            }
        });
    }

    fn recv(&mut self, msg: Message, sender: SocketAddr) {
        tracing::debug!(?self.host, ?msg, " stream.recv()");

        match msg {
            Message::Propose { value } => {
                let res = block_on(self.raft.borrow_mut().propose(value));
                match res {
                    Ok(pos) => {
                        self.requests.push(Request::Propose { pos, sender });
                    }
                    Err(ProposeError::NotLeader(Some(leader))) => {
                        self.io.send(
                            sender,
                            Message::ProposeResult {
                                result: message::ProposeResult::NotLeader(leader),
                            },
                        );
                    }
                    Err(ProposeError::NotLeader(None)) => {
                        let term = self.raft.borrow().term();
                        self.requests.push(Request::WaitLeader {
                            term,
                            sender: sender,
                        });
                    }
                    Err(ProposeError::TooManyUncommitted) => {
                        self.io.send(
                            sender,
                            Message::ProposeResult {
                                result: message::ProposeResult::Failed,
                            },
                        );
                    }
                    Err(e) => todo!("{:?}", e),
                }
            }
            Message::Read { pos: Some(pos) } => {
                self.requests.push(Request::Read { pos, sender });
            }
            Message::ReadIdx { idx } => {
                self.requests.push(Request::ReadIdx { idx, sender });
            }
            Message::Raft(message) => {
                block_on(self.raft.borrow_mut().receive(message)).unwrap();
            }
            Message::WaitLeader { term } => {
                self.requests.push(Request::WaitLeader { term, sender });
            }
            msg => unimplemented!("this message is sent to client; msg={:?}", msg),
        }
    }
}

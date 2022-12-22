use mini_raft::*;

use std::net::SocketAddr;

#[derive(Clone, Debug, serde::Serialize)]
pub enum Message {
    /// The client is proposing a new value to apply to the log
    Propose { value: message::Value<SocketAddr> },

    /// The result of proposing the new value
    ProposeResult { result: ProposeResult },

    /// Get the log entry at the given position
    Read { pos: Option<Pos> },

    /// Get the log entry at the given index
    ReadIdx { idx: Index },

    /// Result of the read request
    ReadResult {
        pos: Pos,
        value: message::Value<SocketAddr>,
    },

    /// Wait until a leader is elected for the given term
    WaitLeader { term: Term },

    /// Result of the WaitLeader request
    WaitLeaderResponse { info: Info<SocketAddr> },

    /// A Raft-internal message
    Raft(message::Message<SocketAddr>),
}

#[derive(Debug, Clone, serde::Serialize)]
pub enum ProposeResult {
    Ok(Pos),
    Failed,
    NotLeader(SocketAddr),
}

#[derive(Debug)]
pub struct ReadResult {
    pub pos: Pos,
    pub value: message::Value<SocketAddr>,
}

impl turmoil::Message for Message {
    fn write_json(&self, dst: &mut dyn std::io::Write) {
        serde_json::to_writer_pretty(dst, self).unwrap();
    }
}

impl Message {
    /// Return a new message proposing to add a new node to the group
    pub fn add_observer(id: SocketAddr) -> Message {
        Message::Propose {
            value: message::Value::Config(message::ConfigChange::AddNode {
                id,
                auto_upgrade: false,
            }),
        }
    }

    /// Return a new message proposing to add a new follower node to the group.
    ///
    /// The node is first added as an observer with the "auto_upgrade" flag set.
    /// Once the new node has received the log, it is then automatically
    /// upgraded to follower.
    pub fn add_follower(id: SocketAddr) -> Message {
        Message::Propose {
            value: message::Value::Config(message::ConfigChange::AddNode {
                id,
                auto_upgrade: true,
            }),
        }
    }

    /// Return a new message proposing to remove a follower
    pub fn remove_follower(id: SocketAddr) -> Message {
        Message::Propose {
            value: message::Value::Config(message::ConfigChange::RemoveNode {
                id,
                phase: message::Phase::One,
            }),
        }
    }

    /// Return a new message proposing an opaque data value to the Raft group.
    pub fn propose_data(data: impl AsRef<[u8]>) -> Message {
        Message::Propose {
            value: message::Value::Data(data.as_ref().to_vec()),
        }
    }

    pub fn read(pos: Pos) -> Message {
        Message::Read { pos: Some(pos) }
    }

    pub fn read_idx(idx: Index) -> Message {
        Message::ReadIdx { idx }
    }

    pub fn read_latest() -> Message {
        Message::Read { pos: None }
    }

    pub fn wait_leader(term: Term) -> Message {
        Message::WaitLeader { term }
    }

    pub fn to_propose_value(self) -> message::Value<SocketAddr> {
        match self {
            Message::Propose { value } => value,
            _ => panic!("expecting Message::Propose; actual={:?}", self),
        }
    }

    /// Assume the message is of variant `ProposeResult`
    pub fn to_propose_result(self) -> Result<Pos, ProposeError<SocketAddr>> {
        match self {
            Message::ProposeResult { result } => match result {
                ProposeResult::Ok(pos) => Ok(pos),
                ProposeResult::Failed => Err(ProposeError::FailedToCommit),
                ProposeResult::NotLeader(leader) => Err(ProposeError::NotLeader(Some(leader))),
            },
            Message::WaitLeaderResponse { info } => Err(ProposeError::NotLeader(info.group.leader)),
            _ => panic!("expected Message::ProposeResult; actual={:?}", self),
        }
    }

    pub fn to_read_result(self) -> ReadResult {
        match self {
            Message::ReadResult { pos, value } => ReadResult { pos, value },
            _ => panic!("expected Message::ReadResult; actual={:?}", self),
        }
    }

    pub fn to_read_value(self) -> message::Value<SocketAddr> {
        self.to_read_result().value
    }

    pub fn to_info(self) -> Info<SocketAddr> {
        match self {
            Message::WaitLeaderResponse { info } => info,
            _ => panic!("expected Message::WaitLeaderResponse; actual={:?}", self),
        }
    }
}

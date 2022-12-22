use crate::*;

#[derive(Clone, Debug, PartialEq, serde::Serialize)]
pub struct Message<T> {
    /// When and whom sent the message
    pub origin: Origin<T>,

    /// Action to perform
    pub action: Action<T>,
}

/// Message sender's context
#[derive(Clone, Debug, PartialEq, serde::Serialize)]
pub struct Origin<T> {
    /// Identifier of node that sent the message
    pub id: T,

    /// Sender's term when the message was sent
    pub term: Term,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize)]
pub enum Action<T> {
    /// Before a node becomes a leader candidate, it must engage in a pre-vote
    /// round. This helps ensure liveliness in the face of network partitions.
    ///
    /// https://decentralizedthoughts.github.io/2020-12-12-raft-liveness-full-omission/
    PreVote(Vote),
    PreVoteResponse(VoteResponse),
    Vote(Vote),
    VoteResponse(VoteResponse),
    AppendEntries(AppendEntries<T>),
    AppendEntriesResponse(AppendEntriesResponse),
}

#[derive(Clone, Debug, PartialEq, serde::Serialize)]
pub struct Vote {
    /// Position of the candidate's last log entry
    pub last_log_pos: Option<Pos>,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize)]
pub struct VoteResponse {
    pub granted: bool,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize)]
pub struct AppendEntries<T> {
    /// Position of the entry in the log (term, index) immedietly preceeding the
    /// entries included in this message.
    pub prev_log_pos: Option<Pos>,

    /// Entries to append
    pub entries: Vec<Entry<T>>,

    /// Index of highest log entry know to be committed **by the leader**
    pub leader_committed_index: Option<Index>,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize)]
pub enum AppendEntriesResponse {
    /// Successfully accepted the append
    Success {
        /// Position of last appended entry
        last_log_pos: Pos,
        // / Index of the last committed log entry
        // committed: Index,
    },

    /// Attempting to append the entries resulted in a conflict due to diverging
    /// logs.
    Conflict {
        /// The index of the rejected message
        rejected: Index,

        /// A hint for the leader indicating where the follower thinks they may
        /// have diverged.
        hint: Option<Pos>,
        // / Index of the last committed log entry
        // committed: Index,
    },

    /// The peer rejected the entries because the leader has fallen out of date.
    Reject,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize)]
pub struct Entry<T> {
    /// Position of the entry in the log (term, index)
    pub pos: Pos,

    /// Entry value, either arbitrary data or a configuration change.
    pub value: Value<T>,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize)]
pub enum Value<T> {
    /// An opaque value
    Data(Vec<u8>),

    /// A Raft group configuration change
    Config(ConfigChange<T>),

    /// Used to puncutate the start of each new term.
    NewTerm,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize)]
pub enum ConfigChange<T> {
    /// Initializes the group. This sets the first leader of the group.
    InitGroup {
        /// Identifier of the first node in the group, which starts as the
        /// defacto leader.
        id: T,
    },

    /// Add a new node to the Raft group.
    ///
    /// The node starts as a non-voting observer.
    AddNode {
        /// Identifier of node being added
        id: T,

        /// When true, the node is automatically upgraded to follower once it
        /// has synchronized its log.
        auto_upgrade: bool,
    },
    /// Upgrade a node from observer to follower.
    UpgradeNode {
        /// Identifier of the node being upgraded
        id: T,

        /// Upgrade phase. Changes to the group's voter set requires a
        /// two-phased approach.
        phase: Phase,
    },
    /// Remove a node from the group
    RemoveNode {
        /// Identifier of the node being removed
        id: T,

        /// Removal phase. Changes to the group's voter set requires a
        /// two-phased approach.
        phase: Phase,
    },
}

#[derive(Clone, Debug, PartialEq, serde::Serialize)]
pub enum Phase {
    One,
    Two,
}

impl<T> Value<T> {
    pub fn data(data: impl Into<Vec<u8>>) -> Value<T> {
        Value::Data(data.into())
    }

    pub fn init_group(id: T) -> Value<T> {
        Value::Config(message::ConfigChange::InitGroup { id })
    }

    pub fn add_node(id: T) -> Value<T> {
        Value::Config(message::ConfigChange::AddNode {
            id,
            auto_upgrade: false,
        })
    }

    pub fn add_node_auto_upgrade(id: T) -> Value<T> {
        Value::Config(message::ConfigChange::AddNode {
            id,
            auto_upgrade: true,
        })
    }

    pub fn upgrade_node_phase_one(id: T) -> Value<T> {
        Value::Config(message::ConfigChange::UpgradeNode {
            id,
            phase: Phase::One,
        })
    }

    pub fn upgrade_node_phase_two(id: T) -> Value<T> {
        Value::Config(message::ConfigChange::UpgradeNode {
            id,
            phase: Phase::Two,
        })
    }

    pub fn remove_node_phase_one(id: T) -> Value<T> {
        Value::Config(message::ConfigChange::RemoveNode {
            id,
            phase: Phase::One,
        })
    }

    pub fn remove_node_phase_two(id: T) -> Value<T> {
        Value::Config(message::ConfigChange::RemoveNode {
            id,
            phase: Phase::Two,
        })
    }

    /// Proposed by the leader at the start of each term.
    pub fn new_term() -> Value<T> {
        Value::NewTerm
    }

    /// Returns true if the value is a configuration change
    pub fn is_config_change(&self) -> bool {
        matches!(self, Value::Config(..))
    }

    /// Returns true if the value is user supplied data
    pub fn is_data(&self) -> bool {
        matches!(self, Value::Data(..))
    }

    /// Returns true if the value is an internal `NewTerm` entry
    pub fn is_new_term(&self) -> bool {
        matches!(self, Value::NewTerm)
    }
}

impl<T> AppendEntries<T> {
    pub fn prev_log_index(&self) -> Option<Index> {
        self.prev_log_pos.map(|pos| pos.index)
    }

    pub fn to_message(self, id: T, term: Term) -> Message<T> {
        Message {
            origin: Origin { id, term },
            action: Action::AppendEntries(self),
        }
    }
}

impl AppendEntriesResponse {
    pub fn to_message<T>(self, id: T, term: Term) -> Message<T> {
        Message {
            origin: Origin { id, term },
            action: Action::AppendEntriesResponse(self),
        }
    }
}

impl Vote {
    pub fn to_prevote_message<T>(self, id: T, term: Term) -> Message<T> {
        Message {
            origin: Origin { id, term },
            action: Action::PreVote(self),
        }
    }

    pub fn to_message<T>(self, id: T, term: Term) -> Message<T> {
        Message {
            origin: Origin { id, term },
            action: Action::Vote(self),
        }
    }
}

impl VoteResponse {
    pub fn to_prevote_message<T>(self, id: T, term: Term) -> Message<T> {
        Message {
            origin: Origin { id, term },
            action: Action::PreVoteResponse(self),
        }
    }

    pub fn to_message<T>(self, id: T, term: Term) -> Message<T> {
        Message {
            origin: Origin { id, term },
            action: Action::VoteResponse(self),
        }
    }
}

impl<T> ConfigChange<T> {
    pub fn id(&self) -> &T {
        use ConfigChange::*;

        match self {
            InitGroup { id: leader } => leader,
            AddNode { id, .. } => id,
            UpgradeNode { id, .. } => id,
            RemoveNode { id, .. } => id,
        }
    }
}

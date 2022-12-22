use crate::*;

use std::ops;

#[derive(Debug, Clone, serde::Serialize)]
pub struct Info<T> {
    /// This node's term
    pub term: Term,

    /// This node's specific stage
    pub stage: Stage,

    /// Index of most recently committed entry
    pub committed: Option<Index>,

    /// Last applied log position
    pub last_applied: Option<Pos>,

    /// Raft group as known by the queried node
    pub group: Group<T>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct Group<T> {
    /// The leader as known by the current node.
    pub leader: Option<T>,

    /// All nodes in the group
    pub peers: Vec<Peer<T>>,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct Peer<T> {
    /// Node identifier
    pub id: T,

    /// Role the node is playing within the group
    pub role: Role,

    /// How much of the peer log has been matched
    pub matched: Option<Index>,
}

#[derive(Debug, Clone, Copy, serde::Serialize)]
pub enum Stage {
    Follower,
    Observer,
    PreCandidate,
    Candidate,
    Leader,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub enum Role {
    Observer {
        auto_upgrade: Option<Pos>,
    },
    Voter {
        transitioning: Option<Transitioning>,
    },
}

#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct Transitioning {
    pub phase_two_at: Pos,
    pub direction: Direction,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub enum Direction {
    Incoming,
    Outgoing,
}

impl<T> Info<T> {
    pub(crate) fn from_raft<D>(raft: &Raft<D>) -> Info<T>
    where
        T: Clone,
        D: Driver<Id = T>,
    {
        Info {
            term: raft.node.term,
            stage: match &raft.stage {
                crate::Stage::Follower(stage) => {
                    if stage.is_observer() {
                        Stage::Observer
                    } else {
                        Stage::Follower
                    }
                }
                crate::Stage::Candidate(..) => Stage::Candidate,
                crate::Stage::Leader(..) => Stage::Leader,
            },
            committed: raft.node.log.last_committed.map(|pos| pos.index),
            last_applied: raft.node.last_applied,
            group: Group {
                leader: raft.node.group.leader.clone(),
                peers: raft
                    .node
                    .group
                    .peers
                    .values()
                    .map(|peer| Peer {
                        id: peer.id.clone(),
                        matched: peer.matched,
                        role: match peer.role {
                            crate::peer::Role::Observer { auto_upgrade } => {
                                Role::Observer { auto_upgrade }
                            }
                            crate::peer::Role::Voter => Role::Voter {
                                transitioning: None,
                            },
                            crate::peer::Role::VoterIncoming { phase_two_at } => Role::Voter {
                                transitioning: Some(Transitioning {
                                    phase_two_at,
                                    direction: Direction::Incoming,
                                }),
                            },
                            crate::peer::Role::VoterOutgoing { phase_two_at } => Role::Voter {
                                transitioning: Some(Transitioning {
                                    phase_two_at,
                                    direction: Direction::Outgoing,
                                }),
                            },
                        },
                    })
                    .collect(),
            },
        }
    }
}

impl Stage {
    pub fn is_follower(&self) -> bool {
        matches!(self, Stage::Follower)
    }

    pub fn is_observer(&self) -> bool {
        matches!(self, Stage::Observer)
    }

    pub fn is_pre_candidate(&self) -> bool {
        matches!(self, Stage::PreCandidate)
    }

    pub fn is_candidate(&self) -> bool {
        matches!(self, Stage::Candidate)
    }

    pub fn is_leader(&self) -> bool {
        matches!(self, Stage::Leader)
    }
}

impl<T> Group<T> {
    pub fn peer_by_id(&self, id: &T) -> Option<&Peer<T>>
    where
        T: PartialEq,
    {
        self.peers.iter().find(|peer| peer.id == *id)
    }
}

impl<T: PartialEq> ops::Index<T> for Group<T> {
    type Output = Peer<T>;

    fn index(&self, index: T) -> &Peer<T> {
        self.peer_by_id(&index).expect("peer missing")
    }
}

impl<T> Peer<T> {
    pub fn is_voter(&self) -> bool {
        matches!(self.role, Role::Voter { .. })
    }

    /// Is a voter that is neither incoming or outgoing
    pub fn is_stable_voter(&self) -> bool {
        matches!(
            self.role,
            Role::Voter {
                transitioning: None
            }
        )
    }

    pub fn is_incoming_voter(&self) -> bool {
        matches!(
            self.role,
            Role::Voter {
                transitioning: Some(Transitioning {
                    direction: Direction::Incoming,
                    ..
                })
            }
        )
    }

    pub fn is_outgoing_voter(&self) -> bool {
        matches!(
            self.role,
            Role::Voter {
                transitioning: Some(Transitioning {
                    direction: Direction::Outgoing,
                    ..
                })
            }
        )
    }
}

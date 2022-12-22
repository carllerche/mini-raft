use crate::*;

use std::io;
use tokio::time::Instant;

pub struct Raft<T: Driver> {
    /// State common to all Raft roles
    pub(crate) node: Node<T>,

    /// Current role the node plays in the raft group. All nodes start in the
    /// follower state.
    pub(crate) stage: Stage<T>,
}

#[derive(Debug)]
#[non_exhaustive]
pub struct Tick {
    pub tick_at: Option<Instant>,
}

#[derive(Debug)]
#[non_exhaustive]
pub enum ProposeError<T> {
    NotLeader(Option<T>),
    TooManyUncommitted,
    InvalidConfig,
    FailedToCommit,
    Io(io::Error),
}

impl<T: Driver> Raft<T> {
    /// Initialize a new raft group with no peers.
    pub fn new_group(driver: T, config: Config<T::Id>, now: Instant) -> Raft<T> {
        let mut node = Node::new(driver, config, now);
        node.group.bootstrap(node.config.id.clone(), now);

        // Append the very first log entry which initializes the Raft group.
        node.log.append_entries(
            &mut node.driver,
            &[message::Entry {
                pos: Pos {
                    term: Term(0),
                    index: Index(0),
                },
                value: message::Value::Config(message::ConfigChange::InitGroup {
                    id: node.config.id.clone(),
                }),
            }],
        );

        // Commit the entry
        node.log.commit(Pos {
            term: Term(0),
            index: Index(0),
        });

        // If the Raft group is initialized with one node, then the current node
        // is the defacto leader.
        let stage = Stage::Leader(Leader::new(&node));

        let raft = Raft {
            node,
            // If the Raft group is initialized with one node, then the current node
            // is the defacto leader.
            stage,
        };

        raft
    }

    /// Initialize a new raft node as an observer to an existing Raft group
    pub fn new_observer(driver: T, config: Config<T::Id>, now: Instant) -> Raft<T> {
        let mut node = Node::new(driver, config, now);

        // Set the role as an observer
        let stage = Stage::Follower(Follower::new_observer(&mut node));

        Raft { node, stage }
    }

    /// Returns the node identifier for this Raft node
    pub fn id(&self) -> T::Id {
        self.node.config.id.clone()
    }

    /// Returns the node's current term
    pub fn term(&self) -> Term {
        self.node.term
    }

    /// Returns a reference to the driver
    pub fn driver(&self) -> &T {
        &self.node.driver
    }

    /// Returns a mutable reference to the driver
    pub fn driver_mut(&mut self) -> &mut T {
        &mut self.node.driver
    }

    /// Return info about the Raft node
    pub fn info(&self) -> Info<T::Id> {
        Info::from_raft(self)
    }

    /// Return the index of the last committed entry. This entry has been safely
    /// replicated to other nodes in the group.
    ///
    /// The method returns `None` when the log is empty.
    pub fn last_committed_index(&self) -> Option<Index> {
        self.node.log.last_committed().map(|pos| pos.index)
    }

    /// Read the value at the specific position
    pub async fn get(&mut self, pos: Pos) -> io::Result<Option<message::Entry<T::Id>>> {
        match self.get_index(pos.index).await? {
            Some(entry) if entry.pos == pos => Ok(Some(entry)),
            _ => Ok(None),
        }
    }

    pub async fn get_index(&mut self, index: Index) -> io::Result<Option<message::Entry<T::Id>>> {
        self.node.log.get(&mut self.node.driver, index).await
    }

    /// Copies committed entries to the
    pub async fn copy_committed_entries_to(
        &mut self,
        dst: &mut Vec<message::Entry<T::Id>>,
    ) -> io::Result<()> {
        self.node.copy_committed_entries_to(dst).await
    }

    /// Advance the "applied" cursor
    pub fn applied_to(&mut self, pos: Pos) {
        self.node.applied_to(pos);
    }

    /// Propose a new value to append to the log
    ///
    /// Returns the log position at which the value is being proposed for
    /// insertion. The caller may use this position to track when the entry is
    /// committed.
    pub async fn propose(
        &mut self,
        value: message::Value<T::Id>,
    ) -> Result<Pos, ProposeError<T::Id>> {
        if matches!(value, message::Value::NewTerm) {
            todo!("deny client proposing NewTerm.");
        }

        match &mut self.stage {
            Stage::Leader(leader) => {
                // If the proposed value is a configuration change, ensure it is valid
                if let message::Value::Config(config_change) = &value {
                    let pos = self.node.next_pos();

                    if !self.node.group.is_valid_config_change(pos, config_change) {
                        return Err(ProposeError::InvalidConfig);
                    }
                } else if self.node.is_max_uncommitted_entries() {
                    return Err(ProposeError::TooManyUncommitted);
                }

                Ok(leader.propose(&mut self.node, value).await?)
            }
            _ => {
                // Not currently the leader, but if we know who is the leader,
                // then we can redirect the client there.
                Err(ProposeError::NotLeader(self.node.group.leader.clone()))
            }
        }
    }

    /// Set this Raft node's current `Instant`
    pub fn set_now(&mut self, now: Instant) {
        assert!(now >= self.node.now);
        self.node.now = now;
    }

    // Do something w/ the message
    pub async fn receive(&mut self, message: Message<T::Id>) -> io::Result<()> {
        use Stage::*;

        let maybe_transition = match &mut self.stage {
            Candidate(candidate) => candidate.receive(&mut self.node, message).await?,
            Follower(follower) => follower.receive(&mut self.node, message).await?,
            Leader(leader) => leader.receive(&mut self.node, message).await?,
        };

        if let Some(stage) = maybe_transition {
            self.stage = stage;
        }

        Ok(())
    }

    pub async fn tick(&mut self) -> io::Result<Tick> {
        use Stage::*;

        loop {
            let transition = match &mut self.stage {
                Candidate(candidate) => candidate.tick(&mut self.node).await?,
                Follower(follower) => follower.tick(&mut self.node),
                Leader(leader) => leader.tick(&mut self.node).await?,
            };

            match transition {
                Step::Transition(stage) => {
                    self.stage = stage;
                }
                Step::Wait(when) => {
                    return Ok(Tick { tick_at: when });
                }
            }
        }
    }
}

impl<T> From<io::Error> for ProposeError<T> {
    fn from(src: io::Error) -> ProposeError<T> {
        ProposeError::Io(src)
    }
}

use mini_raft::*;
use std::io;
use tokio::time::Instant;

/// Wrapper around a `Raft` instance that performs extra validation of the state.
pub struct Validated<T: Driver> {
    raft: Raft<T>,
}

impl<T: Driver> Validated<T> {
    pub fn new(raft: Raft<T>) -> Validated<T> {
        Validated { raft }
    }

    pub fn id(&self) -> T::Id {
        self.raft.id()
    }

    pub fn set_now(&mut self, now: Instant) {
        self.raft.set_now(now);
    }

    pub async fn propose(
        &mut self,
        value: message::Value<T::Id>,
    ) -> Result<Pos, ProposeError<T::Id>> {
        self.raft.propose(value).await
    }

    pub async fn receive(&mut self, message: Message<T::Id>) -> io::Result<()> {
        let info = self.info();
        let term = info.term;
        let stage = info.stage;
        let should_inc_term = self.should_inc_term(stage, term, &message);

        let ret = self.raft.receive(message.clone()).await;
        let new_term = self.info().term;

        match should_inc_term {
            Some(true) => {
                assert_eq!(
                    new_term, message.origin.term,
                    "expected term to be updated to message term; stage={:?}; {:#?}",
                    stage, message
                )
            }
            Some(false) => {
                assert_eq!(
                    new_term, term,
                    "expected term to remain the same; stage={:?}; {:#?}",
                    stage, message
                );
            }
            None => {}
        }

        ret
    }

    /// Copies committed entries to the
    pub async fn copy_committed_entries_to(
        &mut self,
        dst: &mut Vec<message::Entry<T::Id>>,
    ) -> io::Result<()> {
        self.raft.copy_committed_entries_to(dst).await
    }

    /// Advance the "applied" cursor
    pub fn applied_to(&mut self, pos: Pos) {
        self.raft.applied_to(pos)
    }

    /// Read the value at the specific position
    pub async fn get(&mut self, pos: Pos) -> io::Result<Option<message::Entry<T::Id>>> {
        self.raft.get(pos).await
    }

    pub async fn get_index(&mut self, index: Index) -> io::Result<Option<message::Entry<T::Id>>> {
        self.raft.get_index(index).await
    }

    /// Returns the node's current term
    pub fn term(&self) -> Term {
        self.raft.term()
    }

    pub async fn tick(&mut self) -> io::Result<Tick> {
        self.raft.tick().await
    }

    /// Returns a reference to the driver
    pub fn driver(&self) -> &T {
        self.raft.driver()
    }

    /// Returns a mutable reference to the driver
    pub fn driver_mut(&mut self) -> &mut T {
        self.raft.driver_mut()
    }

    pub fn info(&self) -> Info<T::Id> {
        self.raft.info()
    }

    fn should_inc_term(
        &self,
        _stage: info::Stage,
        term: Term,
        message: &Message<T::Id>,
    ) -> Option<bool> {
        match &message.action {
            // Incrementing the term for a vote message is somewhat involved, skip for now.
            message::Action::Vote(_) | message::Action::PreVote(_) => None,
            message::Action::PreVoteResponse(response) => {
                Some(!response.granted && message.origin.term > term)
            }
            _ if message.origin.term > term => Some(true),
            _ => Some(false),
        }
    }
}

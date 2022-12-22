use crate::Message;
use mini_raft::{message, Index, Term};

use rand::rngs::SmallRng;
use std::cell::RefCell;
use std::fmt;
use std::io;
use std::net::SocketAddr;
use std::rc::Rc;
use std::task::{Context, Poll};
use turmoil::Io;

pub(crate) struct Driver {
    /// turmoil I/O handle for sending messages
    pub(crate) io: Io<Message>,

    /// Log entries
    pub(crate) log: Vec<message::Entry<SocketAddr>>,

    /// Random number generator used by `Raft`, but also shared across the test
    /// group.
    pub(crate) rng: Rc<RefCell<SmallRng>>,
}

impl mini_raft::Driver for Driver {
    type Id = SocketAddr;

    fn dispatch(&mut self, dst: Self::Id, message: mini_raft::Message<Self::Id>) {
        self.io.send(dst, Message::Raft(message));
    }

    fn poll_term_for(
        &mut self,
        _cx: &mut Context<'_>,
        index: Index,
    ) -> Poll<io::Result<Option<Term>>> {
        let index: usize = index.0.try_into().unwrap();
        let maybe_term = self.log.get(index).map(|entry| entry.pos.term);
        Poll::Ready(Ok(maybe_term))
    }

    fn poll_read_entry(
        &mut self,
        _cx: &mut Context<'_>,
        index: Index,
    ) -> Poll<io::Result<Option<message::Entry<Self::Id>>>> {
        let index = usize::try_from(index.0).unwrap();
        let entry = self.log.get(index).cloned();
        Poll::Ready(Ok(entry))
    }

    fn poll_read_entries(
        &mut self,
        _cx: &mut Context<'_>,
        start: Index,
        end: Index,
        dst: &mut Vec<message::Entry<Self::Id>>,
    ) -> Poll<io::Result<()>> {
        let start = usize::try_from(start.0).unwrap();
        // `end` is inclusive, but exclusive for rust slices.
        let end = usize::try_from(end.0).unwrap() + 1;
        dst.extend_from_slice(&self.log[start..end]);
        Poll::Ready(Ok(()))
    }

    fn append_entries(&mut self, entries: &[message::Entry<SocketAddr>]) {
        self.log.extend_from_slice(entries);
    }

    fn truncate(&mut self, index: Index) {
        let index = usize::try_from(index.0).unwrap();
        self.log.truncate(index);
    }

    fn rand_election_timeout(&mut self, lower: u64, upper: u64) -> u64 {
        use rand::Rng;

        self.rng.borrow_mut().gen_range(lower..upper)
    }
}

impl fmt::Debug for Driver {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(fmt, "Driver {{ .. }}")
    }
}

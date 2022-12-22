use crate::*;

use std::hash::Hash;
use std::task::{Context, Poll};
use std::{fmt, io};

pub trait Driver: fmt::Debug + Sized + 'static {
    type Id: Hash + Eq + PartialEq + Clone + fmt::Debug + 'static;

    /// Send a message to a peer
    fn dispatch(&mut self, dst: Self::Id, message: Message<Self::Id>);

    /// Asynchronously query the term for the entry at the given index.
    fn poll_term_for(
        &mut self,
        cx: &mut Context<'_>,
        index: Index,
    ) -> Poll<io::Result<Option<Term>>>;

    /// Asynchronously read a single entry from the log
    fn poll_read_entry(
        &mut self,
        cx: &mut Context<'_>,
        index: Index,
    ) -> Poll<io::Result<Option<message::Entry<Self::Id>>>>;

    /// Asynchronously read entries from the log.
    ///
    /// `end` is **inclusive**
    fn poll_read_entries(
        &mut self,
        cx: &mut Context<'_>,
        start: Index,
        end: Index,
        dst: &mut Vec<message::Entry<Self::Id>>,
    ) -> Poll<io::Result<()>>;

    /// Append entries to the log.
    fn append_entries(&mut self, entries: &[message::Entry<Self::Id>]);

    /// Truncate the log at the given index.
    ///
    /// All **earlier** indices are kept. All entries at the given index and
    /// after are discarded.
    fn truncate(&mut self, index: Index);

    /// Generate a random campaign wait time, in milliseconds.
    ///
    /// Section 5.2: Raft uses randomized election timeouts to ensure that split
    /// votes are rare and that they are resolved quickly. To prevent split
    /// votes in the first place, election timeouts are chosen randomly from a
    /// fixed interval (e.g., 150–300ms). This spreads out the servers so that
    /// in most cases only a single server will time out; it wins the election
    /// and sends heartbeats before any other servers time out. The same
    /// mechanism is used to handle split votes.
    fn rand_election_timeout(&mut self, lower: u64, upper: u64) -> u64;
}

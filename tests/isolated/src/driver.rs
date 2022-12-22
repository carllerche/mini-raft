use mini_raft::*;

use std::io;
use std::task::{Context, Poll};

#[derive(Clone, Debug)]
pub struct MockDriver {
    /// Log entries
    pub(crate) log: Vec<message::Entry<&'static str>>,

    /// List of outbound messages
    pub(crate) outbound: Vec<(Message<&'static str>, &'static str)>,

    /// Value returned by `rand_election_timeout`
    pub(crate) election_timeout: ElectionTimeout,
}

#[derive(Clone, Debug)]
pub enum ElectionTimeout {
    /// Always use the minimum election timeout
    Min,

    /// Always offset the minimum election timeout by a specified duration.
    Offset(u64),

    /// Return the following offsets in sequence
    Multi(Vec<u64>),
}

impl mini_raft::Driver for MockDriver {
    type Id = &'static str;

    fn dispatch(&mut self, dst: &'static str, message: Message<&'static str>) {
        self.outbound.push((message, dst));
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
        _index: Index,
    ) -> Poll<io::Result<Option<message::Entry<Self::Id>>>> {
        todo!();
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

    fn append_entries(&mut self, entries: &[message::Entry<&'static str>]) {
        self.log.extend_from_slice(entries);
    }

    fn truncate(&mut self, index: Index) {
        let index = usize::try_from(index.0).unwrap();
        self.log.truncate(index);
    }

    fn rand_election_timeout(&mut self, lower: u64, upper: u64) -> u64 {
        match &mut self.election_timeout {
            ElectionTimeout::Min => lower,
            ElectionTimeout::Offset(offset) => {
                assert!(lower + *offset <= upper);
                lower + *offset
            }
            ElectionTimeout::Multi(vals) => {
                assert!(!vals.is_empty(), "no more specified election timeouts");
                let offset = vals.remove(0);
                assert!(lower + offset <= upper);
                lower + offset
            }
        }
    }
}

impl MockDriver {
    pub fn new() -> MockDriver {
        MockDriver {
            log: vec![],
            outbound: vec![],
            election_timeout: ElectionTimeout::Min,
        }
    }

    pub fn set_election_timeout(&mut self, election_timeout: ElectionTimeout) {
        self.election_timeout = election_timeout;
    }
}

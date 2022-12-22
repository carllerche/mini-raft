#[macro_use]
mod macros;

mod builder;
pub use builder::Builder;

mod driver;
pub use driver::{ElectionTimeout, MockDriver};

use mini_raft::*;
use validated::Validated;

use futures_lite::future::block_on;
use pretty_assertions::assert_eq;
use tokio::time::{Duration, Instant};

pub struct Instance {
    /// Handle to the raft instance
    raft: Validated<MockDriver>,

    /// Instant the instance was created
    epoch: Instant,

    /// Last instant
    now: Instant,

    /// Last `tick_at` return value.
    tick_at: Option<Instant>,
}

pub type Id = &'static str;

impl Instance {
    pub fn builder() -> Builder {
        Builder::new()
    }

    // Returns a new, unconfigured, follower
    pub fn new() -> Instance {
        Instance::builder().build_observer()
    }

    /// Returns a new leader in an empty group.
    pub fn new_group() -> Instance {
        Instance::builder().build_group()
    }

    /// Create a raft leader initialized with the specified group.
    pub fn new_leader(followers: &[&'static str], observers: &[&'static str]) -> Instance {
        Self::builder().build_leader(followers, observers)
    }

    pub fn propose(
        &mut self,
        value: message::Value<&'static str>,
    ) -> Result<Pos, ProposeError<&'static str>> {
        block_on(self.raft.propose(value))
    }

    pub fn propose_ignore_append_entries(&mut self, value: message::Value<&'static str>) {
        self.propose(value).unwrap();

        for (outbound, _) in self.raft.driver_mut().outbound.drain(..) {
            match outbound.action {
                message::Action::AppendEntries(..) => {}
                _ => panic!("unexpected message; {:#?}", outbound),
            }
        }
    }

    /// Tick time to the next action point
    pub fn sleep(&mut self) {
        if let Some(when) = self.tick_at {
            self.now = when;
            self.raft.set_now(when);
        }

        self.tick();
    }

    pub fn sleep_for(&mut self, ms: u64) {
        self.now += Duration::from_millis(ms);
        self.raft.set_now(self.now);
        self.tick();
    }

    pub fn tick(&mut self) {
        let Tick { tick_at, .. } = block_on(self.raft.tick()).unwrap();
        self.tick_at = tick_at;
    }

    pub fn recv_append_entries(
        &mut self,
        id: &'static str,
        term: Term,
        message: message::AppendEntries<&'static str>,
    ) {
        self.recv(Message {
            origin: message::Origin { id, term },
            action: message::Action::AppendEntries(message),
        })
    }

    pub fn receive_append_entries_response(
        &mut self,
        id: &'static str,
        term: Term,
        append_entries_response: message::AppendEntriesResponse,
    ) {
        self.recv(append_entries_response.to_message(id, term))
    }

    pub fn receive_pre_vote_request(&mut self, id: &'static str, term: Term, vote: message::Vote) {
        self.recv(vote.to_prevote_message(id, term))
    }

    pub fn receive_vote_request(&mut self, id: &'static str, term: Term, vote: message::Vote) {
        self.recv(vote.to_message(id, term))
    }

    pub fn receive_pre_vote_response(
        &mut self,
        id: &'static str,
        term: Term,
        vote_response: message::VoteResponse,
    ) {
        self.recv(vote_response.to_prevote_message(id, term))
    }

    pub fn receive_vote_response(
        &mut self,
        id: &'static str,
        term: Term,
        vote_response: message::VoteResponse,
    ) {
        self.recv(vote_response.to_message(id, term))
    }

    pub fn recv(&mut self, message: Message<&'static str>) {
        block_on(async {
            self.raft.receive(message).await.unwrap();
            let Tick { tick_at, .. } = self.raft.tick().await.unwrap();
            self.tick_at = tick_at;
        });
    }

    pub fn log(&self) -> &[message::Entry<&'static str>] {
        &self.raft.driver().log[..]
    }

    /// Assert the raft node is sleeping for a set amount of time
    #[track_caller]
    pub fn assert_sleep_for(&mut self, ms: u64) -> &mut Self {
        assert_eq!(Duration::from_millis(ms), self.tick_at.unwrap() - self.now);
        self
    }

    pub fn info(&self) -> mini_raft::Info<&'static str> {
        self.raft.info()
    }

    /// Return the node's term
    pub fn term(&self) -> Term {
        self.info().term
    }

    pub fn last_appended(&self) -> Pos {
        self.log().last().unwrap().pos
    }

    /// When the instance will tick again, relative to the epoch
    pub fn tick_at(&self) -> Option<Duration> {
        self.tick_at.map(|when| when - self.epoch)
    }

    /// Return the current leader from the node's point of view
    pub fn leader(&self) -> Option<&'static str> {
        self.raft.info().group.leader
    }

    /// Return the set of peers known to the node
    pub fn peers(&self) -> Vec<info::Peer<&'static str>> {
        self.raft.info().group.peers
    }

    #[track_caller]
    pub fn assert_sent(&mut self, to: &'static str, message: Message<&'static str>) -> &mut Self {
        if self.raft.driver().outbound.is_empty() {
            panic!("no pending outbound messages");
        }

        let sent = self.raft.driver_mut().outbound.remove(0);
        assert_eq!(sent, (message, to));
        self
    }

    #[track_caller]
    pub fn assert_idle(&mut self) -> &mut Self {
        let outbound = &self.raft.driver().outbound;

        if !outbound.is_empty() {
            panic!(
                "expected the raft node to be idle, but has pending messages\n{:#?}",
                outbound
            );
        }

        self
    }

    /// Drain all messages in the sent queue without checking them
    pub fn drain_sent(&mut self) -> &mut Self {
        self.raft.driver_mut().outbound.clear();
        self
    }

    /// Assert the peer has replicated the log up to the given index
    #[track_caller]
    pub fn assert_peer_matched(&mut self, peer: &'static str, expect: Option<Index>) -> &mut Self {
        let matched = self
            .info()
            .group
            .peer_by_id(&peer)
            .expect("peer missing")
            .matched;
        assert_eq!(matched, expect);
        self
    }
}

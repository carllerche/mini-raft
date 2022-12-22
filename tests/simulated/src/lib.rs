mod builder;
pub use builder::Builder;

mod driver;
use driver::Driver;

pub mod message;
pub use message::Message;

mod node;
use node::Node;

use validated::Validated;

use std::cell::RefCell;
use std::future::Future;
use std::net::SocketAddr;
use std::rc::Rc;
use tokio::time::{self, Instant};
use turmoil::{Sim, ToSocketAddr};

pub struct TestGroup {
    /// Handle to raft nodes, enabling introspection
    nodes: Vec<Rc<RefCell<Validated<Driver>>>>,

    /// Network simulator
    sim: RefCell<Sim>,

    /// Default client
    client: turmoil::Io<Message>,

    /// Random number generator seed
    seed: [u8; 32],
}

impl TestGroup {
    pub fn builder(num_nodes: usize) -> Builder {
        Builder::new(num_nodes)
    }

    /// Run a test against this group
    pub fn test(&self, f: impl Future<Output = ()>) {
        self.sim.borrow_mut().run_until(&self.client, f);
    }

    /// Not **exactly** a request/response, but does a send followed by a
    /// recv_from
    pub async fn request(&self, host: impl ToSocketAddr, message: Message) -> Message {
        let host = self.lookup(host);
        self.send(host, message);
        self.recv_from(host).await
    }

    /// Send a message to the specified host using the default client
    pub fn send(&self, host: impl ToSocketAddr, message: Message) {
        let host = self.lookup(host);
        self.client.send(host, message);
    }

    /// Receive a message from any host
    pub async fn recv(&self) -> (Message, SocketAddr) {
        self.client.recv().await
    }

    /// Receive a message from the specified host.
    pub async fn recv_from(&self, host: impl ToSocketAddr) -> Message {
        let host = self.lookup(host);
        self.client.recv_from(host).await
    }

    /// Lookup the associated SocketAddr for the given host name
    pub fn lookup(&self, host: impl ToSocketAddr) -> SocketAddr {
        self.client.lookup(host)
    }

    /// Partition a Raft node from others.
    ///
    /// Clients will still be able to communicate with the node.
    pub fn partition(&self, host: &str) {
        let addr = self.client.lookup(host);

        for peer in &self.nodes {
            let peer = peer.borrow();

            if peer.id() != addr {
                turmoil::partition(addr, peer.id());
            }
        }
    }

    /// Repair a Raft node's connection.
    pub fn repair(&self, host: &str) {
        let addr = self.client.lookup(host);

        for peer in &self.nodes {
            let peer = peer.borrow();

            if peer.id() != addr {
                turmoil::repair(addr, peer.id());
            }
        }
    }

    /// Returns the info for the Raft node with the given host name
    pub fn info(&self, host: impl ToSocketAddr) -> mini_raft::Info<SocketAddr> {
        let addr = self.client.lookup(host);

        for node in &self.nodes {
            let node = node.borrow();

            if node.id() == addr {
                return node.info();
            }
        }

        panic!("no Raft node with host name {}", addr);
    }
}

impl Drop for TestGroup {
    fn drop(&mut self) {
        if std::thread::panicking() {
            println!("RANDOM NUMBER GENERATOR SEED = {:?}", self.seed);
        }
    }
}

fn init_tracing() {
    use tracing_subscriber::filter::EnvFilter;
    use tracing_subscriber::fmt::format::FmtSpan;

    let _ = tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_test_writer()
        .with_span_events(FmtSpan::NEW)
        .try_init();
}

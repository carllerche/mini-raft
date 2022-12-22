use crate::*;

use mini_raft::{Config, Raft};
use validated::Validated;

use rand::rngs::SmallRng;
use rand::{RngCore, SeedableRng};
use std::cell::RefCell;
use std::path::Path;
use std::rc::Rc;
use std::time::Duration;
use tokio::time::Instant;

pub struct Builder {
    /// Number of nodes in the raft group
    num_nodes: usize,

    /// Random number generator seed. This is used to reproduce a failing test.
    seed: Option<[u8; 32]>,

    /// How long to fuzz each test for, in real time
    duration: Duration,

    /// Turmoil builder
    turmoil: turmoil::Builder,
}

impl Builder {
    pub(crate) fn new(num_nodes: usize) -> Builder {
        super::init_tracing();

        const DEFAULT_DURATION: u64 = 500;

        let duration = match std::env::var("RAFT_DURATION") {
            Ok(val) => match val.parse() {
                Ok(val) => val,
                Err(_) => DEFAULT_DURATION,
            },
            Err(_) => DEFAULT_DURATION,
        };

        let mut turmoil = turmoil::Builder::new();
        turmoil.fail_rate(0.1).repair_rate(0.5);

        Builder {
            num_nodes,
            seed: None,
            duration: Duration::from_millis(duration),
            turmoil,
        }
    }

    pub fn seed(&mut self, seed: [u8; 32]) -> &mut Self {
        self.seed = Some(seed);
        self
    }

    pub fn duration(&mut self, duration: Duration) -> &mut Self {
        self.duration = duration;
        self
    }

    pub fn simulation_duration(&mut self, duration: Duration) -> &mut Self {
        self.turmoil.simulation_duration(duration);
        self
    }

    pub fn tick_duration(&mut self, duration: Duration) -> &mut Self {
        self.turmoil.tick_duration(duration);
        self
    }

    pub fn max_message_latency(&mut self, duration: Duration) -> &mut Self {
        self.turmoil.max_message_latency(duration);
        self
    }

    /// Log events to the specified file
    pub fn log(&mut self, path: impl AsRef<Path>) -> &mut Self {
        self.turmoil.log(path);
        self
    }

    pub fn fuzz<F>(&self, mut f: F)
    where
        F: FnMut(&TestGroup),
    {
        if self.seed.is_some() {
            // A seed is set, so there is only one possible run.
            let group = self.new_group();
            f(&group);
        } else {
            let now = std::time::Instant::now();

            while now.elapsed() < self.duration {
                for _ in 0..50 {
                    let group = self.new_group();
                    f(&group);
                }
            }
        }
    }

    pub fn test<F>(&self, f: F)
    where
        F: std::future::Future<Output = ()>,
    {
        self.new_group().test(f);
    }

    fn new_group(&self) -> TestGroup {
        let seed = self.seed.unwrap_or_else(|| {
            let mut seed = [0; 32];
            getrandom::getrandom(&mut seed[..]).unwrap();
            seed
        });

        let mut nodes = vec![];
        let rng = Rc::new(RefCell::new(SmallRng::from_seed(seed)));
        let rand = Box::new(Rand(rng.clone()));

        assert!(self.num_nodes > 0);

        let mut sim = self.turmoil.build_with_rng(rand);

        for i in 0..self.num_nodes {
            let host = format!("raft{}", i);

            sim.register(host.clone(), |io| {
                let outbound = Driver {
                    io: io.clone(),
                    log: vec![],
                    rng: rng.clone(),
                };

                let mut config = Config::new(io.local_addr());
                config.max_uncommitted_entries = Some(10);

                let raft = if i == 0 {
                    Rc::new(RefCell::new(Validated::new(Raft::new_group(
                        outbound,
                        config,
                        Instant::now(),
                    ))))
                } else {
                    Rc::new(RefCell::new(Validated::new(Raft::new_observer(
                        outbound,
                        config,
                        Instant::now(),
                    ))))
                };

                nodes.push(raft.clone());

                let mut node = Node::new(raft, host, io);

                async move { node.run().await }
            });
        }

        let client = sim.client("test-group");

        // Don't drop messages from/to the client
        for peer in &nodes {
            let peer = peer.borrow();
            sim.set_link_fail_rate(client.local_addr(), peer.id(), 0.0);
            sim.set_link_max_message_latency(
                client.local_addr(),
                peer.id(),
                Duration::from_millis(0),
            );
        }

        TestGroup {
            nodes,
            sim: RefCell::new(sim),
            client,
            seed,
        }
    }
}

struct Rand(Rc<RefCell<SmallRng>>);

impl RngCore for Rand {
    fn next_u32(&mut self) -> u32 {
        self.0.borrow_mut().next_u32()
    }

    fn next_u64(&mut self) -> u64 {
        self.0.borrow_mut().next_u64()
    }

    fn fill_bytes(&mut self, dest: &mut [u8]) {
        self.0.borrow_mut().fill_bytes(dest)
    }

    fn try_fill_bytes(&mut self, dest: &mut [u8]) -> Result<(), rand::Error> {
        self.0.borrow_mut().try_fill_bytes(dest)
    }
}

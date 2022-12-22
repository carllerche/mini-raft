use tokio::time::Duration;

/// Configure a Raft group's execution details
#[derive(Clone)]
#[non_exhaustive]
pub struct Config<T> {
    pub id: T,

    /// How often the leader sends heartbeats
    pub leader_heartbeat_interval: Duration,

    /// Lower bound after which a follower will initiate an election if it has
    /// not heard from the leader.
    pub min_election_interval: Duration,

    /// Upper bound after which a follower will initiate an election if it has
    /// not heard from the leader.
    pub max_election_interval: Duration,

    /// How many entries can be uncommitted before the leader starts rejecting
    /// proposals.
    pub max_uncommitted_entries: Option<u64>,
}

impl<T> Config<T> {
    pub fn new(id: T) -> Config<T> {
        Config {
            id,
            leader_heartbeat_interval: ms(10),
            min_election_interval: ms(150),
            max_election_interval: ms(400),
            max_uncommitted_entries: None,
        }
    }
}

fn ms(ms: u32) -> Duration {
    Duration::from_millis(ms as _)
}

use crate::*;

pub(crate) enum Stage<T: Driver> {
    /// The node is following a leader
    Follower(Follower<T>),

    /// The node is proposing itself as the new leader.
    Candidate(Candidate<T>),

    // The node believes it is the group's leader.
    Leader(Leader<T>),
}

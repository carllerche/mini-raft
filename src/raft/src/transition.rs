use crate::*;

use tokio::time::Instant;

macro_rules! transition {
    ( $a:expr, $b:expr ) => {{
        let ret = $a;

        return Ok(if ret.is_some() { ret } else { $b.into() });
    }};
}

/// Either transition the raft node's stage or wait until the specified
/// instant.
pub(crate) enum Step<T> {
    Transition(T),
    Wait(Option<Instant>),
}

impl<T: Driver> From<Follower<T>> for Step<Stage<T>> {
    fn from(src: Follower<T>) -> Step<Stage<T>> {
        Step::Transition(Stage::Follower(src))
    }
}

impl<T: Driver> From<Candidate<T>> for Step<Stage<T>> {
    fn from(src: Candidate<T>) -> Step<Stage<T>> {
        Step::Transition(Stage::Candidate(src))
    }
}

impl<T: Driver> From<Leader<T>> for Step<Stage<T>> {
    fn from(src: Leader<T>) -> Step<Stage<T>> {
        Step::Transition(Stage::Leader(src))
    }
}

impl<T: Driver> From<Instant> for Step<Stage<T>> {
    fn from(src: Instant) -> Step<Stage<T>> {
        Step::Wait(Some(src))
    }
}

impl<T: Driver> From<Follower<T>> for Option<Stage<T>> {
    fn from(src: Follower<T>) -> Option<Stage<T>> {
        Some(Stage::Follower(src))
    }
}

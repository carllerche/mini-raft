use std::{cmp, ops};

// A raft term
#[derive(Debug, Default, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, serde::Serialize)]
pub struct Term(pub u64);

impl ops::Add<u64> for Term {
    type Output = Term;

    fn add(self, rhs: u64) -> Self::Output {
        Term(self.0 + rhs)
    }
}

impl ops::AddAssign<u64> for Term {
    fn add_assign(&mut self, rhs: u64) {
        self.0 += rhs;
    }
}

impl cmp::PartialEq<u64> for Term {
    fn eq(&self, other: &u64) -> bool {
        self.0 == *other
    }
}

impl cmp::PartialOrd<u64> for Term {
    fn partial_cmp(&self, other: &u64) -> Option<cmp::Ordering> {
        self.0.partial_cmp(other)
    }
}

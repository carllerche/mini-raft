use crate::*;

use std::fmt;

/// Position of an entry in the log
#[derive(Default, Copy, Clone, Eq, PartialEq, PartialOrd, Ord, Hash, serde::Serialize)]
pub struct Pos {
    /// Term the sequence identifier is part of
    pub term: Term,

    /// Absolute log index.
    pub index: Index,
}

impl fmt::Debug for Pos {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(fmt, "Pos({}:{})", self.index.0, self.term.0)
    }
}

#[test]
fn test_ord() {
    let a = Pos {
        term: Term(0),
        index: Index(1),
    };

    let b = Pos {
        term: Term(1),
        index: Index(0),
    };

    assert!(a < b);
    assert!(b > a);
}

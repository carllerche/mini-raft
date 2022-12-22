use std::{cmp, ops};

// Index of a log entry
#[derive(Debug, Default, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, serde::Serialize)]
pub struct Index(pub u64);

impl Index {
    pub(crate) fn checked_sub(self, rhs: u64) -> Option<Index> {
        self.0.checked_sub(rhs).map(Index)
    }
}

impl cmp::PartialEq<i32> for Index {
    fn eq(&self, other: &i32) -> bool {
        (self.0 as i32).eq(other)
    }
}

impl cmp::PartialEq<usize> for Index {
    fn eq(&self, other: &usize) -> bool {
        self.0.eq(&(*other as _))
    }
}

impl cmp::PartialOrd<usize> for Index {
    fn partial_cmp(&self, rhs: &usize) -> Option<cmp::Ordering> {
        self.0.partial_cmp(&(*rhs as _))
    }
}

impl ops::Add<u64> for Index {
    type Output = Index;

    fn add(self, rhs: u64) -> Index {
        Index(self.0 + rhs)
    }
}

impl ops::Add<usize> for Index {
    type Output = Index;

    fn add(self, rhs: usize) -> Index {
        let rhs = u64::try_from(rhs).unwrap();
        Index(self.0.checked_add(rhs).unwrap())
    }
}

impl ops::Add<i32> for Index {
    type Output = Index;

    fn add(self, rhs: i32) -> Index {
        let rhs = u64::try_from(rhs).unwrap();
        Index(self.0.checked_add(rhs).unwrap())
    }
}

impl ops::Sub<u64> for Index {
    type Output = Index;

    fn sub(self, rhs: u64) -> Index {
        Index(self.0 - rhs)
    }
}

impl ops::Sub<i32> for Index {
    type Output = Index;

    fn sub(self, rhs: i32) -> Index {
        Index(self.0.checked_sub(rhs as _).unwrap())
    }
}

impl ops::Sub<usize> for Index {
    type Output = Index;

    fn sub(self, rhs: usize) -> Index {
        Index(self.0.checked_sub(rhs as _).unwrap())
    }
}

impl ops::AddAssign<u64> for Index {
    fn add_assign(&mut self, rhs: u64) {
        self.0 += rhs;
    }
}

impl ops::AddAssign<i32> for Index {
    fn add_assign(&mut self, rhs: i32) {
        let rhs = u64::try_from(rhs).unwrap();
        self.0 = self.0.checked_add(rhs).unwrap();
    }
}

impl ops::AddAssign<usize> for Index {
    fn add_assign(&mut self, rhs: usize) {
        let rhs = u64::try_from(rhs).unwrap();
        self.0 = self.0.checked_add(rhs).unwrap();
    }
}

impl ops::SubAssign<u64> for Index {
    fn sub_assign(&mut self, rhs: u64) {
        self.0 -= rhs;
    }
}

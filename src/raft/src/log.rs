use crate::*;

use futures_lite::future::poll_fn;
use std::io;
use std::marker::PhantomData;

pub(crate) struct Log<T: Driver> {
    /// Index and term of highest log entry known to be committed.
    pub(crate) last_committed: Option<Pos>,

    /// Index and term of last appended entry.
    pub(crate) last_appended: Option<Pos>,

    _p: PhantomData<T>,
}

impl<T: Driver> Log<T> {
    pub(crate) fn new() -> Log<T> {
        Log {
            last_committed: None,
            last_appended: None,
            _p: PhantomData,
        }
    }

    /// Return the entry stored at the specified log position
    pub(crate) async fn get(
        &self,
        driver: &mut T,
        index: Index,
    ) -> io::Result<Option<message::Entry<T::Id>>> {
        poll_fn(|cx| driver.poll_read_entry(cx, index)).await
    }

    pub(crate) fn last_appended(&self) -> Option<Pos> {
        self.last_appended
    }

    /// Returns the index of the last appended entry
    pub(crate) fn last_appended_index(&self) -> Option<Index> {
        self.last_appended().map(|pos| pos.index)
    }

    pub(crate) fn last_committed(&self) -> Option<Pos> {
        self.last_committed
    }

    pub(crate) fn last_committed_index(&self) -> Option<Index> {
        self.last_committed.map(|pos| pos.index)
    }

    /// Returns true if the log contains an entry matching the given pos.
    pub(crate) async fn contains_pos(&self, driver: &mut T, pos: Pos) -> io::Result<bool> {
        Ok(self.pos_for(driver, pos.index).await? == Some(pos))
    }

    /// Returns the `Pos` (index, term) for the entry at the given index.
    pub(crate) async fn pos_for(&self, driver: &mut T, index: Index) -> io::Result<Option<Pos>> {
        let maybe_term = self.term_for(driver, index).await?;
        Ok(maybe_term.map(|term| Pos { term, index }))
    }

    /// Returns the term for the entry at the given index
    pub(crate) async fn term_for(&self, driver: &mut T, index: Index) -> io::Result<Option<Term>> {
        poll_fn(|cx| driver.poll_term_for(cx, index)).await
    }

    /// Append the given entries to the log
    pub(crate) fn append_entries(&mut self, driver: &mut T, entries: &[message::Entry<T::Id>]) {
        assert!(!entries.is_empty());

        // Verify entries are sequenced correctly
        for entry in entries {
            // Make sure terms are monotonically increasing
            assert!(entry.pos.term >= self.last_appended.map(|pos| pos.term).unwrap_or_default());

            // Make sure the indexes are sequential
            assert_eq!(
                entry.pos.index,
                self.last_appended
                    .map(|pos| pos.index + 1)
                    .unwrap_or_default()
            );

            self.last_appended = Some(entry.pos);
        }

        // Update last_appended
        driver.append_entries(entries);
    }

    /// Truncate the log at the given index. All **earlier** indices are kept.
    /// All after are removed.
    pub(crate) async fn truncate(&mut self, driver: &mut T, index: Index) -> io::Result<()> {
        debug_assert!(
            Some(index) > self.last_committed_index(),
            "truncated committed entry"
        );
        driver.truncate(index);

        if index == 0 {
            self.last_appended = None;
        } else {
            self.last_appended = self.pos_for(driver, index - 1).await?;
        }

        Ok(())
    }

    /// Attempts to commit the given index, returning `true` if successful.
    ///
    /// This is called once the index replication has reached quorum.
    pub(crate) async fn maybe_commit(
        &mut self,
        driver: &mut T,
        index: Index,
        current_term: Term,
    ) -> io::Result<Option<Pos>> {
        if let Some(last_committed) = self.last_committed {
            if index <= last_committed.index {
                return Ok(None);
            }
        }

        match self.pos_for(driver, index).await? {
            Some(pos) if pos.term == current_term => {
                self.commit(pos);
                Ok(Some(pos))
            }
            _ => Ok(None),
        }
    }

    pub(crate) fn commit(&mut self, pos: Pos) {
        // The committed index is monotonically increasing and can never go
        // down.
        if self.last_committed_index() >= Some(pos.index) {
            return;
        }

        // Only commit an index if we have received the entries.
        if Some(pos.index) > self.last_appended_index() {
            return;
        }

        self.last_committed = Some(pos);
    }

    pub(crate) async fn copy_range_to(
        &self,
        driver: &mut T,
        start: Index,
        end: Index,
        dst: &mut Vec<message::Entry<T::Id>>,
    ) -> io::Result<()> {
        debug_assert!(start <= end, "start={:?}; end={:?}", start, end);
        debug_assert!(
            Some(end) <= self.last_appended_index(),
            "copy_range_to out of range; end={:?}; last_appended={:?}",
            end,
            self.last_appended_index()
        );

        poll_fn(|cx| driver.poll_read_entries(cx, start, end, dst)).await
    }

    pub(crate) async fn find_conflict(
        &self,
        driver: &mut T,
        mut index: Index,
        term: Term,
    ) -> io::Result<Option<Pos>> {
        let last_appended = self.last_appended_index();

        assert!(Some(index) <= last_appended);

        loop {
            match self.pos_for(driver, index).await? {
                Some(pos) => {
                    if pos.term > term {
                        index -= 1;
                    } else {
                        return Ok(Some(pos));
                    }
                }
                None => {
                    return Ok(None);
                }
            }
        }
    }
}

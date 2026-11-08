//! Observed-record commit prefixes, independent of numeric gaps in Kafka offsets.

use std::collections::BTreeMap;

use uuid::Uuid;

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub(crate) enum Error {
    #[error("partition pending-record capacity exhausted")]
    Capacity,
    #[error("partition offset is invalid or not strictly increasing")]
    Offset,
    #[error("partition completion belongs to another assignment")]
    Stale,
    #[error("partition completion was not observed")]
    Unknown,
    #[error("partition commit does not match the outstanding commit")]
    Commit,
}

/// Private capabilities prevent completion crossing assignment or partition owners.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct Token {
    owner: Uuid,
    offset: i64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct Commit {
    owner: Uuid,
    attempt: Uuid,
    next: i64,
}

impl Commit {
    pub(crate) fn next_offset(self) -> i64 {
        self.next
    }
}

struct Record<T> {
    completed: bool,
    retained: T,
}

/// `T` retains admission until safe local store, commit confirmation or assignment loss.
/// It must not be business-success state: durable consumers own that separately.
pub(crate) struct Ledger<T> {
    owner: Uuid,
    capacity: usize,
    observed: Option<i64>,
    confirmed: Option<i64>,
    stored: Option<i64>,
    records: BTreeMap<i64, Record<T>>,
    committing: Option<Commit>,
}

impl<T> Ledger<T> {
    pub(crate) fn new(capacity: usize) -> Result<Self, Error> {
        if !(1..=4096).contains(&capacity) {
            return Err(Error::Capacity);
        }
        Ok(Self {
            owner: Uuid::new_v4(),
            capacity,
            observed: None,
            confirmed: None,
            stored: None,
            records: BTreeMap::new(),
            committing: None,
        })
    }

    pub(crate) fn len(&self) -> usize {
        self.records.len()
    }

    pub(crate) fn confirmed_offset(&self) -> Option<i64> {
        self.confirmed
    }

    pub(crate) fn stored_offset(&self) -> Option<i64> {
        self.stored
    }

    pub(crate) fn observe(&mut self, offset: i64, retained: T) -> Result<Token, Error> {
        if offset < 0 || offset == i64::MAX || self.observed.is_some_and(|last| offset <= last) {
            return Err(Error::Offset);
        }
        if self.records.len() >= self.capacity {
            return Err(Error::Capacity);
        }
        self.observed = Some(offset);
        self.records.insert(
            offset,
            Record {
                completed: false,
                retained,
            },
        );
        Ok(Token {
            owner: self.owner,
            offset,
        })
    }

    /// Whether this is a new custody completion, rather than an idempotent repeat.
    pub(crate) fn complete(&mut self, token: Token) -> Result<bool, Error> {
        if token.owner != self.owner {
            return Err(Error::Stale);
        }
        if self
            .confirmed
            .into_iter()
            .chain(self.stored)
            .any(|offset| token.offset < offset)
        {
            return Ok(false);
        }
        let record = self.records.get_mut(&token.offset).ok_or(Error::Unknown)?;
        let new = !record.completed;
        record.completed = true;
        Ok(new)
    }

    pub(crate) fn ready_offset(&self) -> Option<i64> {
        // Compacted records and read_committed filtering leave numeric gaps.
        // Only the *observed sequence* is contiguous; counting offset+1 steps
        // would either stall forever or skip an unfinished earlier delivery.
        let mut completed = None;
        for (offset, record) in &self.records {
            if !record.completed {
                // The next *observed* pending offset is safe; numeric holes
                // before it were already skipped by this ordered broker read.
                return completed.map(|_| *offset);
            }
            completed = Some(*offset);
        }
        completed.map(|offset| offset + 1)
    }

    pub(crate) fn begin_commit(&mut self) -> Option<Commit> {
        let next = self.ready_offset().into_iter().chain(self.stored).max()?;
        if self.confirmed.is_some_and(|confirmed| next <= confirmed) {
            return None;
        }
        self.begin(next)
    }

    pub(crate) fn begin_store(&mut self) -> Option<Commit> {
        self.begin(self.ready_offset()?)
    }

    fn begin(&mut self, next: i64) -> Option<Commit> {
        if self.committing.is_some() {
            return None;
        }
        let commit = Commit {
            owner: self.owner,
            attempt: Uuid::new_v4(),
            next,
        };
        self.committing = Some(commit);
        Some(commit)
    }

    pub(crate) fn finish_commit(
        &mut self,
        commit: Commit,
        confirmed: bool,
    ) -> Result<Vec<T>, Error> {
        let retained = self.finish(commit, confirmed)?;
        if confirmed {
            self.confirmed = Some(commit.next);
        }
        Ok(retained)
    }

    pub(crate) fn finish_store(&mut self, commit: Commit, stored: bool) -> Result<Vec<T>, Error> {
        let retained = self.finish(commit, stored)?;
        if stored {
            self.stored = Some(commit.next);
        }
        Ok(retained)
    }

    fn finish(&mut self, commit: Commit, succeeded: bool) -> Result<Vec<T>, Error> {
        if commit.owner != self.owner {
            return Err(Error::Stale);
        }
        if self.committing != Some(commit) {
            return Err(Error::Commit);
        }
        self.committing = None;
        if !succeeded {
            // Uncertainty never releases admission or forgets completed members.
            return Ok(Vec::new());
        }
        let remaining = self.records.split_off(&commit.next);
        let completed = std::mem::replace(&mut self.records, remaining);
        Ok(completed
            .into_values()
            .map(|record| record.retained)
            .collect())
    }

    pub(crate) fn fence(&mut self) -> Vec<T> {
        // A delayed acknowledgement may not settle a new owner's same offset.
        self.owner = Uuid::new_v4();
        self.observed = None;
        self.confirmed = None;
        self.stored = None;
        self.committing = None;
        std::mem::take(&mut self.records)
            .into_values()
            .map(|record| record.retained)
            .collect()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::{Error, Ledger};

    #[test]
    fn safe_store_releases_admission_without_claiming_broker_confirmation() {
        let mut ledger = Ledger::new(2).unwrap();
        let first = ledger.observe(0, "first").unwrap();
        let last = ledger.observe(3, "last").unwrap();
        assert!(ledger.complete(first).unwrap());
        assert!(!ledger.complete(first).unwrap());
        let store = ledger.begin_store().unwrap();
        assert_eq!(store.next_offset(), 3);
        assert!(ledger.finish_store(store, false).unwrap().is_empty());
        assert_eq!(ledger.len(), 2);
        let store = ledger.begin_store().unwrap();
        assert_eq!(ledger.finish_store(store, true).unwrap(), ["first"]);
        assert_eq!(ledger.confirmed_offset(), None);
        assert_eq!(ledger.stored_offset(), Some(3));
        assert_eq!(ledger.len(), 1);
        ledger.complete(first).unwrap();
        assert!(ledger.begin_store().is_none());
        let commit = ledger.begin_commit().unwrap();
        assert_eq!(commit.next_offset(), 3);
        ledger.finish_commit(commit, true).unwrap();
        assert_eq!(ledger.confirmed_offset(), Some(3));
        assert!(ledger.begin_commit().is_none());
        ledger.complete(last).unwrap();
        let store = ledger.begin_store().unwrap();
        assert_eq!(ledger.finish_store(store, true).unwrap(), ["last"]);
        let commit = ledger.begin_commit().unwrap();
        assert_eq!(commit.next_offset(), 4);
        ledger.finish_commit(commit, true).unwrap();
        assert_eq!(ledger.confirmed_offset(), Some(4));
    }

    #[test]
    fn out_of_order_completions_and_numeric_gaps_do_not_skip_earlier_work() {
        let mut ledger = Ledger::new(8).unwrap();
        let first = ledger.observe(4, "first").unwrap();
        let second = ledger.observe(10, "second").unwrap();
        let third = ledger.observe(19, "third").unwrap();
        ledger.complete(third).unwrap();
        ledger.complete(second).unwrap();
        assert_eq!(ledger.ready_offset(), None);
        assert!(ledger.begin_commit().is_none());
        ledger.complete(first).unwrap();
        let commit = ledger.begin_commit().unwrap();
        assert_eq!(commit.next_offset(), 20);
        assert_eq!(
            ledger.finish_commit(commit, true).unwrap(),
            ["first", "second", "third"]
        );
        assert_eq!(ledger.len(), 0);
    }

    #[test]
    fn successes_survive_failed_commits_and_only_one_commit_can_be_outstanding() {
        let mut ledger = Ledger::new(2).unwrap();
        let first = ledger.observe(0, 1).unwrap();
        let second = ledger.observe(7, 2).unwrap();
        ledger.complete(first).unwrap();
        let older = ledger.begin_commit().unwrap();
        ledger.complete(second).unwrap();
        assert!(ledger.begin_commit().is_none());
        assert!(ledger.finish_commit(older, false).unwrap().is_empty());
        assert_eq!(ledger.len(), 2);
        assert!(matches!(ledger.observe(8, 3), Err(Error::Capacity)));
        let newer = ledger.begin_commit().unwrap();
        assert_eq!(newer.next_offset(), 8);
        assert_eq!(ledger.finish_commit(newer, true).unwrap(), [1, 2]);
        assert!(matches!(
            ledger.finish_commit(older, true),
            Err(Error::Commit)
        ));
    }

    #[test]
    fn revoked_and_cross_partition_handles_cannot_complete_new_assignment() {
        let mut first = Ledger::new(4).unwrap();
        let mut other = Ledger::new(4).unwrap();
        let stale = first.observe(3, ()).unwrap();
        let foreign = other.observe(3, ()).unwrap();
        assert!(matches!(first.complete(foreign), Err(Error::Stale)));
        first.complete(stale).unwrap();
        let old_commit = first.begin_commit().unwrap();
        assert_eq!(first.fence().len(), 1);
        let fresh = first.observe(3, ()).unwrap();
        assert!(matches!(first.complete(stale), Err(Error::Stale)));
        assert!(matches!(
            first.finish_commit(old_commit, true),
            Err(Error::Stale)
        ));
        assert_eq!(first.ready_offset(), None);
        first.complete(fresh).unwrap();
        assert_eq!(first.ready_offset(), Some(4));
    }

    #[test]
    fn late_confirmation_for_same_offset_does_not_settle_a_successor_attempt() {
        let mut ledger = Ledger::new(1).unwrap();
        let record = ledger.observe(4, ()).unwrap();
        ledger.complete(record).unwrap();
        let stale = ledger.begin_commit().unwrap();
        ledger.finish_commit(stale, false).unwrap();
        let current = ledger.begin_commit().unwrap();
        assert_eq!(stale.next_offset(), current.next_offset());
        assert!(matches!(
            ledger.finish_commit(stale, true),
            Err(Error::Commit)
        ));
        assert!(ledger.begin_commit().is_none());
        assert_eq!(ledger.len(), 1);
        assert_eq!(ledger.finish_commit(current, true).unwrap().len(), 1);
    }

    #[test]
    fn prefix_commit_retains_later_unfinished_records_and_rejects_bad_offsets() {
        let mut ledger = Ledger::new(3).unwrap();
        assert!(matches!(ledger.observe(-1, ()), Err(Error::Offset)));
        assert!(matches!(ledger.observe(i64::MAX, ()), Err(Error::Offset)));
        let first = ledger.observe(2, ()).unwrap();
        assert!(matches!(ledger.observe(2, ()), Err(Error::Offset)));
        let last = ledger.observe(8, ()).unwrap();
        ledger.complete(first).unwrap();
        let commit = ledger.begin_commit().unwrap();
        assert_eq!(commit.next_offset(), 8);
        assert_eq!(ledger.finish_commit(commit, true).unwrap().len(), 1);
        assert_eq!(ledger.len(), 1);
        ledger.complete(first).unwrap();
        assert!(ledger.begin_commit().is_none());
        ledger.complete(last).unwrap();
        assert_eq!(ledger.ready_offset(), Some(9));
    }
}

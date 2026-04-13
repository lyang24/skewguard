//! In-memory storage backend for testing and benchmarking.

use crate::error::Result;
use crate::storage::{self, Timestamp};
use parking_lot::RwLock;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// In-memory MVCC storage. Each key stores a history of (timestamp, value) pairs.
///
/// Timestamp contract:
/// - `snapshot()` returns timestamp T (current clock).
/// - `commit()` increments the clock and writes at T+1.
/// - `get_at(key, T)` returns the latest version with `version_ts < T`.
/// - This ensures snapshots never see concurrent or future commits.
///
/// Supports an optional commit delay to simulate fsync cost.
pub struct MemStorage {
    #[allow(clippy::type_complexity)]
    data: RwLock<BTreeMap<Vec<u8>, Vec<(Timestamp, Option<Vec<u8>>)>>>,
    clock: AtomicU64,
    commit_delay: Option<Duration>,
}

impl MemStorage {
    pub fn new() -> Self {
        MemStorage {
            data: RwLock::new(BTreeMap::new()),
            clock: AtomicU64::new(1),
            commit_delay: None,
        }
    }

    /// Create a MemStorage with simulated commit delay.
    pub fn with_commit_delay(delay: Duration) -> Self {
        MemStorage {
            data: RwLock::new(BTreeMap::new()),
            clock: AtomicU64::new(1),
            commit_delay: Some(delay),
        }
    }

    fn tick(&self) -> Timestamp {
        // Returns the OLD value of the clock, then increments.
        // So if clock was 1: write at ts=1, clock becomes 2.
        // current_timestamp() returns 2, get_at(key, 2) sees writes at ts < 2 = ts=1. Correct.
        self.clock.fetch_add(1, Ordering::SeqCst)
    }
}

impl Default for MemStorage {
    fn default() -> Self {
        Self::new()
    }
}

impl storage::Storage for MemStorage {
    type Snapshot = MemSnapshot;
    type WriteBatch = MemWriteBatch;

    fn snapshot(&self) -> MemSnapshot {
        let ts = self.clock.load(Ordering::SeqCst);
        MemSnapshot { ts }
    }

    fn write_batch(&self) -> MemWriteBatch {
        MemWriteBatch { ops: Vec::new() }
    }

    fn commit(&self, batch: MemWriteBatch) -> Result<Timestamp> {
        if let Some(delay) = self.commit_delay {
            std::thread::sleep(delay);
        }
        let ts = self.tick();
        let mut data = self.data.write();
        for (key, value) in batch.ops {
            data.entry(key).or_default().push((ts, value));
        }
        Ok(ts)
    }

    fn current_timestamp(&self) -> Timestamp {
        self.clock.load(Ordering::SeqCst)
    }

    fn get_at(&self, key: &[u8], ts: Timestamp) -> Result<Option<Vec<u8>>> {
        let data = self.data.read();
        if let Some(history) = data.get(key) {
            // Return latest version strictly before ts.
            for &(version_ts, ref value) in history.iter().rev() {
                if version_ts < ts {
                    return Ok(value.clone());
                }
            }
        }
        Ok(None)
    }

    fn was_modified(&self, key: &[u8], after: Timestamp, at_or_before: Timestamp) -> Result<bool> {
        let data = self.data.read();
        if let Some(history) = data.get(key) {
            for &(ts, _) in history {
                if ts >= after && ts <= at_or_before {
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }
}

/// Snapshot: just a timestamp. Reads go through Storage::get_at().
pub struct MemSnapshot {
    ts: Timestamp,
}

impl storage::Snapshot for MemSnapshot {
    fn timestamp(&self) -> Timestamp {
        self.ts
    }
}

/// Buffered writes for the in-memory storage.
pub struct MemWriteBatch {
    ops: Vec<(Vec<u8>, Option<Vec<u8>>)>,
}

impl storage::WriteBatch for MemWriteBatch {
    fn put(&mut self, key: &[u8], value: &[u8]) {
        self.ops.push((key.to_vec(), Some(value.to_vec())));
    }

    fn delete(&mut self, key: &[u8]) {
        self.ops.push((key.to_vec(), None));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{Snapshot, Storage};

    #[test]
    fn basic_read_write() {
        let store = MemStorage::new();
        let mut batch = store.write_batch();
        storage::WriteBatch::put(&mut batch, b"hello", b"world");
        store.commit(batch).unwrap();

        let ts = store.current_timestamp();
        let val = store.get_at(b"hello", ts).unwrap();
        assert_eq!(val, Some(b"world".to_vec()));
    }

    #[test]
    fn snapshot_does_not_see_concurrent_writes() {
        let store = MemStorage::new();

        // Write v1.
        let mut batch = store.write_batch();
        storage::WriteBatch::put(&mut batch, b"key", b"v1");
        store.commit(batch).unwrap();

        // Take snapshot.
        let snap = store.snapshot();
        let snap_ts = snap.timestamp();

        // Write v2 after snapshot.
        let mut batch = store.write_batch();
        storage::WriteBatch::put(&mut batch, b"key", b"v2");
        store.commit(batch).unwrap();

        // Snapshot sees v1, not v2.
        let val = store.get_at(b"key", snap_ts).unwrap();
        assert_eq!(val, Some(b"v1".to_vec()));

        // Current timestamp sees v2.
        let val = store.get_at(b"key", store.current_timestamp()).unwrap();
        assert_eq!(val, Some(b"v2".to_vec()));
    }

    #[test]
    fn was_modified_detection() {
        let store = MemStorage::new();

        let mut batch = store.write_batch();
        storage::WriteBatch::put(&mut batch, b"other_key", b"v0");
        let ts1 = store.commit(batch).unwrap();

        let mut batch = store.write_batch();
        storage::WriteBatch::put(&mut batch, b"key", b"v1");
        let ts2 = store.commit(batch).unwrap();

        assert!(store.was_modified(b"key", ts1, ts2).unwrap());
        assert!(!store.was_modified(b"other", ts1, ts2).unwrap());
    }
}

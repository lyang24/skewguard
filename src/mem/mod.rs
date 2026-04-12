//! In-memory storage backend for testing and benchmarking.

use crate::error::Result;
use crate::storage::{self, Timestamp};
use parking_lot::RwLock;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// In-memory MVCC storage. Each key stores a history of (timestamp, value) pairs.
///
/// Supports an optional commit delay to simulate fsync cost. Without the delay,
/// aborts are essentially free (retry is instant), hiding the difference between
/// pessimistic locking and group locking. With even a small delay (10-50μs),
/// wasted work from aborted transactions becomes measurable.
pub struct MemStorage {
    /// key -> [(timestamp, Option<value>)], sorted by timestamp.
    /// None value means a delete at that timestamp.
    data: RwLock<BTreeMap<Vec<u8>, Vec<(Timestamp, Option<Vec<u8>>)>>>,
    clock: AtomicU64,
    /// Simulated commit latency (e.g., fsync). Applied on every `commit()` call.
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
    /// Even 10μs makes aborted transactions expensive enough to show the
    /// difference between CC strategies.
    pub fn with_commit_delay(delay: Duration) -> Self {
        MemStorage {
            data: RwLock::new(BTreeMap::new()),
            clock: AtomicU64::new(1),
            commit_delay: Some(delay),
        }
    }

    fn tick(&self) -> Timestamp {
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
        let data = self.data.read().clone();
        MemSnapshot { data, ts }
    }

    fn write_batch(&self) -> MemWriteBatch {
        MemWriteBatch { ops: Vec::new() }
    }

    fn commit(&self, batch: MemWriteBatch) -> Result<Timestamp> {
        // Simulate fsync/write latency.
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
            for &(version_ts, ref value) in history.iter().rev() {
                if version_ts <= ts {
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

/// A snapshot of the in-memory storage at a point in time.
pub struct MemSnapshot {
    data: BTreeMap<Vec<u8>, Vec<(Timestamp, Option<Vec<u8>>)>>,
    ts: Timestamp,
}

impl storage::Snapshot for MemSnapshot {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if let Some(history) = self.data.get(key) {
            for &(version_ts, ref value) in history.iter().rev() {
                if version_ts <= self.ts {
                    return Ok(value.clone());
                }
            }
        }
        Ok(None)
    }

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
    use crate::storage::Storage;

    #[test]
    fn basic_read_write() {
        let store = MemStorage::new();
        let mut batch = store.write_batch();
        storage::WriteBatch::put(&mut batch, b"hello", b"world");
        store.commit(batch).unwrap();

        let snap = store.snapshot();
        let val = storage::Snapshot::get(&snap, b"hello").unwrap();
        assert_eq!(val, Some(b"world".to_vec()));
    }

    #[test]
    fn snapshot_isolation() {
        let store = MemStorage::new();

        // Write v1
        let mut batch = store.write_batch();
        storage::WriteBatch::put(&mut batch, b"key", b"v1");
        store.commit(batch).unwrap();

        // Take snapshot
        let snap = store.snapshot();

        // Write v2 after snapshot
        let mut batch = store.write_batch();
        storage::WriteBatch::put(&mut batch, b"key", b"v2");
        store.commit(batch).unwrap();

        // Snapshot still sees v1
        let val = storage::Snapshot::get(&snap, b"key").unwrap();
        assert_eq!(val, Some(b"v1".to_vec()));

        // Fresh snapshot sees v2
        let snap2 = store.snapshot();
        let val2 = storage::Snapshot::get(&snap2, b"key").unwrap();
        assert_eq!(val2, Some(b"v2".to_vec()));
    }

    #[test]
    fn was_modified_detection() {
        let store = MemStorage::new();

        // Write v0 to establish a baseline timestamp.
        let mut batch = store.write_batch();
        storage::WriteBatch::put(&mut batch, b"other_key", b"v0");
        let ts1 = store.commit(batch).unwrap();

        // Write key after ts1.
        let mut batch = store.write_batch();
        storage::WriteBatch::put(&mut batch, b"key", b"v1");
        let ts2 = store.commit(batch).unwrap();

        assert!(store.was_modified(b"key", ts1, ts2).unwrap());
        assert!(!store.was_modified(b"other", ts1, ts2).unwrap());
    }
}

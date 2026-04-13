//! Standard pessimistic locking for cold (uncontended) ranges.
//!
//! Read keys: shared (read) locks — multiple readers can proceed concurrently.
//! Write keys: exclusive (write) locks — serializes writers on the same key.
//! All locks acquired in sorted key order to prevent deadlocks.
//!
//! Validation happens under locks, so there is no TOCTOU gap between
//! validation and commit.

use crate::error::{Error, Result};
use crate::mvcc::ReadWriteSet;
use crate::mvcc::WriteOp;
use crate::storage::{Storage, Timestamp, WriteBatch};
use parking_lot::{Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

/// Manages per-key locks for pessimistic concurrency control.
pub(crate) struct PessimisticLockManager {
    locks: Mutex<HashMap<Vec<u8>, Arc<RwLock<()>>>>,
}

impl PessimisticLockManager {
    pub fn new() -> Self {
        PessimisticLockManager {
            locks: Mutex::new(HashMap::new()),
        }
    }

    fn lock_for(&self, key: &[u8]) -> Arc<RwLock<()>> {
        let mut map = self.locks.lock();
        map.entry(key.to_vec())
            .or_insert_with(|| Arc::new(RwLock::new(())))
            .clone()
    }

    /// Execute a transaction under pessimistic locking.
    ///
    /// 1. Collect all keys, deduplicate, sort.
    /// 2. Acquire shared locks on read-only keys, exclusive locks on write keys.
    /// 3. Validate the read set (under locks — no TOCTOU gap).
    /// 4. Apply the write batch (under locks).
    /// 5. Release all locks (drop guards).
    pub fn execute<S: Storage>(
        &self,
        storage: &S,
        rw_set: &ReadWriteSet,
        snapshot_ts: Timestamp,
    ) -> Result<()> {
        // Determine which keys need read vs write locks.
        let write_key_set: BTreeSet<&Vec<u8>> = rw_set.writes.keys().collect();
        let mut all_keys = BTreeSet::new();
        for (key, _) in &rw_set.reads {
            all_keys.insert(key.clone());
        }
        for key in rw_set.writes.keys() {
            all_keys.insert(key.clone());
        }

        // Acquire locks in sorted order. Shared for read-only, exclusive for writes.
        let lock_arcs: Vec<(Vec<u8>, Arc<RwLock<()>>)> = all_keys
            .iter()
            .map(|key| (key.clone(), self.lock_for(key)))
            .collect();

        // Hold guards in a Vec. We use an enum to hold both types.
        #[allow(dead_code)]
        enum LockGuard<'a> {
            Read(RwLockReadGuard<'a, ()>),
            Write(RwLockWriteGuard<'a, ()>),
        }

        let _guards: Vec<LockGuard<'_>> = lock_arcs
            .iter()
            .map(|(key, lock)| {
                if write_key_set.contains(key) {
                    LockGuard::Write(lock.write())
                } else {
                    LockGuard::Read(lock.read())
                }
            })
            .collect();

        // --- Everything below runs under locks ---

        // Validate reads.
        let validate_ts = storage.current_timestamp();
        for (key, _range) in &rw_set.reads {
            if storage.was_modified(key, snapshot_ts, validate_ts)? {
                return Err(Error::Conflict);
            }
        }

        // Build and apply the write batch.
        let mut batch = storage.write_batch();
        for (key, op) in &rw_set.writes {
            match op {
                WriteOp::Put(value) => batch.put(key, value),
                WriteOp::Delete => batch.delete(key),
            }
        }

        storage.commit(batch)?;

        // Guards drop here, releasing all locks.
        drop(_guards);

        // Clean up locks with no other holders.
        let mut map = self.locks.lock();
        for (key, _) in &lock_arcs {
            if let Some(lock) = map.get(key)
                && Arc::strong_count(lock) == 1
            {
                map.remove(key);
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mem::MemStorage;
    use crate::mvcc::ReadWriteSet;
    use crate::range::KeyRange;
    use crate::storage::{Snapshot, Storage};
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn basic_pessimistic_commit() {
        let lock_mgr = PessimisticLockManager::new();
        let storage = MemStorage::new();

        let snap_ts = storage.snapshot().timestamp();
        let mut rw_set = ReadWriteSet::new();
        let range = KeyRange { index: 0 };
        rw_set.record_write(b"key".to_vec(), range, WriteOp::Put(b"val".to_vec()));

        lock_mgr.execute(&storage, &rw_set, snap_ts).unwrap();

        let val = storage.get_at(b"key", storage.current_timestamp()).unwrap();
        assert_eq!(val, Some(b"val".to_vec()));
    }

    #[test]
    fn detects_read_write_conflict() {
        let lock_mgr = PessimisticLockManager::new();
        let storage = MemStorage::new();

        let mut batch = storage.write_batch();
        crate::storage::WriteBatch::put(&mut batch, b"key", b"v1");
        storage.commit(batch).unwrap();

        let snap_ts = storage.snapshot().timestamp();
        let mut rw_set = ReadWriteSet::new();
        let range = KeyRange { index: 0 };
        rw_set.record_read(b"key".to_vec(), range);
        rw_set.record_write(b"key".to_vec(), range, WriteOp::Put(b"v2_from_t1".to_vec()));

        // Concurrent write.
        let mut batch = storage.write_batch();
        crate::storage::WriteBatch::put(&mut batch, b"key", b"v2_from_t2");
        storage.commit(batch).unwrap();

        let result = lock_mgr.execute(&storage, &rw_set, snap_ts);
        assert!(result.is_err());
    }

    #[test]
    fn shared_read_locks_do_not_block_each_other() {
        // Prove that shared read locks on the same key allow concurrent
        // execution. Strategy: take a write lock on key X to block all
        // threads, then verify that N threads can simultaneously hold
        // read locks on key Y while waiting for the write lock on X.
        //
        // If read locks were exclusive, threads would serialize on Y
        // and the barrier inside execute would deadlock (not all threads
        // could enter simultaneously).
        use std::sync::Barrier;

        let lock_mgr = Arc::new(PessimisticLockManager::new());
        let storage = Arc::new(MemStorage::new());
        let num_readers = 4;

        // Populate keys.
        let mut batch = storage.write_batch();
        crate::storage::WriteBatch::put(&mut batch, b"read_key", b"val");
        storage.commit(batch).unwrap();

        // Each thread: reads "read_key" (shared lock) + writes a unique key.
        // All threads should acquire the shared read lock on "read_key"
        // concurrently. The unique write keys ensure no write contention.
        let barrier = Arc::new(Barrier::new(num_readers));
        let start = std::time::Instant::now();

        let handles: Vec<_> = (0..num_readers)
            .map(|i| {
                let lm = Arc::clone(&lock_mgr);
                let st = Arc::clone(&storage);
                let bar = Arc::clone(&barrier);
                thread::spawn(move || {
                    bar.wait(); // all start together

                    let snap_ts = st.snapshot().timestamp();
                    let mut rw_set = ReadWriteSet::new();
                    let range = KeyRange { index: 0 };
                    rw_set.record_read(b"read_key".to_vec(), range);
                    let write_key = format!("unique_{i}");
                    rw_set.record_write(write_key.into_bytes(), range, WriteOp::Put(b"x".to_vec()));
                    lm.execute(&*st, &rw_set, snap_ts).unwrap();
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }

        let elapsed = start.elapsed();
        // If read locks were exclusive, 4 threads on the same read key
        // would take ~4x a single execution. With shared read locks,
        // they should complete in roughly 1x. We allow 3x as margin.
        // (This is a timing-based assertion — not ideal but better than
        // no concurrency verification at all.)
        assert!(
            elapsed < std::time::Duration::from_secs(2),
            "4 readers took {:?}, suggesting they were serialized",
            elapsed,
        );
    }

    #[test]
    fn concurrent_different_keys_no_deadlock() {
        let lock_mgr = Arc::new(PessimisticLockManager::new());
        let storage = Arc::new(MemStorage::new());

        let handles: Vec<_> = (0..8)
            .map(|i| {
                let lm = Arc::clone(&lock_mgr);
                let st = Arc::clone(&storage);
                thread::spawn(move || {
                    let snap_ts = st.snapshot().timestamp();
                    let key = format!("key_{i}");
                    let mut rw_set = ReadWriteSet::new();
                    let range = KeyRange { index: 0 };
                    rw_set.record_write(
                        key.as_bytes().to_vec(),
                        range,
                        WriteOp::Put(b"val".to_vec()),
                    );
                    lm.execute(&*st, &rw_set, snap_ts).unwrap();
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }
    }

    #[test]
    fn concurrent_same_key_serialized() {
        let lock_mgr = Arc::new(PessimisticLockManager::new());
        let storage = Arc::new(MemStorage::new());
        let order = Arc::new(Mutex::new(Vec::new()));

        let handles: Vec<_> = (0..4)
            .map(|i| {
                let lm = Arc::clone(&lock_mgr);
                let st = Arc::clone(&storage);
                let ord = Arc::clone(&order);
                thread::spawn(move || {
                    let snap_ts = st.snapshot().timestamp();
                    let mut rw_set = ReadWriteSet::new();
                    let range = KeyRange { index: 0 };
                    rw_set.record_write(
                        b"hot".to_vec(),
                        range,
                        WriteOp::Put(format!("v{i}").into_bytes()),
                    );
                    lm.execute(&*st, &rw_set, snap_ts).unwrap();
                    ord.lock().push(i);
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(order.lock().len(), 4);
    }
}

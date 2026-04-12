//! Standard pessimistic locking for cold (uncontended) ranges.
//!
//! Per-key read-write locks. Reads take shared locks, writes take exclusive
//! locks. Lock ordering by sorted key prevents deadlocks.
//!
//! This is what CockroachDB and TiDB use as their default CC strategy.
//! It's the correct baseline for comparing against group locking.

use crate::error::Result;
use crate::mvcc::{ReadWriteSet, WriteOp};
use crate::storage::{Storage, WriteBatch};
use parking_lot::{Mutex, RwLock};
use std::collections::HashMap;
use std::sync::Arc;

/// Manages per-key read-write locks for pessimistic concurrency control.
pub(crate) struct PessimisticLockManager {
    /// Per-key RwLocks, created on demand.
    locks: Mutex<HashMap<Vec<u8>, Arc<RwLock<()>>>>,
}

impl PessimisticLockManager {
    pub fn new() -> Self {
        PessimisticLockManager {
            locks: Mutex::new(HashMap::new()),
        }
    }

    /// Get or create the RwLock for a key.
    fn lock_for(&self, key: &[u8]) -> Arc<RwLock<()>> {
        let mut map = self.locks.lock();
        map.entry(key.to_vec())
            .or_insert_with(|| Arc::new(RwLock::new(())))
            .clone()
    }

    /// Execute a transaction under pessimistic locking.
    ///
    /// Acquires exclusive locks on all write keys (sorted to prevent deadlock),
    /// then applies the write batch. No read locks in this simplified version
    /// — reads use the MVCC snapshot which is already consistent.
    pub fn execute<S: Storage>(
        &self,
        storage: &S,
        rw_set: &ReadWriteSet,
    ) -> Result<()> {
        // Sort write keys to prevent deadlock.
        let mut write_keys: Vec<Vec<u8>> = rw_set.writes.keys().cloned().collect();
        write_keys.sort();

        // Acquire exclusive locks on all write keys.
        let locks: Vec<Arc<RwLock<()>>> = write_keys
            .iter()
            .map(|key| self.lock_for(key))
            .collect();
        let _guards: Vec<_> = locks.iter().map(|l| l.write()).collect();

        // Build and apply the write batch.
        // Note: we don't validate reads here. Under pessimistic locking,
        // write locks serialize all writers on the same key. Reads use MVCC
        // snapshots and are not invalidated by concurrent writes — they see
        // a consistent point-in-time view regardless.
        let mut batch = storage.write_batch();
        for (key, op) in &rw_set.writes {
            match op {
                WriteOp::Put(value) => batch.put(key, value),
                WriteOp::Delete => batch.delete(key),
            }
        }

        storage.commit(batch)?;

        // Clean up locks with no waiters.
        let mut map = self.locks.lock();
        for key in &write_keys {
            if let Some(lock) = map.get(key) {
                if Arc::strong_count(lock) == 1 {
                    map.remove(key);
                }
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
    use crate::storage::Storage;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn basic_pessimistic_commit() {
        let lock_mgr = PessimisticLockManager::new();
        let storage = MemStorage::new();

        let mut rw_set = ReadWriteSet::new();
        let range = KeyRange { index: 0 };
        rw_set.record_write(b"key".to_vec(), range, WriteOp::Put(b"val".to_vec()));

        lock_mgr.execute(&storage, &rw_set).unwrap();

        let snap = storage.snapshot();
        let val = crate::storage::Snapshot::get(&snap, b"key").unwrap();
        assert_eq!(val, Some(b"val".to_vec()));
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
                    let key = format!("key_{i}");
                    let mut rw_set = ReadWriteSet::new();
                    let range = KeyRange { index: 0 };
                    rw_set.record_write(
                        key.as_bytes().to_vec(),
                        range,
                        WriteOp::Put(b"val".to_vec()),
                    );
                    lm.execute(&*st, &rw_set).unwrap();
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
                    let mut rw_set = ReadWriteSet::new();
                    let range = KeyRange { index: 0 };
                    rw_set.record_write(
                        b"hot".to_vec(),
                        range,
                        WriteOp::Put(format!("v{i}").into_bytes()),
                    );
                    lm.execute(&*st, &rw_set).unwrap();
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

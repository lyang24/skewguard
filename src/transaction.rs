use crate::error::{Error, Result};
use crate::group_lock::GroupLockRegistry;
use crate::monitor::{ContentionMonitor, RangeMode};
use crate::mvcc::{ReadWriteSet, WriteOp};
use crate::occ;
use crate::pessimistic::PessimisticLockManager;
use crate::range::KeyRange;
use crate::storage::{Snapshot, Storage, WriteBatch};
use crate::{ColdPathStrategy, TransactionOptions};
use std::sync::Arc;

/// A transaction handle. Reads see a consistent MVCC snapshot.
/// Writes are buffered and applied at commit time.
pub struct Transaction<S: Storage> {
    storage: Arc<S>,
    monitor: Arc<ContentionMonitor>,
    registry: Arc<GroupLockRegistry>,
    lock_mgr: Arc<PessimisticLockManager>,
    snapshot: S::Snapshot,
    rw_set: ReadWriteSet,
    num_ranges: usize,
    cold_path: ColdPathStrategy,
    options: TransactionOptions,
}

impl<S: Storage> Transaction<S> {
    pub(crate) fn new(
        storage: Arc<S>,
        monitor: Arc<ContentionMonitor>,
        registry: Arc<GroupLockRegistry>,
        lock_mgr: Arc<PessimisticLockManager>,
        snapshot: S::Snapshot,
        num_ranges: usize,
        cold_path: ColdPathStrategy,
        options: TransactionOptions,
    ) -> Self {
        Transaction {
            storage,
            monitor,
            registry,
            lock_mgr,
            snapshot,
            rw_set: ReadWriteSet::new(),
            num_ranges,
            cold_path,
            options,
        }
    }

    /// Read a key. Returns None if the key does not exist at the snapshot.
    pub fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let range = KeyRange::for_key(key, self.num_ranges);

        // Check local write buffer first.
        if let Some(op) = self.rw_set.writes.get(key) {
            return match op {
                WriteOp::Put(v) => Ok(Some(v.clone())),
                WriteOp::Delete => Ok(None),
            };
        }

        // Read at the snapshot's timestamp via the storage backend.
        // This works for both MemStorage (which clones data into the snapshot)
        // and RocksDB (which reads from the DB at a logical timestamp).
        let value = self.storage.get_at(key, self.snapshot.timestamp())?;
        self.rw_set.record_read(key.to_vec(), range);
        Ok(value)
    }

    /// Buffer a write. Applied atomically at commit time.
    pub fn put(&mut self, key: &[u8], value: &[u8]) {
        let range = KeyRange::for_key(key, self.num_ranges);
        self.rw_set
            .record_write(key.to_vec(), range, WriteOp::Put(value.to_vec()));
    }

    /// Buffer a delete. Applied atomically at commit time.
    pub fn delete(&mut self, key: &[u8]) {
        let range = KeyRange::for_key(key, self.num_ranges);
        self.rw_set
            .record_write(key.to_vec(), range, WriteOp::Delete);
    }

    /// Commit the transaction.
    ///
    /// Path selection:
    /// 1. `force_hot` option → always group locking (abort-driven promotion)
    /// 2. Any touched range is Hot → group locking
    /// 3. `declared_keys` provided and any declared range is Hot → group locking
    /// 4. Otherwise → cold path (pessimistic or OCC per config)
    pub fn commit(self) -> Result<()> {
        if self.rw_set.is_read_only() {
            return Ok(());
        }

        if self.should_use_hot_path() {
            self.commit_hot()
        } else {
            self.commit_cold()
        }
    }

    /// Determine whether this transaction should use the hot (group locking) path.
    fn should_use_hot_path(&self) -> bool {
        // Abort-driven promotion: caller explicitly requested hot path.
        if self.options.force_hot {
            return true;
        }

        // Check ranges touched by actual operations.
        let any_touched_hot = self
            .rw_set
            .ranges
            .iter()
            .any(|r| self.monitor.mode(*r) == RangeMode::Hot);
        if any_touched_hot {
            return true;
        }

        // Check pre-declared keys (HDCC Rule 3: declared + hot → group locking).
        if let Some(ref keys) = self.options.declared_keys {
            let any_declared_hot = keys
                .iter()
                .any(|k| {
                    let range = KeyRange::for_key(k, self.num_ranges);
                    self.monitor.mode(range) == RangeMode::Hot
                });
            if any_declared_hot {
                return true;
            }
        }

        false
    }

    /// Cold path: pessimistic locking or OCC depending on config.
    fn commit_cold(self) -> Result<()> {
        // Clone what we need for monitoring before consuming self.
        let monitor = Arc::clone(&self.monitor);
        let ranges = self.rw_set.ranges.clone();

        let result = match self.cold_path {
            ColdPathStrategy::Pessimistic => self.commit_pessimistic(),
            ColdPathStrategy::Occ => self.commit_occ(),
        };

        // Record result for contention monitoring.
        match &result {
            Ok(()) => {
                for range in &ranges {
                    monitor.record_commit(*range);
                }
            }
            Err(Error::Conflict) => {
                for range in &ranges {
                    monitor.record_conflict(*range);
                }
            }
            Err(_) => {}
        }

        result
    }

    /// Pessimistic locking commit path.
    fn commit_pessimistic(self) -> Result<()> {
        self.lock_mgr.execute(&*self.storage, &self.rw_set)
    }

    /// OCC commit path.
    fn commit_occ(self) -> Result<()> {
        let snapshot_ts = self.snapshot.timestamp();

        let mut batch = self.storage.write_batch();
        for (key, op) in &self.rw_set.writes {
            match op {
                WriteOp::Put(value) => batch.put(key, value),
                WriteOp::Delete => batch.delete(key),
            }
        }

        let commit_ts = self.storage.current_timestamp();
        occ::validate(&*self.storage, &self.rw_set, snapshot_ts, commit_ts)?;
        self.storage.commit(batch)?;
        Ok(())
    }

    /// Group locking commit path for hot ranges.
    fn commit_hot(self) -> Result<()> {
        let hot_range = self
            .rw_set
            .ranges
            .iter()
            .find(|r| self.monitor.mode(**r) == RangeMode::Hot)
            .copied()
            // If force_hot but no range is actually hot, use the first range.
            .unwrap_or(self.rw_set.ranges[0]);

        let coordinator = self.registry.coordinator(hot_range);
        let result = coordinator.execute(&*self.storage, &self.rw_set);

        match &result {
            Ok(()) => {
                for range in &self.rw_set.ranges {
                    self.monitor.record_commit(*range);
                }
            }
            Err(_) => {
                for range in &self.rw_set.ranges {
                    self.monitor.record_conflict(*range);
                }
            }
        }

        result
    }
}

#[cfg(test)]
mod tests {
    use crate::mem::MemStorage;
    use crate::monitor::MonitorStrategy;
    use crate::{ColdPathStrategy, Config, SkewGuard, TransactionOptions};

    #[test]
    fn basic_commit_pessimistic() {
        let config = Config {
            cold_path: ColdPathStrategy::Pessimistic,
            ..Config::default()
        };
        let sg = SkewGuard::new(MemStorage::new(), config);

        let mut txn = sg.begin();
        txn.put(b"hello", b"world");
        txn.commit().unwrap();

        let mut txn2 = sg.begin();
        let val = txn2.get(b"hello").unwrap();
        assert_eq!(val, Some(b"world".to_vec()));
    }

    #[test]
    fn basic_commit_occ() {
        let config = Config {
            cold_path: ColdPathStrategy::Occ,
            ..Config::default()
        };
        let sg = SkewGuard::new(MemStorage::new(), config);

        let mut txn = sg.begin();
        txn.put(b"key", b"val");
        txn.commit().unwrap();

        let mut txn2 = sg.begin();
        let val = txn2.get(b"key").unwrap();
        assert_eq!(val, Some(b"val".to_vec()));
    }

    #[test]
    fn read_own_writes() {
        let sg = SkewGuard::new(MemStorage::new(), Config::default());

        let mut txn = sg.begin();
        txn.put(b"key", b"val");
        let val = txn.get(b"key").unwrap();
        assert_eq!(val, Some(b"val".to_vec()));
    }

    #[test]
    fn read_only_always_succeeds() {
        let sg = SkewGuard::new(MemStorage::new(), Config::default());

        let mut txn = sg.begin();
        txn.put(b"key", b"val");
        txn.commit().unwrap();

        let mut txn2 = sg.begin();
        let val = txn2.get(b"key").unwrap();
        assert_eq!(val, Some(b"val".to_vec()));
        txn2.commit().unwrap();
    }

    #[test]
    fn force_hot_uses_group_locking() {
        let sg = SkewGuard::new(MemStorage::new(), Config::default());

        // No range is hot, but force_hot bypasses the check.
        let opts = TransactionOptions {
            force_hot: true,
            ..Default::default()
        };
        let mut txn = sg.begin_with_options(opts);
        txn.put(b"key", b"forced_hot");
        txn.commit().unwrap();

        let mut txn2 = sg.begin();
        let val = txn2.get(b"key").unwrap();
        assert_eq!(val, Some(b"forced_hot".to_vec()));
    }

    #[test]
    fn abort_driven_promotion() {
        // Simulate: first attempt on OCC aborts, retry with force_hot succeeds.
        let config = Config {
            cold_path: ColdPathStrategy::Occ,
            ..Config::default()
        };
        let sg = SkewGuard::new(MemStorage::new(), config);

        // Write initial value to establish a baseline timestamp.
        let mut txn_init = sg.begin();
        txn_init.put(b"counter", b"0");
        txn_init.commit().unwrap();

        // txn_a reads counter (takes snapshot at this point).
        let mut txn_a = sg.begin();
        let _ = txn_a.get(b"counter").unwrap();

        // txn_b writes counter AFTER txn_a's snapshot.
        let mut txn_b = sg.begin();
        txn_b.put(b"counter", b"2");
        txn_b.commit().unwrap();

        // txn_a tries to update — OCC validation detects that counter was
        // modified between txn_a's snapshot and now → conflict.
        txn_a.put(b"counter", b"1");
        let result = txn_a.commit();
        assert!(result.is_err(), "OCC should detect conflict");

        // Retry with force_hot → group locking path, no OCC validation.
        let opts = TransactionOptions {
            force_hot: true,
            ..Default::default()
        };
        let mut txn_retry = sg.begin_with_options(opts);
        txn_retry.put(b"counter", b"3");
        txn_retry.commit().unwrap();

        let mut txn_check = sg.begin();
        let val = txn_check.get(b"counter").unwrap();
        assert_eq!(val, Some(b"3".to_vec()));
    }

    #[test]
    fn declared_keys_route_to_hot() {
        let sg = SkewGuard::new(MemStorage::new(), Config::default());
        let range = crate::KeyRange::for_key(b"declared_key", 64);

        // Make the range hot via conflicts.
        sg.monitor().record_conflict(range);
        sg.monitor().record_conflict(range);

        // Transaction with declared_keys should detect hot range upfront.
        let opts = TransactionOptions {
            declared_keys: Some(vec![b"declared_key".to_vec()]),
            ..Default::default()
        };
        let mut txn = sg.begin_with_options(opts);
        txn.put(b"declared_key", b"routed_hot");
        txn.commit().unwrap();

        let mut txn2 = sg.begin();
        let val = txn2.get(b"declared_key").unwrap();
        assert_eq!(val, Some(b"routed_hot".to_vec()));
    }

    #[test]
    fn credit_mode_auto_switches() {
        let config = Config {
            monitor_strategy: MonitorStrategy::Credit {
                initial_credit: 10,
                hotness_threshold: 2,
                aimd_factor: 1,
            },
            ..Config::default()
        };
        let sg = SkewGuard::new(MemStorage::new(), config);
        let range = crate::KeyRange::for_key(b"credit_key", 64);

        // Initially cold.
        assert_eq!(sg.monitor().mode(range), crate::RangeMode::Cold);

        // 2 consecutive conflicts → hot.
        sg.monitor().record_conflict(range);
        sg.monitor().record_conflict(range);
        assert_eq!(sg.monitor().mode(range), crate::RangeMode::Hot);

        // Many commits drain credits → cold.
        for _ in 0..50 {
            sg.monitor().record_commit(range);
        }
        assert_eq!(sg.monitor().mode(range), crate::RangeMode::Cold);
    }

    #[test]
    fn hot_path_commit_with_credit_mode() {
        let config = Config {
            monitor_strategy: MonitorStrategy::Credit {
                initial_credit: 10,
                hotness_threshold: 2,
                aimd_factor: 1,
            },
            num_ranges: 4,
            ..Config::default()
        };
        let sg = SkewGuard::new(MemStorage::new(), config);
        let range = crate::KeyRange::for_key(b"hot_key", 4);

        // Push to hot.
        sg.monitor().record_conflict(range);
        sg.monitor().record_conflict(range);
        assert_eq!(sg.monitor().mode(range), crate::RangeMode::Hot);

        // Commit through hot path.
        let mut txn = sg.begin();
        txn.put(b"hot_key", b"credit_hot");
        txn.commit().unwrap();

        let mut txn2 = sg.begin();
        let val = txn2.get(b"hot_key").unwrap();
        assert_eq!(val, Some(b"credit_hot".to_vec()));
    }
}

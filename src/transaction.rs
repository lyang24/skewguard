use crate::TransactionOptions;
use crate::error::{Error, Result};
use crate::group_lock::GroupLockRegistry;
use crate::monitor::{ContentionMonitor, RangeMode};
use crate::mvcc::{ReadWriteSet, WriteOp};
use crate::pessimistic::PessimisticLockManager;
use crate::range::KeyRange;
use crate::storage::{Snapshot, Storage, Timestamp};
use std::sync::Arc;

/// A transaction handle. Reads see a consistent MVCC snapshot.
/// Writes are buffered and applied at commit time.
pub struct Transaction<S: Storage> {
    storage: Arc<S>,
    monitor: Arc<ContentionMonitor>,
    registry: Arc<GroupLockRegistry>,
    lock_mgr: Arc<PessimisticLockManager>,
    snapshot_ts: Timestamp,
    rw_set: ReadWriteSet,
    num_ranges: usize,
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
        options: TransactionOptions,
    ) -> Self {
        let snapshot_ts = snapshot.timestamp();
        Transaction {
            storage,
            monitor,
            registry,
            lock_mgr,
            snapshot_ts,
            rw_set: ReadWriteSet::new(),
            num_ranges,
            options,
        }
    }

    /// Read a key. Returns None if the key does not exist.
    ///
    /// Reads see a consistent point-in-time view as of the snapshot
    /// timestamp. Writes committed after `begin()` are invisible.
    pub fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let range = KeyRange::for_key(key, self.num_ranges);

        // Check local write buffer first.
        if let Some(op) = self.rw_set.writes.get(key) {
            return match op {
                WriteOp::Put(v) => Ok(Some(v.clone())),
                WriteOp::Delete => Ok(None),
            };
        }

        // Read at the snapshot timestamp. get_at returns the latest version
        // strictly before snapshot_ts, so concurrent commits are invisible.
        let value = self.storage.get_at(key, self.snapshot_ts)?;
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
    /// 1. `force_hot` option -> always group locking (abort-driven promotion)
    /// 2. Any touched range is Hot -> group locking
    /// 3. `declared_keys` provided and any declared range is Hot -> group locking
    /// 4. Otherwise -> cold path (pessimistic locking)
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

    fn should_use_hot_path(&self) -> bool {
        if self.options.force_hot {
            return true;
        }

        let any_touched_hot = self
            .rw_set
            .ranges
            .iter()
            .any(|r| self.monitor.mode(*r) == RangeMode::Hot);
        if any_touched_hot {
            return true;
        }

        if let Some(ref keys) = self.options.declared_keys {
            let any_declared_hot = keys.iter().any(|k| {
                let range = KeyRange::for_key(k, self.num_ranges);
                self.monitor.mode(range) == RangeMode::Hot
            });
            if any_declared_hot {
                return true;
            }
        }

        false
    }

    fn commit_cold(self) -> Result<()> {
        let monitor = Arc::clone(&self.monitor);
        let ranges = self.rw_set.ranges.clone();
        let snapshot_ts = self.snapshot_ts;

        let result = self.commit_pessimistic(snapshot_ts);

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

    fn commit_pessimistic(self, snapshot_ts: Timestamp) -> Result<()> {
        self.lock_mgr
            .execute(&*self.storage, &self.rw_set, snapshot_ts)
    }

    /// Group locking commit path for hot ranges.
    ///
    /// Uses execute() on the coordinator which handles leader/follower
    /// handoff with LeaderGuard safety. For multi-range transactions,
    /// all keys go through a single coordinator (the first hot range's)
    /// to avoid cross-coordinator deadlocks. This serializes multi-range
    /// hot transactions but keeps them correct.
    fn commit_hot(self) -> Result<()> {
        // Pick the coordinator. Use the first hot range, or the first
        // range if force_hot with no range actually hot.
        let coord_range = self
            .rw_set
            .ranges
            .iter()
            .find(|r| self.monitor.mode(**r) == RangeMode::Hot)
            .copied()
            .unwrap_or(self.rw_set.ranges[0]);

        let coordinator = self.registry.coordinator(coord_range);
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
    use crate::{Config, SkewGuard, TransactionOptions};

    #[test]
    fn basic_commit_pessimistic() {
        let config = Config {
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
    fn read_own_writes() {
        let sg = SkewGuard::new(MemStorage::new(), Config::default());

        let mut txn = sg.begin();
        txn.put(b"key", b"val");
        let val = txn.get(b"key").unwrap();
        assert_eq!(val, Some(b"val".to_vec()));
    }

    #[test]
    fn snapshot_isolation_no_dirty_reads() {
        // Bug #1 regression test: t1 should NOT see t2's write.
        let sg = SkewGuard::new(MemStorage::new(), Config::default());

        // Write initial value.
        let mut txn = sg.begin();
        txn.put(b"key", b"v1");
        txn.commit().unwrap();

        // t1 begins, takes snapshot.
        let mut t1 = sg.begin();

        // t2 writes a new value and commits.
        let mut t2 = sg.begin();
        t2.put(b"key", b"v2");
        t2.commit().unwrap();

        // t1 should still see v1, not v2.
        let val = t1.get(b"key").unwrap();
        assert_eq!(val, Some(b"v1".to_vec()));
    }

    #[test]
    fn pessimistic_detects_lost_update() {
        // Bug #2 regression test: pessimistic path must detect read-write conflicts.
        let config = Config {
            ..Config::default()
        };
        let sg = SkewGuard::new(MemStorage::new(), config);

        // Write initial value.
        let mut txn = sg.begin();
        txn.put(b"key", b"0");
        txn.commit().unwrap();

        // t1 reads key=0.
        let mut t1 = sg.begin();
        let _ = t1.get(b"key").unwrap();

        // t2 writes key=2 and commits.
        let mut t2 = sg.begin();
        t2.put(b"key", b"2");
        t2.commit().unwrap();

        // t1 tries to write key=1 based on its stale read.
        t1.put(b"key", b"1");
        let result = t1.commit();

        // Must abort — t1's read of key is stale.
        assert!(
            result.is_err(),
            "pessimistic path should detect lost update"
        );
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
        // Pessimistic path detects stale read → abort.
        // Retry with force_hot → group locking path succeeds.
        let sg = SkewGuard::new(MemStorage::new(), Config::default());

        let mut txn_init = sg.begin();
        txn_init.put(b"counter", b"0");
        txn_init.commit().unwrap();

        // t1 reads counter (takes snapshot).
        let mut t1 = sg.begin();
        let _ = t1.get(b"counter").unwrap();

        // t2 writes counter after t1's snapshot.
        let mut t2 = sg.begin();
        t2.put(b"counter", b"2");
        t2.commit().unwrap();

        // t1 tries to update based on stale read → conflict.
        t1.put(b"counter", b"1");
        let result = t1.commit();
        assert!(result.is_err(), "pessimistic should detect stale read");

        // Retry with force_hot → group locking, no read validation.
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

        sg.monitor().record_conflict(range);
        sg.monitor().record_conflict(range);

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

        assert_eq!(sg.monitor().mode(range), crate::RangeMode::Cold);

        sg.monitor().record_conflict(range);
        sg.monitor().record_conflict(range);
        assert_eq!(sg.monitor().mode(range), crate::RangeMode::Hot);

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
        };
        let sg = SkewGuard::new(MemStorage::new(), config);
        let range = crate::KeyRange::for_key(b"hot_key", 4);

        sg.monitor().record_conflict(range);
        sg.monitor().record_conflict(range);
        assert_eq!(sg.monitor().mode(range), crate::RangeMode::Hot);

        let mut txn = sg.begin();
        txn.put(b"hot_key", b"credit_hot");
        txn.commit().unwrap();

        let mut txn2 = sg.begin();
        let val = txn2.get(b"hot_key").unwrap();
        assert_eq!(val, Some(b"credit_hot".to_vec()));
    }
}

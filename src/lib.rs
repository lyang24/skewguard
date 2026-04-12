//! SkewGuard: Contention-adaptive execution for transactional KV workloads.
//!
//! Monitors per-key-range conflict rates at runtime and dynamically switches
//! between pessimistic locking (cold ranges) and group-commit serial execution
//! with leader/follower handoff (hot ranges).

mod error;
mod group_lock;
pub mod mem;
mod monitor;
mod mvcc;
mod occ;
mod pessimistic;
mod range;
pub mod rocks;
mod storage;
mod transaction;

pub use error::{Error, Result};
pub use monitor::{ContentionMonitor, ContentionStats, MonitorStrategy, RangeMode};
pub use range::KeyRange;
pub use storage::{Snapshot, Storage, WriteBatch};
pub use transaction::Transaction;

use group_lock::GroupLockRegistry;
use pessimistic::PessimisticLockManager;
use std::sync::Arc;

/// Core engine that routes transactions through cold (pessimistic) or hot
/// (group lock) paths based on observed contention.
pub struct SkewGuard<S: Storage> {
    storage: Arc<S>,
    monitor: Arc<ContentionMonitor>,
    registry: Arc<GroupLockRegistry>,
    lock_mgr: Arc<PessimisticLockManager>,
    config: Config,
}

/// Configuration for contention detection and mode switching.
#[derive(Debug, Clone)]
pub struct Config {
    /// Strategy for contention monitoring and mode switching.
    pub monitor_strategy: MonitorStrategy,
    /// Number of key ranges to partition the keyspace into.
    pub num_ranges: usize,
    /// Which CC strategy to use for cold (uncontended) ranges.
    pub cold_path: ColdPathStrategy,
}

/// Which concurrency control strategy to use on cold (uncontended) ranges.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColdPathStrategy {
    /// Standard per-key pessimistic locking. Reads take shared locks, writes
    /// take exclusive locks. What CockroachDB and TiDB use in production.
    Pessimistic,
    /// Optimistic concurrency control. Execute freely, validate at commit.
    /// Only useful for read-heavy, low-contention workloads.
    Occ,
}

/// Options for beginning a transaction. Enables abort-driven promotion
/// (HDCC §4.3.2) and transaction hints (HDCC §4.3.1).
#[derive(Debug, Clone, Default)]
pub struct TransactionOptions {
    /// Force this transaction to use the group locking (hot) path,
    /// regardless of the monitor's current mode for the touched ranges.
    /// Set this on retry after an abort to bypass the cold path.
    /// (HDCC: abort-driven promotion)
    pub force_hot: bool,
    /// Pre-declared keys this transaction will access. If provided,
    /// the system checks the monitor for these ranges upfront and
    /// routes to the optimal path immediately.
    /// (HDCC: Rule 1-4 simplified)
    pub declared_keys: Option<Vec<Vec<u8>>>,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            monitor_strategy: MonitorStrategy::default(),
            num_ranges: 64,
            cold_path: ColdPathStrategy::Pessimistic,
        }
    }
}

impl<S: Storage> SkewGuard<S> {
    /// Create a new SkewGuard instance with the given storage and config.
    pub fn new(storage: S, config: Config) -> Self {
        let monitor = Arc::new(ContentionMonitor::new(
            config.num_ranges,
            config.monitor_strategy.clone(),
        ));
        let registry = Arc::new(GroupLockRegistry::new());
        let lock_mgr = Arc::new(PessimisticLockManager::new());
        SkewGuard {
            storage: Arc::new(storage),
            monitor,
            registry,
            lock_mgr,
            config,
        }
    }

    /// Begin a new transaction with default options.
    pub fn begin(&self) -> Transaction<S> {
        self.begin_with_options(TransactionOptions::default())
    }

    /// Begin a new transaction with explicit options.
    ///
    /// Use `TransactionOptions::force_hot` on retry after an abort to
    /// bypass the cold path (abort-driven promotion, HDCC §4.3.2).
    ///
    /// Use `TransactionOptions::declared_keys` to pre-declare the access
    /// set for immediate routing (HDCC §4.3.1 Rules 1-4).
    pub fn begin_with_options(&self, opts: TransactionOptions) -> Transaction<S> {
        let snapshot = self.storage.snapshot();
        Transaction::new(
            Arc::clone(&self.storage),
            Arc::clone(&self.monitor),
            Arc::clone(&self.registry),
            Arc::clone(&self.lock_mgr),
            snapshot,
            self.config.num_ranges,
            self.config.cold_path,
            opts,
        )
    }

    /// Get a reference to the contention monitor for observability.
    pub fn monitor(&self) -> &ContentionMonitor {
        &self.monitor
    }

    /// Get a reference to the config.
    pub fn config(&self) -> &Config {
        &self.config
    }
}

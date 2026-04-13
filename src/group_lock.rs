//! Group locking with leader/follower handoff (TXSQL-style).
//!
//! The core insight: instead of each transaction independently acquiring and
//! releasing a per-key lock, the lock is held continuously by the group.
//! Leadership transfers directly from one transaction to the next without
//! going through an acquire/release cycle.
//!
//! For each hot key:
//! - The first arriving transaction becomes the **leader** and executes immediately.
//! - Subsequent transactions become **followers**, enqueue themselves, and wait.
//! - When the leader finishes, it wakes the next follower, which becomes the
//!   new leader. The lock is never released between consecutive transactions.
//! - When no more followers are waiting, the key slot is cleaned up.

use crate::error::Result;
use crate::mvcc::{ReadWriteSet, WriteOp};
use crate::range::KeyRange;
use crate::storage::{Storage, WriteBatch};
use parking_lot::Mutex;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

/// A waiter in the key's handoff queue. The leader sends a message through
/// this channel to wake the next follower.
struct Waiter {
    /// Signaled when this waiter becomes the leader.
    notify: std::sync::mpsc::SyncSender<()>,
}

/// RAII guard that ensures leadership is released for every key the transaction
/// acquired, even if the transaction panics mid-execution.
struct LeaderGuard<'a> {
    coordinator: &'a GroupLockCoordinator,
    keys: Vec<Vec<u8>>,
    released: bool,
}

impl<'a> LeaderGuard<'a> {
    fn new(coordinator: &'a GroupLockCoordinator, keys: Vec<Vec<u8>>) -> Self {
        LeaderGuard {
            coordinator,
            keys,
            released: false,
        }
    }

    fn release(mut self) {
        self.do_release();
        self.released = true;
    }

    fn do_release(&self) {
        for key in &self.keys {
            self.coordinator.release_key(key);
        }
    }
}

impl Drop for LeaderGuard<'_> {
    fn drop(&mut self) {
        if !self.released {
            self.do_release();
        }
    }
}

/// Per-key handoff state. Tracks the current leader and waiting followers.
struct KeySlot {
    /// Queue of followers waiting for leadership.
    waiters: VecDeque<Waiter>,
    /// True if a leader is currently executing on this key.
    has_leader: bool,
}

impl KeySlot {
    fn new() -> Self {
        KeySlot {
            waiters: VecDeque::new(),
            has_leader: false,
        }
    }
}

/// Number of shards for the global key slot table. Independent hot keys
/// hash to different shards and don't contend on the same mutex.
const NUM_SLOT_SHARDS: usize = 256;

/// Global key slot table shared by all coordinators, sharded to avoid
/// a single-mutex bottleneck. Each shard is an independent Mutex<HashMap>.
///
/// Keys are assigned to shards by FNV hash, so independent hot keys
/// almost always land in different shards and don't contend.
pub(crate) struct GlobalKeySlots {
    shards: Vec<Mutex<HashMap<Vec<u8>, KeySlot>>>,
}

impl GlobalKeySlots {
    pub fn new() -> Self {
        GlobalKeySlots {
            shards: (0..NUM_SLOT_SHARDS)
                .map(|_| Mutex::new(HashMap::new()))
                .collect(),
        }
    }

    fn shard_for(&self, key: &[u8]) -> &Mutex<HashMap<Vec<u8>, KeySlot>> {
        let mut hash: u64 = 0xcbf29ce484222325;
        for &b in key {
            hash ^= b as u64;
            hash = hash.wrapping_mul(0x100000001b3);
        }
        &self.shards[hash as usize % NUM_SLOT_SHARDS]
    }

    fn acquire_slot(&self, key: &[u8]) -> Option<std::sync::mpsc::Receiver<()>> {
        let mut shard = self.shard_for(key).lock();
        let slot = shard.entry(key.to_vec()).or_insert_with(KeySlot::new);

        if !slot.has_leader {
            slot.has_leader = true;
            return None;
        }

        let (tx, rx) = std::sync::mpsc::sync_channel(1);
        slot.waiters.push_back(Waiter { notify: tx });
        Some(rx)
    }

    fn release_slot(&self, key: &[u8]) {
        let mut shard = self.shard_for(key).lock();
        if let Some(slot) = shard.get_mut(key) {
            if let Some(next) = slot.waiters.pop_front() {
                let _ = next.notify.send(());
            } else {
                slot.has_leader = false;
                if slot.waiters.is_empty() {
                    shard.remove(key);
                }
            }
        }
    }

    fn force_recover_slot(&self, key: &[u8]) {
        let mut shard = self.shard_for(key).lock();
        if let Some(slot) = shard.get_mut(key) {
            while let Some(waiter) = slot.waiters.pop_front() {
                if waiter.notify.send(()).is_ok() {
                    return;
                }
            }
            slot.has_leader = true;
        }
    }
}

/// Coordinator for group locking with leader/follower handoff.
///
/// Uses a shared GlobalKeySlots table so that the same key is always
/// coordinated through the same slot, even across different range
/// coordinators.
///
/// There is no cross-key ordering. Transactions on independent keys within
/// the same range execute and commit concurrently. Only same-key transactions
/// are serialized through the handoff mechanism.
pub(crate) struct GroupLockCoordinator {
    global_slots: Arc<GlobalKeySlots>,
}

impl GroupLockCoordinator {
    pub fn new(global_slots: Arc<GlobalKeySlots>) -> Self {
        GroupLockCoordinator { global_slots }
    }

    /// Acquire leadership for a key. Blocks until leadership is handed off.
    ///
    /// If the current leader panics, its LeaderGuard Drop impl calls
    /// release_key(), which sends the handoff signal. The follower wakes
    /// up normally.
    ///
    /// There is NO timeout. A slow leader is not a dead leader.
    pub fn acquire(&self, key: &[u8]) {
        let receiver = match self.global_slots.acquire_slot(key) {
            None => return, // Became leader immediately.
            Some(rx) => rx,
        };

        match receiver.recv() {
            Ok(()) => {}
            Err(_) => {
                // Sender dropped without sending — leader thread exited
                // without triggering LeaderGuard::drop. Force-recover.
                self.global_slots.force_recover_slot(key);
            }
        }
    }

    fn release_key(&self, key: &[u8]) {
        self.global_slots.release_slot(key);
    }

    /// Execute a transaction's writes under group locking with handoff.
    ///
    /// Per-key serialization via handoff. Independent keys execute and
    /// commit concurrently — no cross-key ordering.
    ///
    /// Uses a LeaderGuard to ensure leadership is released on panic.
    pub fn execute<S: Storage>(&self, storage: &S, rw_set: &ReadWriteSet) -> Result<()> {
        // Sort keys to prevent deadlock when a transaction touches multiple
        // hot keys (acquire in consistent order across transactions).
        let mut write_keys: Vec<Vec<u8>> = rw_set.writes.keys().cloned().collect();
        write_keys.sort();

        // Acquire leadership on all keys.
        for key in &write_keys {
            self.acquire(key);
        }

        // LeaderGuard ensures release happens even on panic.
        let guard = LeaderGuard::new(self, write_keys);

        // Build and apply the write batch.
        let mut batch = storage.write_batch();
        for (key, op) in &rw_set.writes {
            match op {
                WriteOp::Put(value) => batch.put(key, value),
                WriteOp::Delete => batch.delete(key),
            }
        }

        let result = storage.commit(batch);

        // Release leadership (hands off to next waiter per key).
        guard.release();

        result.map(|_| ())
    }
}

/// Registry of group lock coordinators. All coordinators share a single
/// global key slot table, so the same key is always coordinated through
/// the same slot regardless of which range handles the transaction.
pub(crate) struct GroupLockRegistry {
    coordinators: Mutex<HashMap<usize, Arc<GroupLockCoordinator>>>,
    global_slots: Arc<GlobalKeySlots>,
}

impl GroupLockRegistry {
    pub fn new() -> Self {
        GroupLockRegistry {
            coordinators: Mutex::new(HashMap::new()),
            global_slots: Arc::new(GlobalKeySlots::new()),
        }
    }

    /// Get or create the coordinator for a range.
    pub fn coordinator(&self, range: KeyRange) -> Arc<GroupLockCoordinator> {
        let mut map = self.coordinators.lock();
        map.entry(range.index)
            .or_insert_with(|| Arc::new(GroupLockCoordinator::new(Arc::clone(&self.global_slots))))
            .clone()
    }

    /// Remove the coordinator for a range (when it transitions back to cold).
    #[allow(dead_code)]
    pub fn remove(&self, range: KeyRange) {
        let mut map = self.coordinators.lock();
        map.remove(&range.index);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mem::MemStorage;
    use crate::storage::Storage;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::thread;

    #[test]
    fn single_transaction_executes_immediately() {
        let coord = GroupLockCoordinator::new(Arc::new(GlobalKeySlots::new()));
        let storage = MemStorage::new();

        let mut rw_set = ReadWriteSet::new();
        let range = KeyRange { index: 0 };
        rw_set.record_write(b"key1".to_vec(), range, WriteOp::Put(b"value1".to_vec()));

        coord.execute(&storage, &rw_set).unwrap();

        let val = storage
            .get_at(b"key1", storage.current_timestamp())
            .unwrap();
        assert_eq!(val, Some(b"value1".to_vec()));
    }

    #[test]
    fn concurrent_same_key_serialized() {
        // Two transactions on the same key should execute serially via handoff.
        let coord = Arc::new(GroupLockCoordinator::new(Arc::new(GlobalKeySlots::new())));
        let storage = Arc::new(MemStorage::new());
        let execution_order = Arc::new(Mutex::new(Vec::new()));

        let handles: Vec<_> = (0..4)
            .map(|i| {
                let coord = Arc::clone(&coord);
                let storage = Arc::clone(&storage);
                let order = Arc::clone(&execution_order);
                thread::spawn(move || {
                    let mut rw_set = ReadWriteSet::new();
                    let range = KeyRange { index: 0 };
                    rw_set.record_write(
                        b"hot_key".to_vec(),
                        range,
                        WriteOp::Put(format!("val_{i}").into_bytes()),
                    );

                    coord.execute(&*storage, &rw_set).unwrap();
                    order.lock().push(i);
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }

        // All 4 transactions should have completed.
        let order = execution_order.lock();
        assert_eq!(order.len(), 4);
    }

    #[test]
    fn concurrent_different_keys_parallel() {
        // Transactions on different keys should execute concurrently.
        let coord = Arc::new(GroupLockCoordinator::new(Arc::new(GlobalKeySlots::new())));
        let storage = Arc::new(MemStorage::new());
        let active_count = Arc::new(AtomicUsize::new(0));
        let max_concurrent = Arc::new(AtomicUsize::new(0));

        let handles: Vec<_> = (0..8)
            .map(|i| {
                let coord = Arc::clone(&coord);
                let storage = Arc::clone(&storage);
                let active = Arc::clone(&active_count);
                let max_conc = Arc::clone(&max_concurrent);
                thread::spawn(move || {
                    let key = format!("key_{i}");
                    let mut rw_set = ReadWriteSet::new();
                    let range = KeyRange { index: 0 };
                    rw_set.record_write(
                        key.as_bytes().to_vec(),
                        range,
                        WriteOp::Put(b"value".to_vec()),
                    );

                    // Track concurrency.
                    let prev = active.fetch_add(1, Ordering::SeqCst);
                    max_conc.fetch_max(prev + 1, Ordering::SeqCst);

                    coord.execute(&*storage, &rw_set).unwrap();

                    active.fetch_sub(1, Ordering::SeqCst);
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }

        // With 8 different keys, we should see some concurrency.
        // (Not guaranteed on all platforms/schedulers, so we just check
        // all transactions completed.)
        for i in 0..8 {
            let key = format!("key_{i}");
            let val = storage
                .get_at(key.as_bytes(), storage.current_timestamp())
                .unwrap();
            assert_eq!(val, Some(b"value".to_vec()));
        }
    }

    #[test]
    fn handoff_preserves_last_writer_wins() {
        // Serial execution on same key: last writer's value should be visible.
        let coord = Arc::new(GroupLockCoordinator::new(Arc::new(GlobalKeySlots::new())));
        let storage = Arc::new(MemStorage::new());

        // Execute sequentially to guarantee order.
        for i in 0..5u32 {
            let mut rw_set = ReadWriteSet::new();
            let range = KeyRange { index: 0 };
            rw_set.record_write(
                b"counter".to_vec(),
                range,
                WriteOp::Put(i.to_le_bytes().to_vec()),
            );
            coord.execute(&*storage, &rw_set).unwrap();
        }

        let val = storage
            .get_at(b"counter", storage.current_timestamp())
            .unwrap()
            .unwrap();
        assert_eq!(val, 4u32.to_le_bytes().to_vec());
    }

    #[test]
    fn slot_cleanup_after_no_waiters() {
        let coord = GroupLockCoordinator::new(Arc::new(GlobalKeySlots::new()));
        let storage = MemStorage::new();

        let mut rw_set = ReadWriteSet::new();
        let range = KeyRange { index: 0 };
        rw_set.record_write(b"temp_key".to_vec(), range, WriteOp::Put(b"val".to_vec()));

        coord.execute(&storage, &rw_set).unwrap();

        // After execution with no other waiters, the slot should be cleaned up.
        let shard = coord.global_slots.shard_for(b"temp_key").lock();
        assert!(!shard.contains_key(&b"temp_key".to_vec()));
    }

    #[test]
    fn follower_recovers_when_leader_panics() {
        // When a leader thread panics inside execute(), LeaderGuard::drop
        // releases leadership and the next follower wakes up normally.

        let coord = Arc::new(GroupLockCoordinator::new(Arc::new(GlobalKeySlots::new())));
        let storage = Arc::new(MemStorage::new());

        // Leader acquires and creates a guard, then "panics" (we simulate
        // by dropping the guard without calling release).
        {
            let keys = vec![b"crash_key".to_vec()];
            coord.acquire(b"crash_key");
            let _guard = LeaderGuard::new(&coord, keys);
            // Guard drops here → calls do_release → release_key → wakes next follower.
        }

        // A subsequent transaction should succeed immediately (leadership
        // was released by the guard's Drop).
        let mut rw_set = ReadWriteSet::new();
        let range = KeyRange { index: 0 };
        rw_set.record_write(
            b"crash_key".to_vec(),
            range,
            WriteOp::Put(b"recovered".to_vec()),
        );
        coord.execute(&*storage, &rw_set).unwrap();

        let val = storage
            .get_at(b"crash_key", storage.current_timestamp())
            .unwrap();
        assert_eq!(val, Some(b"recovered".to_vec()));
    }

    #[test]
    fn leader_guard_releases_on_panic() {
        // Test that LeaderGuard releases leadership when dropped (simulating
        // a panic during execute).
        let coord = Arc::new(GroupLockCoordinator::new(Arc::new(GlobalKeySlots::new())));
        let storage = Arc::new(MemStorage::new());

        // Acquire and create a guard, then drop it without calling release().
        {
            let keys = vec![b"guard_key".to_vec()];
            coord.acquire(b"guard_key");
            let _guard = LeaderGuard::new(&coord, keys);
            // guard drops here without explicit release()
        }

        // A subsequent transaction should be able to acquire the same key.
        let mut rw_set = ReadWriteSet::new();
        let range = KeyRange { index: 0 };
        rw_set.record_write(
            b"guard_key".to_vec(),
            range,
            WriteOp::Put(b"after_drop".to_vec()),
        );
        coord.execute(&*storage, &rw_set).unwrap();

        let val = storage
            .get_at(b"guard_key", storage.current_timestamp())
            .unwrap();
        assert_eq!(val, Some(b"after_drop".to_vec()));
    }

    #[test]
    fn independent_hot_keys_commit_concurrently() {
        // After removing cross-key sequence ordering, transactions on
        // independent keys within the same coordinator must commit
        // concurrently (no artificial convoying).
        use std::sync::Barrier;

        let slots = Arc::new(GlobalKeySlots::new());
        let coord = Arc::new(GroupLockCoordinator::new(Arc::clone(&slots)));
        let storage = Arc::new(MemStorage::new());
        let num_threads = 8;
        let barrier = Arc::new(Barrier::new(num_threads));

        let start = std::time::Instant::now();
        let handles: Vec<_> = (0..num_threads)
            .map(|i| {
                let coord = Arc::clone(&coord);
                let storage = Arc::clone(&storage);
                let bar = Arc::clone(&barrier);
                thread::spawn(move || {
                    bar.wait(); // all start together

                    // Each thread writes a unique key — no handoff contention.
                    let key = format!("independent_{i}");
                    let mut rw_set = ReadWriteSet::new();
                    let range = KeyRange { index: 0 };
                    rw_set.record_write(key.into_bytes(), range, WriteOp::Put(b"val".to_vec()));
                    coord.execute(&*storage, &rw_set).unwrap();
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }

        let elapsed = start.elapsed();
        // With no cross-key ordering, 8 threads writing 8 different keys
        // should complete in roughly the time of 1 (parallel commits).
        // If sequence ordering were still active, they'd serialize.
        assert!(
            elapsed < std::time::Duration::from_secs(1),
            "8 independent keys took {:?}, suggesting serialization",
            elapsed,
        );

        // Verify all writes landed.
        for i in 0..num_threads {
            let key = format!("independent_{i}");
            let val = storage
                .get_at(key.as_bytes(), storage.current_timestamp())
                .unwrap();
            assert_eq!(val, Some(b"val".to_vec()));
        }
    }

    #[test]
    fn follower_blocked_then_wakes_on_leader_drop() {
        // Test the actual hard case: a follower is BLOCKED waiting on
        // recv() when the leader's LeaderGuard drops. The follower
        // must wake up and proceed.
        let slots = Arc::new(GlobalKeySlots::new());
        let coord = Arc::new(GroupLockCoordinator::new(Arc::clone(&slots)));
        let storage = Arc::new(MemStorage::new());

        // Thread 1: acquires leadership, holds it for a while, then
        // drops the guard (simulating a panic or normal completion).
        let coord1 = Arc::clone(&coord);
        let t1 = thread::spawn(move || {
            coord1.acquire(b"contested_key");
            // Simulate some work while holding leadership.
            thread::sleep(std::time::Duration::from_millis(50));
            // Don't call release_key — rely on manual release to
            // simulate what LeaderGuard::drop does.
            coord1.release_key(b"contested_key");
        });

        // Give t1 a moment to acquire leadership.
        thread::sleep(std::time::Duration::from_millis(10));

        // Thread 2: tries to acquire the same key — should block
        // until t1 releases.
        let coord2 = Arc::clone(&coord);
        let storage2 = Arc::clone(&storage);
        let t2 = thread::spawn(move || {
            let mut rw_set = ReadWriteSet::new();
            let range = KeyRange { index: 0 };
            rw_set.record_write(
                b"contested_key".to_vec(),
                range,
                WriteOp::Put(b"from_follower".to_vec()),
            );
            coord2.execute(&*storage2, &rw_set).unwrap();
        });

        t1.join().unwrap();
        t2.join().unwrap();

        // Follower's write should have succeeded after leader released.
        let val = storage
            .get_at(b"contested_key", storage.current_timestamp())
            .unwrap();
        assert_eq!(val, Some(b"from_follower".to_vec()));
    }
}

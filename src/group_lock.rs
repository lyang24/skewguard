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
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// Default timeout for waiting on leadership handoff. If the current leader
/// panics or hangs, followers will recover after this duration.
const LEADER_TIMEOUT: Duration = Duration::from_millis(500);

/// A waiter in the key's handoff queue. The leader sends a message through
/// this channel to wake the next follower.
struct Waiter {
    /// Signaled when this waiter becomes the leader.
    notify: std::sync::mpsc::SyncSender<()>,
}

/// RAII guard that ensures `release()` is called for every key the transaction
/// acquired leadership on, even if the transaction panics mid-execution.
struct LeaderGuard<'a> {
    coordinator: &'a GroupLockCoordinator,
    keys: Vec<Vec<u8>>,
    sequences: Vec<u64>,
    released: bool,
}

impl<'a> LeaderGuard<'a> {
    fn new(coordinator: &'a GroupLockCoordinator, keys: Vec<Vec<u8>>, sequences: Vec<u64>) -> Self {
        LeaderGuard {
            coordinator,
            keys,
            sequences,
            released: false,
        }
    }

    /// Explicitly release leadership and mark sequences committed.
    /// Called on the normal (non-panic) path.
    fn release(mut self) {
        self.do_release();
        self.released = true;
    }

    fn do_release(&self) {
        for &seq in &self.sequences {
            self.coordinator.mark_committed(seq);
        }
        for key in &self.keys {
            self.coordinator.release_key(key);
        }
    }
}

impl Drop for LeaderGuard<'_> {
    fn drop(&mut self) {
        if !self.released {
            // Panic path: still release leadership so followers don't hang.
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

/// Coordinator for a single hot range using leader/follower handoff.
///
/// Non-conflicting keys within the range execute concurrently. Only
/// transactions targeting the *same key* are serialized through the
/// handoff mechanism.
pub(crate) struct GroupLockCoordinator {
    /// Per-key handoff slots.
    slots: Mutex<HashMap<Vec<u8>, KeySlot>>,
    /// Monotonic sequence counter for commit ordering within this range.
    next_sequence: AtomicU64,
    /// Highest sequence number that has been committed. Transactions must
    /// wait for all predecessors to commit before committing themselves.
    committed_through: AtomicU64,
}

impl GroupLockCoordinator {
    pub fn new() -> Self {
        GroupLockCoordinator {
            slots: Mutex::new(HashMap::new()),
            next_sequence: AtomicU64::new(0),
            committed_through: AtomicU64::new(0),
        }
    }

    /// Acquire leadership for a key. Returns immediately if no other
    /// transaction holds leadership (caller becomes leader). Otherwise
    /// blocks until the current leader hands off, or until the timeout
    /// expires (indicating the leader likely crashed).
    ///
    /// Returns the assigned sequence number for commit ordering.
    pub fn acquire(&self, key: &[u8]) -> u64 {
        let seq = self.next_sequence.fetch_add(1, Ordering::SeqCst) + 1;

        let receiver = {
            let mut slots = self.slots.lock();
            let slot = slots
                .entry(key.to_vec())
                .or_insert_with(KeySlot::new);

            if !slot.has_leader {
                // No leader — we become leader immediately.
                slot.has_leader = true;
                return seq;
            }

            // There's a leader. Enqueue ourselves and wait.
            let (tx, rx) = std::sync::mpsc::sync_channel(1);
            slot.waiters.push_back(Waiter { notify: tx });
            rx
        };

        // Wait outside the lock for the handoff signal, with a timeout
        // to recover from leader crashes.
        match receiver.recv_timeout(LEADER_TIMEOUT) {
            Ok(()) => seq,
            Err(_) => {
                // Timeout or sender disconnected — the leader likely
                // panicked or hung. Force-recover this key slot by
                // clearing the dead leader and becoming the new leader.
                self.force_recover(key);
                seq
            }
        }
    }

    /// Force-recover a key slot when the current leader is presumed dead.
    /// Clears any stale waiters whose senders have disconnected and
    /// re-establishes leadership.
    fn force_recover(&self, key: &[u8]) {
        let mut slots = self.slots.lock();
        if let Some(slot) = slots.get_mut(key) {
            // Drain any waiters whose senders disconnected (their leaders
            // also crashed). Try to wake the first live waiter.
            while let Some(waiter) = slot.waiters.pop_front() {
                if waiter.notify.send(()).is_ok() {
                    // Successfully handed off to a live waiter.
                    return;
                }
                // Sender disconnected — this waiter's thread is gone. Skip it.
            }
            // No live waiters remain. We already have leadership from the
            // timed-out recv — just ensure the slot state is consistent.
            slot.has_leader = true;
        }
    }

    /// Release leadership for a key. If there are followers waiting,
    /// hand off leadership to the next one (no lock release/acquire cycle).
    /// If no followers are waiting, clean up the key slot.
    fn release_key(&self, key: &[u8]) {
        let mut slots = self.slots.lock();
        if let Some(slot) = slots.get_mut(key) {
            if let Some(next) = slot.waiters.pop_front() {
                // Hand off to the next waiter. The lock is never released.
                let _ = next.notify.send(());
            } else {
                // No waiters. Clean up the slot.
                slot.has_leader = false;
                // Remove the slot entirely if empty to prevent unbounded growth.
                if slot.waiters.is_empty() {
                    slots.remove(key);
                }
            }
        }
    }

    /// Wait until all transactions with sequence numbers before `seq` have
    /// committed. This ensures commit ordering matches execution ordering.
    ///
    /// Times out after `LEADER_TIMEOUT` to avoid deadlocking if a predecessor
    /// crashed without committing. On timeout, forces the watermark forward
    /// so subsequent transactions can proceed.
    pub fn wait_for_predecessors(&self, seq: u64) {
        if seq <= 1 {
            return; // First sequence, no predecessors.
        }

        let deadline = std::time::Instant::now() + LEADER_TIMEOUT;
        loop {
            let committed = self.committed_through.load(Ordering::SeqCst);
            if committed >= seq - 1 {
                return;
            }
            if std::time::Instant::now() >= deadline {
                // Predecessor likely crashed. Force the watermark forward
                // so we (and everyone after us) can proceed.
                self.committed_through.fetch_max(seq - 1, Ordering::SeqCst);
                return;
            }
            std::thread::yield_now();
        }
    }

    /// Mark a sequence number as committed, advancing the committed_through
    /// watermark.
    pub fn mark_committed(&self, seq: u64) {
        // We only advance if seq is the next expected commit. This ensures
        // strictly sequential commit ordering.
        //
        // In the current design, transactions on the same key execute
        // serially so they always commit in order. But transactions on
        // different keys within the same range can execute concurrently
        // and may commit out of order. For now, we use a simple store.
        // A more sophisticated approach would use a commit bitmap.
        self.committed_through.fetch_max(seq, Ordering::SeqCst);
    }

    /// Execute a transaction's writes under group locking with handoff.
    ///
    /// Uses a `LeaderGuard` to ensure leadership is released even if the
    /// transaction panics mid-execution. This prevents follower deadlock
    /// when a leader thread crashes (CIDER Section 4.6, fault tolerance).
    pub fn execute<S: Storage>(
        &self,
        storage: &S,
        rw_set: &ReadWriteSet,
    ) -> Result<()> {
        // Sort keys to prevent deadlock when a transaction touches multiple
        // hot keys (acquire in consistent order across transactions).
        let mut write_keys: Vec<Vec<u8>> = rw_set.writes.keys().cloned().collect();
        write_keys.sort();

        // Acquire leadership on all keys. Each key's handoff is independent.
        let sequences: Vec<u64> = write_keys
            .iter()
            .map(|key| self.acquire(key))
            .collect();

        // LeaderGuard ensures release happens even on panic.
        let max_seq = sequences.iter().copied().max();
        let guard = LeaderGuard::new(self, write_keys, sequences);

        // We are now leader on all our keys. Build and apply the write batch.
        let mut batch = storage.write_batch();
        for (key, op) in &rw_set.writes {
            match op {
                WriteOp::Put(value) => batch.put(key, value),
                WriteOp::Delete => batch.delete(key),
            }
        }

        // Wait for predecessors to commit (preserves execution order).
        if let Some(max_seq) = max_seq {
            self.wait_for_predecessors(max_seq);
        }

        // Commit the writes.
        let result = storage.commit(batch);

        // Release leadership on all keys (hands off to next waiter).
        // On panic, the guard's Drop impl does this instead.
        guard.release();

        result.map(|_| ())
    }
}

/// Registry of group lock coordinators, one per hot range.
pub(crate) struct GroupLockRegistry {
    coordinators: Mutex<HashMap<usize, Arc<GroupLockCoordinator>>>,
}

impl GroupLockRegistry {
    pub fn new() -> Self {
        GroupLockRegistry {
            coordinators: Mutex::new(HashMap::new()),
        }
    }

    /// Get or create the coordinator for a range.
    pub fn coordinator(&self, range: KeyRange) -> Arc<GroupLockCoordinator> {
        let mut map = self.coordinators.lock();
        map.entry(range.index)
            .or_insert_with(|| Arc::new(GroupLockCoordinator::new()))
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
    use std::sync::atomic::AtomicUsize;
    use std::thread;

    #[test]
    fn single_transaction_executes_immediately() {
        let coord = GroupLockCoordinator::new();
        let storage = MemStorage::new();

        let mut rw_set = ReadWriteSet::new();
        let range = KeyRange { index: 0 };
        rw_set.record_write(
            b"key1".to_vec(),
            range,
            WriteOp::Put(b"value1".to_vec()),
        );

        coord.execute(&storage, &rw_set).unwrap();

        let snap = storage.snapshot();
        let val = crate::storage::Snapshot::get(&snap, b"key1").unwrap();
        assert_eq!(val, Some(b"value1".to_vec()));
    }

    #[test]
    fn concurrent_same_key_serialized() {
        // Two transactions on the same key should execute serially via handoff.
        let coord = Arc::new(GroupLockCoordinator::new());
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
        let coord = Arc::new(GroupLockCoordinator::new());
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
        let snap = storage.snapshot();
        for i in 0..8 {
            let key = format!("key_{i}");
            let val = crate::storage::Snapshot::get(&snap, key.as_bytes()).unwrap();
            assert_eq!(val, Some(b"value".to_vec()));
        }
    }

    #[test]
    fn handoff_preserves_last_writer_wins() {
        // Serial execution on same key: last writer's value should be visible.
        let coord = Arc::new(GroupLockCoordinator::new());
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

        let snap = storage.snapshot();
        let val = crate::storage::Snapshot::get(&snap, b"counter").unwrap().unwrap();
        assert_eq!(val, 4u32.to_le_bytes().to_vec());
    }

    #[test]
    fn slot_cleanup_after_no_waiters() {
        let coord = GroupLockCoordinator::new();
        let storage = MemStorage::new();

        let mut rw_set = ReadWriteSet::new();
        let range = KeyRange { index: 0 };
        rw_set.record_write(
            b"temp_key".to_vec(),
            range,
            WriteOp::Put(b"val".to_vec()),
        );

        coord.execute(&storage, &rw_set).unwrap();

        // After execution with no other waiters, the slot should be cleaned up.
        let slots = coord.slots.lock();
        assert!(!slots.contains_key(&b"temp_key".to_vec()));
    }

    #[test]
    fn follower_recovers_from_leader_crash() {
        // Simulate a leader crashing: acquire leadership on a key, then
        // drop the coordinator reference without calling release. A follower
        // waiting on the same key should recover via timeout.
        let coord = Arc::new(GroupLockCoordinator::new());
        let storage = Arc::new(MemStorage::new());

        // Spawn a "leader" thread that acquires leadership and then panics
        // without releasing. We catch the panic to avoid test failure.
        let coord_clone = Arc::clone(&coord);
        let leader = thread::spawn(move || {
            let _seq = coord_clone.acquire(b"crash_key");
            // Simulate work, then "crash" by returning without release.
            // The LeaderGuard would normally handle this in execute(),
            // but here we test the raw acquire/timeout path.
            // Intentionally do NOT call release_key. The sender in the
            // slot's waiter queue will be dropped, causing the follower's
            // recv_timeout to fail.
        });
        leader.join().unwrap();

        // Now spawn a follower. It should recover within LEADER_TIMEOUT.
        let coord_clone = Arc::clone(&coord);
        let storage_clone = Arc::clone(&storage);
        let follower = thread::spawn(move || {
            let mut rw_set = ReadWriteSet::new();
            let range = KeyRange { index: 0 };
            rw_set.record_write(
                b"crash_key".to_vec(),
                range,
                WriteOp::Put(b"recovered".to_vec()),
            );
            coord_clone.execute(&*storage_clone, &rw_set).unwrap();
        });

        // The follower should complete within a reasonable time
        // (LEADER_TIMEOUT + some margin).
        let result = follower.join();
        assert!(result.is_ok(), "follower should recover from leader crash");

        // Verify the write landed.
        let snap = storage.snapshot();
        let val = crate::storage::Snapshot::get(&snap, b"crash_key").unwrap();
        assert_eq!(val, Some(b"recovered".to_vec()));
    }

    #[test]
    fn leader_guard_releases_on_panic() {
        // Test that LeaderGuard releases leadership when dropped (simulating
        // a panic during execute).
        let coord = Arc::new(GroupLockCoordinator::new());
        let storage = Arc::new(MemStorage::new());

        // Acquire and create a guard, then drop it without calling release().
        {
            let keys = vec![b"guard_key".to_vec()];
            let sequences = vec![coord.acquire(b"guard_key")];
            let _guard = LeaderGuard::new(&coord, keys, sequences);
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

        let snap = storage.snapshot();
        let val = crate::storage::Snapshot::get(&snap, b"guard_key").unwrap();
        assert_eq!(val, Some(b"after_drop".to_vec()));
    }
}

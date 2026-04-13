use crate::error::Result;

/// Timestamp for MVCC versioning.
pub type Timestamp = u64;

/// A point-in-time view of the storage. Captures the logical timestamp
/// at which the snapshot was taken. All reads through this snapshot see
/// data as of this timestamp — writes committed after this point are
/// invisible.
pub trait Snapshot: Send + Sync {
    /// The timestamp this snapshot was taken at.
    fn timestamp(&self) -> Timestamp;
}

/// A batch of writes to apply atomically.
pub trait WriteBatch: Send {
    /// Put a key-value pair into the batch.
    fn put(&mut self, key: &[u8], value: &[u8]);

    /// Delete a key in the batch.
    fn delete(&mut self, key: &[u8]);
}

/// Storage backend trait. SkewGuard is storage-agnostic.
pub trait Storage: Send + Sync + 'static {
    /// The snapshot type for this storage.
    type Snapshot: Snapshot;

    /// The write batch type for this storage.
    type WriteBatch: WriteBatch;

    /// Take a consistent snapshot at the current timestamp. The returned
    /// snapshot's timestamp must be strictly less than any future commit
    /// timestamp, so that reads at the snapshot timestamp never see
    /// concurrent or future writes.
    fn snapshot(&self) -> Self::Snapshot;

    /// Create an empty write batch.
    fn write_batch(&self) -> Self::WriteBatch;

    /// Apply a write batch atomically. Returns the commit timestamp,
    /// which must be strictly greater than the current snapshot timestamp.
    fn commit(&self, batch: Self::WriteBatch) -> Result<Timestamp>;

    /// Get the current timestamp (monotonically increasing).
    fn current_timestamp(&self) -> Timestamp;

    /// Read a key at a specific timestamp. Returns the latest version
    /// of the key written at a timestamp strictly less than `ts`.
    /// This is the primary read path for transactions.
    fn get_at(&self, key: &[u8], ts: Timestamp) -> Result<Option<Vec<u8>>>;

    /// Check if a key was modified at any timestamp `t` where
    /// `after <= t <= at_or_before`. Used for conflict validation.
    fn was_modified(&self, key: &[u8], after: Timestamp, at_or_before: Timestamp) -> Result<bool>;
}

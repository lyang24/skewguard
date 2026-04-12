use crate::error::Result;

/// Timestamp for MVCC versioning.
pub type Timestamp = u64;

/// A point-in-time snapshot of the storage.
pub trait Snapshot: Send + Sync {
    /// Read a key at the snapshot's timestamp.
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;

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

    /// Take a consistent snapshot at the current timestamp.
    fn snapshot(&self) -> Self::Snapshot;

    /// Create an empty write batch.
    fn write_batch(&self) -> Self::WriteBatch;

    /// Apply a write batch atomically. Returns the commit timestamp.
    fn commit(&self, batch: Self::WriteBatch) -> Result<Timestamp>;

    /// Get the current timestamp (monotonically increasing).
    fn current_timestamp(&self) -> Timestamp;

    /// Read a key at a specific timestamp. Used for conflict validation.
    fn get_at(&self, key: &[u8], ts: Timestamp) -> Result<Option<Vec<u8>>>;

    /// Check if a key was modified between two timestamps (inclusive on both
    /// ends: `after <= ts <= at_or_before`). Used for OCC validation.
    fn was_modified(&self, key: &[u8], after: Timestamp, at_or_before: Timestamp) -> Result<bool>;
}

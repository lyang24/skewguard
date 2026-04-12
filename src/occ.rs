use crate::error::{Error, Result};
use crate::mvcc::ReadWriteSet;
use crate::storage::{Storage, Timestamp};

/// Validates a transaction's read set against the storage to detect conflicts.
///
/// OCC validation: for every key the transaction read, check if it was
/// modified between the snapshot timestamp and now. If any read key was
/// modified, the transaction must abort.
pub(crate) fn validate<S: Storage>(
    storage: &S,
    rw_set: &ReadWriteSet,
    snapshot_ts: Timestamp,
    commit_ts: Timestamp,
) -> Result<()> {
    for (key, _range) in &rw_set.reads {
        if storage.was_modified(key, snapshot_ts, commit_ts)? {
            return Err(Error::Conflict);
        }
    }
    Ok(())
}

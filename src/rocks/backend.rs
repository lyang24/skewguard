use crate::error::{Error, Result};
use crate::storage::{self, Timestamp};
use rust_rocksdb::{DB, Options, WriteBatch as RocksWriteBatch, WriteOptions};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

/// Encode a user key + timestamp into a RocksDB key.
/// Format: [user_key_len: 4 BE][user_key][inverted_timestamp: 8 BE]
/// Inverted timestamp = u64::MAX - ts, so latest version sorts first.
fn encode_key(user_key: &[u8], ts: Timestamp) -> Vec<u8> {
    let len = user_key.len() as u32;
    let inv_ts = u64::MAX - ts;
    let mut encoded = Vec::with_capacity(4 + user_key.len() + 8);
    encoded.extend_from_slice(&len.to_be_bytes());
    encoded.extend_from_slice(user_key);
    encoded.extend_from_slice(&inv_ts.to_be_bytes());
    encoded
}

/// Encode a prefix for seeking: all versions of a user key.
fn encode_prefix(user_key: &[u8]) -> Vec<u8> {
    let len = user_key.len() as u32;
    let mut prefix = Vec::with_capacity(4 + user_key.len());
    prefix.extend_from_slice(&len.to_be_bytes());
    prefix.extend_from_slice(user_key);
    prefix
}

/// Decode a RocksDB key back into (user_key, timestamp).
fn decode_key(encoded: &[u8]) -> Option<(&[u8], Timestamp)> {
    if encoded.len() < 12 {
        return None;
    }
    let len = u32::from_be_bytes(encoded[..4].try_into().ok()?) as usize;
    if encoded.len() < 4 + len + 8 {
        return None;
    }
    let user_key = &encoded[4..4 + len];
    let inv_ts_bytes: [u8; 8] = encoded[4 + len..4 + len + 8].try_into().ok()?;
    let inv_ts = u64::from_be_bytes(inv_ts_bytes);
    let ts = u64::MAX - inv_ts;
    Some((user_key, ts))
}

/// RocksDB-backed MVCC storage.
pub struct RocksStorage {
    db: DB,
    clock: AtomicU64,
    sync_writes: bool,
}

impl RocksStorage {
    /// Open or create a RocksDB instance at the given path.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        Self::open_with_options(path, true)
    }

    /// Open with configurable sync writes. Disabling sync is faster for
    /// benchmarks but unsafe for production (data loss on crash).
    pub fn open_with_options<P: AsRef<Path>>(path: P, sync_writes: bool) -> Result<Self> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.set_write_buffer_size(64 * 1024 * 1024);
        opts.set_max_write_buffer_number(3);
        opts.set_target_file_size_base(64 * 1024 * 1024);
        opts.set_level_zero_file_num_compaction_trigger(4);
        opts.set_max_background_jobs(4);

        let db = DB::open(&opts, path).map_err(|e| Error::Storage(Box::new(e)))?;

        // Recover the clock from persisted data. Scan for the highest
        // timestamp in the DB so we never reuse a timestamp after restart.
        // We set clock to max_ts + 2 because:
        //   - max_ts is a write timestamp
        //   - max_ts + 1 was the current_timestamp() after that write
        //   - We need to start strictly after that to avoid confusion
        let max_ts = Self::recover_max_timestamp(&db);

        Ok(RocksStorage {
            db,
            clock: AtomicU64::new(max_ts + 2),
            sync_writes,
        })
    }

    /// Scan the DB to find the highest persisted timestamp.
    fn recover_max_timestamp(db: &DB) -> Timestamp {
        let mut max_ts: Timestamp = 0;
        let mut iter = db.raw_iterator();
        iter.seek_to_first();
        while iter.valid() {
            if let Some(k) = iter.key()
                && let Some((_, ts)) = decode_key(k)
                && ts > max_ts
            {
                max_ts = ts;
            }
            iter.next();
        }
        max_ts
    }

    fn tick(&self) -> Timestamp {
        self.clock.fetch_add(1, Ordering::SeqCst)
    }

    /// Read the latest version of a key at or before the given timestamp.
    fn get_at_internal(&self, user_key: &[u8], ts: Timestamp) -> Result<Option<Vec<u8>>> {
        let prefix = encode_prefix(user_key);
        let seek_key = encode_key(user_key, ts);

        let mut iter = self.db.raw_iterator();
        iter.seek(&seek_key);

        // Because timestamp is inverted, seeking to (key, inverted_ts) positions
        // us at the first version with ts <= our target. But we need to verify
        // the user key matches (we might have seeked past all versions of this key).
        while iter.valid() {
            if let Some(k) = iter.key() {
                // Check the key still has our prefix.
                if !k.starts_with(&prefix) {
                    break;
                }
                if let Some((found_key, found_ts)) = decode_key(k)
                    && found_key == user_key
                    && found_ts < ts
                {
                    // Found the latest version strictly before ts.
                    if let Some(v) = iter.value() {
                        if v.is_empty() {
                            // Empty value = tombstone (delete marker).
                            return Ok(None);
                        }
                        return Ok(Some(v.to_vec()));
                    }
                }
            }
            iter.next();
        }

        Ok(None)
    }
}

impl storage::Storage for RocksStorage {
    type Snapshot = RocksSnapshot;
    type WriteBatch = RocksWriteBatchWrapper;

    fn snapshot(&self) -> RocksSnapshot {
        let ts = self.clock.load(Ordering::SeqCst);
        RocksSnapshot {
            ts,
            // We store a reference-counted handle to the DB for reads.
            // RocksDB snapshots are expensive; for our MVCC design we
            // don't need them — we just read at a logical timestamp.
        }
    }

    fn write_batch(&self) -> RocksWriteBatchWrapper {
        RocksWriteBatchWrapper::default()
    }

    fn commit(&self, mut batch: RocksWriteBatchWrapper) -> Result<Timestamp> {
        let ts = self.tick();
        batch.ts = ts;

        // Re-encode all operations with the commit timestamp.
        let mut final_batch = RocksWriteBatch::default();
        for (user_key, value) in &batch.ops {
            let encoded = encode_key(user_key, ts);
            match value {
                Some(v) => final_batch.put(&encoded, v),
                None => final_batch.put(&encoded, b""), // tombstone
            }
        }

        let mut write_opts = WriteOptions::default();
        write_opts.set_sync(self.sync_writes);

        self.db
            .write_opt(&final_batch, &write_opts)
            .map_err(|e| Error::Storage(Box::new(e)))?;

        Ok(ts)
    }

    fn current_timestamp(&self) -> Timestamp {
        self.clock.load(Ordering::SeqCst)
    }

    fn get_at(&self, key: &[u8], ts: Timestamp) -> Result<Option<Vec<u8>>> {
        self.get_at_internal(key, ts)
    }

    fn was_modified(
        &self,
        user_key: &[u8],
        after: Timestamp,
        at_or_before: Timestamp,
    ) -> Result<bool> {
        let prefix = encode_prefix(user_key);
        // Seek to the version at at_or_before.
        let seek_key = encode_key(user_key, at_or_before);

        let mut iter = self.db.raw_iterator();
        iter.seek(&seek_key);

        while iter.valid() {
            if let Some(k) = iter.key() {
                if !k.starts_with(&prefix) {
                    break;
                }
                if let Some((found_key, found_ts)) = decode_key(k)
                    && found_key == user_key
                {
                    if found_ts >= after && found_ts <= at_or_before {
                        return Ok(true);
                    }
                    if found_ts < after {
                        break; // past our window
                    }
                }
            }
            iter.next();
        }

        Ok(false)
    }
}

/// Snapshot is just a timestamp — MVCC reads use the logical clock,
/// not RocksDB's built-in snapshot mechanism.
pub struct RocksSnapshot {
    ts: Timestamp,
}

impl storage::Snapshot for RocksSnapshot {
    fn timestamp(&self) -> Timestamp {
        self.ts
    }
}

/// Write batch wrapper that buffers operations until commit.
#[derive(Default)]
pub struct RocksWriteBatchWrapper {
    ops: Vec<(Vec<u8>, Option<Vec<u8>>)>,
    ts: Timestamp,
}

impl storage::WriteBatch for RocksWriteBatchWrapper {
    fn put(&mut self, key: &[u8], value: &[u8]) {
        self.ops.push((key.to_vec(), Some(value.to_vec())));
    }

    fn delete(&mut self, key: &[u8]) {
        self.ops.push((key.to_vec(), None));
    }
}

#[cfg(test)]
#[cfg(feature = "rocksdb")]
mod tests {
    use super::*;
    use crate::storage::Storage;
    use tempfile::TempDir;

    fn open_test_db() -> (RocksStorage, TempDir) {
        let dir = TempDir::new().unwrap();
        let storage = RocksStorage::open_with_options(dir.path(), false).unwrap();
        (storage, dir)
    }

    #[test]
    fn basic_read_write() {
        let (store, _dir) = open_test_db();

        let mut batch = store.write_batch();
        storage::WriteBatch::put(&mut batch, b"hello", b"world");
        store.commit(batch).unwrap();

        let val = store.get_at(b"hello", store.current_timestamp()).unwrap();
        assert_eq!(val, Some(b"world".to_vec()));
    }

    #[test]
    fn mvcc_versioning() {
        let (store, _dir) = open_test_db();

        // Write v1.
        let mut batch = store.write_batch();
        storage::WriteBatch::put(&mut batch, b"key", b"v1");
        store.commit(batch).unwrap();

        // Snapshot after v1.
        let after_v1 = store.current_timestamp();

        // Write v2.
        let mut batch = store.write_batch();
        storage::WriteBatch::put(&mut batch, b"key", b"v2");
        store.commit(batch).unwrap();

        let after_v2 = store.current_timestamp();

        // Read after v1 → v1 (strict less-than: sees writes before after_v1).
        let val = store.get_at(b"key", after_v1).unwrap();
        assert_eq!(val, Some(b"v1".to_vec()));

        // Read after v2 → v2.
        let val = store.get_at(b"key", after_v2).unwrap();
        assert_eq!(val, Some(b"v2".to_vec()));
    }

    #[test]
    fn was_modified_detection() {
        let (store, _dir) = open_test_db();

        let mut batch = store.write_batch();
        storage::WriteBatch::put(&mut batch, b"baseline", b"v0");
        let ts1 = store.commit(batch).unwrap();

        let mut batch = store.write_batch();
        storage::WriteBatch::put(&mut batch, b"key", b"v1");
        let ts2 = store.commit(batch).unwrap();

        assert!(store.was_modified(b"key", ts1, ts2).unwrap());
        assert!(!store.was_modified(b"other", ts1, ts2).unwrap());
    }

    #[test]
    fn delete_creates_tombstone() {
        let (store, _dir) = open_test_db();

        let mut batch = store.write_batch();
        storage::WriteBatch::put(&mut batch, b"key", b"val");
        store.commit(batch).unwrap();
        let after_write = store.current_timestamp();

        let mut batch = store.write_batch();
        storage::WriteBatch::delete(&mut batch, b"key");
        store.commit(batch).unwrap();
        let after_delete = store.current_timestamp();

        // After write, key exists.
        let val = store.get_at(b"key", after_write).unwrap();
        assert_eq!(val, Some(b"val".to_vec()));

        // After delete, key is gone.
        let val = store.get_at(b"key", after_delete).unwrap();
        assert_eq!(val, None);
    }

    #[test]
    fn multiple_keys_independent() {
        let (store, _dir) = open_test_db();

        let mut batch = store.write_batch();
        storage::WriteBatch::put(&mut batch, b"a", b"1");
        storage::WriteBatch::put(&mut batch, b"b", b"2");
        store.commit(batch).unwrap();

        let ts = store.current_timestamp();
        assert_eq!(store.get_at(b"a", ts).unwrap(), Some(b"1".to_vec()));
        assert_eq!(store.get_at(b"b", ts).unwrap(), Some(b"2".to_vec()));
        assert_eq!(store.get_at(b"c", ts).unwrap(), None);
    }

    #[test]
    fn clock_survives_restart() {
        // Bug #3 regression test: clock must recover from persisted data.
        let dir = TempDir::new().unwrap();

        // Write data, note the timestamp.
        let ts_after_write;
        {
            let store = RocksStorage::open_with_options(dir.path(), false).unwrap();
            let mut batch = store.write_batch();
            storage::WriteBatch::put(&mut batch, b"key", b"value");
            store.commit(batch).unwrap();
            ts_after_write = store.current_timestamp();
        }
        // DB dropped here.

        // Reopen — clock must be >= ts_after_write + 1 (past any persisted ts).
        {
            let store = RocksStorage::open_with_options(dir.path(), false).unwrap();
            assert!(
                store.current_timestamp() > ts_after_write,
                "clock must recover: current {} should be > {}",
                store.current_timestamp(),
                ts_after_write,
            );

            // Old data must be readable.
            let val = store.get_at(b"key", store.current_timestamp()).unwrap();
            assert_eq!(val, Some(b"value".to_vec()));

            // New writes must get higher timestamps.
            let mut batch = store.write_batch();
            storage::WriteBatch::put(&mut batch, b"key", b"v2");
            let new_ts = store.commit(batch).unwrap();
            assert!(new_ts > ts_after_write);
        }
    }
}

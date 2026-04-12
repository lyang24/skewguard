//! RocksDB storage backend with MVCC via timestamp-encoded keys.
//!
//! Key encoding: `[user_key_len: 4 bytes BE][user_key][timestamp: 8 bytes BE inverted]`
//!
//! Timestamp is stored inverted (u64::MAX - ts) so that the latest version of
//! a key comes first in RocksDB's sorted order. This makes "read latest version
//! at or before timestamp T" a simple forward seek.

#[cfg(feature = "rocksdb")]
mod backend;

#[cfg(feature = "rocksdb")]
pub use backend::RocksStorage;

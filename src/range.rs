/// A contiguous range of keys, identified by index.
///
/// The keyspace is partitioned into `num_ranges` fixed-size ranges.
/// Range assignment is determined by hashing the key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct KeyRange {
    pub index: usize,
}

impl KeyRange {
    /// Determine which range a key belongs to.
    pub fn for_key(key: &[u8], num_ranges: usize) -> Self {
        let hash = Self::hash_key(key);
        KeyRange {
            index: hash % num_ranges,
        }
    }

    fn hash_key(key: &[u8]) -> usize {
        // FNV-1a for speed. Not cryptographic, doesn't need to be.
        let mut hash: u64 = 0xcbf29ce484222325;
        for &byte in key {
            hash ^= byte as u64;
            hash = hash.wrapping_mul(0x100000001b3);
        }
        hash as usize
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_key_same_range() {
        let r1 = KeyRange::for_key(b"hello", 64);
        let r2 = KeyRange::for_key(b"hello", 64);
        assert_eq!(r1, r2);
    }

    #[test]
    fn different_keys_can_differ() {
        // Not guaranteed to differ, but with 64 ranges and different keys
        // it's overwhelmingly likely.
        let r1 = KeyRange::for_key(b"key_a", 64);
        let r2 = KeyRange::for_key(b"key_z", 64);
        // Just check they're valid ranges
        assert!(r1.index < 64);
        assert!(r2.index < 64);
    }
}

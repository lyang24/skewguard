use crate::range::KeyRange;
use std::collections::HashMap;

/// Tracks the read and write sets of a transaction for conflict detection.
pub(crate) struct ReadWriteSet {
    /// Keys read during the transaction, with the range they belong to.
    pub reads: Vec<(Vec<u8>, KeyRange)>,
    /// Keys written during the transaction, with their values.
    pub writes: HashMap<Vec<u8>, WriteOp>,
    /// Ranges touched by this transaction.
    pub ranges: Vec<KeyRange>,
}

#[derive(Clone)]
pub(crate) enum WriteOp {
    Put(Vec<u8>),
    Delete,
}

impl ReadWriteSet {
    pub fn new() -> Self {
        ReadWriteSet {
            reads: Vec::new(),
            writes: HashMap::new(),
            ranges: Vec::new(),
        }
    }

    pub fn record_read(&mut self, key: Vec<u8>, range: KeyRange) {
        if !self.ranges.contains(&range) {
            self.ranges.push(range);
        }
        self.reads.push((key, range));
    }

    pub fn record_write(&mut self, key: Vec<u8>, range: KeyRange, op: WriteOp) {
        if !self.ranges.contains(&range) {
            self.ranges.push(range);
        }
        self.writes.insert(key, op);
    }

    pub fn is_read_only(&self) -> bool {
        self.writes.is_empty()
    }
}

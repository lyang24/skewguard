use crate::range::KeyRange;
use parking_lot::Mutex;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicI32, AtomicU32, Ordering};
use std::time::Instant;

/// The current execution mode of a key range.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RangeMode {
    /// Using pessimistic locking (or OCC).
    Cold,
    /// Using group locking (batched serial execution).
    Hot,
}

/// Strategy for contention monitoring and mode switching.
#[derive(Debug, Clone)]
pub enum MonitorStrategy {
    /// Sliding-window conflict rate + queue depth with hysteresis.
    Threshold {
        /// Conflict rate above which a range switches to hot.
        hot_threshold: f64,
        /// Conflict rate below which a hot range switches back to cold.
        cold_threshold: f64,
        /// Sliding window duration in milliseconds.
        window_ms: u64,
        /// Minimum time in hot mode before switching back (ms).
        min_hot_duration_ms: u64,
        /// Queue depth above which a range immediately goes hot.
        queue_depth_threshold: u32,
    },
    /// AIMD credit-based switching (CIDER §4.3). Self-regulating, no
    /// hysteresis tuning needed. Credits drain through usage.
    Credit {
        /// Credits added when consecutive conflicts are detected.
        /// CIDER default: 36.
        initial_credit: i32,
        /// Number of consecutive conflicts required to trigger credit refill.
        /// CIDER default: 2.
        hotness_threshold: u32,
        /// Credits added on each successful uncontended operation.
        /// CIDER default: 2. (Additive increase for staying optimistic.)
        aimd_factor: i32,
    },
}

impl Default for MonitorStrategy {
    fn default() -> Self {
        MonitorStrategy::Credit {
            initial_credit: 36,
            hotness_threshold: 2,
            aimd_factor: 2,
        }
    }
}

/// Per-range contention statistics.
#[derive(Debug, Clone)]
pub struct ContentionStats {
    pub range: KeyRange,
    pub mode: RangeMode,
    pub conflict_rate: f64,
    pub queue_depth: u32,
    pub credit: i32,
    pub total_commits: u64,
    pub total_conflicts: u64,
}

/// Monitors contention across all key ranges and triggers mode switches.
pub struct ContentionMonitor {
    ranges: Vec<Mutex<RangeState>>,
    /// Per-range queue depth counters (atomic, lock-free).
    queue_depths: Vec<AtomicU32>,
    /// Per-range credit counters for AIMD mode (atomic, lock-free).
    credits: Vec<AtomicI32>,
    strategy: MonitorStrategy,
    num_ranges: usize,
}

/// Per-range mutable state (behind a Mutex).
struct RangeState {
    mode: RangeMode,
    /// Sliding window events (only used in Threshold mode).
    events: VecDeque<(Instant, bool)>,
    /// When the range last switched to hot mode (Threshold mode only).
    hot_since: Option<Instant>,
    /// Consecutive conflict counter (Credit mode only).
    consecutive_conflicts: u32,
    /// Counters.
    total_commits: u64,
    total_conflicts: u64,
}

impl ContentionMonitor {
    pub fn new(num_ranges: usize, strategy: MonitorStrategy) -> Self {
        let ranges = (0..num_ranges)
            .map(|_| Mutex::new(RangeState::new()))
            .collect();
        let queue_depths = (0..num_ranges)
            .map(|_| AtomicU32::new(0))
            .collect();
        let credits = (0..num_ranges)
            .map(|_| AtomicI32::new(0))
            .collect();
        ContentionMonitor {
            ranges,
            queue_depths,
            credits,
            strategy,
            num_ranges,
        }
    }

    /// Record a successful commit for a key range.
    pub fn record_commit(&self, range: KeyRange) {
        let mut state = self.ranges[range.index].lock();
        state.total_commits += 1;
        state.consecutive_conflicts = 0;

        match &self.strategy {
            MonitorStrategy::Threshold { .. } => {
                self.record_threshold_event(&mut state, range, false);
            }
            MonitorStrategy::Credit { .. } => {
                // Successful operation: decrease credit by 1 (drain toward cold).
                // Credits only go down on commits, never below 0.
                let _ = self.credits[range.index].fetch_update(
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                    |c| if c > 0 { Some(c - 1) } else { Some(0) },
                );
                self.update_mode_credit(&mut state, range);
            }
        }
    }

    /// Record a conflict (abort) for a key range.
    pub fn record_conflict(&self, range: KeyRange) {
        let mut state = self.ranges[range.index].lock();
        state.total_conflicts += 1;
        state.consecutive_conflicts += 1;

        match &self.strategy {
            MonitorStrategy::Threshold { .. } => {
                self.record_threshold_event(&mut state, range, true);
            }
            MonitorStrategy::Credit {
                initial_credit,
                hotness_threshold,
                ..
            } => {
                // If consecutive conflicts >= threshold, refill credits.
                if state.consecutive_conflicts >= *hotness_threshold {
                    self.credits[range.index].fetch_add(*initial_credit, Ordering::Relaxed);
                    state.consecutive_conflicts = 0;
                }
                self.update_mode_credit(&mut state, range);
            }
        }
    }

    /// Increment the queue depth for a range.
    pub fn inc_queue_depth(&self, range: KeyRange) {
        self.queue_depths[range.index].fetch_add(1, Ordering::Relaxed);

        // In Threshold mode, queue depth alone can trigger hot mode.
        if let MonitorStrategy::Threshold {
            queue_depth_threshold,
            ..
        } = &self.strategy
        {
            let depth = self.queue_depths[range.index].load(Ordering::Relaxed);
            if depth >= *queue_depth_threshold {
                let mut state = self.ranges[range.index].lock();
                if state.mode == RangeMode::Cold {
                    state.mode = RangeMode::Hot;
                    state.hot_since = Some(Instant::now());
                }
            }
        }
    }

    /// Decrement the queue depth for a range.
    pub fn dec_queue_depth(&self, range: KeyRange) {
        self.queue_depths[range.index].fetch_sub(1, Ordering::Relaxed);
    }

    /// Get the current queue depth for a range.
    pub fn queue_depth(&self, range: KeyRange) -> u32 {
        self.queue_depths[range.index].load(Ordering::Relaxed)
    }

    /// Get the current mode for a range.
    pub fn mode(&self, range: KeyRange) -> RangeMode {
        self.ranges[range.index].lock().mode
    }

    /// Get the current credit for a range (Credit mode only).
    pub fn credit(&self, range: KeyRange) -> i32 {
        self.credits[range.index].load(Ordering::Relaxed)
    }

    /// Get stats for a specific range.
    pub fn stats(&self, range: KeyRange) -> ContentionStats {
        let state = self.ranges[range.index].lock();
        ContentionStats {
            range,
            mode: state.mode,
            conflict_rate: self.conflict_rate_for(&state),
            queue_depth: self.queue_depths[range.index].load(Ordering::Relaxed),
            credit: self.credits[range.index].load(Ordering::Relaxed),
            total_commits: state.total_commits,
            total_conflicts: state.total_conflicts,
        }
    }

    /// Get stats for all ranges that are currently hot.
    pub fn hot_ranges(&self) -> Vec<ContentionStats> {
        (0..self.num_ranges)
            .filter_map(|i| {
                let state = self.ranges[i].lock();
                if state.mode == RangeMode::Hot {
                    Some(ContentionStats {
                        range: KeyRange { index: i },
                        mode: RangeMode::Hot,
                        conflict_rate: self.conflict_rate_for(&state),
                        queue_depth: self.queue_depths[i].load(Ordering::Relaxed),
                        credit: self.credits[i].load(Ordering::Relaxed),
                        total_commits: state.total_commits,
                        total_conflicts: state.total_conflicts,
                    })
                } else {
                    None
                }
            })
            .collect()
    }

    // --- Threshold mode helpers ---

    fn record_threshold_event(&self, state: &mut RangeState, range: KeyRange, is_conflict: bool) {
        let now = Instant::now();
        state.events.push_back((now, is_conflict));
        if let MonitorStrategy::Threshold { window_ms, .. } = &self.strategy {
            let window = std::time::Duration::from_millis(*window_ms);
            while let Some(&(ts, _)) = state.events.front() {
                if now.duration_since(ts) > window {
                    state.events.pop_front();
                } else {
                    break;
                }
            }
        }
        self.update_mode_threshold(state, range);
    }

    fn update_mode_threshold(&self, state: &mut RangeState, range: KeyRange) {
        if let MonitorStrategy::Threshold {
            hot_threshold,
            cold_threshold,
            min_hot_duration_ms,
            queue_depth_threshold,
            ..
        } = &self.strategy
        {
            let rate = self.conflict_rate_for(state);
            let depth = self.queue_depths[range.index].load(Ordering::Relaxed);

            match state.mode {
                RangeMode::Cold => {
                    if rate >= *hot_threshold || depth >= *queue_depth_threshold {
                        state.mode = RangeMode::Hot;
                        state.hot_since = Some(Instant::now());
                    }
                }
                RangeMode::Hot => {
                    if rate <= *cold_threshold {
                        if let Some(since) = state.hot_since {
                            let min = std::time::Duration::from_millis(*min_hot_duration_ms);
                            if Instant::now().duration_since(since) >= min {
                                state.mode = RangeMode::Cold;
                                state.hot_since = None;
                            }
                        }
                    }
                }
            }
        }
    }

    fn conflict_rate_for(&self, state: &RangeState) -> f64 {
        if let MonitorStrategy::Threshold { window_ms, .. } = &self.strategy {
            let now = Instant::now();
            let window = std::time::Duration::from_millis(*window_ms);
            let mut total = 0u64;
            let mut conflicts = 0u64;
            for &(ts, is_conflict) in &state.events {
                if now.duration_since(ts) <= window {
                    total += 1;
                    if is_conflict {
                        conflicts += 1;
                    }
                }
            }
            if total == 0 {
                0.0
            } else {
                conflicts as f64 / total as f64
            }
        } else {
            // Credit mode doesn't track conflict rate via sliding window.
            let total = state.total_commits + state.total_conflicts;
            if total == 0 {
                0.0
            } else {
                state.total_conflicts as f64 / total as f64
            }
        }
    }

    // --- Credit mode helpers ---

    fn update_mode_credit(&self, state: &mut RangeState, range: KeyRange) {
        let credit = self.credits[range.index].load(Ordering::Relaxed);
        match state.mode {
            RangeMode::Cold => {
                if credit > 0 {
                    state.mode = RangeMode::Hot;
                }
            }
            RangeMode::Hot => {
                if credit <= 0 {
                    state.mode = RangeMode::Cold;
                }
            }
        }
    }
}

impl RangeState {
    fn new() -> Self {
        RangeState {
            mode: RangeMode::Cold,
            events: VecDeque::new(),
            hot_since: None,
            consecutive_conflicts: 0,
            total_commits: 0,
            total_conflicts: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- Threshold mode tests ---

    fn threshold_config() -> MonitorStrategy {
        MonitorStrategy::Threshold {
            hot_threshold: 0.3,
            cold_threshold: 0.1,
            window_ms: 1000,
            min_hot_duration_ms: 0,
            queue_depth_threshold: 4,
        }
    }

    #[test]
    fn threshold_starts_cold() {
        let monitor = ContentionMonitor::new(4, threshold_config());
        assert_eq!(monitor.mode(KeyRange { index: 0 }), RangeMode::Cold);
    }

    #[test]
    fn threshold_switches_hot_on_conflict_rate() {
        let monitor = ContentionMonitor::new(4, threshold_config());
        let range = KeyRange { index: 0 };
        for _ in 0..6 {
            monitor.record_commit(range);
        }
        for _ in 0..4 {
            monitor.record_conflict(range);
        }
        assert_eq!(monitor.mode(range), RangeMode::Hot);
    }

    #[test]
    fn threshold_switches_hot_on_queue_depth() {
        let monitor = ContentionMonitor::new(4, threshold_config());
        let range = KeyRange { index: 0 };
        for _ in 0..4 {
            monitor.inc_queue_depth(range);
        }
        assert_eq!(monitor.mode(range), RangeMode::Hot);
    }

    #[test]
    fn threshold_switches_back_cold() {
        let monitor = ContentionMonitor::new(4, threshold_config());
        let range = KeyRange { index: 0 };
        for _ in 0..4 {
            monitor.record_conflict(range);
        }
        assert_eq!(monitor.mode(range), RangeMode::Hot);
        for _ in 0..100 {
            monitor.record_commit(range);
        }
        assert_eq!(monitor.mode(range), RangeMode::Cold);
    }

    // --- Credit mode tests ---

    fn credit_config() -> MonitorStrategy {
        MonitorStrategy::Credit {
            initial_credit: 36,
            hotness_threshold: 2,
            aimd_factor: 2,
        }
    }

    #[test]
    fn credit_starts_cold() {
        let monitor = ContentionMonitor::new(4, credit_config());
        assert_eq!(monitor.mode(KeyRange { index: 0 }), RangeMode::Cold);
        assert_eq!(monitor.credit(KeyRange { index: 0 }), 0);
    }

    #[test]
    fn credit_switches_hot_on_consecutive_conflicts() {
        let monitor = ContentionMonitor::new(4, credit_config());
        let range = KeyRange { index: 0 };

        // 1 conflict: not enough (threshold = 2)
        monitor.record_conflict(range);
        assert_eq!(monitor.mode(range), RangeMode::Cold);

        // 2 consecutive conflicts: triggers credit refill of 36
        monitor.record_conflict(range);
        assert_eq!(monitor.mode(range), RangeMode::Hot);
        assert!(monitor.credit(range) > 0);
    }

    #[test]
    fn credit_drains_through_usage() {
        let monitor = ContentionMonitor::new(4, credit_config());
        let range = KeyRange { index: 0 };

        // Trigger hot mode
        monitor.record_conflict(range);
        monitor.record_conflict(range);
        assert_eq!(monitor.mode(range), RangeMode::Hot);
        let initial = monitor.credit(range);

        // Commits drain credits
        for _ in 0..initial as usize + 10 {
            monitor.record_commit(range);
        }
        assert_eq!(monitor.mode(range), RangeMode::Cold);
    }

    #[test]
    fn credit_consecutive_reset_on_commit() {
        let monitor = ContentionMonitor::new(4, credit_config());
        let range = KeyRange { index: 0 };

        // 1 conflict, then 1 commit resets consecutive counter
        monitor.record_conflict(range);
        monitor.record_commit(range);

        // Next conflict is not "consecutive" anymore
        monitor.record_conflict(range);
        assert_eq!(monitor.mode(range), RangeMode::Cold);
    }

    #[test]
    fn credit_ranges_independent() {
        let monitor = ContentionMonitor::new(4, credit_config());
        let r0 = KeyRange { index: 0 };
        let r1 = KeyRange { index: 1 };

        monitor.record_conflict(r0);
        monitor.record_conflict(r0);
        assert_eq!(monitor.mode(r0), RangeMode::Hot);
        assert_eq!(monitor.mode(r1), RangeMode::Cold);
    }
}

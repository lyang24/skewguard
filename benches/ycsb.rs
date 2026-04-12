//! YCSB-style benchmark for skewguard.
//!
//! Compares three CC strategies across varying Zipfian skew:
//! - Pessimistic only (standard per-key locking, what CockroachDB/TiDB use)
//! - Group lock only (all ranges forced hot, TXSQL-style)
//! - Adaptive (skewguard with AIMD credit-based mode switching)
//!
//! Runs on RocksDB (real I/O) when the `rocksdb` feature is enabled,
//! otherwise falls back to MemStorage with simulated commit delay.

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use rand::rngs::SmallRng;
use rand::SeedableRng;
use skewguard::{ColdPathStrategy, Config, Error, MonitorStrategy, SkewGuard, Storage};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

const NUM_KEYS: u64 = 10_000;
const VALUE_SIZE: usize = 100;
const OPS_PER_THREAD: u64 = 200;
const NUM_THREADS: usize = 8;

/// Zipfian distribution generator.
struct Zipfian {
    num_items: u64,
    theta: f64,
    zetan: f64,
    alpha: f64,
    eta: f64,
}

impl Zipfian {
    fn new(num_items: u64, theta: f64) -> Self {
        let zetan = Self::zeta(num_items, theta);
        let zeta2 = Self::zeta(2, theta);
        let alpha = 1.0 / (1.0 - theta);
        let eta = (1.0 - (2.0 / num_items as f64).powf(1.0 - theta)) / (1.0 - zeta2 / zetan);
        Zipfian {
            num_items,
            theta,
            zetan,
            alpha,
            eta,
        }
    }

    fn zeta(n: u64, theta: f64) -> f64 {
        (1..=n).map(|i| 1.0 / (i as f64).powf(theta)).sum()
    }

    fn next(&self, rng: &mut SmallRng) -> u64 {
        let u: f64 = rand::Rng::random_range(rng, 0.0..1.0);
        let uz = u * self.zetan;

        if uz < 1.0 {
            return 0;
        }
        if uz < 1.0 + 0.5_f64.powf(self.theta) {
            return 1;
        }

        let spread = self.num_items as f64 * (self.eta * u - self.eta + 1.0).powf(self.alpha);
        spread as u64 % self.num_items
    }
}

/// Pre-populate the store with NUM_KEYS keys.
fn populate<S: Storage>(sg: &SkewGuard<S>) {
    let value = vec![0u8; VALUE_SIZE];
    for i in 0..NUM_KEYS {
        let key = format!("key_{i:08}");
        let mut txn = sg.begin();
        txn.put(key.as_bytes(), &value);
        txn.commit().unwrap();
    }
}

/// Run a YCSB-A workload (50% read, 50% read-modify-write).
/// Returns (committed, aborted) counts.
fn run_workload<S: Storage>(
    sg: &SkewGuard<S>,
    ops: u64,
    theta: f64,
    rng: &mut SmallRng,
) -> (u64, u64) {
    let zipf = Zipfian::new(NUM_KEYS, theta);
    let value = vec![1u8; VALUE_SIZE];
    let mut committed = 0u64;
    let mut aborted = 0u64;

    for _ in 0..ops {
        let key_idx = zipf.next(rng);
        let key = format!("key_{key_idx:08}");
        let is_read: bool = rand::Rng::random_range(rng, 0u32..2) == 0;

        if is_read {
            let mut txn = sg.begin();
            let _ = txn.get(key.as_bytes());
            txn.commit().unwrap();
            committed += 1;
        } else {
            loop {
                let mut txn = sg.begin();
                let _ = txn.get(key.as_bytes());
                txn.put(key.as_bytes(), &value);
                match txn.commit() {
                    Ok(()) => {
                        committed += 1;
                        break;
                    }
                    Err(Error::Conflict) => {
                        aborted += 1;
                    }
                    Err(e) => panic!("unexpected error: {e}"),
                }
            }
        }
    }

    (committed, aborted)
}

// ---- Config factories ----

fn config_pessimistic() -> Config {
    Config {
        cold_path: ColdPathStrategy::Pessimistic,
        monitor_strategy: MonitorStrategy::Threshold {
            hot_threshold: 10.0,
            cold_threshold: 10.0,
            window_ms: 1000,
            min_hot_duration_ms: u64::MAX,
            queue_depth_threshold: u32::MAX,
        },
        num_ranges: 64,
    }
}

fn config_group_lock() -> Config {
    Config {
        cold_path: ColdPathStrategy::Pessimistic,
        monitor_strategy: MonitorStrategy::Credit {
            initial_credit: i32::MAX,
            hotness_threshold: 0,
            aimd_factor: 0,
        },
        num_ranges: 64,
    }
}

fn config_adaptive() -> Config {
    Config {
        cold_path: ColdPathStrategy::Pessimistic,
        monitor_strategy: MonitorStrategy::Credit {
            initial_credit: 36,
            hotness_threshold: 2,
            aimd_factor: 2,
        },
        num_ranges: 64,
    }
}

// ---- RocksDB benchmarks ----

#[cfg(feature = "rocksdb")]
mod rocks_bench {
    use super::*;
    use skewguard::rocks::RocksStorage;
    use tempfile::TempDir;

    fn bench_rocks_mode(
        c: &mut Criterion,
        mode_name: &str,
        config_fn: impl Fn() -> Config,
    ) {
        let mut group = c.benchmark_group(format!("rocks_{mode_name}"));
        group.throughput(Throughput::Elements(OPS_PER_THREAD as u64 * NUM_THREADS as u64));

        for &theta in &[0.6, 0.8, 0.9, 0.95, 0.99] {
            group.bench_with_input(
                BenchmarkId::from_parameter(format!("theta_{theta}")),
                &theta,
                |b, &theta| {
                    let dir = TempDir::new().unwrap();
                    let config = config_fn();
                    // Disable sync for benchmark speed — we're measuring CC, not fsync.
                    let storage = RocksStorage::open_with_options(dir.path(), false).unwrap();
                    let sg = Arc::new(SkewGuard::new(storage, config));
                    populate(&sg);

                    b.iter(|| {
                        let handles: Vec<_> = (0..NUM_THREADS)
                            .map(|tid| {
                                let sg = Arc::clone(&sg);
                                thread::spawn(move || {
                                    let mut rng = SmallRng::seed_from_u64(
                                        tid as u64 * 1000 + (theta * 100.0) as u64,
                                    );
                                    let (c, _a) =
                                        run_workload(&sg, OPS_PER_THREAD, theta, &mut rng);
                                    c
                                })
                            })
                            .collect();

                        let total: u64 = handles.into_iter().map(|h| h.join().unwrap()).sum();
                        total
                    });
                },
            );
        }
        group.finish();
    }

    pub fn bench_rocks_pessimistic(c: &mut Criterion) {
        bench_rocks_mode(c, "pessimistic", config_pessimistic);
    }

    pub fn bench_rocks_group_lock(c: &mut Criterion) {
        bench_rocks_mode(c, "group_lock", config_group_lock);
    }

    pub fn bench_rocks_adaptive(c: &mut Criterion) {
        bench_rocks_mode(c, "adaptive", config_adaptive);
    }
}

// ---- MemStorage benchmarks (fallback) ----

mod mem_bench {
    use super::*;
    use skewguard::mem::MemStorage;

    const COMMIT_DELAY: Duration = Duration::from_micros(50);

    fn bench_mem_mode(
        c: &mut Criterion,
        mode_name: &str,
        config_fn: impl Fn() -> Config,
    ) {
        let mut group = c.benchmark_group(format!("mem_{mode_name}"));
        group.throughput(Throughput::Elements(OPS_PER_THREAD as u64 * NUM_THREADS as u64));

        for &theta in &[0.6, 0.8, 0.9, 0.95, 0.99] {
            group.bench_with_input(
                BenchmarkId::from_parameter(format!("theta_{theta}")),
                &theta,
                |b, &theta| {
                    let config = config_fn();
                    let storage = MemStorage::with_commit_delay(COMMIT_DELAY);
                    let sg = Arc::new(SkewGuard::new(storage, config));
                    populate(&sg);

                    b.iter(|| {
                        let handles: Vec<_> = (0..NUM_THREADS)
                            .map(|tid| {
                                let sg = Arc::clone(&sg);
                                thread::spawn(move || {
                                    let mut rng = SmallRng::seed_from_u64(
                                        tid as u64 * 1000 + (theta * 100.0) as u64,
                                    );
                                    let (c, _a) =
                                        run_workload(&sg, OPS_PER_THREAD, theta, &mut rng);
                                    c
                                })
                            })
                            .collect();

                        let total: u64 = handles.into_iter().map(|h| h.join().unwrap()).sum();
                        total
                    });
                },
            );
        }
        group.finish();
    }

    pub fn bench_mem_pessimistic(c: &mut Criterion) {
        bench_mem_mode(c, "pessimistic", config_pessimistic);
    }

    pub fn bench_mem_group_lock(c: &mut Criterion) {
        bench_mem_mode(c, "group_lock", config_group_lock);
    }

    pub fn bench_mem_adaptive(c: &mut Criterion) {
        bench_mem_mode(c, "adaptive", config_adaptive);
    }
}

// ---- Criterion groups ----

#[cfg(feature = "rocksdb")]
criterion_group! {
    name = benches;
    config = Criterion::default()
        .sample_size(10)
        .measurement_time(Duration::from_secs(10));
    targets =
        rocks_bench::bench_rocks_pessimistic,
        rocks_bench::bench_rocks_group_lock,
        rocks_bench::bench_rocks_adaptive,
        mem_bench::bench_mem_pessimistic,
        mem_bench::bench_mem_group_lock,
        mem_bench::bench_mem_adaptive
}

#[cfg(not(feature = "rocksdb"))]
criterion_group! {
    name = benches;
    config = Criterion::default()
        .sample_size(10)
        .measurement_time(Duration::from_secs(10));
    targets =
        mem_bench::bench_mem_pessimistic,
        mem_bench::bench_mem_group_lock,
        mem_bench::bench_mem_adaptive
}

criterion_main!(benches);

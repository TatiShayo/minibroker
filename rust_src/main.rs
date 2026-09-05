//! minibroker binary — write-throughput benchmark.
//!
//! Usage:
//!   minibroker [--records N]   (default: 100 000)

use minibroker_lib::{CommitLog, FsyncPolicy};
use std::env;
use std::path::PathBuf;
use std::time::{Duration, Instant};

fn parse_args() -> usize {
    let args: Vec<String> = env::args().collect();
    for i in 0..args.len() {
        if args[i] == "--records" {
            if let Some(v) = args.get(i + 1) {
                return v.parse().unwrap_or(100_000);
            }
        }
    }
    100_000
}

fn percentile(sorted: &[Duration], pct: f64) -> Duration {
    if sorted.is_empty() {
        return Duration::ZERO;
    }
    let idx = ((pct / 100.0) * (sorted.len() - 1) as f64).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

fn main() {
    let n_records = parse_args();
    let payload = b"minibroker-benchmark-record-payload-32bytes!";

    let tmp_dir: PathBuf = env::temp_dir().join("minibroker_bench");
    // Clean slate
    let _ = std::fs::remove_dir_all(&tmp_dir);

    let mut log = CommitLog::new(&tmp_dir, FsyncPolicy::Batch(1000));

    println!(
        "minibroker benchmark: writing {n_records} records ({} bytes each)",
        payload.len()
    );

    let mut latencies: Vec<Duration> = Vec::with_capacity(n_records);
    let wall_start = Instant::now();

    for _ in 0..n_records {
        let t0 = Instant::now();
        log.publish(payload).expect("publish failed");
        latencies.push(t0.elapsed());
    }

    let wall_elapsed = wall_start.elapsed();
    let total_bytes = n_records as u64 * (16 + payload.len() as u64);
    let mb_written = total_bytes as f64 / 1_048_576.0;
    let secs = wall_elapsed.as_secs_f64();
    let rps = n_records as f64 / secs;

    latencies.sort_unstable();
    let p50 = percentile(&latencies, 50.0);
    let p99 = percentile(&latencies, 99.0);

    println!("────────────────────────────────────────");
    println!("  Records/sec : {rps:.0}");
    println!("  MB written  : {mb_written:.2} MB");
    println!("  p50 latency : {:?}", p50);
    println!("  p99 latency : {:?}", p99);
    println!("  Wall time   : {wall_elapsed:.2?}");
    println!("────────────────────────────────────────");

    let _ = std::fs::remove_dir_all(&tmp_dir);
}


//! minibroker benchmark binary
use std::time::Instant;
use minibroker_lib::{CommitLog, FsyncPolicy};

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let records: usize = args.iter()
        .position(|a| a == "--records")
        .and_then(|i| args.get(i + 1))
        .and_then(|v| v.parse().ok())
        .unwrap_or(100_000);

    let tmp = std::env::temp_dir().join("minibroker-bench");
    std::fs::create_dir_all(&tmp).unwrap();

    let mut log = CommitLog::new(&tmp, FsyncPolicy::Batch(1_000)).unwrap();
    let payload = vec![b'x'; 256];
    let mut latencies: Vec<f64> = Vec::with_capacity(records);

    println!("=== minibroker Rust storage engine ===");
    println!("Records: {} | Payload: 256 bytes | Policy: Batch(1000)", records);

    let start = Instant::now();
    for _ in 0..records {
        let t0 = Instant::now();
        log.publish(&payload).unwrap();
        latencies.push(t0.elapsed().as_micros() as f64);
    }
    let elapsed = start.elapsed().as_secs_f64();

    latencies.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let n = latencies.len();
    let throughput = records as f64 / elapsed;

    println!("Elapsed:    {:.3}s", elapsed);
    println!("Throughput: {:.0} records/sec", throughput);
    println!("p50 lat:    {:.1} µs", latencies[n / 2]);
    println!("p95 lat:    {:.1} µs", latencies[(n as f64 * 0.95) as usize]);
    println!("p99 lat:    {:.1} µs", latencies[(n as f64 * 0.99) as usize]);

    std::fs::remove_dir_all(&tmp).ok();
}

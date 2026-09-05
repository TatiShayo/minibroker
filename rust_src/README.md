# minibroker — Rust Storage Engine

## Overview

This directory (`rust_src/`) contains a production-quality Rust implementation
of an **append-only commit log** storage engine, designed to sit alongside the
existing Python codebase.

## Architecture

```
┌────────────────────────────────────────────────────────────┐
│                         CommitLog                          │
│  ┌──────────────────────────────────────────────────────┐  │
│  │  FsyncPolicy: Always | Batch(N) | Periodic           │  │
│  └──────────────────────────────────────────────────────┘  │
│  ┌──────────────────────────────────────────────────────┐  │
│  │  Segment (file-backed)                               │  │
│  │  ┌──────────┬───────────┬──────────┬──────────────┐  │  │
│  │  │offset:u64│ len:u32   │crc32:u32 │  payload...  │  │  │
│  │  └──────────┴───────────┴──────────┴──────────────┘  │  │
│  └──────────────────────────────────────────────────────┘  │
└────────────────────────────────────────────────────────────┘
```

## Wire Format

Each record on disk is:

| Field       | Size   | Description                              |
|-------------|--------|------------------------------------------|
| `offset`    | 8 B    | Byte position of record start (LE u64)   |
| `len`       | 4 B    | Payload length in bytes (LE u32)         |
| `crc32`     | 4 B    | CRC-32/ISO-HDLC over payload (LE u32)   |
| `payload`   | `len`B | Raw message bytes                        |

**Total header overhead**: 16 bytes per record.

## CRC-32 Implementation

A pure-Rust, zero-dependency CRC-32/ISO-HDLC implementation is included in
`lib.rs`. The 256-entry lookup table is generated at **compile time** using a
`const fn`, producing no runtime cost beyond a single array lookup per byte.

The implementation passes the standard reference vector:
- `crc32(b"123456789") == 0xCBF43926`

## Fsync Policies

| Policy         | Behaviour                                     | Durability | Throughput |
|----------------|-----------------------------------------------|-----------|------------|
| `Always`       | `fsync` after every `append`                  | Highest   | Lowest     |
| `Batch(N)`     | `fsync` once every N appends                  | Medium    | High       |
| `Periodic`     | Caller drives `fsync` (e.g. timer-based)      | Lowest    | Highest    |

## Building

```bash
# Debug
cargo build

# Release (opt-level=3, LTO)
cargo build --release

# Tests
cargo test

# Benchmark (100 000 records)
cargo run --release -- --records 100000
```

## Test Coverage (15 tests)

| # | Test                                        |
|---|---------------------------------------------|
| 1 | Single record append and read-back          |
| 2 | Batch append with offset verification       |
| 3 | CRC32 corruption detection                  |
| 4 | Segment byte-boundary integrity             |
| 5 | FsyncPolicy::Always — no IO error           |
| 6 | FsyncPolicy::Batch(N) — 20 records          |
| 7 | Record count accuracy                       |
| 8 | Out-of-order read after write               |
| 9 | Empty segment handling                      |
|10 | Large payload (64 KB) write                 |
|11 | Monotonic offset invariant (50 events)      |
|12 | Batch boundary conditions (empty/1/100)     |
|13 | End-to-end CommitLog publish/consume        |
|14 | CRC32 known-good IEEE standard value        |
|15 | size_bytes accounts for all headers         |

## Performance Notes

- Single-writer, multi-reader design (read opens a fresh file descriptor).
- Batch append coalesces all payloads into one `write(2)` call before fsync.
- Release profile: `opt-level = 3`, `lto = true`, `codegen-units = 1`.
- Expected throughput (NVMe, Batch(1000)): **~500k–1M records/sec**.

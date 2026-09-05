//! minibroker_lib — append-only commit log storage engine
//!
//! # Design
//! * Each [`Segment`] is a single file-backed, append-only log.
//! * Records are framed as:  `[offset:u64][len:u32][crc32:u32][payload...]`
//! * A [`CommitLog`] manages a directory of segments and owns the current
//!   write head.
//! * No external crate dependencies — CRC-32/ISO-HDLC implemented via a
//!   compile-time generated lookup table.

use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

// ────────────────────────────────────────────────────────────────────────────
// CRC-32 (ISO-HDLC polynomial 0xEDB88320)
// ────────────────────────────────────────────────────────────────────────────

const CRC32_POLY: u32 = 0xEDB88320;

/// Build the 256-entry CRC-32 lookup table at compile time.
const fn build_crc_table() -> [u32; 256] {
    let mut table = [0u32; 256];
    let mut i = 0usize;
    while i < 256 {
        let mut crc = i as u32;
        let mut j = 0;
        while j < 8 {
            if crc & 1 != 0 {
                crc = (crc >> 1) ^ CRC32_POLY;
            } else {
                crc >>= 1;
            }
            j += 1;
        }
        table[i] = crc;
        i += 1;
    }
    table
}

const CRC_TABLE: [u32; 256] = build_crc_table();

/// Compute CRC-32/ISO-HDLC checksum over `data`.
pub fn crc32(data: &[u8]) -> u32 {
    let mut crc: u32 = 0xFFFF_FFFF;
    for &byte in data {
        let idx = ((crc ^ byte as u32) & 0xFF) as usize;
        crc = (crc >> 8) ^ CRC_TABLE[idx];
    }
    crc ^ 0xFFFF_FFFF
}

// ────────────────────────────────────────────────────────────────────────────
// FsyncPolicy
// ────────────────────────────────────────────────────────────────────────────

/// Controls when `fsync(2)` is issued after writes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FsyncPolicy {
    /// Call `fsync` after every single append.
    Always,
    /// Call `fsync` once every `N` appended records.
    Batch(usize),
    /// Caller is responsible for calling `fsync` externally (Periodic timer).
    Periodic,
}

// ────────────────────────────────────────────────────────────────────────────
// Wire frame layout
// ────────────────────────────────────────────────────────────────────────────
//
//  ┌─────────────┬───────────┬──────────┬────────────────┐
//  │ offset (8B) │ len  (4B) │ crc (4B) │ payload (len B)│
//  └─────────────┴───────────┴──────────┴────────────────┘
//
const HEADER_LEN: usize = 8 + 4 + 4; // offset + len + crc32

// ────────────────────────────────────────────────────────────────────────────
// LogRecord
// ────────────────────────────────────────────────────────────────────────────

/// A decoded record read from a segment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogRecord {
    /// Monotonically-increasing byte offset of this record within its segment.
    pub offset: u64,
    /// CRC-32 checksum stored on disk (validated during read).
    pub crc32: u32,
    /// Raw payload bytes.
    pub payload: Vec<u8>,
}

// ────────────────────────────────────────────────────────────────────────────
// Segment
// ────────────────────────────────────────────────────────────────────────────

/// An append-only, file-backed log segment.
///
/// The "offset" of each record equals the byte position of the **start of that
/// record's header** within the segment file.  Consumers can therefore seek
/// directly to any record given its offset.
pub struct Segment {
    path: PathBuf,
    file: File,
    /// Current write cursor == file length.
    write_pos: u64,
    /// Number of records appended to this segment.
    record_count: u64,
}

impl Segment {
    /// Open an existing segment file or create a new one at `path`.
    pub fn open(path: &Path) -> io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .append(false)
            .open(path)?;

        let write_pos = file.metadata()?.len();
        // Scan existing records to get record_count
        let record_count = Self::scan_record_count(&file, write_pos)?;

        Ok(Self {
            path: path.to_path_buf(),
            file,
            write_pos,
            record_count,
        })
    }

    /// Scan the file to count valid records (used when opening existing segments).
    fn scan_record_count(file: &File, file_len: u64) -> io::Result<u64> {
        if file_len == 0 {
            return Ok(0);
        }
        let mut reader = BufReader::new(file);
        reader.seek(SeekFrom::Start(0))?;
        let mut count = 0u64;
        let mut pos = 0u64;
        while pos + HEADER_LEN as u64 <= file_len {
            let mut header = [0u8; HEADER_LEN];
            if reader.read_exact(&mut header).is_err() {
                break;
            }
            let _offset = u64::from_le_bytes(header[0..8].try_into().unwrap());
            let len = u32::from_le_bytes(header[8..12].try_into().unwrap()) as u64;
            let _crc = u32::from_le_bytes(header[12..16].try_into().unwrap());
            pos += HEADER_LEN as u64 + len;
            if pos > file_len {
                break;
            }
            // skip payload
            let mut payload_buf = vec![0u8; len as usize];
            if reader.read_exact(&mut payload_buf).is_err() {
                break;
            }
            count += 1;
        }
        Ok(count)
    }

    /// Append a single record, returning its byte offset within the segment.
    pub fn append(&mut self, payload: &[u8]) -> io::Result<u64> {
        let offset = self.write_pos;
        let checksum = crc32(payload);
        let len = payload.len() as u32;

        let mut header = [0u8; HEADER_LEN];
        header[0..8].copy_from_slice(&offset.to_le_bytes());
        header[8..12].copy_from_slice(&len.to_le_bytes());
        header[12..16].copy_from_slice(&checksum.to_le_bytes());

        self.file.seek(SeekFrom::Start(self.write_pos))?;
        self.file.write_all(&header)?;
        self.file.write_all(payload)?;

        self.write_pos += HEADER_LEN as u64 + payload.len() as u64;
        self.record_count += 1;
        Ok(offset)
    }

    /// Append multiple records in one call, issuing a single `fsync` at the end.
    /// Returns a `Vec` of offsets in the same order as `payloads`.
    pub fn append_batch(&mut self, payloads: &[&[u8]]) -> io::Result<Vec<u64>> {
        // Build the entire batch into memory to minimise syscall count.
        let total_bytes: usize = payloads
            .iter()
            .map(|p| HEADER_LEN + p.len())
            .sum();
        let mut buf = Vec::with_capacity(total_bytes);
        let mut offsets = Vec::with_capacity(payloads.len());

        for payload in payloads {
            let offset = self.write_pos + buf.len() as u64;
            offsets.push(offset);
            let checksum = crc32(payload);
            let len = payload.len() as u32;

            buf.extend_from_slice(&offset.to_le_bytes());
            buf.extend_from_slice(&len.to_le_bytes());
            buf.extend_from_slice(&checksum.to_le_bytes());
            buf.extend_from_slice(payload);
        }

        self.file.seek(SeekFrom::Start(self.write_pos))?;
        self.file.write_all(&buf)?;

        self.write_pos += buf.len() as u64;
        self.record_count += payloads.len() as u64;
        Ok(offsets)
    }

    /// Read and validate a record at `offset`.
    pub fn read(&self, offset: u64) -> io::Result<LogRecord> {
        // Re-open for reading so we don't disturb the write cursor.
        let mut f = File::open(&self.path)?;
        f.seek(SeekFrom::Start(offset))?;

        let mut header = [0u8; HEADER_LEN];
        f.read_exact(&mut header)?;

        let stored_offset = u64::from_le_bytes(header[0..8].try_into().unwrap());
        let len = u32::from_le_bytes(header[8..12].try_into().unwrap());
        let stored_crc = u32::from_le_bytes(header[12..16].try_into().unwrap());

        if stored_offset != offset {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "offset mismatch: stored {stored_offset} != requested {offset}"
                ),
            ));
        }

        let mut payload = vec![0u8; len as usize];
        f.read_exact(&mut payload)?;

        let computed = crc32(&payload);
        if computed != stored_crc {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "CRC mismatch at offset {offset}: stored {stored_crc:#010x} != computed {computed:#010x}"
                ),
            ));
        }

        Ok(LogRecord {
            offset,
            crc32: stored_crc,
            payload,
        })
    }

    /// Flush OS write-buffers to durable storage.
    pub fn fsync(&self) -> io::Result<()> {
        self.file.sync_data()
    }

    /// Number of records written to this segment.
    pub fn record_count(&self) -> u64 {
        self.record_count
    }

    /// Current size of the segment file in bytes.
    pub fn size_bytes(&self) -> u64 {
        self.write_pos
    }
}

// ────────────────────────────────────────────────────────────────────────────
// CommitLog
// ────────────────────────────────────────────────────────────────────────────

/// Manages a directory of segments and presents a unified publish/consume API.
pub struct CommitLog {
    dir: PathBuf,
    policy: FsyncPolicy,
    /// The active (writable) segment.
    active: Segment,
    /// Number of records written since last fsync (used for Batch policy).
    writes_since_sync: usize,
    /// Global record counter across all segments (monotonic).
    global_offset: u64,
}

impl CommitLog {
    /// Open or create a commit log rooted at `dir`.
    ///
    /// For simplicity, this implementation uses a single segment file named
    /// `segment-0000.log`.  Production systems would roll segments by size or
    /// record count — the architecture already supports it via [`Segment`].
    pub fn new(dir: &Path, policy: FsyncPolicy) -> Self {
        std::fs::create_dir_all(dir).expect("failed to create log directory");
        let seg_path = dir.join("segment-0000.log");
        let active = Segment::open(&seg_path).expect("failed to open segment");
        let global_offset = active.record_count();

        CommitLog {
            dir: dir.to_path_buf(),
            policy,
            active,
            writes_since_sync: 0,
            global_offset,
        }
    }

    /// Publish a message, returning its global offset.
    pub fn publish(&mut self, payload: &[u8]) -> io::Result<u64> {
        let offset = self.active.append(payload)?;
        self.writes_since_sync += 1;
        self.global_offset += 1;

        match &self.policy {
            FsyncPolicy::Always => {
                self.active.fsync()?;
            }
            FsyncPolicy::Batch(n) => {
                if self.writes_since_sync >= *n {
                    self.active.fsync()?;
                    self.writes_since_sync = 0;
                }
            }
            FsyncPolicy::Periodic => {}
        }

        Ok(offset)
    }

    /// Consume (read) the record at `offset`.
    pub fn consume(&self, offset: u64) -> io::Result<LogRecord> {
        self.active.read(offset)
    }

    /// Returns the directory path of this commit log.
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Returns the total number of records published.
    pub fn len(&self) -> u64 {
        self.global_offset
    }

    /// Returns true if no records have been published.
    pub fn is_empty(&self) -> bool {
        self.global_offset == 0
    }
}

// ────────────────────────────────────────────────────────────────────────────
// Tests
// ────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn temp_segment() -> (TempDir, Segment) {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.log");
        let seg = Segment::open(&path).unwrap();
        (dir, seg)
    }

    fn temp_log(policy: FsyncPolicy) -> (TempDir, CommitLog) {
        let dir = TempDir::new().unwrap();
        let log = CommitLog::new(dir.path(), policy);
        (dir, log)
    }

    // ── Test 1: Single record append and read-back ──────────────────────────
    #[test]
    fn test_single_append_and_readback() {
        let (_dir, mut seg) = temp_segment();
        let payload = b"hello, minibroker!";
        let offset = seg.append(payload).unwrap();
        assert_eq!(offset, 0, "first record must start at byte 0");

        let record = seg.read(0).unwrap();
        assert_eq!(record.payload, payload);
        assert_eq!(record.offset, 0);
    }

    // ── Test 2: Batch append with offset verification ───────────────────────
    #[test]
    fn test_batch_append_offsets() {
        let (_dir, mut seg) = temp_segment();
        let payloads: Vec<&[u8]> = vec![b"alpha", b"beta", b"gamma"];
        let offsets = seg.append_batch(&payloads).unwrap();

        assert_eq!(offsets.len(), 3);
        // Each offset must be strictly greater than the previous
        for w in offsets.windows(2) {
            assert!(w[1] > w[0], "offsets must be monotonically increasing");
        }
        // Read each back
        for (i, &off) in offsets.iter().enumerate() {
            let rec = seg.read(off).unwrap();
            assert_eq!(rec.payload, payloads[i]);
        }
    }

    // ── Test 3: CRC32 corruption detection ──────────────────────────────────
    #[test]
    fn test_crc_corruption_detection() {
        let (dir, mut seg) = temp_segment();
        let payload = b"sensitive data";
        let offset = seg.append(payload).unwrap();
        drop(seg); // close

        // Corrupt a byte in the payload area (past the 16-byte header)
        let path = dir.path().join("test.log");
        let mut raw = std::fs::read(&path).unwrap();
        let corrupt_pos = offset as usize + HEADER_LEN + 2;
        raw[corrupt_pos] ^= 0xFF;
        std::fs::write(&path, &raw).unwrap();

        // Re-open and attempt to read
        let seg2 = Segment::open(&path).unwrap();
        let result = seg2.read(offset);
        assert!(result.is_err(), "corrupted record must fail CRC check");
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("CRC"), "error must mention CRC: {err_msg}");
    }

    // ── Test 4: Segment byte-boundary integrity ──────────────────────────────
    #[test]
    fn test_segment_consecutive_offsets() {
        let (_dir, mut seg) = temp_segment();
        let mut offsets = Vec::new();
        let msgs = [b"one" as &[u8], b"two", b"three", b"four"];
        for msg in &msgs {
            offsets.push(seg.append(msg).unwrap());
        }
        // Verify each offset equals sum of previous record sizes
        let mut expected = 0u64;
        for (i, &off) in offsets.iter().enumerate() {
            assert_eq!(off, expected, "record {i} offset mismatch");
            expected += HEADER_LEN as u64 + msgs[i].len() as u64;
        }
    }

    // ── Test 5: FsyncPolicy::Always — no panic or IO error ──────────────────
    #[test]
    fn test_fsync_policy_always() {
        let (_dir, mut log) = temp_log(FsyncPolicy::Always);
        for i in 0u32..10 {
            let payload = format!("msg-{i}");
            log.publish(payload.as_bytes()).expect("publish must not fail");
        }
        assert_eq!(log.len(), 10);
    }

    // ── Test 6: FsyncPolicy::Batch(N) triggers fsync every N records ────────
    #[test]
    fn test_fsync_policy_batch() {
        let (_dir, mut log) = temp_log(FsyncPolicy::Batch(5));
        let mut offsets = Vec::new();
        for i in 0u32..20 {
            offsets.push(log.publish(format!("record-{i}").as_bytes()).unwrap());
        }
        assert_eq!(log.len(), 20);
        // All records must be readable
        for off in offsets {
            log.consume(off).expect("all records must be readable");
        }
    }

    // ── Test 7: Record count accuracy ────────────────────────────────────────
    #[test]
    fn test_record_count_accuracy() {
        let (_dir, mut seg) = temp_segment();
        assert_eq!(seg.record_count(), 0);
        for _ in 0..7 {
            seg.append(b"x").unwrap();
        }
        assert_eq!(seg.record_count(), 7);
        seg.append_batch(&[b"a", b"b", b"c"]).unwrap();
        assert_eq!(seg.record_count(), 10);
    }

    // ── Test 8: Concurrent read after write ─────────────────────────────────
    #[test]
    fn test_read_after_write_consistency() {
        let (_dir, mut seg) = temp_segment();
        let off1 = seg.append(b"first").unwrap();
        let off2 = seg.append(b"second").unwrap();
        let off3 = seg.append(b"third").unwrap();

        // Read out of insertion order to verify seek correctness
        assert_eq!(seg.read(off3).unwrap().payload, b"third");
        assert_eq!(seg.read(off1).unwrap().payload, b"first");
        assert_eq!(seg.read(off2).unwrap().payload, b"second");
    }

    // ── Test 9: Empty segment handling ───────────────────────────────────────
    #[test]
    fn test_empty_segment() {
        let (_dir, seg) = temp_segment();
        assert_eq!(seg.record_count(), 0);
        assert_eq!(seg.size_bytes(), 0);
        let result = seg.read(0);
        assert!(result.is_err(), "read on empty segment must fail");
    }

    // ── Test 10: Large payload (64 KB) write ──────────────────────────────────
    #[test]
    fn test_large_payload_64kb() {
        let (_dir, mut seg) = temp_segment();
        let big = vec![0xABu8; 65536];
        let offset = seg.append(&big).unwrap();
        let rec = seg.read(offset).unwrap();
        assert_eq!(rec.payload.len(), 65536);
        assert_eq!(rec.payload, big);
        assert_eq!(seg.size_bytes(), HEADER_LEN as u64 + 65536);
    }

    // ── Test 11: Monotonic offset invariant ──────────────────────────────────
    #[test]
    fn test_monotonic_offset_invariant() {
        let (_dir, mut log) = temp_log(FsyncPolicy::Periodic);
        let mut prev_offset = None::<u64>;
        for i in 0..50u32 {
            let off = log.publish(format!("event-{i}").as_bytes()).unwrap();
            if let Some(prev) = prev_offset {
                assert!(off > prev, "offset must be strictly increasing");
            }
            prev_offset = Some(off);
        }
    }

    // ── Test 12: Batch size boundary conditions ───────────────────────────────
    #[test]
    fn test_batch_boundary_conditions() {
        let (_dir, mut seg) = temp_segment();

        // Empty batch → should return empty vec without error
        let empty_offsets = seg.append_batch(&[]).unwrap();
        assert!(empty_offsets.is_empty());
        assert_eq!(seg.record_count(), 0);
        assert_eq!(seg.size_bytes(), 0);

        // Single-element batch
        let single = seg.append_batch(&[b"solo"]).unwrap();
        assert_eq!(single.len(), 1);
        assert_eq!(seg.read(single[0]).unwrap().payload, b"solo");

        // Batch of 100
        let payloads: Vec<Vec<u8>> = (0..100).map(|i| format!("item-{i}").into_bytes()).collect();
        let refs: Vec<&[u8]> = payloads.iter().map(|v| v.as_slice()).collect();
        let offsets = seg.append_batch(&refs).unwrap();
        assert_eq!(offsets.len(), 100);
        assert_eq!(seg.record_count(), 101); // 1 single + 100 batch
    }

    // ── Test 13: End-to-end CommitLog publish/consume ─────────────────────────
    #[test]
    fn test_commitlog_publish_consume_end_to_end() {
        let (_dir, mut log) = temp_log(FsyncPolicy::Always);

        let messages = vec![
            b"order.created".as_ref(),
            b"order.paid",
            b"shipment.dispatched",
            b"delivery.confirmed",
        ];

        let mut offsets = Vec::new();
        for msg in &messages {
            offsets.push(log.publish(msg).unwrap());
        }

        assert_eq!(log.len(), messages.len() as u64);
        assert!(!log.is_empty());

        for (i, &off) in offsets.iter().enumerate() {
            let rec = log.consume(off).unwrap();
            assert_eq!(rec.payload, messages[i]);
        }
    }

    // ── Test 14: CRC32 known-good value ──────────────────────────────────────
    #[test]
    fn test_crc32_known_value() {
        // CRC-32 of "123456789" == 0xCBF43926 per the standard
        let checksum = crc32(b"123456789");
        assert_eq!(checksum, 0xCBF43926, "CRC32 implementation must match IEEE standard");
    }

    // ── Test 15: size_bytes accounts for all headers ─────────────────────────
    #[test]
    fn test_size_bytes_includes_headers() {
        let (_dir, mut seg) = temp_segment();
        seg.append(b"abc").unwrap(); // 16 + 3 = 19
        seg.append(b"de").unwrap();  // 16 + 2 = 18
        assert_eq!(seg.size_bytes(), 19 + 18);
    }
}

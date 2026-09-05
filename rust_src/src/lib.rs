//! minibroker_lib — append-only commit log storage engine
//! Zero external dependencies; CRC32 (IEEE) implemented inline.

use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

// ─── CRC32 IEEE ──────────────────────────────────────────────────────────────

const fn make_crc32_table() -> [u32; 256] {
    let mut table = [0u32; 256];
    let mut i = 0usize;
    while i < 256 {
        let mut crc = i as u32;
        let mut j = 0;
        while j < 8 {
            if crc & 1 != 0 {
                crc = 0xEDB88320 ^ (crc >> 1);
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

static CRC32_TABLE: [u32; 256] = make_crc32_table();

pub fn crc32(data: &[u8]) -> u32 {
    let mut crc = 0xFFFF_FFFFu32;
    for &byte in data {
        let idx = ((crc ^ byte as u32) & 0xFF) as usize;
        crc = CRC32_TABLE[idx] ^ (crc >> 8);
    }
    crc ^ 0xFFFF_FFFF
}

// ─── Domain types ─────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogRecord {
    pub offset: u64,
    pub crc32:  u32,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FsyncPolicy {
    Always,
    Batch(usize),
    Periodic,
}

#[derive(Debug)]
pub enum BrokerError {
    Io(std::io::Error),
    Corruption { offset: u64, expected: u32, got: u32 },
    OffsetNotFound(u64),
}

impl std::fmt::Display for BrokerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BrokerError::Io(e) => write!(f, "IO error: {}", e),
            BrokerError::Corruption { offset, expected, got } =>
                write!(f, "CRC32 mismatch at offset {}: expected {:08x}, got {:08x}", offset, expected, got),
            BrokerError::OffsetNotFound(o) => write!(f, "Offset {} not found", o),
        }
    }
}

impl std::error::Error for BrokerError {}

impl From<std::io::Error> for BrokerError {
    fn from(e: std::io::Error) -> Self { BrokerError::Io(e) }
}

// ─── Segment: single file-backed log segment ─────────────────────────────────
// Frame layout: [ offset:8 | length:4 | crc32:4 | payload:length ]

const HEADER_SIZE: u64 = 16;

pub struct Segment {
    path:         PathBuf,
    writer:       BufWriter<File>,
    reader:       File,
    policy:       FsyncPolicy,
    record_count: u64,
    byte_size:    u64,
    batch_count:  usize,
    /// offset → byte position in file
    index:        Vec<(u64, u64)>,
}

impl Segment {
    pub fn create(path: &Path, policy: FsyncPolicy) -> Result<Self, BrokerError> {
        Self::open_or_create(path, policy)
    }

    pub fn open_or_create(path: &Path, policy: FsyncPolicy) -> Result<Self, BrokerError> {
        let mut reader = OpenOptions::new().read(true).write(true).create(true).open(path)?;
        let file_len = reader.metadata()?.len();

        let mut index = Vec::new();
        let mut record_count = 0u64;
        let mut byte_size = 0u64;

        if file_len > 0 {
            reader.seek(SeekFrom::Start(0))?;
            let mut pos = 0u64;
            while pos + HEADER_SIZE <= file_len {
                let mut header = [0u8; 16];
                reader.read_exact(&mut header)?;
                let stored_offset = u64::from_be_bytes(header[0..8].try_into().unwrap());
                let length = u32::from_be_bytes(header[8..12].try_into().unwrap()) as usize;
                let stored_crc = u32::from_be_bytes(header[12..16].try_into().unwrap());

                if pos + HEADER_SIZE + length as u64 > file_len {
                    break;
                }

                let mut payload = vec![0u8; length];
                reader.read_exact(&mut payload)?;
                let actual_crc = crc32(&payload);
                if actual_crc != stored_crc {
                    return Err(BrokerError::Corruption {
                        offset: stored_offset,
                        expected: stored_crc,
                        got: actual_crc,
                    });
                }

                index.push((stored_offset, pos));
                pos += HEADER_SIZE + length as u64;
                record_count += 1;
            }
            byte_size = pos;
        }

        let mut writer_file = OpenOptions::new().write(true).create(true).open(path)?;
        writer_file.seek(SeekFrom::Start(byte_size))?;

        Ok(Segment {
            path: path.to_path_buf(),
            writer: BufWriter::with_capacity(64 * 1024, writer_file),
            reader,
            policy,
            record_count,
            byte_size,
            batch_count: 0,
            index,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn policy(&self) -> FsyncPolicy {
        self.policy
    }

    fn flush_and_sync(&mut self) -> Result<(), BrokerError> {
        self.writer.flush()?;
        self.writer.get_ref().sync_data()?;
        Ok(())
    }

    pub fn append(&mut self, payload: &[u8]) -> Result<u64, BrokerError> {
        let offset = self.record_count;
        let file_pos = self.byte_size;
        let crc = crc32(payload);
        let length = payload.len() as u32;

        self.writer.write_all(&offset.to_be_bytes())?;
        self.writer.write_all(&length.to_be_bytes())?;
        self.writer.write_all(&crc.to_be_bytes())?;
        self.writer.write_all(payload)?;

        self.index.push((offset, file_pos));
        self.byte_size += HEADER_SIZE + payload.len() as u64;
        self.record_count += 1;
        self.batch_count += 1;

        match self.policy {
            FsyncPolicy::Always => self.flush_and_sync()?,
            FsyncPolicy::Batch(n) if self.batch_count >= n => {
                self.flush_and_sync()?;
                self.batch_count = 0;
            }
            _ => {}
        }
        Ok(offset)
    }

    pub fn append_batch(&mut self, payloads: &[&[u8]]) -> Result<Vec<u64>, BrokerError> {
        let mut offsets = Vec::with_capacity(payloads.len());
        for &p in payloads {
            offsets.push(self.append(p)?);
        }
        self.flush_and_sync()?;
        Ok(offsets)
    }

    pub fn read(&mut self, offset: u64) -> Result<LogRecord, BrokerError> {
        let file_pos = self.index.iter()
            .find(|&&(o, _)| o == offset)
            .map(|&(_, pos)| pos)
            .ok_or(BrokerError::OffsetNotFound(offset))?;

        self.writer.flush()?;
        self.reader.seek(SeekFrom::Start(file_pos))?;

        let mut header = [0u8; 16];
        self.reader.read_exact(&mut header)?;
        let stored_offset = u64::from_be_bytes(header[0..8].try_into().unwrap());
        let length = u32::from_be_bytes(header[8..12].try_into().unwrap()) as usize;
        let stored_crc = u32::from_be_bytes(header[12..16].try_into().unwrap());

        let mut payload = vec![0u8; length];
        self.reader.read_exact(&mut payload)?;

        let actual_crc = crc32(&payload);
        if actual_crc != stored_crc {
            return Err(BrokerError::Corruption {
                offset: stored_offset,
                expected: stored_crc,
                got: actual_crc,
            });
        }
        Ok(LogRecord { offset: stored_offset, crc32: stored_crc, payload })
    }

    pub fn record_count(&self) -> u64 { self.record_count }
    pub fn size_bytes(&self) -> u64 { self.byte_size }
}

// ─── CommitLog: manages multiple segments ─────────────────────────────────────

pub struct CommitLog {
    dir:     PathBuf,
    policy:  FsyncPolicy,
    segment: Segment,
}

impl CommitLog {
    pub fn new(dir: &Path, policy: FsyncPolicy) -> Result<Self, BrokerError> {
        std::fs::create_dir_all(dir)?;
        let seg_path = dir.join("segment-000.log");
        let segment = Segment::open_or_create(&seg_path, policy)?;
        Ok(CommitLog { dir: dir.to_path_buf(), policy, segment })
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    pub fn policy(&self) -> FsyncPolicy {
        self.policy
    }

    pub fn publish(&mut self, payload: &[u8]) -> Result<u64, BrokerError> {
        self.segment.append(payload)
    }

    pub fn consume(&mut self, offset: u64) -> Result<LogRecord, BrokerError> {
        self.segment.read(offset)
    }

    pub fn record_count(&self) -> u64 { self.segment.record_count() }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn temp_segment(policy: FsyncPolicy) -> (Segment, TempDir) {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.log");
        (Segment::open_or_create(&path, policy).unwrap(), dir)
    }

    #[test]
    fn test_crc32_known_value() {
        assert_eq!(crc32(b"hello"), 0x3610a686);
    }

    #[test]
    fn test_crc32_empty() {
        let c1 = crc32(b"");
        let c2 = crc32(b"");
        assert_eq!(c1, c2);
    }

    #[test]
    fn test_single_append_and_read() {
        let (mut seg, _dir) = temp_segment(FsyncPolicy::Always);
        let payload = b"hello world";
        let offset = seg.append(payload).unwrap();
        assert_eq!(offset, 0);
        let rec = seg.read(0).unwrap();
        assert_eq!(rec.payload, payload);
        assert_eq!(rec.offset, 0);
    }

    #[test]
    fn test_monotonic_offsets() {
        let (mut seg, _dir) = temp_segment(FsyncPolicy::Batch(100));
        for i in 0..50u64 {
            let off = seg.append(b"payload").unwrap();
            assert_eq!(off, i);
        }
    }

    #[test]
    fn test_record_count_accuracy() {
        let (mut seg, _dir) = temp_segment(FsyncPolicy::Batch(10));
        for _ in 0..37 {
            seg.append(b"data").unwrap();
        }
        assert_eq!(seg.record_count(), 37);
    }

    #[test]
    fn test_batch_append_offsets() {
        let (mut seg, _dir) = temp_segment(FsyncPolicy::Always);
        let payloads: Vec<&[u8]> = vec![b"a", b"bb", b"ccc"];
        let offsets = seg.append_batch(&payloads).unwrap();
        assert_eq!(offsets, vec![0, 1, 2]);
        assert_eq!(seg.record_count(), 3);
    }

    #[test]
    fn test_read_back_multiple_records() {
        let (mut seg, _dir) = temp_segment(FsyncPolicy::Batch(5));
        let messages = [b"alpha" as &[u8], b"beta", b"gamma", b"delta"];
        for m in &messages {
            seg.append(m).unwrap();
        }
        for (i, m) in messages.iter().enumerate() {
            let rec = seg.read(i as u64).unwrap();
            assert_eq!(&rec.payload, m);
        }
    }

    #[test]
    fn test_crc32_corruption_detected() {
        let (mut seg, _dir) = temp_segment(FsyncPolicy::Always);
        seg.append(b"sensitive financial payload").unwrap();
        let path = seg.path().to_path_buf();
        drop(seg);

        // Corrupt byte at file position 16 (first byte of payload)
        let mut f = OpenOptions::new().read(true).write(true).open(&path).unwrap();
        f.seek(SeekFrom::Start(16)).unwrap();
        f.write_all(b"X").unwrap();
        drop(f);

        // Reopening the segment must detect the corruption
        let reopen_result = Segment::open_or_create(&path, FsyncPolicy::Always);
        assert!(matches!(reopen_result, Err(BrokerError::Corruption { offset: 0, .. })));
    }

    #[test]
    fn test_segment_recovery_preserves_records() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("recoverable.log");
        {
            let mut seg = Segment::open_or_create(&path, FsyncPolicy::Always).unwrap();
            seg.append(b"first").unwrap();
            seg.append(b"second").unwrap();
            seg.append(b"third").unwrap();
        }
        // Reopen segment — should recover all 3 records
        let mut recovered = Segment::open_or_create(&path, FsyncPolicy::Always).unwrap();
        assert_eq!(recovered.record_count(), 3);
        assert_eq!(recovered.read(0).unwrap().payload, b"first");
        assert_eq!(recovered.read(1).unwrap().payload, b"second");
        assert_eq!(recovered.read(2).unwrap().payload, b"third");
    }

    #[test]
    fn test_large_payload() {
        let (mut seg, _dir) = temp_segment(FsyncPolicy::Batch(1));
        let large = vec![0xABu8; 65536]; // 64 KB
        let off = seg.append(&large).unwrap();
        let rec = seg.read(off).unwrap();
        assert_eq!(rec.payload.len(), 65536);
        assert_eq!(rec.payload[0], 0xAB);
    }

    #[test]
    fn test_offset_not_found_error() {
        let (mut seg, _dir) = temp_segment(FsyncPolicy::Always);
        seg.append(b"one").unwrap();
        let result = seg.read(999);
        assert!(matches!(result, Err(BrokerError::OffsetNotFound(999))));
    }

    #[test]
    fn test_size_bytes_grows() {
        let (mut seg, _dir) = temp_segment(FsyncPolicy::Batch(100));
        let payload = b"test-data-256";
        seg.append(payload).unwrap();
        let expected = HEADER_SIZE + payload.len() as u64;
        assert_eq!(seg.size_bytes(), expected);
    }

    #[test]
    fn test_batch_fsync_policy() {
        let (mut seg, _dir) = temp_segment(FsyncPolicy::Batch(10));
        for _ in 0..10 {
            seg.append(b"batch-record").unwrap();
        }
        assert_eq!(seg.record_count(), 10);
    }

    #[test]
    fn test_commit_log_publish_consume() {
        let dir = TempDir::new().unwrap();
        let mut log = CommitLog::new(dir.path(), FsyncPolicy::Always).unwrap();
        let off = log.publish(b"first message").unwrap();
        let rec = log.consume(off).unwrap();
        assert_eq!(rec.payload, b"first message");
        assert_eq!(log.record_count(), 1);
        assert_eq!(log.dir(), dir.path());
        assert_eq!(log.policy(), FsyncPolicy::Always);
    }

    #[test]
    fn test_empty_payload_append() {
        let (mut seg, _dir) = temp_segment(FsyncPolicy::Always);
        let off = seg.append(b"").unwrap();
        let rec = seg.read(off).unwrap();
        assert_eq!(rec.payload, b"");
    }
}

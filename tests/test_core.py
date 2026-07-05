import os
import struct
import zlib
import json
import time
import pytest
import aiofiles
import tempfile
import shutil

from core.index import OffsetIndex
from core.segment import Segment
from core.log import Log

def test_offset_index_basics():
    idx = OffsetIndex(interval=10)
    # Test addition
    idx.add(0, 0)
    idx.add(10, 100)
    idx.add(20, 200)
    idx.add(15, 150)  # out of order insertion should maintain sort order
    
    # Check that they are sorted by offset
    assert idx._entries == [(0, 0), (10, 100), (15, 150), (20, 200)]
    
    # Test find (binary search logic)
    # largest offset less than or equal to target offset
    assert idx.find(0) == 0
    assert idx.find(5) == 0
    assert idx.find(10) == 100
    assert idx.find(12) == 100
    assert idx.find(17) == 150
    assert idx.find(20) == 200
    assert idx.find(25) == 200
    
    # Test clear
    idx.clear()
    assert len(idx._entries) == 0
    assert idx.find(10) == 0  # Should return 0 if empty


@pytest.mark.asyncio
async def test_offset_index_rebuild():
    with tempfile.TemporaryDirectory() as tmpdir:
        seg_path = os.path.join(tmpdir, "00000000000000000000.log")
        idx = OffsetIndex(interval=2)
        
        # Write dummy packed records to the log
        # Format: crc(4B) | key_len(4B) | value_len(4B) | headers_len(4B) | timestamp(8B) | offset(8B)
        # Total header size = 32
        async with aiofiles.open(seg_path, "wb") as f:
            for offset in range(5):
                key = f"key-{offset}".encode('utf-8')
                value = f"value-{offset}".encode('utf-8')
                headers = json.dumps({"o": offset}).encode('utf-8')
                payload = key + value + headers
                crc = zlib.crc32(payload) & 0xffffffff
                header = struct.pack("<IIIIQQ", crc, len(key), len(value), len(headers), 1234567, offset)
                await f.write(header + payload)
                
        # Rebuild index
        await idx.rebuild_from_segments(seg_path)
        
        # Interval is 2, so offset % 2 == 0 should be in the index: 0, 2, 4
        # We can calculate the expected positions:
        # Each record size = 32 (header) + key_len + value_len + headers_len
        # Offset 0: key_len=5, val_len=7, hdrs_len=8. Record size = 32 + 20 = 52. Pos = 0.
        # Offset 1: key_len=5, val_len=7, hdrs_len=8. Record size = 32 + 20 = 52. Pos = 52.
        # Offset 2: key_len=5, val_len=7, hdrs_len=8. Record size = 32 + 20 = 52. Pos = 104.
        # Offset 3: key_len=5, val_len=7, hdrs_len=8. Record size = 32 + 20 = 52. Pos = 156.
        # Offset 4: key_len=5, val_len=7, hdrs_len=8. Record size = 32 + 20 = 52. Pos = 208.
        
        assert len(idx._entries) == 3
        assert idx._entries[0] == (0, 0)
        assert idx._entries[1] == (2, 104)
        assert idx._entries[2] == (4, 208)


@pytest.mark.asyncio
async def test_segment_operations():
    with tempfile.TemporaryDirectory() as tmpdir:
        seg_path = os.path.join(tmpdir, "00000000000000000000.log")
        seg = Segment(seg_path, base_offset=10, max_size=1024)
        
        # Test creation & initial values
        assert seg.path == seg_path
        assert seg.base_offset == 10
        assert seg.next_offset == 10
        assert seg.get_size() == 0
        assert seg.is_active() is True
        
        # Test append
        res1 = await seg.append("key1", "value1", {"h1": "v1"})
        assert res1["offset"] == 10
        assert res1["key"] == "key1"
        assert res1["value"] == "value1"
        assert res1["headers"] == {"h1": "v1"}
        assert seg.next_offset == 11
        assert seg.get_size() > 0
        
        res2 = await seg.append("key2", {"a": 1}, None)
        assert res2["offset"] == 11
        assert res2["value"] == {"a": 1}
        assert res2["headers"] == {}
        
        # Test read back
        records = await seg.read(start_offset=10, count=2)
        assert len(records) == 2
        assert records[0]["offset"] == 10
        assert records[0]["key"] == "key1"
        assert records[0]["value"] == "value1"
        assert records[0]["headers"] == {"h1": "v1"}
        
        assert records[1]["offset"] == 11
        assert records[1]["value"] == {"a": 1}
        
        # Test reading with start_offset offset
        records_from_11 = await seg.read(start_offset=11, count=1)
        assert len(records_from_11) == 1
        assert records_from_11[0]["offset"] == 11
        
        # Test verify binary format correctness
        # Format: crc(4B) | key_len(4B) | value_len(4B) | headers_len(4B) | timestamp(8B) | offset(8B) | key | value | headers_json
        with open(seg_path, "rb") as f:
            header_bytes = f.read(32)
            crc, key_len, val_len, hdrs_len, timestamp, offset = struct.unpack("<IIIIQQ", header_bytes)
            assert offset == 10
            assert key_len == 4  # "key1"
            assert val_len == 6  # json encoded or utf8 representation length
            payload = f.read(key_len + val_len + hdrs_len)
            assert zlib.crc32(payload) & 0xffffffff == crc


@pytest.mark.asyncio
async def test_segment_crc_tampering():
    with tempfile.TemporaryDirectory() as tmpdir:
        seg_path = os.path.join(tmpdir, "00000000000000000000.log")
        seg = Segment(seg_path, base_offset=0, max_size=1024)
        await seg.append("k", "v", None)
        
        # Tamper with the CRC of the record
        with open(seg_path, "r+b") as f:
            orig_crc_bytes = f.read(4)
            orig_crc = struct.unpack("<I", orig_crc_bytes)[0]
            f.seek(0)
            f.write(struct.pack("<I", (orig_crc + 1) & 0xffffffff))
            
        # Reading should raise ValueError due to CRC mismatch
        seg_read = Segment(seg_path, base_offset=0, max_size=1024)
        with pytest.raises(ValueError) as excinfo:
            await seg_read.read(start_offset=0, count=1)
        assert "CRC mismatch" in str(excinfo.value)


@pytest.mark.asyncio
async def test_segment_rollover_limit():
    with tempfile.TemporaryDirectory() as tmpdir:
        seg_path = os.path.join(tmpdir, "00000000000000000000.log")
        # Let's make max_size very small (e.g. 50 bytes)
        seg = Segment(seg_path, base_offset=0, max_size=50)
        assert seg.is_active() is True
        
        await seg.append("k", "v", None)
        # Size after first append: 32 + 1 (k) + 1 (v) + 2 ('{}') = 36 bytes.
        # Since 36 < 50, it should still be active
        assert seg.is_active() is True
        
        await seg.append("k", "v", None)
        # Size after second append: 72 bytes.
        # Since 72 >= 50, it should be inactive
        assert seg.is_active() is False


@pytest.mark.asyncio
async def test_log_operations(monkeypatch):
    # Set high retention hours to make sure we don't clean up unless expected
    with tempfile.TemporaryDirectory() as tmpdir:
        log = Log(topic="test-topic", partition=0, base_dir=tmpdir)
        await log.rebuild_index()
        
        # Test initial offsets
        assert log.get_earliest_offset() == 0
        assert log.get_latest_offset() == 0
        
        # Append sequence
        for i in range(5):
            await log.append(f"key-{i}", f"val-{i}", None)
            
        assert log.get_latest_offset() == 5
        assert log.get_earliest_offset() == 0
        
        # Read from offset
        records = await log.read(start_offset=2, max_count=2)
        assert len(records) == 2
        assert records[0]["offset"] == 2
        assert records[0]["key"] == "key-2"
        assert records[1]["offset"] == 3
        assert records[1]["key"] == "key-3"


@pytest.mark.asyncio
async def test_log_rollover_and_cleanup(monkeypatch):
    import core.log
    monkeypatch.setattr(core.log, "SEGMENT_MAX_SIZE", 50)
    
    with tempfile.TemporaryDirectory() as tmpdir:
        log = Log(topic="test-topic", partition=0, base_dir=tmpdir)
        await log.rebuild_index()
        
        # Append sequence triggering rollover
        await log.append("k", "v", None)  # size = 36
        assert len(log.segments) == 1
        
        await log.append("k", "v", None)  # size = 72
        assert len(log.segments) == 1
        
        await log.append("k", "v", None)  # triggers rollover because size 72 >= 50
        assert len(log.segments) == 2
        
        # Check active segment and offsets
        assert log.segments[0].base_offset == 0
        assert log.segments[1].base_offset == 2
        assert log.active_segment == log.segments[1]
        
        # Modify mtime of first segment to be in the past
        past_time = time.time() - (100 * 3600)  # 100 hours ago
        os.utime(log.segments[0].path, (past_time, past_time))
        
        # Run cleanup with retention_hours=72
        await log.cleanup(retention_hours=72)
        
        # First segment should be deleted/removed from segments list
        assert len(log.segments) == 1
        assert log.segments[0].base_offset == 2

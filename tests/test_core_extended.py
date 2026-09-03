import os
import struct
import zlib
import json
import time
import pytest
import tempfile
import aiofiles

from core.index import OffsetIndex
from core.segment import Segment
from core.log import Log
from core.partition import Partition
from core.topic import Topic

# -----------------------------------------------------------------------------
# OffsetIndex Tests
# -----------------------------------------------------------------------------

def test_offset_index_find_edge_cases():
    idx = OffsetIndex(interval=10)
    # Empty index
    assert idx.find(0) == 0
    assert idx.find(100) == 0
    assert len(idx) == 0
    
    # Add entries
    idx.add(10, 500)
    idx.add(20, 1000)
    idx.add(30, 1500)
    assert len(idx) == 3
    
    # Target smaller than first entry must return 0
    assert idx.find(0) == 0
    assert idx.find(5) == 0
    assert idx.find(9) == 0
    
    # Exact matches
    assert idx.find(10) == 500
    assert idx.find(20) == 1000
    assert idx.find(30) == 1500
    
    # Intermediate matches
    assert idx.find(15) == 500
    assert idx.find(25) == 1000
    assert idx.find(99) == 1500

def test_offset_index_update_existing():
    idx = OffsetIndex(interval=5)
    idx.add(10, 100)
    idx.add(10, 200)  # overwrite offset 10 with new pos
    assert len(idx) == 1
    assert idx.find(10) == 200

# -----------------------------------------------------------------------------
# Segment Tests
# -----------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_segment_batch_append_and_read():
    with tempfile.TemporaryDirectory() as tmpdir:
        seg_path = os.path.join(tmpdir, "00000000000000000000.log")
        seg = Segment(seg_path, base_offset=0, max_size=100_000)
        
        messages = [
            {"key": f"k-{i}", "value": {"item": i}, "headers": {"idx": i}}
            for i in range(25)
        ]
        results = await seg.append_batch(messages)
        assert len(results) == 25
        assert results[0]["offset"] == 0
        assert results[24]["offset"] == 24
        assert seg.next_offset == 25
        
        # Read all
        records = await seg.read(start_offset=0, count=30)
        assert len(records) == 25
        assert records[0]["key"] == "k-0"
        assert records[24]["value"] == {"item": 24}
        assert records[10]["headers"] == {"idx": 10}
        
        # Read slice
        slice_records = await seg.read(start_offset=10, count=5)
        assert len(slice_records) == 5
        assert slice_records[0]["offset"] == 10
        assert slice_records[-1]["offset"] == 14

@pytest.mark.asyncio
async def test_segment_data_types_serialization():
    with tempfile.TemporaryDirectory() as tmpdir:
        seg_path = os.path.join(tmpdir, "00000000000000000000.log")
        seg = Segment(seg_path, base_offset=100)
        
        # Binary data, None key, integer/list value
        res1 = await seg.append(key=b"bytes-key", value=[1, 2, 3], headers={"type": "array"})
        assert res1["key"] == "bytes-key"
        assert res1["value"] == [1, 2, 3]
        
        res2 = await seg.append(key=None, value="plain string value", headers=None)
        assert res2["key"] is None
        assert res2["value"] == "plain string value"
        assert res2["headers"] == {}
        
        # Read back
        records = await seg.read(start_offset=100, count=2)
        assert len(records) == 2
        assert records[0]["key"] == "bytes-key"
        assert records[0]["value"] == [1, 2, 3]
        assert records[1]["key"] is None
        assert records[1]["value"] == "plain string value"

@pytest.mark.asyncio
async def test_segment_read_all_generator():
    with tempfile.TemporaryDirectory() as tmpdir:
        seg_path = os.path.join(tmpdir, "00000000000000000000.log")
        seg = Segment(seg_path, base_offset=0)
        
        for i in range(10):
            await seg.append(f"k{i}", f"v{i}", None)
            
        collected = []
        async for r in seg.read_all():
            collected.append(r)
            
        assert len(collected) == 10
        assert [r["offset"] for r in collected] == list(range(10))

@pytest.mark.asyncio
async def test_segment_corrupt_tail_recovery():
    with tempfile.TemporaryDirectory() as tmpdir:
        seg_path = os.path.join(tmpdir, "00000000000000000000.log")
        seg = Segment(seg_path, base_offset=0)
        
        await seg.append("k0", "v0", None)
        await seg.append("k1", "v1", None)
        
        # Append corrupt partial bytes to file tail
        async with aiofiles.open(seg_path, "ab") as f:
            await f.write(b"garbage_partial_header_data_xyz")
            
        # Create new segment instance pointing to same file and recover
        seg2 = Segment(seg_path, base_offset=0)
        await seg2.recover()
        
        assert seg2.next_offset == 2
        records = await seg2.read(start_offset=0, count=10)
        assert len(records) == 2
        assert records[0]["key"] == "k0"
        assert records[1]["key"] == "k1"

# -----------------------------------------------------------------------------
# Log & Multi-segment Tests
# -----------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_log_multi_segment_rollover_and_read():
    with tempfile.TemporaryDirectory() as tmpdir:
        log = Log(topic="multi-seg", partition=0, base_dir=tmpdir)
        # Use small max_size to force multiple segments
        import core.log
        original_max = core.log.SEGMENT_MAX_SIZE
        core.log.SEGMENT_MAX_SIZE = 80
        
        try:
            await log.rebuild_index()
            
            # Append 10 messages
            for i in range(10):
                await log.append(f"key-{i}", f"val-{i}", {"idx": i})
                
            assert len(log.segments) >= 3
            assert log.get_earliest_offset() == 0
            assert log.get_latest_offset() == 10
            
            # Read across all segments sequentially
            records = await log.read(start_offset=0, max_count=10)
            assert len(records) == 10
            assert [r["offset"] for r in records] == list(range(10))
            
            # Read middle slice across segment boundaries
            slice_records = await log.read(start_offset=3, max_count=5)
            assert len(slice_records) == 5
            assert slice_records[0]["offset"] == 3
            assert slice_records[-1]["offset"] == 7
        finally:
            core.log.SEGMENT_MAX_SIZE = original_max

@pytest.mark.asyncio
async def test_log_size_based_retention_cleanup():
    with tempfile.TemporaryDirectory() as tmpdir:
        import core.log
        original_max = core.log.SEGMENT_MAX_SIZE
        core.log.SEGMENT_MAX_SIZE = 80
        
        try:
            log = Log(topic="size-retention", partition=0, base_dir=tmpdir)
            await log.rebuild_index()
            
            for i in range(12):
                await log.append(f"k{i}", f"v{i}", None)
                
            initial_seg_count = len(log.segments)
            assert initial_seg_count >= 4
            
            # Run cleanup with very small max_bytes (e.g. 100 bytes)
            cleaned = await log.cleanup(retention_hours=1000, max_bytes=100)
            assert cleaned > 0
            assert len(log.segments) < initial_seg_count
            assert log.active_segment in log.segments
        finally:
            core.log.SEGMENT_MAX_SIZE = original_max

@pytest.mark.asyncio
async def test_partition_lock_linearizability():
    with tempfile.TemporaryDirectory() as tmpdir:
        partition = Partition(topic="linear-topic", partition_id=0, base_dir=tmpdir)
        await partition.startup()
        
        import asyncio
        async def worker(w_id):
            return await partition.append(f"key-{w_id}", f"val-{w_id}", None)
            
        tasks = [worker(i) for i in range(20)]
        results = await asyncio.gather(*tasks)
        
        assert len(results) == 20
        offsets = [r["offset"] for r in results]
        assert sorted(offsets) == list(range(20))
        assert partition.get_high_watermark() == 20
        assert partition.get_offset_count() == 20

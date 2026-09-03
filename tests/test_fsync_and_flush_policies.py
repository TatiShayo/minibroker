import os
import pytest
import asyncio
import tempfile
import shutil
from core.segment import Segment
import config

@pytest.mark.asyncio
async def test_fsync_always_policy():
    temp_dir = tempfile.mkdtemp()
    try:
        seg_path = os.path.join(temp_dir, f'{0:020d}.log')
        orig_mode = config.FSYNC_MODE
        config.FSYNC_MODE = 'always'
        
        seg = Segment(seg_path, 0)
        await seg.recover()
        
        rec = await seg.append('key1', 'val1', {'h': '1'})
        assert rec['offset'] == 0
        assert os.path.exists(seg_path)
        assert os.path.getsize(seg_path) > 0
        
        seg2 = Segment(seg_path, 0)
        await seg2.recover()
        assert seg2.next_offset == 1
        read_msgs = await seg2.read(0, 1)
        assert len(read_msgs) == 1
        assert read_msgs[0]['value'] == 'val1'
    finally:
        config.FSYNC_MODE = orig_mode
        shutil.rmtree(temp_dir, ignore_errors=True)

@pytest.mark.asyncio
async def test_fsync_batch_policy():
    temp_dir = tempfile.mkdtemp()
    try:
        seg_path = os.path.join(temp_dir, f'{0:020d}.log')
        orig_mode = config.FSYNC_MODE
        config.FSYNC_MODE = 'batch'
        
        seg = Segment(seg_path, 0)
        await seg.recover()
        
        messages = [
            {'key': f'k{i}', 'value': f'v{i}', 'headers': {}}
            for i in range(5)
        ]
        results = await seg.append_batch(messages)
        assert len(results) == 5
        assert seg.next_offset == 5
        
        records = await seg.read(0, 10)
        assert len(records) == 5
        assert [r['value'] for r in records] == [f'v{i}' for i in range(5)]
    finally:
        config.FSYNC_MODE = orig_mode
        shutil.rmtree(temp_dir, ignore_errors=True)

@pytest.mark.asyncio
async def test_fsync_none_policy():
    temp_dir = tempfile.mkdtemp()
    try:
        seg_path = os.path.join(temp_dir, f'{0:020d}.log')
        orig_mode = config.FSYNC_MODE
        config.FSYNC_MODE = 'none'
        
        seg = Segment(seg_path, 0)
        await seg.recover()
        
        rec = await seg.append('k_none', 'v_none', {})
        assert rec['offset'] == 0
        
        read_msgs = await seg.read(0, 1)
        assert len(read_msgs) == 1
        assert read_msgs[0]['key'] == 'k_none'
    finally:
        config.FSYNC_MODE = orig_mode
        shutil.rmtree(temp_dir, ignore_errors=True)

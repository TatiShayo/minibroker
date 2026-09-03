import os
import pytest
import asyncio
import tempfile

from core.topic import Topic
from core.segment import Segment
from core.log import Log
from storage.persistence import save_topic_metadata, load_all_topics
from core.offsets import init_db, commit_offset, get_offset

@pytest.mark.asyncio
async def test_full_broker_cold_restart_recovery(client_factory):
    topic1 = "restart-topic-1"
    topic2 = "restart-topic-2"
    group = "persistent-group"
    
    # 1. First server session: create topics, produce data, commit offsets
    async with client_factory() as client:
        await client.post("/topics", json={"name": topic1, "partitions": 2})
        await client.post("/topics", json={"name": topic2, "partitions": 3})
        
        # Produce 50 messages to topic1, 30 to topic2
        for i in range(50):
            await client.post(f"/topics/{topic1}/messages", json={"value": f"t1-val-{i}"})
        for i in range(30):
            await client.post(f"/topics/{topic2}/messages", json={"value": f"t2-val-{i}"})
            
        # Commit consumer offsets
        await client.post(f"/topics/{topic1}/commit", json={"group": group, "partition": 0, "offset": 15})
        await client.post(f"/topics/{topic2}/commit", json={"group": group, "partition": 1, "offset": 10})

    # 2. Simulate broker process restart (triggers brand new lifespan and disk recovery)
    async with client_factory() as client:
        # Check topic list restored
        topics_resp = await client.get("/topics")
        assert topics_resp.status_code == 200
        topics_list = topics_resp.json()
        topic_names = [t["name"] for t in topics_list]
        assert topic1 in topic_names
        assert topic2 in topic_names
        
        t1_info = next(t for t in topics_list if t["name"] == topic1)
        assert t1_info["total_messages"] == 50
        
        t2_info = next(t for t in topics_list if t["name"] == topic2)
        assert t2_info["total_messages"] == 30
        
        # Check consumer groups lag restored
        groups_resp = await client.get("/groups")
        assert groups_resp.status_code == 200
        groups = groups_resp.json()
        assert any(g["group"] == group for g in groups)
        
        # Verify can produce new messages seamlessly after restart
        new_prod = await client.post(f"/topics/{topic1}/messages", json={"value": "after-restart-msg"})
        assert new_prod.status_code == 200
        
        # Verify can consume historical and newly produced messages
        cons_resp = await client.get(f"/topics/{topic1}/messages?partition=0&offset=0&limit=100")
        assert cons_resp.status_code == 200
        assert len(cons_resp.json()["messages"]) > 0

@pytest.mark.asyncio
async def test_segment_recovery_with_nonzero_base_offset():
    with tempfile.TemporaryDirectory() as tmpdir:
        base_off = 500
        seg_path = os.path.join(tmpdir, f"{base_off:020d}.log")
        seg = Segment(seg_path, base_offset=base_off)
        
        # Append 10 messages (offsets 500..509)
        for i in range(10):
            await seg.append(f"k-{i}", f"v-{i}", None)
            
        assert seg.next_offset == 510
        
        # Create fresh segment instance and recover
        recovered_seg = Segment(seg_path, base_offset=base_off)
        await recovered_seg.recover()
        
        assert recovered_seg.next_offset == 510
        records = await recovered_seg.read(start_offset=500, count=10)
        assert len(records) == 10
        assert records[0]["offset"] == 500
        assert records[-1]["offset"] == 509

@pytest.mark.asyncio
async def test_log_rebuild_from_multiple_recovered_segments():
    with tempfile.TemporaryDirectory() as tmpdir:
        import core.log
        orig = core.log.SEGMENT_MAX_SIZE
        core.log.SEGMENT_MAX_SIZE = 75
        
        try:
            log1 = Log(topic="rebuild-test", partition=0, base_dir=tmpdir)
            await log1.rebuild_index()
            
            for i in range(15):
                await log1.append(f"key-{i}", f"val-{i}", None)
                
            assert len(log1.segments) >= 3
            last_off = log1.get_latest_offset()
            assert last_off == 15
            
            # Create fresh Log instance and rebuild
            log2 = Log(topic="rebuild-test", partition=0, base_dir=tmpdir)
            await log2.rebuild_index()
            
            assert len(log2.segments) == len(log1.segments)
            assert log2.get_latest_offset() == 15
            assert log2.get_earliest_offset() == 0
            
            # Read all 15 records
            all_records = await log2.read(start_offset=0, max_count=20)
            assert len(all_records) == 15
            assert [r["offset"] for r in all_records] == list(range(15))
        finally:
            core.log.SEGMENT_MAX_SIZE = orig

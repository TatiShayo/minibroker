import pytest
import tempfile
import os
from core.topic import Topic
from core.segment import Segment
from core.rebalance import assign_round_robin, assign_range
from core.offsets import init_db, commit_offsets_batch, get_offset

@pytest.mark.asyncio
async def test_invariant_monotonic_offsets_mixed_produce():
    topic = Topic("inv-monotonic", partitions_count=1)
    await topic.startup()
    p = topic.get_partition(0)
    
    # Mix single and batch writes
    res1 = await p.append("k1", "v1", None)
    res2 = await p.append_batch([
        {"key": "k2", "value": "v2", "headers": None},
        {"key": "k3", "value": "v3", "headers": None}
    ])
    res3 = await p.append("k4", "v4", None)
    res4 = await p.append_batch([
        {"key": "k5", "value": "v5", "headers": None}
    ])
    
    assigned = [res1["offset"]] + [r["offset"] for r in res2] + [res3["offset"]] + [r["offset"] for r in res4]
    assert assigned == [0, 1, 2, 3, 4]
    assert p.get_high_watermark() == 5

def test_invariant_round_robin_assignor_fairness():
    # For any partition count P and consumer count C, max - min assigned partitions <= 1
    for num_p in range(1, 25):
        for num_c in range(1, 10):
            partitions = list(range(num_p))
            consumers = [f"c{i}" for i in range(num_c)]
            assignment = assign_round_robin(partitions, consumers)
            
            counts = [len(parts) for parts in assignment.values()]
            assert max(counts) - min(counts) <= 1
            # Union of all partitions equals original set
            all_assigned = [p for parts in assignment.values() for p in parts]
            assert sorted(all_assigned) == partitions

def test_invariant_range_assignor_contiguous_coverage():
    for num_p in range(1, 20):
        for num_c in range(1, 8):
            partitions = list(range(num_p))
            consumers = [f"c{i}" for i in range(num_c)]
            assignment = assign_range(partitions, consumers)
            
            all_assigned = []
            for c_id, parts in assignment.items():
                if parts:
                    # Check contiguous
                    for idx in range(len(parts) - 1):
                        assert parts[idx + 1] == parts[idx] + 1
                    all_assigned.extend(parts)
            assert sorted(all_assigned) == partitions

@pytest.mark.asyncio
async def test_invariant_sparse_index_monotonicity():
    with tempfile.TemporaryDirectory() as tmpdir:
        seg_path = os.path.join(tmpdir, "00000000000000000000.log")
        seg = Segment(seg_path, base_offset=0)
        
        # Write 50 messages with small interval so index is populated
        seg.index.interval = 5
        messages = [{"key": f"k{i}", "value": f"v{i}", "headers": None} for i in range(50)]
        await seg.append_batch(messages)
        
        entries = seg.index._entries
        assert len(entries) >= 5
        for i in range(len(entries) - 1):
            assert entries[i][0] < entries[i + 1][0]  # offsets strictly increasing
            assert entries[i][1] < entries[i + 1][1]  # file positions strictly increasing

@pytest.mark.asyncio
async def test_invariant_empty_message_roundtrip():
    topic = Topic("inv-empty-topic", partitions_count=1)
    await topic.startup()
    p = topic.get_partition(0)
    
    # Append empty/None values (0-byte payloads decode to None)
    await p.append(key="", value="", headers={})
    await p.append(key=None, value=None, headers=None)
    await p.append(key="k", value={"item": "box"}, headers={"flag": True})
    
    records = await p.read(offset=0, max_count=10)
    assert len(records) == 3
    assert records[0]["key"] is None
    assert records[0]["value"] is None
    assert records[1]["key"] is None
    assert records[1]["value"] is None
    assert records[2]["key"] == "k"
    assert records[2]["value"] == {"item": "box"}
    assert records[2]["headers"] == {"flag": True}

@pytest.mark.asyncio
async def test_invariant_multi_topic_data_isolation():
    topic_a = Topic("topic-iso-A", partitions_count=1)
    topic_b = Topic("topic-iso-B", partitions_count=1)
    await topic_a.startup()
    await topic_b.startup()
    
    await topic_a.get_partition(0).append("keyA", "valA", None)
    await topic_b.get_partition(0).append("keyB", "valB", None)
    
    records_a = await topic_a.get_partition(0).read(0, 10)
    records_b = await topic_b.get_partition(0).read(0, 10)
    
    assert len(records_a) == 1
    assert records_a[0]["value"] == "valA"
    assert len(records_b) == 1
    assert records_b[0]["value"] == "valB"

@pytest.mark.asyncio
async def test_invariant_large_batch_integrity():
    topic = Topic("inv-large-batch", partitions_count=1)
    await topic.startup()
    p = topic.get_partition(0)
    
    count = 200
    batch = [{"key": f"k-{i}", "value": {"seq": i, "data": "x" * 50}, "headers": {"h": i}} for i in range(count)]
    results = await p.append_batch(batch)
    assert len(results) == count
    
    # Read back all
    read_records = await p.read(0, max_count=count)
    assert len(read_records) == count
    for i in range(count):
        assert read_records[i]["offset"] == i
        assert read_records[i]["key"] == f"k-{i}"
        assert read_records[i]["value"]["seq"] == i
        assert read_records[i]["headers"]["h"] == i

@pytest.mark.asyncio
async def test_invariant_offset_reset_extremes():
    await init_db()
    topic = Topic("inv-reset-topic", partitions_count=2)
    await topic.startup()
    
    # Produce 10 to p0, 20 to p1
    for i in range(10):
        await topic.get_partition(0).append("k", i, None)
    for i in range(20):
        await topic.get_partition(1).append("k", i, None)
        
    group = "reset-inv-group"
    # Commit initial offsets
    await commit_offsets_batch(group, "inv-reset-topic", {0: 5, 1: 15})
    assert await get_offset(group, "inv-reset-topic", 0) == 5
    assert await get_offset(group, "inv-reset-topic", 1) == 15

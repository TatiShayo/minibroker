import pytest
import asyncio
from core.topic import Topic

@pytest.mark.asyncio
async def test_topic_keyless_round_robin_distribution():
    topic = Topic("rr-topic", partitions_count=4)
    await topic.startup()
    
    # 8 keyless messages should route evenly: 0, 1, 2, 3, 0, 1, 2, 3
    parts = [topic.partition(None) for _ in range(8)]
    assert parts == [0, 1, 2, 3, 0, 1, 2, 3]

@pytest.mark.asyncio
async def test_topic_key_deterministic_hashing():
    topic = Topic("hash-topic", partitions_count=8)
    await topic.startup()
    
    # Same key must always map to exact same partition
    part_user1_a = topic.partition("user_1001")
    part_user1_b = topic.partition("user_1001")
    assert part_user1_a == part_user1_b
    
    # Different keys should distribute
    keys = [f"account_{i}" for i in range(50)]
    destinations = set(topic.partition(k) for k in keys)
    assert len(destinations) > 1  # Should hit multiple partitions

@pytest.mark.asyncio
async def test_topic_dynamic_partition_expansion():
    topic = Topic("expand-topic", partitions_count=2)
    await topic.startup()
    assert len(topic.partitions) == 2
    
    # Produce message to partition 0 and 1
    await topic.get_partition(0).append("k1", "v1", None)
    await topic.get_partition(1).append("k2", "v2", None)
    
    # Expand to 4 partitions
    new_count = await topic.add_partitions(4)
    assert new_count == 4
    assert len(topic.partitions) == 4
    assert topic.partitions_count == 4
    
    # New partitions exist and accept writes
    res = await topic.get_partition(3).append("k3", "v3", None)
    assert res["offset"] == 0
    assert res["value"] == "v3"
    
    # Existing messages remain untouched
    rec0 = await topic.get_partition(0).read(0, 1)
    assert rec0[0]["value"] == "v1"

@pytest.mark.asyncio
async def test_topic_invalid_partition_expansion():
    topic = Topic("invalid-expand", partitions_count=3)
    await topic.startup()
    
    # Trying to expand to equal or fewer partitions raises ValueError
    with pytest.raises(ValueError) as exc:
        await topic.add_partitions(3)
    assert "must be greater" in str(exc.value)

    with pytest.raises(ValueError) as exc:
        await topic.add_partitions(2)
    assert "must be greater" in str(exc.value)

@pytest.mark.asyncio
async def test_topic_batch_produce_multi_partition_parallel():
    topic = Topic("batch-multi-topic", partitions_count=3)
    await topic.startup()
    
    messages = [
        {"key": "user-A", "value": "msg-0", "headers": None},
        {"key": "user-B", "value": "msg-1", "headers": None},
        {"key": "user-C", "value": "msg-2", "headers": None},
        {"key": "user-A", "value": "msg-3", "headers": None},
        {"partition": 2, "value": "explicit-part-2", "headers": None},
    ]
    
    results = await topic.produce_batch(messages)
    assert len(results) == 5
    assert results[4]["partition"] == 2
    assert results[4]["value"] == "explicit-part-2"
    
    # Verify records written to partitions
    total_written = sum(p.get_offset_count() for p in topic.partitions.values())
    assert total_written == 5

@pytest.mark.asyncio
async def test_topic_batch_produce_invalid_explicit_partition():
    topic = Topic("invalid-part-topic", partitions_count=2)
    await topic.startup()
    
    messages = [
        {"partition": 99, "value": "out-of-bounds"}
    ]
    with pytest.raises(ValueError) as exc:
        await topic.produce_batch(messages)
    assert "does not exist" in str(exc.value)

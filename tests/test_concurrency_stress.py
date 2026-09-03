import asyncio
import pytest
from core.topic import Topic
from core.offsets import init_db, commit_offset, get_offset

@pytest.mark.asyncio
async def test_high_concurrency_producers_strict_offsets():
    topic = Topic("stress-topic", partitions_count=1)
    await topic.startup()
    part = topic.get_partition(0)
    
    num_tasks = 20
    msgs_per_task = 50
    total_expected = num_tasks * msgs_per_task
    
    async def produce_worker(worker_id: int):
        results = []
        for i in range(msgs_per_task):
            res = await part.append(
                key=f"worker-{worker_id}",
                value=f"payload-{worker_id}-{i}",
                headers={"worker": worker_id}
            )
            results.append(res)
        return results

    tasks = [produce_worker(i) for i in range(num_tasks)]
    all_results = await asyncio.gather(*tasks)
    
    # Flatten
    flat = [r for sublist in all_results for r in sublist]
    assert len(flat) == total_expected
    
    # Check that all offsets from 0 to 999 are present and unique
    assigned_offsets = [r["offset"] for r in flat]
    assert sorted(assigned_offsets) == list(range(total_expected))
    assert part.get_high_watermark() == total_expected

@pytest.mark.asyncio
async def test_concurrent_batch_producers():
    topic = Topic("batch-stress-topic", partitions_count=3)
    await topic.startup()
    
    num_producers = 5
    batch_size = 50
    
    async def batch_producer(p_id: int):
        batch = [
            {"key": f"key-{p_id}-{i}", "value": f"val-{p_id}-{i}", "headers": None}
            for i in range(batch_size)
        ]
        return await topic.produce_batch(batch)

    tasks = [batch_producer(i) for i in range(num_producers)]
    results = await asyncio.gather(*tasks)
    
    total_produced = sum(len(r) for r in results)
    assert total_produced == num_producers * batch_size
    
    # Verify sum of partition message counts
    total_in_partitions = sum(p.get_offset_count() for p in topic.partitions.values())
    assert total_in_partitions == total_produced

@pytest.mark.asyncio
async def test_concurrent_sqlite_offset_commits():
    await init_db()
    
    num_committers = 25
    group = "stress-group"
    topic = "orders"
    
    async def committer(partition_id: int, offset_val: int):
        await commit_offset(group, topic, partition_id, offset_val)
        return await get_offset(group, topic, partition_id)

    tasks = [committer(i, i * 10) for i in range(num_committers)]
    results = await asyncio.gather(*tasks)
    
    # Verify all 25 offsets were safely committed without SQLite locking errors
    for i, res in enumerate(results):
        assert res == i * 10

@pytest.mark.asyncio
async def test_simultaneous_producers_and_consumers():
    topic = Topic("simul-topic", partitions_count=1)
    await topic.startup()
    part = topic.get_partition(0)
    
    produced_count = 100
    consumed_records = []
    
    async def producer():
        for i in range(produced_count):
            await part.append(f"k{i}", f"v{i}", None)
            await asyncio.sleep(0.001)

    async def consumer():
        current_offset = 0
        while len(consumed_records) < produced_count:
            msgs = await part.read(current_offset, max_count=20)
            if msgs:
                consumed_records.extend(msgs)
                current_offset = msgs[-1]["offset"] + 1
            else:
                await asyncio.sleep(0.005)

    await asyncio.gather(producer(), consumer())
    assert len(consumed_records) == produced_count
    assert [r["offset"] for r in consumed_records] == list(range(produced_count))

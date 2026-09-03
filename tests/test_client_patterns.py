import pytest
import asyncio
from typing import List, Optional, Any, Dict

class MiniProducer:
    """High-level client producer utility."""
    def __init__(self, client, default_topic: Optional[str] = None):
        self.client = client
        self.default_topic = default_topic

    async def send(self, topic: Optional[str] = None, key: Optional[str] = None, value: Any = None, headers: Optional[Dict] = None, partition: Optional[int] = None):
        t = topic or self.default_topic
        payload = {"key": key, "value": value, "headers": headers, "partition": partition}
        resp = await self.client.post(f"/topics/{t}/messages", json=payload)
        resp.raise_for_status()
        return resp.json()

    async def send_batch(self, topic: Optional[str] = None, messages: List[dict] = None):
        t = topic or self.default_topic
        resp = await self.client.post(f"/topics/{t}/messages/batch", json={"messages": messages or []})
        resp.raise_for_status()
        return resp.json()

class MiniConsumer:
    """High-level client consumer utility."""
    def __init__(self, client, topic: str, group: str, auto_commit: bool = True):
        self.client = client
        self.topic = topic
        self.group = group
        self.auto_commit = auto_commit

    async def register(self, initial_offset: str = "earliest", strategy: str = "range"):
        resp = await self.client.post(f"/topics/{self.topic}/consumer/register", json={
            "group": self.group,
            "initial_offset": initial_offset,
            "strategy": strategy
        })
        resp.raise_for_status()
        return resp.json()

    async def poll(self, partition: int = 0, limit: int = 10):
        resp = await self.client.get(
            f"/topics/{self.topic}/messages?partition={partition}&group={self.group}&limit={limit}&auto_commit={str(self.auto_commit).lower()}"
        )
        resp.raise_for_status()
        data = resp.json()
        return data["messages"]

    async def commit(self, partition: int, offset: int):
        resp = await self.client.post(f"/topics/{self.topic}/commit", json={
            "group": self.group,
            "partition": partition,
            "offset": offset
        })
        resp.raise_for_status()
        return resp.json()

@pytest.mark.asyncio
async def test_high_level_producer_consumer_flow(client):
    topic = "client-flow-topic"
    await client.post("/topics", json={"name": topic, "partitions": 2})
    
    producer = MiniProducer(client, default_topic=topic)
    consumer = MiniConsumer(client, topic=topic, group="flow-group", auto_commit=False)
    
    # 1. Register consumer
    await consumer.register(initial_offset="earliest")
    
    # 2. Produce messages
    await producer.send(key="user1", value={"order": 1})
    await producer.send(key="user2", value={"order": 2})
    await producer.send_batch(messages=[
        {"key": "user1", "value": {"order": 3}},
        {"key": "user2", "value": {"order": 4}}
    ])
    
    # 3. Poll without auto-commit (at-least-once)
    for p in range(2):
        messages = await consumer.poll(partition=p, limit=10)
        for msg in messages:
            # Explicit commit after processing
            await consumer.commit(partition=p, offset=msg["offset"] + 1)

    # 4. Check lag is now 0
    groups_resp = await client.get("/groups")
    assert groups_resp.status_code == 200
    group_info = next(g for g in groups_resp.json() if g["group"] == "flow-group")
    assert group_info["lag"] == 0

@pytest.mark.asyncio
async def test_at_least_once_replay_on_failure(client):
    topic = "at-least-once-topic"
    group = "alo-group"
    await client.post("/topics", json={"name": topic, "partitions": 1})
    
    producer = MiniProducer(client, default_topic=topic)
    # auto_commit = False for manual control
    consumer = MiniConsumer(client, topic=topic, group=group, auto_commit=False)
    await consumer.register(initial_offset="earliest")
    
    # Produce message
    await producer.send(value="critical-event-001")
    
    # Poll message
    msgs_1 = await consumer.poll(partition=0, limit=1)
    assert len(msgs_1) == 1
    assert msgs_1[0]["value"] == "critical-event-001"
    
    # Simulate worker crash BEFORE commit: poll again!
    # Because offset was not committed, it must re-deliver the same message
    msgs_retry = await consumer.poll(partition=0, limit=1)
    assert len(msgs_retry) == 1
    assert msgs_retry[0]["offset"] == 0
    assert msgs_retry[0]["value"] == "critical-event-001"
    
    # Now worker succeeds and commits offset
    await consumer.commit(partition=0, offset=1)
    
    # Subsequent poll returns no new messages
    msgs_empty = await consumer.poll(partition=0, limit=1)
    assert len(msgs_empty) == 0

@pytest.mark.asyncio
async def test_at_most_once_delivery_semantics(client):
    topic = "at-most-once-topic"
    group = "amo-group"
    await client.post("/topics", json={"name": topic, "partitions": 1})
    
    producer = MiniProducer(client, default_topic=topic)
    # auto_commit = True commits automatically on read
    consumer = MiniConsumer(client, topic=topic, group=group, auto_commit=True)
    await consumer.register(initial_offset="earliest")
    
    await producer.send(value="fire-and-forget-1")
    
    # Poll with auto-commit
    msgs_1 = await consumer.poll(partition=0, limit=1)
    assert len(msgs_1) == 1
    assert msgs_1[0]["offset"] == 0
    
    # Poll again - message was already committed on delivery, so nothing returned
    msgs_2 = await consumer.poll(partition=0, limit=1)
    assert len(msgs_2) == 0

@pytest.mark.asyncio
async def test_multi_partition_worker_processing(client):
    topic = "multi-worker-topic"
    group = "worker-pool"
    await client.post("/topics", json={"name": topic, "partitions": 3})
    
    producer = MiniProducer(client, default_topic=topic)
    
    # Produce 30 messages across 3 partitions
    for i in range(30):
        await producer.send(value=f"work-item-{i}")
        
    # 3 worker consumers each processing 1 partition
    processed_total = 0
    for p_id in range(3):
        cons = MiniConsumer(client, topic=topic, group=group, auto_commit=True)
        items = await cons.poll(partition=p_id, limit=50)
        processed_total += len(items)
        
    assert processed_total == 30

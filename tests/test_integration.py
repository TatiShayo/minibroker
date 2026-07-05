import pytest
import asyncio
from httpx import AsyncClient, ASGITransport
from main import app

from contextlib import asynccontextmanager

@pytest.fixture
def client_factory():
    # Factory to create clean clients and trigger lifespan
    @asynccontextmanager
    async def _factory():
        async with app.router.lifespan_context(app):
            async with AsyncClient(transport=ASGITransport(app=app), base_url="http://testserver") as ac:
                yield ac
    return _factory

@pytest.mark.asyncio
async def test_complete_pipeline(client_factory):
    async with client_factory() as client:
        topic = "pipeline-topic"
        # 1. Create a topic with 3 partitions
        response = await client.post("/topics", json={"name": topic, "partitions": 3})
        assert response.status_code == 200
        
        # 2. Produce 1000 messages
        # We can send them in 5 batches of 200 to keep it efficient
        total_messages = 1000
        batch_size = 200
        sent_messages = {}
        
        for batch_idx in range(total_messages // batch_size):
            messages = []
            for i in range(batch_size):
                global_idx = batch_idx * batch_size + i
                key = f"key-{global_idx}"
                value = f"value-{global_idx}"
                messages.append({"key": key, "value": value})
                sent_messages[key] = value
                
            response = await client.post(f"/topics/{topic}/messages/batch", json={"messages": messages})
            assert response.status_code == 200
            
        # 3. Consume all 1000 messages from the 3 partitions
        consumed_messages = {}
        for partition in range(3):
            offset = 0
            while True:
                response = await client.get(f"/topics/{topic}/messages?partition={partition}&offset={offset}&limit=100")
                assert response.status_code == 200
                data = response.json()
                msgs = data["messages"]
                if not msgs:
                    break
                for m in msgs:
                    consumed_messages[m["key"]] = m["value"]
                offset = data["next_offset"]
                if not data["has_more"]:
                    break
                    
        # 4. Verify total count and content correctness
        assert len(consumed_messages) == total_messages
        for key, val in sent_messages.items():
            assert consumed_messages.get(key) == val

@pytest.mark.asyncio
async def test_consumer_group_offset_persistence(client_factory):
    topic = "resume-topic"
    group = "resume-group"
    
    async with client_factory() as client:
        # Create topic with 1 partition
        response = await client.post("/topics", json={"name": topic, "partitions": 1})
        assert response.status_code == 200
        
        # Produce 100 messages
        messages = [{"key": f"k-{i}", "value": f"v-{i}"} for i in range(100)]
        response = await client.post(f"/topics/{topic}/messages/batch", json={"messages": messages})
        assert response.status_code == 200
        
        # Register consumer group
        response = await client.post(f"/topics/{topic}/consumer/register", json={
            "group": group,
            "initial_offset": "earliest"
        })
        assert response.status_code == 200
        
        # Consume 50 messages with group
        response = await client.get(f"/topics/{topic}/messages?partition=0&group={group}&limit=50")
        assert response.status_code == 200
        data = response.json()
        assert len(data["messages"]) == 50
        assert data["messages"][0]["offset"] == 0
        assert data["messages"][-1]["offset"] == 49
        
    # Simulate client stopping and restarting by using a fresh client context
    async with client_factory() as client:
        # Verify it resumes from offset 50
        response = await client.get(f"/topics/{topic}/messages?partition=0&group={group}&limit=50")
        assert response.status_code == 200
        data = response.json()
        assert len(data["messages"]) == 50
        assert data["messages"][0]["offset"] == 50
        assert data["messages"][-1]["offset"] == 99

@pytest.mark.asyncio
async def test_server_metadata_persistence(client_factory):
    topic = "persist-topic"
    
    # 1. Produce 100 messages to a topic
    async with client_factory() as client:
        response = await client.post("/topics", json={"name": topic, "partitions": 1})
        assert response.status_code == 200
        
        messages = [{"key": f"kp-{i}", "value": f"vp-{i}"} for i in range(100)]
        response = await client.post(f"/topics/{topic}/messages/batch", json={"messages": messages})
        assert response.status_code == 200
        
    # 2. Simulate server restart (new client scope triggers new lifespan)
    async with client_factory() as client:
        # 3. Consume from offset 0 and verify all 100 messages are still present
        response = await client.get(f"/topics/{topic}/messages?partition=0&offset=0&limit=100")
        assert response.status_code == 200
        data = response.json()
        assert len(data["messages"]) == 100
        assert data["messages"][0]["offset"] == 0
        assert data["messages"][-1]["offset"] == 99
        assert data["messages"][0]["value"] == "vp-0"
        assert data["messages"][-1]["value"] == "vp-99"

@pytest.mark.asyncio
async def test_concurrent_producers(client_factory):
    topic = "concurrent-topic"
    
    async with client_factory() as client:
        # Create topic with 1 partition
        response = await client.post("/topics", json={"name": topic, "partitions": 1})
        assert response.status_code == 200
        
        # 5 concurrent tasks producing 100 messages each
        num_tasks = 5
        msgs_per_task = 100
        
        async def produce_task(task_id):
            for i in range(msgs_per_task):
                resp = await client.post(f"/topics/{topic}/messages", json={
                    "key": f"t-{task_id}",
                    "value": f"val-{task_id}-{i}"
                })
                assert resp.status_code == 200
                
        # Run concurrently
        await asyncio.gather(*(produce_task(i) for i in range(num_tasks)))
        
        # Consume all messages and verify
        response = await client.get(f"/topics/{topic}/messages?partition=0&offset=0&limit=600")
        assert response.status_code == 200
        data = response.json()
        messages = data["messages"]
        
        # Total messages should be 500
        assert len(messages) == num_tasks * msgs_per_task
        
        # Verify offsets are strictly sequential (0 to 499)
        offsets = [m["offset"] for m in messages]
        assert offsets == list(range(num_tasks * msgs_per_task))

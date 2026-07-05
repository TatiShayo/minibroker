import pytest
from httpx import AsyncClient, ASGITransport
from main import app

@pytest.fixture
async def client():
    async with app.router.lifespan_context(app):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://testserver") as ac:
            yield ac

@pytest.mark.asyncio
async def test_health_endpoint(client):
    response = await client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "healthy"}

@pytest.mark.asyncio
async def test_topic_lifecycle(client):
    # 1. Create a topic
    response = await client.post("/topics", json={"name": "test-topic-1", "partitions": 2})
    assert response.status_code == 200
    data = response.json()
    assert data["name"] == "test-topic-1"
    assert len(data["partitions"]) == 2
    assert data["total_messages"] == 0
    
    # 2. List topics
    response = await client.get("/topics")
    assert response.status_code == 200
    topics_list = response.json()
    assert any(t["name"] == "test-topic-1" for t in topics_list)
    
    # 3. Delete topic
    response = await client.delete("/topics/test-topic-1")
    assert response.status_code == 200
    assert response.json() == {"status": "deleted"}
    
    # Verify deleted
    response = await client.get("/topics")
    topics_list = response.json()
    assert not any(t["name"] == "test-topic-1" for t in topics_list)

@pytest.mark.asyncio
async def test_producer_endpoints(client):
    # Create topic first
    await client.post("/topics", json={"name": "prod-topic", "partitions": 1})
    
    # Single message
    response = await client.post("/topics/prod-topic/messages", json={
        "key": "key1",
        "value": "hello world",
        "headers": {"foo": "bar"}
    })
    assert response.status_code == 200
    data = response.json()
    assert data["topic"] == "prod-topic"
    assert data["partition"] == 0
    assert data["offset"] == 0
    
    # Batch produce
    response = await client.post("/topics/prod-topic/messages/batch", json={
        "messages": [
            {"key": "key2", "value": "msg2"},
            {"key": "key3", "value": "msg3"}
        ]
    })
    assert response.status_code == 200
    batch_data = response.json()
    assert batch_data["topic"] == "prod-topic"
    assert len(batch_data["results"]) == 2
    assert batch_data["results"][0]["offset"] == 1
    assert batch_data["results"][1]["offset"] == 2

@pytest.mark.asyncio
async def test_consumer_endpoints(client):
    # Create topic
    await client.post("/topics", json={"name": "cons-topic", "partitions": 1})
    
    # Produce 3 messages
    await client.post("/topics/cons-topic/messages", json={"key": "k", "value": "v0"})
    await client.post("/topics/cons-topic/messages", json={"key": "k", "value": "v1"})
    await client.post("/topics/cons-topic/messages", json={"key": "k", "value": "v2"})
    
    # Pull messages without group, specifying offset
    response = await client.get("/topics/cons-topic/messages?partition=0&offset=1&limit=2")
    assert response.status_code == 200
    data = response.json()
    assert len(data["messages"]) == 2
    assert data["messages"][0]["offset"] == 1
    assert data["messages"][0]["value"] == "v1"
    assert data["next_offset"] == 3
    assert data["has_more"] is False
    
    # Register consumer group with initial_offset earliest
    response = await client.post("/topics/cons-topic/consumer/register", json={
        "group": "g1",
        "initial_offset": "earliest"
    })
    assert response.status_code == 200
    assert response.json()["status"] == "registered"
    
    # Pull messages with group (should start from 0)
    response = await client.get("/topics/cons-topic/messages?partition=0&group=g1&limit=2")
    assert response.status_code == 200
    data = response.json()
    assert len(data["messages"]) == 2
    assert data["messages"][0]["offset"] == 0
    assert data["next_offset"] == 2
    
    # Pull again with group (should start from 2, since last pull auto-committed offset=2)
    response = await client.get("/topics/cons-topic/messages?partition=0&group=g1&limit=2")
    assert response.status_code == 200
    data = response.json()
    assert len(data["messages"]) == 1
    assert data["messages"][0]["offset"] == 2
    assert data["next_offset"] == 3
    
    # Commit offset manually
    response = await client.post("/topics/cons-topic/commit", json={
        "group": "g1",
        "partition": 0,
        "offset": 1
    })
    assert response.status_code == 200
    
    # Check consumer groups listing
    response = await client.get("/groups")
    assert response.status_code == 200
    groups = response.json()
    assert len(groups) == 1
    assert groups[0]["group"] == "g1"
    assert groups[0]["topic"] == "cons-topic"
    assert groups[0]["current_offset"] == 1
    assert groups[0]["latest_offset"] == 3
    assert groups[0]["lag"] == 2

@pytest.mark.asyncio
async def test_stats_endpoint(client):
    # Initially 0
    response = await client.get("/stats")
    assert response.status_code == 200
    assert response.json()["topics"] == 0
    
    # Create topic and produce message
    await client.post("/topics", json={"name": "stats-topic", "partitions": 2})
    await client.post("/topics/stats-topic/messages", json={"key": "k", "value": "val"})
    
    response = await client.get("/stats")
    assert response.status_code == 200
    data = response.json()
    assert data["topics"] == 1
    assert data["partitions"] == 2
    assert data["total_messages"] == 1

@pytest.mark.asyncio
async def test_edge_cases(client):
    # 1. Nonexistent topic produce
    response = await client.post("/topics/nonexistent/messages", json={"value": "v"})
    assert response.status_code == 404
    
    # 2. Nonexistent topic consume
    response = await client.get("/topics/nonexistent/messages?partition=0")
    assert response.status_code == 404
    
    # 3. Invalid topic name
    response = await client.post("/topics", json={"name": "../unsafe", "partitions": 1})
    assert response.status_code == 400
    
    # Create valid topic first for partition/offset testing
    await client.post("/topics", json={"name": "edge-topic", "partitions": 1})
    
    # 4. Negative partition
    response = await client.get("/topics/edge-topic/messages?partition=-1")
    assert response.status_code == 404
    
    # 5. Negative offset
    response = await client.get("/topics/edge-topic/messages?partition=0&offset=-5")
    assert response.status_code == 400
    
    # 6. Bad payload (limit <= 0)
    response = await client.get("/topics/edge-topic/messages?partition=0&limit=-1")
    assert response.status_code == 400

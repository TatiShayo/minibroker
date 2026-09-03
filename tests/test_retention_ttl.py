import time
import json
import pytest
from core.filter import is_message_expired, matches_filter
from core.topic import Topic

def test_message_ttl_expiration_logic():
    now_ms = int(time.time() * 1000)
    
    # Non-expired message with 1 hour TTL
    fresh_msg = {
        "timestamp": now_ms,
        "headers": {"x-ttl-ms": 3600_000}
    }
    assert is_message_expired(fresh_msg, current_time_ms=now_ms) is False
    
    # Expired message (timestamp 10s ago, TTL 5s)
    expired_msg = {
        "timestamp": now_ms - 10_000,
        "headers": {"x-ttl-ms": 5000}
    }
    assert is_message_expired(expired_msg, current_time_ms=now_ms) is True

def test_message_explicit_expires_at():
    now_ms = 1_000_000
    
    msg_future = {"timestamp": 900_000, "headers": {"x-expires-at": 1_000_500}}
    assert is_message_expired(msg_future, current_time_ms=now_ms) is False
    
    msg_past = {"timestamp": 900_000, "headers": {"x-expires-at": 999_999}}
    assert is_message_expired(msg_past, current_time_ms=now_ms) is True

def test_matches_filter_predicates():
    now_ms = 1_000_000
    msg = {
        "timestamp": now_ms,
        "key": "sensor-1",
        "value": {"temp": 22.5},
        "headers": {"region": "us-east", "env": "prod"}
    }
    
    # Key match
    assert matches_filter(msg, key_filter="sensor-1", current_time_ms=now_ms) is True
    assert matches_filter(msg, key_filter="sensor-2", current_time_ms=now_ms) is False
    
    # Header match
    assert matches_filter(msg, header_filter={"region": "us-east"}, current_time_ms=now_ms) is True
    assert matches_filter(msg, header_filter={"region": "us-east", "env": "prod"}, current_time_ms=now_ms) is True
    assert matches_filter(msg, header_filter={"region": "eu-west"}, current_time_ms=now_ms) is False
    
    # Timestamp bounds
    assert matches_filter(msg, since_timestamp=900_000, until_timestamp=1_100_000, current_time_ms=now_ms) is True
    assert matches_filter(msg, since_timestamp=1_000_001, current_time_ms=now_ms) is False
    assert matches_filter(msg, until_timestamp=999_999, current_time_ms=now_ms) is False

@pytest.mark.asyncio
async def test_consume_with_filters_api(client):
    topic = "filter-topic"
    await client.post("/topics", json={"name": topic, "partitions": 1})
    
    # Produce messages with diverse keys and headers
    await client.post(f"/topics/{topic}/messages", json={"key": "k1", "value": "v1", "headers": {"env": "prod"}})
    await client.post(f"/topics/{topic}/messages", json={"key": "k2", "value": "v2", "headers": {"env": "dev"}})
    await client.post(f"/topics/{topic}/messages", json={"key": "k1", "value": "v3", "headers": {"env": "staging"}})
    
    # Filter by key
    resp_key = await client.get(f"/topics/{topic}/messages?partition=0&key=k1")
    assert resp_key.status_code == 200
    data_key = resp_key.json()
    assert len(data_key["messages"]) == 2
    assert all(m["key"] == "k1" for m in data_key["messages"])
    
    # Filter by header JSON
    header_json = json.dumps({"env": "dev"})
    resp_header = await client.get(f"/topics/{topic}/messages?partition=0&header_filter={header_json}")
    assert resp_header.status_code == 200
    data_header = resp_header.json()
    assert len(data_header["messages"]) == 1
    assert data_header["messages"][0]["key"] == "k2"

@pytest.mark.asyncio
async def test_consume_drop_expired_ttl_api(client):
    topic = "ttl-topic"
    await client.post("/topics", json={"name": topic, "partitions": 1})
    
    # Produce 1 expired message (expires in past) and 1 fresh message
    past_ts = int(time.time() * 1000) - 100_000
    await client.post(f"/topics/{topic}/messages", json={
        "key": "expired",
        "value": "expired_val",
        "headers": {"x-expires-at": past_ts}
    })
    await client.post(f"/topics/{topic}/messages", json={
        "key": "fresh",
        "value": "fresh_val",
        "headers": {"x-ttl-ms": 3600_000}
    })
    
    # Consume without drop_expired -> gets both
    resp_all = await client.get(f"/topics/{topic}/messages?partition=0&drop_expired=false")
    assert resp_all.status_code == 200
    assert len(resp_all.json()["messages"]) == 2
    
    # Consume with drop_expired=true -> gets only fresh
    resp_filtered = await client.get(f"/topics/{topic}/messages?partition=0&drop_expired=true")
    assert resp_filtered.status_code == 200
    msgs = resp_filtered.json()["messages"]
    assert len(msgs) == 1
    assert msgs[0]["key"] == "fresh"

@pytest.mark.asyncio
async def test_admin_cleanup_endpoints(client):
    topic = "cleanup-topic"
    await client.post("/topics", json={"name": topic, "partitions": 1})
    await client.post(f"/topics/{topic}/messages", json={"value": "v"})
    
    # Trigger topic cleanup
    resp_topic = await client.post(f"/topics/{topic}/cleanup?retention_hours=0.01")
    assert resp_topic.status_code == 200
    assert resp_topic.json()["status"] == "cleaned"
    
    # Trigger global cleanup
    resp_global = await client.post("/admin/cleanup?retention_hours=0.01")
    assert resp_global.status_code == 200
    assert resp_global.json()["status"] == "cleaned"

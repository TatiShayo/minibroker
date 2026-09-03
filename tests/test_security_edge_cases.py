import pytest
import os
import json
from storage.persistence import is_safe_topic_name, get_metadata_path
from core.topic import Topic
from core.segment import Segment

# -----------------------------------------------------------------------------
# Security & Path Traversal Tests
# -----------------------------------------------------------------------------

def test_path_traversal_topic_validation():
    unsafe_names = [
        "../unsafe",
        "..\\unsafe",
        "../../etc/passwd",
        ".",
        "..",
        "topic/subpath",
        "topic\\subpath",
        "topic\x00null",
        "topic name with spaces",
        "topic*star",
        "topic?question",
        "",
        None
    ]
    for name in unsafe_names:
        assert is_safe_topic_name(name) is False

    safe_names = [
        "orders",
        "orders.DLQ",
        "events-2026",
        "user_activity_log",
        "topic.name-123_v1"
    ]
    for name in safe_names:
        assert is_safe_topic_name(name) is True

def test_get_metadata_path_traversal_detection():
    with pytest.raises(ValueError):
        get_metadata_path("../../../escape_dir")

# -----------------------------------------------------------------------------
# Edge Cases & Boundary Conditions
# -----------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_consume_from_empty_topic(client):
    topic = "empty-topic"
    await client.post("/topics", json={"name": topic, "partitions": 1})
    
    resp = await client.get(f"/topics/{topic}/messages?partition=0&offset=0")
    assert resp.status_code == 200
    data = resp.json()
    assert len(data["messages"]) == 0
    assert data["next_offset"] == 0
    assert data["has_more"] is False

@pytest.mark.asyncio
async def test_consume_beyond_high_watermark(client):
    topic = "beyond-hw-topic"
    await client.post("/topics", json={"name": topic, "partitions": 1})
    await client.post(f"/topics/{topic}/messages", json={"value": "v0"})
    
    # High watermark is 1. Request offset 100.
    resp = await client.get(f"/topics/{topic}/messages?partition=0&offset=100")
    assert resp.status_code == 200
    data = resp.json()
    assert len(data["messages"]) == 0
    assert data["next_offset"] == 100
    assert data["has_more"] is False

@pytest.mark.asyncio
async def test_deeply_nested_json_payloads(client):
    topic = "nested-json-topic"
    await client.post("/topics", json={"name": topic, "partitions": 1})
    
    nested_value = {
        "level1": {
            "level2": {
                "level3": [1, 2, {"key": "val"}]
            }
        },
        "flags": [True, False, None]
    }
    nested_headers = {
        "meta": {"trace_id": "xyz-123", "tags": ["prod", "us"]}
    }
    
    prod_resp = await client.post(f"/topics/{topic}/messages", json={
        "key": "complex_key",
        "value": nested_value,
        "headers": nested_headers
    })
    assert prod_resp.status_code == 200
    
    cons_resp = await client.get(f"/topics/{topic}/messages?partition=0&offset=0")
    assert cons_resp.status_code == 200
    rec = cons_resp.json()["messages"][0]
    assert rec["value"] == nested_value
    assert rec["headers"] == nested_headers

@pytest.mark.asyncio
async def test_multi_partition_lag_calculation_accuracy(client):
    topic = "lag-test-topic"
    group = "lag-group"
    await client.post("/topics", json={"name": topic, "partitions": 3})
    
    # Produce 10 to partition 0, 20 to partition 1, 30 to partition 2
    for i in range(10):
        await client.post(f"/topics/{topic}/messages", json={"partition": 0, "value": i})
    for i in range(20):
        await client.post(f"/topics/{topic}/messages", json={"partition": 1, "value": i})
    for i in range(30):
        await client.post(f"/topics/{topic}/messages", json={"partition": 2, "value": i})
        
    # Commit partition 0 at 10 (lag 0), partition 1 at 15 (lag 5), partition 2 at 10 (lag 20)
    await client.post(f"/topics/{topic}/commit/batch", json={
        "group": group,
        "offsets": {0: 10, 1: 15, 2: 10}
    })
    
    groups_resp = await client.get("/groups")
    assert groups_resp.status_code == 200
    groups = groups_resp.json()
    group_info = next(g for g in groups if g["group"] == group and g["topic"] == topic)
    
    assert group_info["current_offset"] == 35  # 10 + 15 + 10
    assert group_info["latest_offset"] == 60   # 10 + 20 + 30
    assert group_info["lag"] == 25             # 60 - 35
    
    part_details = {p["partition"]: p for p in group_info["partition_details"]}
    assert part_details[0]["lag"] == 0
    assert part_details[1]["lag"] == 5
    assert part_details[2]["lag"] == 20

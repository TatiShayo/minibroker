import pytest
from core.dlq import route_to_dlq, redrive_dlq_messages, get_dlq_topic_name
from core.topic import Topic
from core.offsets import init_db, get_dead_letters, delete_dead_letter

@pytest.mark.asyncio
async def test_dlq_topic_name_generation():
    assert get_dlq_topic_name("orders") == "orders.DLQ"
    assert get_dlq_topic_name("orders.DLQ") == "orders.DLQ"

@pytest.mark.asyncio
async def test_route_to_dlq_auto_creates_topic():
    await init_db()
    topics = {}
    
    # Create main topic
    main_topic = Topic("orders", 2)
    await main_topic.startup()
    topics["orders"] = main_topic
    
    # Produce a message to partition 1 of main topic
    rec = await main_topic.get_partition(1).append("key1", {"order_id": 123}, {"source": "web"})
    
    # Route to DLQ
    dlq_res = await route_to_dlq(
        topics=topics,
        original_topic="orders",
        original_partition=1,
        original_offset=rec["offset"],
        key="key1",
        value={"order_id": 123},
        headers={"source": "web"},
        reason="deserialization_error",
        retry_count=3
    )
    
    assert dlq_res["status"] == "routed_to_dlq"
    assert dlq_res["dlq_topic"] == "orders.DLQ"
    assert "orders.DLQ" in topics
    
    # Verify DLQ partition contains message with enriched headers
    dlq_topic = topics["orders.DLQ"]
    dlq_part = dlq_topic.get_partition(0)
    dlq_records = await dlq_part.read(0, 10)
    
    assert len(dlq_records) == 1
    dlq_msg = dlq_records[0]
    assert dlq_msg["key"] == "key1"
    assert dlq_msg["value"] == {"order_id": 123}
    assert dlq_msg["headers"]["x-original-topic"] == "orders"
    assert dlq_msg["headers"]["x-original-partition"] == 1
    assert dlq_msg["headers"]["x-death-reason"] == "deserialization_error"
    assert dlq_msg["headers"]["x-retry-count"] == 3
    assert "x-death-timestamp" in dlq_msg["headers"]

@pytest.mark.asyncio
async def test_dlq_audit_database():
    await init_db()
    topics = {}
    main_topic = Topic("payments", 1)
    await main_topic.startup()
    topics["payments"] = main_topic
    
    await route_to_dlq(
        topics=topics,
        original_topic="payments",
        original_partition=0,
        original_offset=5,
        key="tx-99",
        value={"amount": 500},
        headers={},
        reason="timeout",
        retry_count=4
    )
    
    # Query database audit entries
    audit_entries = await get_dead_letters(topic="payments.DLQ")
    assert len(audit_entries) >= 1
    latest = audit_entries[0]
    assert latest["original_topic"] == "payments"
    assert latest["death_reason"] == "timeout"
    assert latest["retry_count"] == 4
    
    # Delete audit entry
    deleted = await delete_dead_letter(latest["id"])
    assert deleted is True

@pytest.mark.asyncio
async def test_redrive_dlq_messages():
    await init_db()
    topics = {}
    
    main_topic = Topic("notifications", 2)
    await main_topic.startup()
    topics["notifications"] = main_topic
    
    # Route 2 messages to DLQ
    await route_to_dlq(topics, "notifications", 0, 10, "k1", "val1", {}, "err1", 3)
    await route_to_dlq(topics, "notifications", 1, 11, "k2", "val2", {}, "err2", 3)
    
    # Redrive DLQ back to notifications
    redrive_res = await redrive_dlq_messages(topics, "notifications.DLQ", max_count=10)
    assert redrive_res["replayed_count"] == 2
    
    # Check that main topic received the re-driven messages
    total_in_main = sum(p.get_offset_count() for p in main_topic.partitions.values())
    assert total_in_main == 2

@pytest.mark.asyncio
async def test_dlq_api_endpoints(client):
    # 1. Create topic
    await client.post("/topics", json={"name": "orders-api", "partitions": 1})
    
    # 2. Produce message
    prod_resp = await client.post("/topics/orders-api/messages", json={"key": "k", "value": {"item": "watch"}})
    assert prod_resp.status_code == 200
    
    # 3. Route to DLQ via API
    dlq_resp = await client.post("/topics/orders-api/dlq/route", json={
        "partition": 0,
        "offset": 0,
        "key": "k",
        "value": {"item": "watch"},
        "reason": "payment_failed",
        "retry_count": 3
    })
    assert dlq_resp.status_code == 200
    assert dlq_resp.json()["status"] == "routed_to_dlq"
    
    # 4. Read DLQ messages via API
    get_dlq_resp = await client.get("/topics/orders-api/dlq/messages")
    assert get_dlq_resp.status_code == 200
    dlq_data = get_dlq_resp.json()
    assert len(dlq_data["messages"]) == 1
    assert dlq_data["messages"][0]["value"] == {"item": "watch"}
    
    # 5. Audit query API
    audit_resp = await client.get("/dlq/audit")
    assert audit_resp.status_code == 200
    assert audit_resp.json()["count"] >= 1
    
    # 6. Redrive DLQ API
    redrive_resp = await client.post("/topics/orders-api/dlq/redrive", json={"max_count": 10})
    assert redrive_resp.status_code == 200
    assert redrive_resp.json()["replayed_count"] == 1

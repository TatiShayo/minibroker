import pytest

@pytest.mark.asyncio
async def test_admin_api_topic_crud(client):
    # 1. Create Topic
    resp = await client.post("/topics", json={"name": "admin-topic", "partitions": 3})
    assert resp.status_code == 200
    topic_data = resp.json()
    assert topic_data["name"] == "admin-topic"
    assert len(topic_data["partitions"]) == 3
    assert topic_data["total_messages"] == 0
    
    # 2. Duplicate topic creation fails
    resp_dup = await client.post("/topics", json={"name": "admin-topic", "partitions": 1})
    assert resp_dup.status_code == 400
    assert "already exists" in resp_dup.json()["detail"]
    
    # 3. Unsafe topic name fails
    resp_bad = await client.post("/topics", json={"name": "../bad_path", "partitions": 1})
    assert resp_bad.status_code == 400
    
    # 4. Get single topic details
    resp_get = await client.get("/topics/admin-topic")
    assert resp_get.status_code == 200
    assert resp_get.json()["name"] == "admin-topic"
    
    # 5. Non-existent topic
    resp_404 = await client.get("/topics/nonexistent-topic")
    assert resp_404.status_code == 404
    
    # 6. Expand topic partitions
    resp_exp = await client.post("/topics/admin-topic/partitions", json={"partitions": 5})
    assert resp_exp.status_code == 200
    assert len(resp_exp.json()["partitions"]) == 5
    
    # 7. Invalid partition expansion
    resp_exp_bad = await client.post("/topics/admin-topic/partitions", json={"partitions": 3})
    assert resp_exp_bad.status_code == 400
    
    # 8. Delete topic
    resp_del = await client.delete("/topics/admin-topic")
    assert resp_del.status_code == 200
    assert resp_del.json()["status"] == "deleted"
    
    # 9. Delete non-existent topic fails with 404
    resp_del_404 = await client.delete("/topics/admin-topic")
    assert resp_del_404.status_code == 404

@pytest.mark.asyncio
async def test_producer_api_advanced(client):
    topic = "prod-adv-topic"
    await client.post("/topics", json={"name": topic, "partitions": 2})
    
    # Produce with explicit partition
    resp_p1 = await client.post(f"/topics/{topic}/messages", json={
        "partition": 1,
        "key": "k1",
        "value": {"event": "login"},
        "headers": {"auth": "bearer"}
    })
    assert resp_p1.status_code == 200
    data_p1 = resp_p1.json()
    assert data_p1["partition"] == 1
    assert data_p1["offset"] == 0
    
    # Produce with out of bounds partition fails
    resp_bad_part = await client.post(f"/topics/{topic}/messages", json={
        "partition": 9,
        "value": "v"
    })
    assert resp_bad_part.status_code == 400
    
    # Batch produce
    batch_msgs = [
        {"key": f"key-{i}", "value": f"val-{i}", "headers": {"idx": i}}
        for i in range(20)
    ]
    resp_batch = await client.post(f"/topics/{topic}/messages/batch", json={"messages": batch_msgs})
    assert resp_batch.status_code == 200
    batch_data = resp_batch.json()
    assert len(batch_data["results"]) == 20

@pytest.mark.asyncio
async def test_consumer_api_offsets_and_rebalance(client):
    topic = "consumer-adv-topic"
    group = "adv-group"
    await client.post("/topics", json={"name": topic, "partitions": 2})
    
    # Produce 10 messages to partition 0
    for i in range(10):
        await client.post(f"/topics/{topic}/messages", json={"partition": 0, "value": f"val-{i}"})
        
    # Register consumer with consumer_id and range strategy
    reg_resp = await client.post(f"/topics/{topic}/consumer/register", json={
        "group": group,
        "initial_offset": "earliest",
        "consumer_id": "c1",
        "strategy": "range"
    })
    assert reg_resp.status_code == 200
    reg_data = reg_resp.json()
    assert reg_data["consumer_id"] == "c1"
    assert 0 in reg_data["assigned_partitions"]
    
    # Heartbeat
    hb_resp = await client.post(f"/topics/{topic}/consumer/heartbeat", json={
        "group": group,
        "consumer_id": "c1"
    })
    assert hb_resp.status_code == 200
    assert hb_resp.json()["status"] == "heartbeat_ok"
    
    # Consume 4 messages with group (auto-commits offset to 4)
    cons_resp = await client.get(f"/topics/{topic}/messages?partition=0&group={group}&limit=4")
    assert cons_resp.status_code == 200
    assert len(cons_resp.json()["messages"]) == 4
    assert cons_resp.json()["next_offset"] == 4
    
    # Check lag in /groups
    groups_resp = await client.get("/groups")
    assert groups_resp.status_code == 200
    groups_list = groups_resp.json()
    assert len(groups_list) >= 1
    g_info = next(g for g in groups_list if g["group"] == group and g["topic"] == topic)
    assert g_info["current_offset"] == 4
    assert g_info["latest_offset"] == 10
    assert g_info["lag"] == 6
    
    # Batch commit offsets
    commit_resp = await client.post(f"/topics/{topic}/commit/batch", json={
        "group": group,
        "offsets": {0: 8, 1: 0}
    })
    assert commit_resp.status_code == 200
    assert commit_resp.json()["status"] == "committed_batch"
    
    # Reset offsets to earliest
    reset_resp = await client.post(f"/topics/{topic}/consumer/reset", json={
        "group": group,
        "partition": 0,
        "offset": "earliest"
    })
    assert reset_resp.status_code == 200
    assert reset_resp.json()["status"] == "reset"
    
    # Consume again should start from 0
    cons_reset_resp = await client.get(f"/topics/{topic}/messages?partition=0&group={group}&limit=2")
    assert cons_reset_resp.json()["messages"][0]["offset"] == 0
    
    # Delete consumer group
    del_group_resp = await client.delete(f"/groups/{group}")
    assert del_group_resp.status_code == 200
    assert del_group_resp.json()["status"] == "deleted"

@pytest.mark.asyncio
async def test_stats_and_metrics_api(client):
    topic = "metrics-test-topic"
    await client.post("/topics", json={"name": topic, "partitions": 1})
    await client.post(f"/topics/{topic}/messages", json={"value": "payload-abc"})
    await client.get(f"/topics/{topic}/messages?partition=0")
    
    # Get stats
    stats_resp = await client.get("/stats")
    assert stats_resp.status_code == 200
    stats = stats_resp.json()
    assert stats["topics"] >= 1
    assert stats["messages_produced_total"] >= 1
    assert stats["messages_consumed_total"] >= 1
    assert stats["uptime_seconds"] > 0
    
    # Get metrics
    metrics_resp = await client.get("/metrics")
    assert metrics_resp.status_code == 200
    metrics_data = metrics_resp.json()
    assert "throughput_msg_sec" in metrics_data
    assert "topic_breakdown" in metrics_data

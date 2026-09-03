import pytest
import time
import asyncio

from core.rebalance import assign_range, assign_round_robin, assign_sticky, assign_partitions
from core.consumer_group import ConsumerGroupCoordinator, ConsumerMember

# -----------------------------------------------------------------------------
# Rebalancing Algorithm Unit Tests
# -----------------------------------------------------------------------------

def test_assign_range_balanced():
    # 6 partitions, 3 consumers -> 2 each
    res = assign_range([0, 1, 2, 3, 4, 5], ["c1", "c2", "c3"])
    assert res == {
        "c1": [0, 1],
        "c2": [2, 3],
        "c3": [4, 5]
    }

def test_assign_range_uneven():
    # 4 partitions, 3 consumers -> 2, 1, 1
    res = assign_range([0, 1, 2, 3], ["c1", "c2", "c3"])
    assert res == {
        "c1": [0, 1],
        "c2": [2],
        "c3": [3]
    }

def test_assign_range_more_consumers_than_partitions():
    # 2 partitions, 4 consumers -> 2 consumers get 1, 2 get 0
    res = assign_range([0, 1], ["c1", "c2", "c3", "c4"])
    assert res["c1"] == [0]
    assert res["c2"] == [1]
    assert res["c3"] == []
    assert res["c4"] == []

def test_assign_round_robin():
    # 5 partitions, 2 consumers -> c1 gets [0, 2, 4], c2 gets [1, 3]
    res = assign_round_robin([0, 1, 2, 3, 4], ["c1", "c2"])
    assert res["c1"] == [0, 2, 4]
    assert res["c2"] == [1, 3]

def test_assign_sticky_retains_existing():
    partitions = [0, 1, 2, 3]
    consumers = ["c1", "c2"]
    current = {
        "c1": [0, 1],
        "c2": [2, 3]
    }
    # Add partition 4
    new_partitions = [0, 1, 2, 3, 4]
    res = assign_sticky(new_partitions, consumers, current)
    # Existing partitions should remain with c1 and c2
    assert set([0, 1]).issubset(set(res["c1"]))
    assert set([2, 3]).issubset(set(res["c2"]))
    # Partition 4 assigned to one of them
    assert len(res["c1"]) + len(res["c2"]) == 5

def test_assign_sticky_consumer_leave():
    consumers = ["c1", "c2", "c3"]
    partitions = [0, 1, 2]
    current = {"c1": [0], "c2": [1], "c3": [2]}
    
    # c3 leaves
    surviving = ["c1", "c2"]
    res = assign_sticky(partitions, surviving, current)
    assert 0 in res["c1"]
    assert 1 in res["c2"]
    # Partition 2 redistributed to c1 or c2
    assert (2 in res["c1"]) or (2 in res["c2"])
    assert len(res["c1"]) + len(res["c2"]) == 3

def test_assign_partitions_empty_edge_cases():
    assert assign_partitions("range", [], ["c1"]) == {"c1": []}
    assert assign_partitions("round_robin", [0, 1], []) == {}
    assert assign_partitions("sticky", [], []) == {}

# -----------------------------------------------------------------------------
# Coordinator Integration Tests
# -----------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_coordinator_join_and_rebalance():
    coord = ConsumerGroupCoordinator()
    
    # Consumer 1 joins
    parts_c1 = await coord.join_group(
        group_id="group-A",
        topic="orders",
        consumer_id="c1",
        total_partitions=4,
        strategy="range"
    )
    assert parts_c1 == [0, 1, 2, 3]
    
    # Consumer 2 joins -> rebalance
    parts_c2 = await coord.join_group(
        group_id="group-A",
        topic="orders",
        consumer_id="c2",
        total_partitions=4,
        strategy="range"
    )
    assert parts_c2 == [2, 3]
    
    # Verify c1 now has [0, 1]
    assignments = await coord.get_assignments("group-A", "orders")
    assert assignments["c1"] == [0, 1]
    assert assignments["c2"] == [2, 3]

@pytest.mark.asyncio
async def test_coordinator_leave_group():
    coord = ConsumerGroupCoordinator()
    await coord.join_group("g1", "events", "c1", 3)
    await coord.join_group("g1", "events", "c2", 3)
    
    # c1 leaves
    left = await coord.leave_group("g1", "events", "c1", 3)
    assert left is True
    
    assignments = await coord.get_assignments("g1", "events")
    assert "c1" not in assignments
    assert assignments["c2"] == [0, 1, 2]

@pytest.mark.asyncio
async def test_coordinator_heartbeat_and_reap():
    coord = ConsumerGroupCoordinator()
    await coord.join_group("g1", "metrics", "c1", 2)
    await coord.join_group("g1", "metrics", "c2", 2)
    
    # Simulate c1 missing heartbeats
    member_c1 = await coord.get_member("g1", "metrics", "c1")
    member_c1.last_heartbeat = time.time() - 100.0  # 100s ago
    
    # c2 sent a fresh heartbeat
    hb_ok = await coord.heartbeat("g1", "metrics", "c2")
    assert hb_ok is True
    
    # Reap with 30s timeout
    reaped = await coord.reap_stale_consumers(timeout_seconds=30.0, get_topic_partitions_fn=lambda t: 2)
    assert reaped == 1
    
    assignments = await coord.get_assignments("g1", "metrics")
    assert "c1" not in assignments
    assert assignments["c2"] == [0, 1]

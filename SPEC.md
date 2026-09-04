# SPEC 001: Append-Only Commit Log & Consumer Group Rebalancing

## Problem Statement
Microservice architectures need reliable, lightweight messaging without the operational complexity of Kafka or ZooKeeper.

## Solution
A fast append-only commit log engine with configurable fsync policies, range-based partition assignment, and consumer offset tracking.

## User Stories
1. As a producer, I want to publish messages with guaranteed disk commit, so that events are never lost.
2. As a consumer group worker, I want partitions rebalanced automatically when peers crash, so that processing continues uninterrupted.

## Implementation Decisions
- Fsync policy controller in `minibroker/storage/log.py`.
- Consumer range assignor in `minibroker/coordinator/rebalance.py`.

## Testing Decisions
- Seam: `tests/test_fsync_and_flush_policies.py`.
- Verify data durability, flush batch triggers, and consumer partition allocations.

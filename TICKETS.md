# TICKETS — MiniBroker Core Pipeline

## [TICKET-001] Tri-Mode Fsync Durability Controller
- **Blocked by**: None
- **Delivers**: Log flush manager supporting ALWAYS, BATCH, and PERIODIC policies.
- **Verification**: `tests/test_fsync_and_flush_policies.py`

## [TICKET-002] Consumer Group Range Rebalance Coordinator
- **Blocked by**: TICKET-001
- **Delivers**: Partition assignor allocating topic partitions evenly across active consumer cluster members.
- **Verification**: Partition distribution unit tests.

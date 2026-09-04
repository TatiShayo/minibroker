# ADR 0001: Group Commit Fsync Policies and Deterministic Partition Balancing

## Context
Message brokers require balance between zero-data-loss durability and high write IOPS.

## Decision
1. **Tri-Mode Fsync**: Support `ALWAYS`, `BATCH`, and `PERIODIC` disk synchronization.
2. **Range Partition Assignor**: Allocate partition ranges to consumers deterministically.
3. **Immutable Segments**: Roll log files atomically at byte-size boundaries.

## Consequences
- **Positive**: Blazing fast disk appends with configurable durability guarantees.
- **Negative**: In `PERIODIC` mode, an abrupt ungraceful kernel crash can drop unflushed buffer cache.

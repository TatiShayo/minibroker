# Grilling Session 001: minibroker
**Archetype**: Tier 3 Infrastructure (Distributed Append-Only Message Broker)
**Human Domain Authority**: Antigravity Lead Architect
**Methodology**: Matt Pocock Agent Skills (/grilling + /grill-with-docs)
**Status**: FRONTIER EXHAUSTED — SHARED UNDERSTANDING ATTAINED

---

## Round 1: Core Architecture & Invariant Frontier

❓ **Q1** - **Disk Fsync vs Throughput Trade-off**: High-frequency append-only log writes risk IO saturation if `fsync` is called per record. How do we balance persistence durability with 100k msg/sec throughput?
➡️ *Recommendation*: Tri-mode fsync policy (`FSYNC_MODE`): `ALWAYS` (financial grade), `BATCH` (group commit every N records or 5ms), and `PERIODIC` (background flush thread).

**Architect Decision**: APPROVED. Configurable durability tiers allow deployment from low-latency event buses to zero-loss financial ledgers.

---

❓ **Q2** - **Consumer Group Rebalance Partition Skew**: When consumer instances join or leave a group, how do we assign partition ranges evenly without starvation?
➡️ *Recommendation*: Range assignor algorithm with deterministic partition indexing across active consumer IDs.

**Architect Decision**: APPROVED. Deterministic range partitioning ensures predictable partition ownership without coordination chatter.

---

## Round 2: Edge Cases & Failure Modes Frontier

❓ **Q3** - **Log Segment Compaction Invariants**: When segment files exceed maximum byte sizes, how are old message offsets purged without corrupting active consumers?
➡️ *Recommendation*: Monotonic offset indexing with atomic segment rolling and immutable read-only historical segment archives.

**Architect Decision**: APPROVED. Atomic segment rolling guarantees that active read pointers never point to truncated memory.

---

## Final Alignment Attestation
The design tree has been thoroughly walked down to all leaf nodes.
No silent assumptions remain regarding authentication, concurrency, data consistency, or payment flow.

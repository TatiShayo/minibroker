You are a research agent. Study message broker architecture for a project called "minibroker" — a lightweight Kafka-inspired broker built from scratch.

## Research targets

Study these systems in depth:

### 1. Apache Kafka
- Partition and segment structure (how are files organized on disk?)
- Binary record format (what bytes make up a stored message?)
- Consumer groups (how are offsets tracked? How does rebalancing work?)
- Log compaction (how does it retain only the latest value per key?)
- Exactly-once vs at-least-once vs at-most-once semantics
- How producers route messages to partitions (hash, sticky, round-robin)

### 2. NATS
- Lightweight pub/sub architecture
- JetStream for persistence
- At-most-once vs at-least-once delivery

### 3. Redis Streams
- Message IDs (how are they generated? What's the format?)
- Consumer groups (XREADGROUP, XACK, pending entries)
- Memory-first vs disk persistence

## Technical research areas

### Append-only log design
- Why are append-only logs fast for writes?
- What is sequential vs random disk I/O and how does it matter?
- How to design a binary record format (CRC, length-prefixed fields, fixed-width headers)

### Offset management
- Offset-based vs ID-based message addressing
- How Kafka tracks the "high-water mark" (last committed message visible to consumers)
- Sparse index design for fast offset → file_position lookup

### Segment lifecycle
- When does a segment roll over?
- Retention policies (time-based, size-based, compacted)
- Segment deletion without losing data

## Deliverable

Save comprehensive report to:
C:\Users\TATI\Desktop\Clients\minibroker\research_phase0.md

Required sections:
1. Kafka internals deep-dive (partition, segment, record format, offset, consumer groups)
2. Comparison: Kafka vs NATS vs Redis Streams
3. Binary record format design (specific byte layout with sizes)
4. Sparse index design (offset → position lookup strategy)
5. Consumer group offset tracking
6. API design recommendations for minibroker
7. Implementation decisions (what to implement, what to skip for v0)

## Constraints
- Research only, no code
- Be specific with byte layouts, file structures, and API designs

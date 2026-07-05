# Mini Message Broker — Phase Plan
**Project:** minibroker  
**Budget:** $0 (all free/open-source)  
**Builder:** Gemini CLI  
**Status:** Planned, awaiting execution after contentrec → vectordb  

---

## What it is

A lightweight, Kafka-inspired message broker. Topics, partitions, append-only logs, consumer groups, offset tracking. REST API + Python client library + CLI tools + Web dashboard.

## Why it has high recruiter impact

| What it proves | Why it matters |
|---------------|----------------|
| Append-only log design | Foundation of Kafka, databases, event sourcing |
| Sequential vs random I/O | Shows you understand how storage actually works |
| Consumer groups + offsets | Exactly-once, at-least-once semantics |
| Partition-based parallelism | Distributed systems thinking |
| Producer/consumer pattern | Messaging is #1 system design interview topic |

---

## Architecture

```
Producer Apps → REST API → Broker Core → Append-Only Log (disk) 
                                       → In-Memory Index
                                        
Consumer Apps ← REST API ← Broker Core ← Segment Files
                                       ← Offset Tracking

Web Dashboard ← Broker Stats API (topics, partitions, consumer lag)
```

---

## Phases

### Phase 1: Core Broker
**Files created:** ~12  
**Gemini sessions:** 2

```
minibroker/
├── main.py                    # FastAPI + admin server
├── config.py                  # Config (data dir, port, segment size, retention)
├── requirements.txt           # Dependencies
├── core/
│   ├── __init__.py
│   ├── log.py                 # Append-only log (segment files, sequential writes)
│   ├── segment.py             # Individual segment file (read, write, compact)
│   ├── topic.py               # Topic metadata + partition management
│   ├── partition.py           # Partition (log + high-water mark + leader)
│   └── index.py               # In-memory offset → position index
├── api/
│   ├── __init__.py
│   ├── producer.py            # POST /topics/{topic}/messages
│   ├── consumer.py            # GET /topics/{topic}/messages?offset=N&group=G
│   └── admin.py               # POST /topics, GET /topics, GET /stats
├── storage/
│   ├── __init__.py
│   ├── directory.py           # Topic directory management
│   └── persistence.py         # Save/load topic metadata
└── models/
    ├── __init__.py
    └── schemas.py             # Pydantic models
```

**API endpoints:**
```
POST   /topics                           → {"topic": str, "partitions": int}
GET    /topics                           → [{"topic": str, "partitions": int, "messages": int}]
DELETE /topics/{topic}                   → {"status": "deleted"}

POST   /topics/{topic}/messages          → {"topic": str, "partition": int, "offset": int}
                                          Body: {"key": str, "value": any, "headers": {}}
POST   /topics/{topic}/messages/batch    → multiple messages

GET    /topics/{topic}/messages?offset=N&limit=100&group=mygroup
                                          → {"messages": [...], "next_offset": int}
POST   /topics/{topic}/commit            → {"group": str, "offset": int}

GET    /stats                            → {"topics": N, "partitions": N, "messages": N, "disk_usage": "12MB"}
GET    /groups                           → [{"group": str, "topic": str, "current_offset": int, "latest_offset": int, "lag": int}]
```

**Core design:**
- Each partition = a directory with segment files
- Each segment = sequential binary file (fixed-size record format)
- Records: [crc32(4) | key_len(4) | value_len(4) | headers_len(4) | timestamp(8) | offset(8) | key | value | headers]
- Segments roll over at configurable size (default 1MB)
- Old segments deleted based on retention policy (time or size)
- In-memory sparse offset index for fast seek in segments
- Consumer groups store offset in SQLite or in-memory

---

### Phase 2: Consumer Groups + Offsets

Consumer group semantics:
```
POST /topics/{topic}/consumer/register
    → {"group": str, "initial_offset": "earliest" | "latest"}

GET /topics/{topic}/messages?group=mygroup
    → Returns next unread message, advances group offset
    → Auto-commits offset after delivery

POST /topics/{topic}/consumer/commit
    → Manually commit offset for a group

GET /groups
    → All groups with lag (latest offset - committed offset)
```

---

### Phase 3: Command-Line Tools + Client Library

```
cli/
├── minibroker          # CLI entry (topic create, produce, consume, groups, stats)
├── setup.py            # pip-installable
└── README.md

client/
├── broker_client.py    # Python client (Producer, Consumer classes)
├── setup.py
└── README.md
```

**Usage:**
```bash
# Create topic
minibroker topic create orders --partitions 3

# Produce
minibroker produce orders --key "order-123" --value '{"item":"shoes"}'

# Consume
minibroker consume orders --group email-svc --from-beginning

# Stats
minibroker stats
minibroker groups
```

---

### Phase 4: Web Dashboard

Basic monitoring dashboard with:
- Topic list with message counts and disk usage
- Partition details per topic
- Consumer groups with lag visualization
- Produce test messages from the UI
- Simple HTML + Chart.js (no React, keep it simple)

---

### Phase 5: Advanced Features (if time)

- Replication simulation (multi-process leader/follower)
- Exactly-once semantics via idempotent producers
- Log compaction (retain latest value per key — like Kafka compacted topics)
- Disk I/O benchmarks (seq vs random, KB/s, messages/s)

---

## PROMPT_SESSION_RESEARCH.md

```

You are a research agent. Study message broker architecture for a project called "minibroker" — a lightweight Kafka-inspired broker.

Research these systems in depth:

1. **Apache Kafka** — architecture: partitions, segments, offsets, consumer groups, ISR (in-sync replicas), log compaction
2. **NATS** — lightweight pub/sub, at-most-once vs at-least-once, JetStream for persistence
3. **Redis Streams** — consumer groups, message IDs, pending entries

For each, answer:
- How are messages stored on disk (format, segments, retention)?
- How do consumer groups work (offset tracking, rebalancing, commits)?
- How does the producer know which partition to write to?
- How does the consumer track position?
- Exactly-once vs at-least-once vs at-most-once semantics

Also research:
- Append-only log design — why it's fast for writes
- Sequential vs random disk I/O — what it means practically
- Binary record format design (CRC, length-prefixed fields)
- Offset-based vs ID-based message addressing

Deliverable: Save comprehensive report to:
C:\Users\TATI\Desktop\Clients\minibroker\research_phase0.md

Cover: Kafka internals in detail, comparison with NATS/Redis, binary record format design, disk I/O strategy, API design recommendations.
```

---

## PROMPT_SESSION_BUILD.md

The build prompt has been separated into its own file:
`C:\Users\TATI\Desktop\Clients\minibroker\PROMPT_SESSION_BUILD.md`

### Architect review changes applied to the build prompt:
- **Async I/O**: All file operations use `aiofiles` instead of sync `open()/write()`
- **Partition Lock**: Each partition has its own `asyncio.Lock` — only one append at a time
- **Startup Recovery**: On server start, `Partition.startup()` rebuilds the sparse offset index by scanning existing segment files
- **aiofiles + aiosqlite**: Added to requirements for non-blocking I/O
- **Full test suite**: Unit tests, API tests, integration tests with producer/consumer correctness validation

---

## Project Pipeline (Complete)

```
Phase 0 → contentrec  (recommendation engine)
Phase 1 → vectordb    (vector database)
Phase 2 → minibroker  (message broker)
Phase 3 → GitHub profile refresh
```

Each project demonstrates a different engineering skill:
- **contentrec** — ML + backend + hybrid algorithms
- **vectordb** — systems design + data structures + search  
- **minibroker** — distributed systems + storage + messaging + I/O

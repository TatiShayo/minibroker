# CONTEXT.md — Ubiquitous Domain Language (MiniBroker)

## Core Entities
- **MessageTopic**: Named logical event stream partitioned across multiple log segments.
- **LogSegment**: Immutable file-backed binary partition slice storing sequential records.
- **ConsumerGroup**: Cluster of cooperating workers sharing partition subscriptions.
- **FsyncPolicy**: Flush frequency invariant determining data durability guarantees on disk.

## Domain Invariants
- Offsets within a topic partition are strictly monotonically increasing 64-bit integers.
- An active consumer offset commit cannot regress to a lower offset value.

## Forbidden Terminology
- Do not call topics "channels"; use "Topic".
- Do not refer to messages as "events" in storage engine layers; use "LogRecord".

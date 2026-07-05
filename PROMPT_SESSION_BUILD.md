You are a backend engineer. Build a lightweight message broker — a Mini Kafka.

## Rules
- Use only free, open-source libraries
- No paid APIs
- All storage on local disk
- Python standard library wherever possible
- IMPORTANT: All file I/O MUST be async (use aiofiles). Synchronous open()/write() blocks the event loop and destroys broker performance.

## Tech Stack
- FastAPI + Uvicorn
- aiofiles for async file I/O (pip install aiofiles)
- Pure Python async file I/O (no database for core storage)
- SQLite with aiosqlite for consumer offsets
- Pydantic for schemas
- asyncio.Lock per partition for concurrency safety

## Storage Design

### Binary Record Format
Each message stored on disk as a binary record:

[crc32(4B) | key_len(4B) | value_len(4B) | headers_len(4B) | timestamp(8B) | offset(8B) | key | value | headers_json]

Total header = 32 bytes before key/value. Little-endian encoding.

### Segment Files
- Each partition stores segments in: data/topics/{topic}/{partition}/
- Segment file name: {base_offset:020d}.log (e.g., 00000000000000000000.log)
- Segment rolls over at 1MB (configurable)
- Active segment is append-only via aiofiles
- Read segments are read-only after rollover
- Deleted based on retention.ms or retention.bytes

### Offset Index
In-memory sparse index: [offset → file_position]
- Every 1000th record gets indexed (configurable)
- Fast seek: binary search on index → linear scan within range
- Rebuilt on startup by scanning existing segment files

### Partition Lock
Each Partition has an `asyncio.Lock`. Only one append at a time per partition.
This prevents interleaved writes and file corruption when multiple producers hit the same partition.

## What to Build

### 1. core/segment.py
Class `Segment`:
- `__init__(path, base_offset, max_size=1_048_576)` — 1MB default
- `async append(key, value, headers, timestamp)` → record bytes, fsync via aiofiles
  - Calculate CRC32 of key+value+headers, pack binary record with struct.pack
  - Write via aiofiles (async), call fsync after write
  - Update offset counter and sparse index
- `async read(offset, count=1)` → list of records from given offset
  - Seek via offset index → async read records → parse binary format
- `async read_all()` → async iterator over all records
- `get_size()` → current file size (from os.path.getsize or cached)
- `is_active()` → can still append (size < max_size)
- `close()` → flush pending writes
- Binary record format: [crc32(4B) | key_len(4B) | value_len(4B) | headers_len(4B) | timestamp(8B) | offset(8B) | key | value | headers_json]

### 2. core/index.py
Class `OffsetIndex`:
- `__init__(interval=1000)` — index every Nth record
- `add(offset, position)` — add entry
- `find(offset)` → position closest to offset (binary search)
- `rebuild_from_segments(segment_paths)` — scan existing segment files, rebuild index
- `clear()` — reset for new segment
- In-memory sorted list of (offset, position) tuples

### 3. core/log.py
Class `Log`:
- `__init__(topic, partition, base_dir)`
- `async append(key, value, headers, timestamp)` → record appended to active segment
  - Calls self._lock.acquire, writes, releases
- `async read(offset, max_count=100)` → list of records from offset
- `get_latest_offset()` → current high-water mark
- `get_earliest_offset()` → start of oldest segment
- `async cleanup(retention_hours=72)` — delete expired segments
- `async rebuild_index()` — scan all segment files, rebuild sparse OffsetIndex
- Manages Segment lifecycle: active + list of read-only segments
- Uses an asyncio.Lock for the entire append operation

### 4. core/partition.py
Class `Partition`:
- `__init__(topic, partition_id, base_dir)`
- `async append(key, value, headers)` → record with offset
  - Acquires self._lock (asyncio.Lock), delegates to Log.append, releases
- `async read(offset, max_count)` → records
- `get_leader()` → current leader (always 0 for single-node)
- `get_high_watermark()` → latest offset
- `get_offset_count()` → total messages
- `async startup()` — rebuild offset index from disk segments
- Wraps Log with partition lock and metadata

### 5. core/topic.py
Class `Topic`:
- `__init__(name, partitions_count)`
- `partition(key)` → partition index (hash-based routing using hashlib.md5)
- `get_partition(index)` → Partition object
- `get_all_partitions()` → list
- `async startup()` — call startup on all partitions to rebuild indexes
- Metadata management

### 6. api/producer.py (FastAPI Router)
- `POST /topics/{topic}/messages`
  - Body: `{"key": str, "value": any, "headers": dict}`
  - Hash key to partition, append asynchronously to partition log
  - Returns: `{"topic": str, "partition": int, "offset": int, "timestamp": int}`

- `POST /topics/{topic}/messages/batch`
  - Body: `{"messages": [{"key": str, "value": any, "headers": dict}, ...]}`
  - Returns: `{"topic": str, "results": [{"partition": int, "offset": int}, ...]}`

### 7. api/consumer.py (FastAPI Router)
- `GET /topics/{topic}/messages?offset=0&limit=100&group=mygroup`
  - If group provided: lookup group offset, use that instead
  - Returns: `{"topic": str, "partition": int, "messages": [...], "next_offset": int, "has_more": bool}`
  - Auto-increment group offset on successful read

- `POST /topics/{topic}/consumer/register`
  - Body: `{"group": str, "initial_offset": "earliest" | "latest" | int}`
  - Creates consumer group with offset

- `GET /groups`
  - Returns all consumer groups with their current offset and calculated lag

### 8. api/admin.py (FastAPI Router)
- `POST /topics`
  - Body: `{"name": str, "partitions": int (default 1)}`
  - Creates topic directory structure and metadata

- `GET /topics`
  - Returns: `{"topics": [{"name": str, "partitions": [{"id": int, "messages": int, "size": int}], "total_messages": int}]}`

- `DELETE /topics/{topic}`
  - Deletes topic and all data on disk

- `GET /stats`
  - Returns: `{"topics": int, "partitions": int, "total_messages": int, "disk_usage_bytes": int}`

### 9. models/schemas.py
Pydantic models for all request/response schemas.

### 10. main.py
- FastAPI app with title="MiniBroker", version="0.1.0"
- On startup: scan data directory, load existing topics, CALL startup() on each topic to rebuild offset indexes from disk
- Include all routers
- CORS enabled
- Startup health check on /health
- Startup is async — uses asyncio to rebuild indexes in parallel

### 11. config.py
```python
DATA_DIR = "data"
SEGMENT_MAX_SIZE = 1_048_576  # 1MB
INDEX_INTERVAL = 1000  # index every 1000th message
RETENTION_HOURS = 72
DEFAULT_PARTITIONS = 1
HOST = "0.0.0.0"
PORT = 8000
```

### 12. requirements.txt
```
fastapi
uvicorn
pydantic
aiofiles
aiosqlite
```

### 13. tests/test_core.py
Using pytest with pytest-asyncio:
- Test Segment: create, append records, read them back, verify binary format correctness
- Test Segment: CRC validation (tampered data raises error)
- Test Segment: rollover at size limit
- Test Log: append sequence, read from offset, get_latest_offset
- Test OffsetIndex: add entries, find by offset, binary search correctness
- Test OffsetIndex: rebuild from segment files

### 14. tests/test_api.py
Using FastAPI TestClient (httpx):
- Test all endpoints
- Test produce then consume returns the same message
- Test consumer group offset tracking
- Test topic creation and deletion
- Test /stats correctness
- Test edge cases: empty topic, nonexistent topic, bad requests

### 15. tests/test_integration.py
- Full pipeline: create topic → produce 1000 messages → consume all 1000 → verify count
- Consumer group: produce 100 → consume 50 → stop → restart consumer → verify it continues from offset 50
- Server restart: produce 100 → simulated restart → consume from 0 → all 100 messages still present
- Concurrent producers: two producers writing simultaneously → verify no corruption, offsets are sequential per partition

## Manual Verification
After building and running tests, verify:
1. `pytest tests/ -v` passes all tests
2. uvicorn main:app --reload starts without errors
3. Create a topic via curl
4. Produce 10 messages
5. Consume all 10 — every message received
6. Restart server — data persists, messages still there
7. Consumer group — offset advances correctly
8. Produce to multi-partition topic — messages distributed across partitions

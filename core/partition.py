import asyncio
from typing import Any, Dict, List, Optional, Set
from core.log import Log
from config import RETENTION_HOURS, RETENTION_MAX_BYTES

class Partition:
    """Manages a single partition's append-only log with concurrency locking and pub/sub streaming."""
    
    def __init__(self, topic: str, partition_id: int, base_dir: str):
        self.topic = topic
        self.partition_id = partition_id
        self.base_dir = base_dir
        self.log = Log(topic, partition_id, base_dir)
        self._lock = asyncio.Lock()
        self._subscribers: Set[asyncio.Queue] = set()

    async def startup(self):
        """Rebuild offset index from disk segments on partition startup."""
        await self.log.rebuild_index()

    async def append(self, key: Any, value: Any, headers: Optional[Dict[str, Any]], timestamp: Optional[int] = None) -> dict:
        """Append a record to the partition's log under lock and notify streaming subscribers."""
        async with self._lock:
            record = await self.log.append(key, value, headers, timestamp)
            self._notify_subscribers([record])
            return record

    async def append_batch(self, messages: List[dict]) -> List[dict]:
        """Append multiple records in a batch under lock and notify streaming subscribers."""
        if not messages:
            return []
        async with self._lock:
            records = await self.log.append_batch(messages)
            self._notify_subscribers(records)
            return records

    async def read(
        self,
        offset: int,
        max_count: int = 100,
        key_filter: Optional[str] = None,
        header_filter: Optional[Dict[str, Any]] = None,
        drop_expired: bool = False
    ) -> list:
        """Read records starting from offset up to max_count matching optional filters."""
        return await self.log.read(
            offset,
            max_count,
            key_filter=key_filter,
            header_filter=header_filter,
            drop_expired=drop_expired
        )

    def get_leader(self) -> int:
        """Get partition leader ID (always 0 for single-node broker)."""
        return 0

    def get_high_watermark(self) -> int:
        """Get the latest offset (next offset to be written)."""
        return self.log.get_latest_offset()

    def get_earliest_offset(self) -> int:
        """Get the earliest available offset in the partition."""
        return self.log.get_earliest_offset()

    def get_offset_count(self) -> int:
        """Get total message count currently retained in partition log."""
        return max(0, self.log.get_latest_offset() - self.log.get_earliest_offset())

    def get_size_bytes(self) -> int:
        """Get total disk usage in bytes for this partition."""
        return self.log.get_total_bytes()

    async def cleanup(
        self,
        retention_hours: float = RETENTION_HOURS,
        max_bytes: int = RETENTION_MAX_BYTES
    ) -> int:
        """Run retention cleanup on partition log."""
        return await self.log.cleanup(retention_hours, max_bytes)

    def subscribe(self, queue: asyncio.Queue):
        """Add real-time streaming subscriber queue."""
        self._subscribers.add(queue)

    def unsubscribe(self, queue: asyncio.Queue):
        """Remove real-time streaming subscriber queue."""
        self._subscribers.discard(queue)

    def _notify_subscribers(self, records: List[dict]):
        """Push newly produced records to active subscriber queues."""
        if not self._subscribers or not records:
            return
        for queue in list(self._subscribers):
            for record in records:
                try:
                    queue.put_nowait(record)
                except asyncio.QueueFull:
                    pass
                except Exception:
                    pass

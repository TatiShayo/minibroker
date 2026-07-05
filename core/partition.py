import asyncio
from core.log import Log

class Partition:
    def __init__(self, topic: str, partition_id: int, base_dir: str):
        self.topic = topic
        self.partition_id = partition_id
        self.base_dir = base_dir
        self.log = Log(topic, partition_id, base_dir)
        self._lock = asyncio.Lock()

    async def startup(self):
        """Rebuild offset index from disk segments on partition startup."""
        await self.log.rebuild_index()

    async def append(self, key, value, headers, timestamp=None) -> dict:
        """
        Append a record to the partition's log under a concurrency lock.
        Returns the appended record metadata including assigned offset.
        """
        async with self._lock:
            return await self.log.append(key, value, headers, timestamp)

    async def read(self, offset: int, max_count: int) -> list:
        """Read records starting from offset up to max_count."""
        return await self.log.read(offset, max_count)

    def get_leader(self) -> int:
        """Get partition leader ID (always 0 for single-node cluster)."""
        return 0

    def get_high_watermark(self) -> int:
        """Get the latest offset (next offset to be written)."""
        return self.log.get_latest_offset()

    def get_offset_count(self) -> int:
        """Get the total count of messages currently in the partition log."""
        return self.log.get_latest_offset() - self.log.get_earliest_offset()

import os
import hashlib
import asyncio
from typing import Any, Dict, List, Optional
from config import DATA_DIR, RETENTION_HOURS, RETENTION_MAX_BYTES
from core.partition import Partition
from storage.persistence import save_topic_metadata, is_safe_topic_name

class Topic:
    """Manages a topic comprising multiple partitions with routing, batching, and lifecycle operations."""
    
    def __init__(self, name: str, partitions_count: int):
        if not is_safe_topic_name(name):
            raise ValueError(f"Invalid or unsafe topic name: {name}")
        if partitions_count < 1:
            raise ValueError("partitions_count must be at least 1")

        self.name = name
        self.partitions_count = partitions_count
        self._rr_counter = 0
        self._lock = asyncio.Lock()
        
        topics_parent = os.path.abspath(os.path.join(DATA_DIR, "topics"))
        raw_topic_dir = os.path.join(topics_parent, name)
        self.topic_dir = os.path.abspath(raw_topic_dir)
        
        try:
            common = os.path.commonpath([topics_parent, self.topic_dir])
            if os.path.abspath(common) != topics_parent:
                raise ValueError("Path traversal detected")
        except ValueError:
            raise ValueError("Path traversal detected")
        
        self.partitions: Dict[int, Partition] = {}
        for i in range(partitions_count):
            partition_dir = os.path.join(self.topic_dir, str(i))
            self.partitions[i] = Partition(name, i, partition_dir)

    def partition(self, key: Any) -> int:
        """Route message to a partition index using hashing of key, or round-robin if no key."""
        if key is None:
            part_idx = self._rr_counter % self.partitions_count
            self._rr_counter = (self._rr_counter + 1) % 1_000_000_000
            return part_idx

        if isinstance(key, str):
            key_bytes = key.encode('utf-8')
        elif isinstance(key, bytes):
            key_bytes = key
        else:
            key_bytes = str(key).encode('utf-8')

        h = hashlib.sha256(key_bytes).hexdigest()
        return int(h, 16) % self.partitions_count

    def get_partition(self, index: int) -> Optional[Partition]:
        """Retrieve partition object by index."""
        return self.partitions.get(index)

    def get_all_partitions(self) -> List[Partition]:
        """Retrieve all partition objects as a list."""
        return list(self.partitions.values())

    async def startup(self):
        """Asynchronously start up all partitions in parallel, rebuilding sparse indexes."""
        await asyncio.gather(*(p.startup() for p in self.partitions.values()))

    async def add_partitions(self, new_total_count: int) -> int:
        """Dynamically expand the number of partitions for this topic."""
        async with self._lock:
            if new_total_count <= self.partitions_count:
                raise ValueError(f"New partition count ({new_total_count}) must be greater than current count ({self.partitions_count})")
            
            new_partitions_to_start = []
            for i in range(self.partitions_count, new_total_count):
                partition_dir = os.path.join(self.topic_dir, str(i))
                part = Partition(self.name, i, partition_dir)
                self.partitions[i] = part
                new_partitions_to_start.append(part)
                
            self.partitions_count = new_total_count
            await asyncio.gather(*(p.startup() for p in new_partitions_to_start))
            await save_topic_metadata(self.name, self.partitions_count)
            return self.partitions_count

    async def produce_batch(self, messages: List[dict]) -> List[dict]:
        """
        Partition messages and append them concurrently across target partitions.
        Each message can optionally specify 'partition' or provide a 'key' for automatic routing.
        """
        partition_buckets: Dict[int, List[tuple[int, dict]]] = {}
        for idx, msg in enumerate(messages):
            part_id = msg.get("partition")
            if part_id is None:
                part_id = self.partition(msg.get("key"))
            elif part_id not in self.partitions:
                raise ValueError(f"Partition {part_id} does not exist in topic {self.name}")
                
            if part_id not in partition_buckets:
                partition_buckets[part_id] = []
            partition_buckets[part_id].append((idx, msg))

        # Parallel appends per partition
        async def _append_to_partition(p_id: int, items: List[tuple[int, dict]]):
            partition_obj = self.partitions[p_id]
            batch_msgs = [item[1] for item in items]
            records = await partition_obj.append_batch(batch_msgs)
            return [(items[i][0], p_id, records[i]) for i in range(len(items))]

        tasks = [
            _append_to_partition(p_id, items)
            for p_id, items in partition_buckets.items()
        ]
        results_nested = await asyncio.gather(*tasks)

        # Flatten and restore original batch ordering
        flat_results = []
        for partition_results in results_nested:
            flat_results.extend(partition_results)
            
        flat_results.sort(key=lambda x: x[0])
        return [
            {
                "topic": self.name,
                "partition": r[1],
                "offset": r[2]["offset"],
                "timestamp": r[2]["timestamp"],
                "key": r[2].get("key"),
                "value": r[2].get("value"),
                "headers": r[2].get("headers")
            }
            for r in flat_results
        ]

    async def cleanup(
        self,
        retention_hours: float = RETENTION_HOURS,
        max_bytes: int = RETENTION_MAX_BYTES
    ) -> int:
        """Run retention cleanup across all partitions in parallel."""
        cleaned = await asyncio.gather(*(p.cleanup(retention_hours, max_bytes) for p in self.partitions.values()))
        return sum(cleaned)

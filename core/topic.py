import os
import hashlib
import asyncio
from config import DATA_DIR
from core.partition import Partition

class Topic:
    def __init__(self, name: str, partitions_count: int):
        self.name = name
        self.partitions_count = partitions_count
        self._rr_counter = 0
        
        topics_parent = os.path.abspath(os.path.join(DATA_DIR, "topics"))
        raw_topic_dir = os.path.join(topics_parent, name)
        self.topic_dir = os.path.abspath(raw_topic_dir)
        
        try:
            common = os.path.commonpath([topics_parent, self.topic_dir])
            if os.path.abspath(common) != topics_parent:
                raise ValueError("Path traversal detected")
        except ValueError:
            raise ValueError("Path traversal detected")
        
        self.partitions = {}
        for i in range(partitions_count):
            partition_dir = os.path.join(self.topic_dir, str(i))
            self.partitions[i] = Partition(name, i, partition_dir)

    def partition(self, key) -> int:
        """Route message to a partition index using MD5 of the key, or round-robin if no key is provided."""
        if key is None:
            part_idx = self._rr_counter
            self._rr_counter = (self._rr_counter + 1) % self.partitions_count
            return part_idx

        if isinstance(key, str):
            key_bytes = key.encode('utf-8')
        elif isinstance(key, bytes):
            key_bytes = key
        else:
            key_bytes = str(key).encode('utf-8')

        h = hashlib.md5(key_bytes).hexdigest()
        return int(h, 16) % self.partitions_count

    def get_partition(self, index: int) -> Partition:
        """Retrieve partition object by index."""
        return self.partitions.get(index)

    def get_all_partitions(self) -> list:
        """Retrieve all partition objects as a list."""
        return list(self.partitions.values())

    async def startup(self):
        """Asynchronously start up all partitions in parallel, rebuilding sparse indexes."""
        await asyncio.gather(*(p.startup() for p in self.partitions.values()))

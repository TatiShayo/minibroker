import os
import time
import asyncio
from core.segment import Segment
from config import SEGMENT_MAX_SIZE, RETENTION_HOURS

class Log:
    def __init__(self, topic: str, partition: int, base_dir: str):
        self.topic = topic
        self.partition = partition
        self.base_dir = base_dir
        self.segments = []
        self.active_segment = None
        self._lock = asyncio.Lock()

    async def rebuild_index(self):
        """Scan partition directory to resolve and recover all segment files."""
        os.makedirs(self.base_dir, exist_ok=True)
        
        log_files = []
        for filename in os.listdir(self.base_dir):
            if filename.endswith(".log"):
                name_without_ext = filename[:-4]
                try:
                    base_offset = int(name_without_ext)
                    log_files.append((base_offset, os.path.join(self.base_dir, filename)))
                except ValueError:
                    continue

        # Sort segments by base offset
        log_files.sort(key=lambda x: x[0])

        self.segments = []
        for base_offset, path in log_files:
            seg = Segment(path, base_offset, max_size=SEGMENT_MAX_SIZE)
            await seg.recover()
            self.segments.append(seg)

        # Create initial segment if none exist
        if not self.segments:
            initial_path = os.path.join(self.base_dir, f"{0:020d}.log")
            seg = Segment(initial_path, 0, max_size=SEGMENT_MAX_SIZE)
            await seg.recover()
            self.segments.append(seg)

        self.active_segment = self.segments[-1]

    async def append(self, key, value, headers, timestamp=None) -> dict:
        """Append record to active segment, handling rollover if size limit exceeded."""
        async with self._lock:
            # Check for rollover
            if self.active_segment.get_size() >= SEGMENT_MAX_SIZE:
                new_base_offset = self.active_segment.next_offset
                new_path = os.path.join(self.base_dir, f"{new_base_offset:020d}.log")
                
                await self.active_segment.close()
                
                new_seg = Segment(new_path, new_base_offset, max_size=SEGMENT_MAX_SIZE)
                await new_seg.recover()
                
                self.segments.append(new_seg)
                self.active_segment = new_seg

            return await self.active_segment.append(key, value, headers, timestamp)

    async def read(self, start_offset: int, max_count: int = 100) -> list:
        """Read records sequentially across segments starting from the specified offset."""
        records = []
        
        # Find starting segment index
        seg_idx = -1
        for i, seg in enumerate(self.segments):
            if seg.base_offset <= start_offset:
                seg_idx = i
            else:
                break
                
        if seg_idx == -1:
            seg_idx = 0

        current_offset = start_offset
        while len(records) < max_count and seg_idx < len(self.segments):
            seg = self.segments[seg_idx]
            needed = max_count - len(records)
            
            seg_records = await seg.read(current_offset, needed)
            records.extend(seg_records)
            
            if seg_records:
                current_offset = seg_records[-1]["offset"] + 1
            else:
                # If segment read returned nothing but there's a next segment, jump to its base offset
                if seg_idx + 1 < len(self.segments):
                    current_offset = max(current_offset, self.segments[seg_idx + 1].base_offset)
                else:
                    break
            
            seg_idx += 1

        return records

    def get_latest_offset(self) -> int:
        """Get the current high-water mark (next offset to be written)."""
        if not self.active_segment:
            return 0
        return self.active_segment.next_offset

    def get_earliest_offset(self) -> int:
        """Get the base offset of the oldest available segment."""
        if not self.segments:
            return 0
        return self.segments[0].base_offset

    async def cleanup(self, retention_hours: float = RETENTION_HOURS):
        """Asynchronously delete segment files older than the retention period, except the active segment."""
        async with self._lock:
            cutoff = time.time() - (retention_hours * 3600)
            new_segments = []

            for seg in self.segments:
                if seg == self.active_segment:
                    new_segments.append(seg)
                    continue

                if os.path.exists(seg.path):
                    mtime = os.path.getmtime(seg.path)
                    if mtime < cutoff:
                        try:
                            await asyncio.to_thread(os.remove, seg.path)
                        except Exception:
                            pass
                        seg.index.clear()
                        continue

                new_segments.append(seg)

            self.segments = new_segments

import os
import time
import asyncio
from typing import Any, Dict, List, Optional
from core.segment import Segment
from config import SEGMENT_MAX_SIZE, RETENTION_HOURS, RETENTION_MAX_BYTES

class Log:
    """Manages ordered sequence of segment files for an individual partition log."""
    
    def __init__(self, topic: str, partition: int, base_dir: str):
        self.topic = topic
        self.partition = partition
        self.base_dir = base_dir
        self.segments: List[Segment] = []
        self.active_segment: Optional[Segment] = None
        self._lock = asyncio.Lock()

    async def rebuild_index(self):
        """Scan partition directory to discover and recover all segment files."""
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

        # Sort segments strictly by base offset
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

    async def _roll_segment_if_needed(self):
        """Roll over to a new active segment if the current active segment is full."""
        if self.active_segment and self.active_segment.get_size() >= SEGMENT_MAX_SIZE:
            new_base_offset = self.active_segment.next_offset
            new_path = os.path.join(self.base_dir, f"{new_base_offset:020d}.log")
            
            await self.active_segment.close()
            
            new_seg = Segment(new_path, new_base_offset, max_size=SEGMENT_MAX_SIZE)
            await new_seg.recover()
            
            self.segments.append(new_seg)
            self.active_segment = new_seg

    async def append(self, key: Any, value: Any, headers: Optional[Dict[str, Any]], timestamp: Optional[int] = None) -> dict:
        """Append record to active segment, handling rollover if size limit exceeded."""
        async with self._lock:
            await self._roll_segment_if_needed()
            return await self.active_segment.append(key, value, headers, timestamp)

    async def append_batch(self, messages: List[dict]) -> List[dict]:
        """High-throughput batch append across segments."""
        if not messages:
            return []
            
        async with self._lock:
            results = []
            current_idx = 0
            
            while current_idx < len(messages):
                await self._roll_segment_if_needed()
                
                # Check how much capacity remains in active segment
                active_size = self.active_segment.get_size()
                if active_size >= SEGMENT_MAX_SIZE:
                    continue
                    
                # Estimate remaining batch capacity or append all if fits
                # If active segment is fresh (empty), write at least 1 message or chunk
                chunk = messages[current_idx:]
                chunk_results = await self.active_segment.append_batch(chunk)
                results.extend(chunk_results)
                current_idx += len(chunk_results)
                
            return results

    async def read(
        self,
        start_offset: int,
        max_count: int = 100,
        key_filter: Optional[str] = None,
        header_filter: Optional[Dict[str, Any]] = None,
        drop_expired: bool = False
    ) -> list:
        """Read records sequentially across segments starting from the specified offset."""
        records = []
        
        # Locate starting segment
        seg_idx = 0
        for i, seg in enumerate(self.segments):
            if seg.base_offset <= start_offset:
                seg_idx = i
            else:
                break

        current_offset = start_offset
        while len(records) < max_count and seg_idx < len(self.segments):
            seg = self.segments[seg_idx]
            needed = max_count - len(records)
            
            seg_records = await seg.read(
                current_offset,
                needed,
                key_filter=key_filter,
                header_filter=header_filter,
                drop_expired=drop_expired
            )
            records.extend(seg_records)
            
            if seg_records:
                current_offset = seg_records[-1]["offset"] + 1
            else:
                if seg_idx + 1 < len(self.segments):
                    current_offset = max(current_offset, self.segments[seg_idx + 1].base_offset)
                else:
                    break
            
            seg_idx += 1

        return records

    def get_latest_offset(self) -> int:
        """Get the current high-water mark (next offset to be assigned)."""
        if not self.active_segment:
            return 0
        return self.active_segment.next_offset

    def get_earliest_offset(self) -> int:
        """Get the base offset of the oldest available segment."""
        if not self.segments:
            return 0
        return self.segments[0].base_offset

    def get_total_bytes(self) -> int:
        """Sum of file sizes of all segments in this log."""
        return sum(seg.get_size() for seg in self.segments)

    async def cleanup(
        self,
        retention_hours: float = RETENTION_HOURS,
        max_bytes: int = RETENTION_MAX_BYTES
    ) -> int:
        """
        Delete segment files exceeding time or total size retention limits.
        Active segment is never deleted.
        Returns the number of cleaned segments.
        """
        async with self._lock:
            cutoff = time.time() - (retention_hours * 3600)
            surviving_segments = []
            deleted_count = 0

            # 1. Time-based cleanup
            for seg in self.segments:
                if seg == self.active_segment:
                    surviving_segments.append(seg)
                    continue

                if os.path.exists(seg.path):
                    mtime = os.path.getmtime(seg.path)
                    if mtime < cutoff:
                        try:
                            await asyncio.to_thread(os.remove, seg.path)
                            deleted_count += 1
                        except Exception:
                            pass
                        seg.index.clear()
                        continue

                surviving_segments.append(seg)

            # 2. Size-based cleanup (if total size still exceeds max_bytes)
            total_size = sum(seg.get_size() for seg in surviving_segments)
            final_segments = []
            
            # Oldest to newest
            for seg in surviving_segments:
                if seg == self.active_segment:
                    final_segments.append(seg)
                    continue
                    
                seg_size = seg.get_size()
                if total_size > max_bytes:
                    if os.path.exists(seg.path):
                        try:
                            await asyncio.to_thread(os.remove, seg.path)
                            deleted_count += 1
                            total_size -= seg_size
                        except Exception:
                            final_segments.append(seg)
                            continue
                        seg.index.clear()
                        continue
                final_segments.append(seg)

            self.segments = final_segments
            return deleted_count

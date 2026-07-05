import os
import struct
import bisect
import aiofiles

class OffsetIndex:
    def __init__(self, interval=1000):
        self.interval = interval
        self._entries = []  # List of (offset, position)

    def add(self, offset: int, position: int):
        """Add an offset and its corresponding file position to the index."""
        # Entries should be strictly increasing in offset.
        if not self._entries or self._entries[-1][0] < offset:
            self._entries.append((offset, position))
        elif self._entries[-1][0] == offset:
            self._entries[-1] = (offset, position)
        else:
            # In case they are added out of order, insert maintaining sort order
            idx = bisect.bisect_left(self._entries, (offset, 0))
            if idx < len(self._entries) and self._entries[idx][0] == offset:
                self._entries[idx] = (offset, position)
            else:
                self._entries.insert(idx, (offset, position))

    def find(self, offset: int) -> int:
        """
        Find the file position for the largest offset less than or equal to the target offset.
        Returns 0 if no index matches or the index is empty.
        """
        if not self._entries:
            return 0
        
        # bisect_right finds the insertion point for (offset, infinity)
        idx = bisect.bisect_right(self._entries, (offset, float('inf')))
        if idx == 0:
            return self._entries[0][1]
        return self._entries[idx - 1][1]

    def clear(self):
        """Clear the in-memory index entries."""
        self._entries.clear()

    async def rebuild_from_segments(self, segment_paths):
        """
        Scan the given segment files to rebuild the offset index.
        segment_paths can be a single path or a list of paths.
        """
        self.clear()
        if isinstance(segment_paths, str):
            segment_paths = [segment_paths]

        HEADER_FORMAT = "<IIIIQQ"
        HEADER_SIZE = 32

        for path in segment_paths:
            if not os.path.exists(path):
                continue
            try:
                async with aiofiles.open(path, "rb") as f:
                    position = 0
                    while True:
                        await f.seek(position)
                        header_bytes = await f.read(HEADER_SIZE)
                        if len(header_bytes) < HEADER_SIZE:
                            break
                        
                        try:
                            crc, key_len, val_len, hdrs_len, timestamp, offset = struct.unpack(
                                HEADER_FORMAT, header_bytes
                            )
                        except struct.error:
                            # Truncated or corrupt header
                            break
                        
                        if offset % self.interval == 0:
                            self.add(offset, position)
                        
                        position += HEADER_SIZE + key_len + val_len + hdrs_len
            except Exception:
                # Handle potential I/O errors gracefully
                continue

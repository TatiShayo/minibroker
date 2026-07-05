import os
import struct
import zlib
import json
import time
import asyncio
import aiofiles
from core.index import OffsetIndex
from config import INDEX_INTERVAL

class Segment:
    def __init__(self, path: str, base_offset: int, max_size: int = 1_048_576):
        self.path = path
        self.base_offset = base_offset
        self.max_size = max_size
        self.index = OffsetIndex(interval=INDEX_INTERVAL)
        self.next_offset = base_offset

    async def recover(self):
        """Scan the segment file to populate the index and determine the next offset."""
        self.index.clear()
        if not os.path.exists(self.path):
            self.next_offset = self.base_offset
            return

        HEADER_FORMAT = "<IIIIQQ"
        HEADER_SIZE = 32
        last_offset = self.base_offset - 1
        last_valid_position = 0

        try:
            file_size = os.path.getsize(self.path)
            async with aiofiles.open(self.path, "rb") as f:
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
                        break
                    
                    payload_size = key_len + val_len + hdrs_len
                    # Validate that the unpacked payload_size is less than self.max_size and fits within remaining file size
                    if payload_size >= self.max_size or position + HEADER_SIZE + payload_size > file_size:
                        break
                    
                    payload_bytes = await f.read(payload_size)
                    if len(payload_bytes) < payload_size:
                        break
                    
                    # Verify the CRC matches
                    actual_crc = zlib.crc32(payload_bytes) & 0xffffffff
                    if actual_crc != crc:
                        break
                    
                    if offset % self.index.interval == 0:
                        self.index.add(offset, position)
                    
                    last_offset = offset
                    last_valid_position = position + HEADER_SIZE + payload_size
                    position = last_valid_position
        except Exception:
            pass

        # Truncate the file to last valid position to discard any corrupt or partial records
        try:
            if os.path.exists(self.path):
                if os.path.getsize(self.path) > last_valid_position:
                    async with aiofiles.open(self.path, "r+b") as f:
                        await f.truncate(last_valid_position)
        except Exception:
            pass

        self.next_offset = last_offset + 1

    def get_size(self) -> int:
        """Get the current size of the segment file on disk."""
        if os.path.exists(self.path):
            return os.path.getsize(self.path)
        return 0

    def is_active(self) -> bool:
        """Check if the segment can still be appended to (size < max_size)."""
        return self.get_size() < self.max_size

    async def append(self, key, value, headers, timestamp=None) -> dict:
        """
        Append a message to the segment file.
        Format: [crc32(4B) | key_len(4B) | value_len(4B) | headers_len(4B) | timestamp(8B) | offset(8B) | key | value | headers_json]
        """
        if timestamp is None:
            timestamp = int(time.time() * 1000)

        # Encode key
        if isinstance(key, str):
            key_bytes = key.encode('utf-8')
        elif isinstance(key, bytes):
            key_bytes = key
        else:
            key_bytes = b""

        # Encode value
        if isinstance(value, bytes):
            val_bytes = value
        elif isinstance(value, str):
            val_bytes = value.encode('utf-8')
        elif value is not None:
            val_bytes = json.dumps(value).encode('utf-8')
        else:
            val_bytes = b""

        # Encode headers
        headers_dict = headers if headers is not None else {}
        hdrs_bytes = json.dumps(headers_dict).encode('utf-8')

        key_len = len(key_bytes)
        val_len = len(val_bytes)
        hdrs_len = len(hdrs_bytes)

        payload = key_bytes + val_bytes + hdrs_bytes
        crc = zlib.crc32(payload) & 0xffffffff

        offset = self.next_offset
        HEADER_FORMAT = "<IIIIQQ"
        header_bytes = struct.pack(HEADER_FORMAT, crc, key_len, val_len, hdrs_len, timestamp, offset)
        record_bytes = header_bytes + payload

        # Ensure directory structure exists
        os.makedirs(os.path.dirname(self.path), exist_ok=True)

        position = self.get_size()

        # Write record
        async with aiofiles.open(self.path, "ab") as f:
            await f.write(record_bytes)
            await f.flush()
            
            # Non-blocking fsync
            try:
                fd = f.fileno()
                if asyncio.iscoroutine(fd):
                    fd = await fd
                await asyncio.to_thread(os.fsync, fd)
            except Exception:
                # Fallback if fileno or fsync is not supported in the current context
                pass

        # Update sparse index
        if offset % self.index.interval == 0:
            self.index.add(offset, position)

        self.next_offset += 1

        # Re-convert values to their original types for returning
        return {
            "offset": offset,
            "timestamp": timestamp,
            "key": key if isinstance(key, str) else (key.decode('utf-8') if isinstance(key, bytes) else None),
            "value": value,
            "headers": headers_dict
        }

    async def read(self, start_offset: int, count: int = 1) -> list:
        """Read up to `count` messages starting from `start_offset` using the sparse index."""
        if count < 0:
            raise ValueError("Limit cannot be negative")
            
        records = []
        if not os.path.exists(self.path):
            return records

        position = self.index.find(start_offset)
        HEADER_FORMAT = "<IIIIQQ"
        HEADER_SIZE = 32

        try:
            async with aiofiles.open(self.path, "rb") as f:
                await f.seek(position)
                while len(records) < count:
                    header_bytes = await f.read(HEADER_SIZE)
                    if len(header_bytes) < HEADER_SIZE:
                        break
                    
                    try:
                        crc, key_len, val_len, hdrs_len, timestamp, offset = struct.unpack(
                            HEADER_FORMAT, header_bytes
                        )
                    except struct.error as e:
                        raise ValueError("Corrupt record header: truncated or invalid format") from e
                    
                    payload_size = key_len + val_len + hdrs_len
                    if payload_size > self.max_size:
                        raise ValueError("Payload size exceeds segment max size limit")
                        
                    payload_bytes = await f.read(payload_size)
                    if len(payload_bytes) < payload_size:
                        raise ValueError("Corrupt record: truncated payload")
                    
                    actual_crc = zlib.crc32(payload_bytes) & 0xffffffff
                    if actual_crc != crc:
                        raise ValueError(f"CRC mismatch: record is corrupt. Expected {crc}, got {actual_crc}")
                    
                    if offset >= start_offset:
                        key = payload_bytes[:key_len]
                        val_bytes = payload_bytes[key_len:key_len + val_len]
                        hdrs_bytes = payload_bytes[key_len + val_len:]
                        
                        try:
                            key_str = key.decode('utf-8') if key_len > 0 else None
                        except UnicodeDecodeError:
                            key_str = key.hex() if key_len > 0 else None
                        
                        value = None
                        if val_len > 0:
                            try:
                                val_str = val_bytes.decode('utf-8')
                            except UnicodeDecodeError:
                                val_str = val_bytes.hex()
                            try:
                                value = json.loads(val_str)
                            except json.JSONDecodeError:
                                value = val_str
                        
                        headers = {}
                        if hdrs_len > 0:
                            try:
                                hdrs_str = hdrs_bytes.decode('utf-8')
                                headers = json.loads(hdrs_str)
                            except Exception:
                                try:
                                    headers = {"raw_headers_hex": hdrs_bytes.hex()}
                                except Exception:
                                    headers = {}

                        records.append({
                            "offset": offset,
                            "timestamp": timestamp,
                            "key": key_str,
                            "value": value,
                            "headers": headers
                        })
        except FileNotFoundError:
            return records
            
        return records

    async def read_all(self):
        """Async generator yielding all records in the segment file."""
        if not os.path.exists(self.path):
            return

        HEADER_FORMAT = "<IIIIQQ"
        HEADER_SIZE = 32

        try:
            async with aiofiles.open(self.path, "rb") as f:
                while True:
                    header_bytes = await f.read(HEADER_SIZE)
                    if len(header_bytes) < HEADER_SIZE:
                        break
                    
                    try:
                        crc, key_len, val_len, hdrs_len, timestamp, offset = struct.unpack(
                            HEADER_FORMAT, header_bytes
                        )
                    except struct.error as e:
                        raise ValueError("Corrupt record: truncated header") from e
                    
                    payload_size = key_len + val_len + hdrs_len
                    if payload_size > self.max_size:
                        raise ValueError("Payload size exceeds segment max size limit")
                        
                    payload_bytes = await f.read(payload_size)
                    if len(payload_bytes) < payload_size:
                        raise ValueError("Corrupt record: truncated payload")
                    
                    actual_crc = zlib.crc32(payload_bytes) & 0xffffffff
                    if actual_crc != crc:
                        raise ValueError(f"CRC mismatch: record is corrupt. Expected {crc}, got {actual_crc}")
                    
                    key = payload_bytes[:key_len]
                    val_bytes = payload_bytes[key_len:key_len + val_len]
                    hdrs_bytes = payload_bytes[key_len + val_len:]
                    
                    try:
                        key_str = key.decode('utf-8') if key_len > 0 else None
                    except UnicodeDecodeError:
                        key_str = key.hex() if key_len > 0 else None
                    
                    value = None
                    if val_len > 0:
                        try:
                            val_str = val_bytes.decode('utf-8')
                        except UnicodeDecodeError:
                            val_str = val_bytes.hex()
                        try:
                            value = json.loads(val_str)
                        except json.JSONDecodeError:
                            value = val_str
                    
                    headers = {}
                    if hdrs_len > 0:
                        try:
                            hdrs_str = hdrs_bytes.decode('utf-8')
                            headers = json.loads(hdrs_str)
                        except Exception:
                            try:
                                headers = {"raw_headers_hex": hdrs_bytes.hex()}
                            except Exception:
                                headers = {}

                    yield {
                        "offset": offset,
                        "timestamp": timestamp,
                        "key": key_str,
                        "value": value,
                        "headers": headers
                    }
        except FileNotFoundError:
            return

    async def close(self):
        """Flush and close any operations (no-op since we use context managers)."""
        pass

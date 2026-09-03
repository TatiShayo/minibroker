import os
import struct
import zlib
import json
import time
import asyncio
import aiofiles
from typing import Any, Dict, List, Optional
from core.index import OffsetIndex
from core.filter import matches_filter
from config import INDEX_INTERVAL, SYNC_WRITES

HEADER_FORMAT = "<IIIIQQ"
HEADER_SIZE = 32

def _encode_message(key: Any, value: Any, headers: Optional[Dict[str, Any]], timestamp: int, offset: int) -> tuple[bytes, dict]:
    """Encode a message record into binary format [crc(4)|key_len(4)|val_len(4)|hdrs_len(4)|timestamp(8)|offset(8)|payload]."""
    # Key
    if isinstance(key, str):
        key_bytes = key.encode('utf-8')
        key_str = key
    elif isinstance(key, bytes):
        key_bytes = key
        try:
            key_str = key.decode('utf-8')
        except UnicodeDecodeError:
            key_str = key.hex()
    elif key is not None:
        key_str = str(key)
        key_bytes = key_str.encode('utf-8')
    else:
        key_bytes = b""
        key_str = None

    # Value
    if isinstance(value, bytes):
        val_bytes = value
    elif isinstance(value, str):
        val_bytes = value.encode('utf-8')
    elif value is not None:
        val_bytes = json.dumps(value).encode('utf-8')
    else:
        val_bytes = b""

    # Headers
    headers_dict = headers if isinstance(headers, dict) else {}
    hdrs_bytes = json.dumps(headers_dict).encode('utf-8') if headers_dict else b"{}"

    key_len = len(key_bytes)
    val_len = len(val_bytes)
    hdrs_len = len(hdrs_bytes)

    payload = key_bytes + val_bytes + hdrs_bytes
    crc = zlib.crc32(payload) & 0xffffffff

    header_bytes = struct.pack(HEADER_FORMAT, crc, key_len, val_len, hdrs_len, timestamp, offset)
    record_bytes = header_bytes + payload

    record_meta = {
        "offset": offset,
        "timestamp": timestamp,
        "key": key_str,
        "value": value,
        "headers": headers_dict
    }
    return record_bytes, record_meta

class Segment:
    """Represents an individual append-only log segment file with binary record serialization."""
    
    def __init__(self, path: str, base_offset: int, max_size: int = 1_048_576):
        self.path = path
        self.base_offset = base_offset
        self.max_size = max_size
        self.index = OffsetIndex(interval=INDEX_INTERVAL)
        self.next_offset = base_offset

    async def recover(self):
        """Scan the segment file to populate the index, verify CRC integrity, and determine next offset."""
        self.index.clear()
        if not os.path.exists(self.path):
            self.next_offset = self.base_offset
            return

        last_offset = self.base_offset - 1
        last_valid_position = 0

        try:
            file_size = os.path.getsize(self.path)
            async with aiofiles.open(self.path, "rb") as f:
                position = 0
                while position + HEADER_SIZE <= file_size:
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
                    if payload_size >= self.max_size or position + HEADER_SIZE + payload_size > file_size:
                        break
                    
                    payload_bytes = await f.read(payload_size)
                    if len(payload_bytes) < payload_size:
                        break
                    
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

        # Truncate any corrupt or partial tail
        try:
            if os.path.exists(self.path):
                if os.path.getsize(self.path) > last_valid_position:
                    async with aiofiles.open(self.path, "r+b") as f:
                        await f.truncate(last_valid_position)
        except Exception:
            pass

        self.next_offset = last_offset + 1

    def get_size(self) -> int:
        """Get the current physical size of the segment file on disk."""
        if os.path.exists(self.path):
            return os.path.getsize(self.path)
        return 0

    def is_active(self) -> bool:
        """Check if the segment can accept new writes."""
        return self.get_size() < self.max_size

    async def append(self, key: Any, value: Any, headers: Optional[Dict[str, Any]], timestamp: Optional[int] = None) -> dict:
        """Append a single message to the segment file."""
        if timestamp is None:
            timestamp = int(time.time() * 1000)

        offset = self.next_offset
        record_bytes, record_meta = _encode_message(key, value, headers, timestamp, offset)
        
        os.makedirs(os.path.dirname(os.path.abspath(self.path)), exist_ok=True)
        position = self.get_size()

        async with aiofiles.open(self.path, "ab") as f:
            await f.write(record_bytes)
            await f.flush()
            if SYNC_WRITES:
                try:
                    fd = f.fileno()
                    if asyncio.iscoroutine(fd):
                        fd = await fd
                    await asyncio.to_thread(os.fsync, fd)
                except Exception:
                    pass

        if offset % self.index.interval == 0:
            self.index.add(offset, position)

        self.next_offset += 1
        return record_meta

    async def append_batch(self, messages: List[dict]) -> List[dict]:
        """
        High-throughput batch append: encodes all messages and writes them in a single I/O operation.
        """
        if not messages:
            return []

        os.makedirs(os.path.dirname(os.path.abspath(self.path)), exist_ok=True)
        current_pos = self.get_size()
        
        all_bytes = bytearray()
        results = []
        new_index_entries = []

        for msg in messages:
            offset = self.next_offset
            ts = msg.get("timestamp") or int(time.time() * 1000)
            key = msg.get("key")
            val = msg.get("value")
            hdrs = msg.get("headers")

            record_bytes, record_meta = _encode_message(key, val, hdrs, ts, offset)
            
            if offset % self.index.interval == 0:
                new_index_entries.append((offset, current_pos + len(all_bytes)))

            all_bytes.extend(record_bytes)
            results.append(record_meta)
            self.next_offset += 1

        async with aiofiles.open(self.path, "ab") as f:
            await f.write(all_bytes)
            await f.flush()
            if SYNC_WRITES:
                try:
                    fd = f.fileno()
                    if asyncio.iscoroutine(fd):
                        fd = await fd
                    await asyncio.to_thread(os.fsync, fd)
                except Exception:
                    pass

        for off, pos in new_index_entries:
            self.index.add(off, pos)

        return results

    async def read(
        self,
        start_offset: int,
        count: int = 1,
        key_filter: Optional[str] = None,
        header_filter: Optional[Dict[str, Any]] = None,
        drop_expired: bool = False
    ) -> list:
        """Read up to `count` messages starting from `start_offset` using sparse index and filtering."""
        if count < 0:
            raise ValueError("Limit cannot be negative")
            
        records = []
        if not os.path.exists(self.path) or count == 0:
            return records

        position = self.index.find(start_offset)
        now_ms = int(time.time() * 1000)

        try:
            file_size = os.path.getsize(self.path)
            async with aiofiles.open(self.path, "rb") as f:
                await f.seek(position)
                while len(records) < count and position + HEADER_SIZE <= file_size:
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
                    
                    position += HEADER_SIZE + payload_size

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
                                try:
                                    value = json.loads(val_str)
                                except json.JSONDecodeError:
                                    value = val_str
                            except UnicodeDecodeError:
                                value = val_bytes.hex()
                        
                        headers = {}
                        if hdrs_len > 0:
                            try:
                                hdrs_str = hdrs_bytes.decode('utf-8')
                                headers = json.loads(hdrs_str)
                            except Exception:
                                headers = {}

                        record_dict = {
                            "offset": offset,
                            "timestamp": timestamp,
                            "key": key_str,
                            "value": value,
                            "headers": headers
                        }

                        if matches_filter(record_dict, key_filter=key_filter, header_filter=header_filter, drop_expired=drop_expired, current_time_ms=now_ms):
                            records.append(record_dict)
        except FileNotFoundError:
            return records
            
        return records

    async def read_all(self):
        """Async generator yielding all records in the segment file."""
        if not os.path.exists(self.path):
            return

        try:
            file_size = os.path.getsize(self.path)
            async with aiofiles.open(self.path, "rb") as f:
                position = 0
                while position + HEADER_SIZE <= file_size:
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
                    
                    position += HEADER_SIZE + payload_size

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
                            try:
                                value = json.loads(val_str)
                            except json.JSONDecodeError:
                                value = val_str
                        except UnicodeDecodeError:
                            value = val_bytes.hex()
                    
                    headers = {}
                    if hdrs_len > 0:
                        try:
                            hdrs_str = hdrs_bytes.decode('utf-8')
                            headers = json.loads(hdrs_str)
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
        """Flush and release any held resources."""
        pass

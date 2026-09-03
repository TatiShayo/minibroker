import os
import time
import asyncio
import aiosqlite
from typing import Optional, List, Dict, Any
from config import DATA_DIR

DB_PATH = os.path.join(DATA_DIR, "offsets.db")
_db_lock = asyncio.Lock()

async def get_db_connection() -> aiosqlite.Connection:
    """Create and configure a robust aiosqlite connection with WAL mode and pragmas."""
    os.makedirs(os.path.dirname(os.path.abspath(DB_PATH)), exist_ok=True)
    db = await aiosqlite.connect(DB_PATH)
    await db.execute("PRAGMA journal_mode=WAL;")
    await db.execute("PRAGMA synchronous=NORMAL;")
    await db.execute("PRAGMA busy_timeout=10000;")
    db.row_factory = aiosqlite.Row
    return db

async def init_db():
    """Initialize database tables for consumer offsets, group metadata, and DLQ tracking."""
    async with _db_lock:
        db = await get_db_connection()
        try:
            await db.execute("""
                CREATE TABLE IF NOT EXISTS consumer_offsets (
                    group_name TEXT,
                    topic TEXT,
                    partition_id INTEGER,
                    offset INTEGER,
                    committed_at INTEGER,
                    metadata TEXT,
                    PRIMARY KEY (group_name, topic, partition_id)
                );
            """)
            await db.execute("""
                CREATE TABLE IF NOT EXISTS dead_letter_records (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    topic TEXT NOT NULL,
                    partition_id INTEGER NOT NULL,
                    offset INTEGER NOT NULL,
                    original_topic TEXT,
                    original_partition INTEGER,
                    original_offset INTEGER,
                    retry_count INTEGER DEFAULT 0,
                    death_reason TEXT,
                    payload TEXT,
                    headers TEXT,
                    created_at INTEGER NOT NULL
                );
            """)
            await db.commit()
        finally:
            await db.close()

async def get_offset(group: str, topic: str, partition: int) -> Optional[int]:
    """Retrieve committed offset for a specific consumer group, topic, and partition."""
    db = await get_db_connection()
    try:
        async with db.execute(
            "SELECT offset FROM consumer_offsets WHERE group_name = ? AND topic = ? AND partition_id = ?",
            (group, topic, partition)
        ) as cursor:
            row = await cursor.fetchone()
            return row["offset"] if row else None
    finally:
        await db.close()

async def get_group_offsets(group: str, topic: Optional[str] = None) -> Dict[str, Dict[int, int]]:
    """Retrieve all committed offsets for a consumer group, optionally filtered by topic."""
    db = await get_db_connection()
    try:
        if topic:
            query = "SELECT topic, partition_id, offset FROM consumer_offsets WHERE group_name = ? AND topic = ?"
            params = (group, topic)
        else:
            query = "SELECT topic, partition_id, offset FROM consumer_offsets WHERE group_name = ?"
            params = (group,)
            
        async with db.execute(query, params) as cursor:
            rows = await cursor.fetchall()
            results = {}
            for row in rows:
                t = row["topic"]
                p = row["partition_id"]
                off = row["offset"]
                if t not in results:
                    results[t] = {}
                results[t][p] = off
            return results
    finally:
        await db.close()

async def commit_offset(group: str, topic: str, partition: int, offset: int, metadata: str = ""):
    """Commit offset for a consumer group with concurrency lock."""
    now = int(time.time() * 1000)
    async with _db_lock:
        db = await get_db_connection()
        try:
            await db.execute("""
                INSERT OR REPLACE INTO consumer_offsets (group_name, topic, partition_id, offset, committed_at, metadata)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (group, topic, partition, offset, now, metadata))
            await db.commit()
        finally:
            await db.close()

async def commit_offsets_batch(group: str, topic: str, partition_offsets: Dict[int, int]):
    """Batch commit multiple partition offsets in a single atomic transaction."""
    now = int(time.time() * 1000)
    async with _db_lock:
        db = await get_db_connection()
        try:
            records = [
                (group, topic, p_id, offset, now, "")
                for p_id, offset in partition_offsets.items()
            ]
            await db.executemany("""
                INSERT OR REPLACE INTO consumer_offsets (group_name, topic, partition_id, offset, committed_at, metadata)
                VALUES (?, ?, ?, ?, ?, ?)
            """, records)
            await db.commit()
        finally:
            await db.close()

async def get_all_offsets() -> List[dict]:
    """Retrieve all consumer offsets across all groups and topics."""
    db = await get_db_connection()
    try:
        async with db.execute(
            "SELECT group_name, topic, partition_id, offset, committed_at, metadata FROM consumer_offsets"
        ) as cursor:
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]
    finally:
        await db.close()

async def delete_group(group: str, topic: Optional[str] = None) -> int:
    """Delete committed offsets for a consumer group."""
    async with _db_lock:
        db = await get_db_connection()
        try:
            if topic:
                cursor = await db.execute(
                    "DELETE FROM consumer_offsets WHERE group_name = ? AND topic = ?",
                    (group, topic)
                )
            else:
                cursor = await db.execute(
                    "DELETE FROM consumer_offsets WHERE group_name = ?",
                    (group,)
                )
            deleted = cursor.rowcount
            await db.commit()
            return deleted
        finally:
            await db.close()

async def record_dead_letter(
    topic: str,
    partition_id: int,
    offset: int,
    original_topic: Optional[str] = None,
    original_partition: Optional[int] = None,
    original_offset: Optional[int] = None,
    retry_count: int = 0,
    death_reason: Optional[str] = None,
    payload: Optional[str] = None,
    headers: Optional[str] = None
) -> int:
    """Record a dead letter queue entry into the database."""
    now = int(time.time() * 1000)
    async with _db_lock:
        db = await get_db_connection()
        try:
            cursor = await db.execute("""
                INSERT INTO dead_letter_records (
                    topic, partition_id, offset, original_topic, original_partition,
                    original_offset, retry_count, death_reason, payload, headers, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                topic, partition_id, offset, original_topic, original_partition,
                original_offset, retry_count, death_reason, payload, headers, now
            ))
            await db.commit()
            return cursor.lastrowid
        finally:
            await db.close()

async def get_dead_letters(topic: Optional[str] = None, limit: int = 100) -> List[dict]:
    """Retrieve dead letter queue records."""
    db = await get_db_connection()
    try:
        if topic:
            query = "SELECT * FROM dead_letter_records WHERE topic = ? ORDER BY id DESC LIMIT ?"
            params = (topic, limit)
        else:
            query = "SELECT * FROM dead_letter_records ORDER BY id DESC LIMIT ?"
            params = (limit,)
        async with db.execute(query, params) as cursor:
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]
    finally:
        await db.close()

async def delete_dead_letter(record_id: int) -> bool:
    """Delete a dead letter entry by id."""
    async with _db_lock:
        db = await get_db_connection()
        try:
            cursor = await db.execute("DELETE FROM dead_letter_records WHERE id = ?", (record_id,))
            await db.commit()
            return cursor.rowcount > 0
        finally:
            await db.close()

import os
import aiosqlite
from typing import Optional, List, Dict
from config import DATA_DIR

DB_PATH = os.path.join(DATA_DIR, "offsets.db")

async def init_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("PRAGMA journal_mode=WAL;")
        await db.execute("""
            CREATE TABLE IF NOT EXISTS consumer_offsets (
                group_name TEXT,
                topic TEXT,
                partition_id INTEGER,
                offset INTEGER,
                PRIMARY KEY (group_name, topic, partition_id)
            )
        """)
        await db.commit()

async def get_offset(group: str, topic: str, partition: int) -> Optional[int]:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT offset FROM consumer_offsets WHERE group_name = ? AND topic = ? AND partition_id = ?",
            (group, topic, partition)
        ) as cursor:
            row = await cursor.fetchone()
            return row[0] if row else None

async def commit_offset(group: str, topic: str, partition: int, offset: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            INSERT OR REPLACE INTO consumer_offsets (group_name, topic, partition_id, offset)
            VALUES (?, ?, ?, ?)
        """, (group, topic, partition, offset))
        await db.commit()

async def get_all_offsets() -> List[dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT group_name, topic, partition_id, offset FROM consumer_offsets") as cursor:
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]

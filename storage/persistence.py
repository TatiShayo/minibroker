import os
import re
import json
import shutil
import asyncio
import aiofiles
from config import DATA_DIR

def is_safe_topic_name(topic_name: str) -> bool:
    """Validate that topic name is safe and contains no directory traversal."""
    if not topic_name or topic_name in (".", "..") or ".." in topic_name:
        return False
    return bool(re.match(r"^[a-zA-Z0-9_\-\.]+$", topic_name))

def get_topics_dir() -> str:
    """Get absolute path to topics parent directory."""
    return os.path.abspath(os.path.join(DATA_DIR, "topics"))

def get_metadata_path(topic_name: str) -> str:
    """Get validated metadata file path for a topic."""
    topics_parent = get_topics_dir()
    raw_path = os.path.join(topics_parent, topic_name, "metadata.json")
    abs_path = os.path.abspath(raw_path)
    
    try:
        common = os.path.commonpath([topics_parent, abs_path])
        if os.path.abspath(common) != topics_parent:
            raise ValueError("Path traversal detected")
    except ValueError:
        raise ValueError("Path traversal detected")
        
    return abs_path

async def save_topic_metadata(topic_name: str, partitions_count: int, config: dict = None):
    """Persist topic metadata to disk asynchronously."""
    if not is_safe_topic_name(topic_name):
        raise ValueError(f"Invalid or unsafe topic name: {topic_name}")
    path = get_metadata_path(topic_name)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    payload = {
        "topic": topic_name,
        "partitions_count": partitions_count,
        "config": config or {}
    }
    async with aiofiles.open(path, "w", encoding="utf-8") as f:
        await f.write(json.dumps(payload, indent=2))

async def load_all_topics() -> dict:
    """Scan topics directory and load all stored topic definitions."""
    topics_dir = get_topics_dir()
    if not os.path.exists(topics_dir):
        return {}
    
    topics = {}
    for topic_name in os.listdir(topics_dir):
        if not is_safe_topic_name(topic_name):
            continue
        try:
            path = get_metadata_path(topic_name)
        except ValueError:
            continue
            
        if os.path.exists(path):
            try:
                async with aiofiles.open(path, "r", encoding="utf-8") as f:
                    content = await f.read()
                    data = json.loads(content)
                    topics[topic_name] = data.get("partitions_count", 1)
            except Exception:
                pass
    return topics

async def delete_topic_storage(topic_name: str) -> bool:
    """Remove topic directory and all its partitions/metadata from disk."""
    if not is_safe_topic_name(topic_name):
        raise ValueError(f"Invalid or unsafe topic name: {topic_name}")
    topics_parent = get_topics_dir()
    topic_dir = os.path.abspath(os.path.join(topics_parent, topic_name))
    
    common = os.path.commonpath([topics_parent, topic_dir])
    if os.path.abspath(common) != topics_parent:
        raise ValueError("Path traversal detected")
        
    if os.path.exists(topic_dir):
        await asyncio.to_thread(shutil.rmtree, topic_dir)
        return True
    return False

import os
import re
import json
import aiofiles
from config import DATA_DIR

def is_safe_topic_name(topic_name: str) -> bool:
    if not topic_name or topic_name in (".", "..") or ".." in topic_name:
        return False
    return bool(re.match(r"^[a-zA-Z0-9_\-\.]+$", topic_name))

def get_metadata_path(topic_name: str) -> str:
    topics_parent = os.path.abspath(os.path.join(DATA_DIR, "topics"))
    raw_path = os.path.join(topics_parent, topic_name, "metadata.json")
    abs_path = os.path.abspath(raw_path)
    
    try:
        common = os.path.commonpath([topics_parent, abs_path])
        if os.path.abspath(common) != topics_parent:
            raise ValueError("Path traversal detected")
    except ValueError:
        raise ValueError("Path traversal detected")
        
    return abs_path

async def save_topic_metadata(topic_name: str, partitions_count: int):
    if not is_safe_topic_name(topic_name):
        raise ValueError(f"Invalid or unsafe topic name: {topic_name}")
    path = get_metadata_path(topic_name)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    async with aiofiles.open(path, "w", encoding="utf-8") as f:
        await f.write(json.dumps({"partitions_count": partitions_count}))

async def load_all_topics() -> dict:
    topics_dir = os.path.abspath(os.path.join(DATA_DIR, "topics"))
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

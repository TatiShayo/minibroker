import json
import time
from typing import Any, Dict, Optional, Tuple
from core.offsets import record_dead_letter
from config import DLQ_MAX_RETRIES, DLQ_TOPIC_SUFFIX

def get_dlq_topic_name(original_topic: str) -> str:
    """Derive standard DLQ topic name from original topic."""
    if original_topic.endswith(DLQ_TOPIC_SUFFIX):
        return original_topic
    return f"{original_topic}{DLQ_TOPIC_SUFFIX}"

async def route_to_dlq(
    topics: dict,
    original_topic: str,
    original_partition: int,
    original_offset: int,
    key: Any,
    value: Any,
    headers: Optional[Dict[str, Any]],
    reason: str = "max_retries_exceeded",
    retry_count: int = 0
) -> dict:
    """
    Route a poison or rejected message to the Dead Letter Queue (DLQ) topic.
    Auto-creates the DLQ topic if it does not already exist.
    """
    from core.topic import Topic
    from storage.persistence import save_topic_metadata

    dlq_topic_name = get_dlq_topic_name(original_topic)
    
    # Ensure DLQ topic exists
    if dlq_topic_name not in topics:
        await save_topic_metadata(dlq_topic_name, 1)
        dlq_topic = Topic(dlq_topic_name, 1)
        await dlq_topic.startup()
        topics[dlq_topic_name] = dlq_topic
    else:
        dlq_topic = topics[dlq_topic_name]

    now_ms = int(time.time() * 1000)
    dlq_headers = dict(headers or {})
    dlq_headers["x-original-topic"] = original_topic
    dlq_headers["x-original-partition"] = original_partition
    dlq_headers["x-original-offset"] = original_offset
    dlq_headers["x-death-reason"] = reason
    dlq_headers["x-retry-count"] = retry_count
    dlq_headers["x-death-timestamp"] = now_ms

    # Append to partition 0 of DLQ
    dlq_partition = dlq_topic.get_partition(0)
    dlq_record = await dlq_partition.append(
        key=key,
        value=value,
        headers=dlq_headers,
        timestamp=now_ms
    )

    # Record in persistent DLQ audit database
    payload_str = json.dumps(value) if value is not None else None
    headers_str = json.dumps(dlq_headers)
    await record_dead_letter(
        topic=dlq_topic_name,
        partition_id=0,
        offset=dlq_record["offset"],
        original_topic=original_topic,
        original_partition=original_partition,
        original_offset=original_offset,
        retry_count=retry_count,
        death_reason=reason,
        payload=payload_str,
        headers=headers_str
    )

    return {
        "status": "routed_to_dlq",
        "dlq_topic": dlq_topic_name,
        "dlq_partition": 0,
        "dlq_offset": dlq_record["offset"],
        "reason": reason
    }

async def redrive_dlq_messages(
    topics: dict,
    dlq_topic_name: str,
    max_count: int = 100
) -> dict:
    """
    Replay messages from DLQ topic back into their original destination topics.
    """
    if dlq_topic_name not in topics:
        raise ValueError(f"DLQ topic {dlq_topic_name} does not exist")

    dlq_topic = topics[dlq_topic_name]
    dlq_partition = dlq_topic.get_partition(0)
    if not dlq_partition:
        return {"replayed": 0, "messages": []}

    records = await dlq_partition.read(offset=0, max_count=max_count)
    replayed = []

    for rec in records:
        hdrs = rec.get("headers") or {}
        orig_topic = hdrs.get("x-original-topic")
        if not orig_topic or orig_topic not in topics:
            continue

        target_topic = topics[orig_topic]
        # Clear DLQ death headers for redrive
        clean_hdrs = {k: v for k, v in hdrs.items() if not k.startswith("x-death-")}
        clean_hdrs["x-redriven"] = True
        
        target_part_id = target_topic.partition(rec.get("key"))
        target_part = target_topic.get_partition(target_part_id)
        if target_part:
            res = await target_part.append(
                key=rec.get("key"),
                value=rec.get("value"),
                headers=clean_hdrs
            )
            replayed.append({
                "from_dlq_offset": rec["offset"],
                "target_topic": orig_topic,
                "target_partition": target_part_id,
                "new_offset": res["offset"]
            })

    return {
        "replayed_count": len(replayed),
        "details": replayed
    }

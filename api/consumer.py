from fastapi import APIRouter, Request, HTTPException
from typing import List, Optional
from pydantic import BaseModel

from models.schemas import (
    MessageRecord,
    MessageConsumeResponse,
    ConsumerRegisterRequest,
    ConsumerGroupInfo
)
from core.offsets import get_offset, commit_offset, get_all_offsets

router = APIRouter()

class CommitOffsetRequest(BaseModel):
    group: str
    partition: int
    offset: int

@router.get("/topics/{topic}/messages", response_model=MessageConsumeResponse)
async def consume_messages(
    request: Request,
    topic: str,
    partition: int = 0,
    offset: Optional[int] = None,
    limit: int = 100,
    group: Optional[str] = None
):
    if limit <= 0:
        raise HTTPException(status_code=400, detail="limit must be greater than 0")
    if limit > 1000:
        limit = 1000
    if offset is not None and offset < 0:
        raise HTTPException(status_code=400, detail="offset must be non-negative")
        
    topics = request.app.state.topics
    if topic not in topics:
        raise HTTPException(status_code=404, detail="Topic not found")
        
    topic_obj = topics[topic]
    partition_obj = topic_obj.get_partition(partition)
    if not partition_obj:
        raise HTTPException(status_code=404, detail=f"Partition {partition} not found")
        
    # Determine current starting offset
    current_offset = None
    if group:
        current_offset = await get_offset(group, topic, partition)
        
    if current_offset is None:
        if offset is not None:
            current_offset = offset
        else:
            current_offset = 0
            
    # Read messages from partition log
    messages = await partition_obj.read(current_offset, limit)
    
    # Map raw messages to MessageRecord schema
    records = [
        MessageRecord(
            offset=msg["offset"],
            timestamp=msg["timestamp"],
            key=msg.get("key"),
            value=msg.get("value"),
            headers=msg.get("headers")
        ) for msg in messages
    ]
    
    # Calculate next_offset
    if messages:
        next_offset = messages[-1]["offset"] + 1
    else:
        next_offset = current_offset
        
    # Auto-commit if group is provided and messages were read
    if group and messages:
        await commit_offset(group, topic, partition, next_offset)
        
    # Determine if there are more messages
    high_watermark = partition_obj.get_high_watermark()
    has_more = next_offset < high_watermark
    
    return MessageConsumeResponse(
        topic=topic,
        partition=partition,
        messages=records,
        next_offset=next_offset,
        has_more=has_more
    )

@router.post("/topics/{topic}/consumer/register")
async def register_consumer(request: Request, topic: str, payload: ConsumerRegisterRequest):
    topics = request.app.state.topics
    if topic not in topics:
        raise HTTPException(status_code=404, detail="Topic not found")
        
    topic_obj = topics[topic]
    
    for p_id, p in topic_obj.partitions.items():
        if payload.initial_offset == "earliest":
            val = p.log.get_earliest_offset()
        elif payload.initial_offset == "latest":
            val = p.get_high_watermark()
        else:
            try:
                val = int(payload.initial_offset)
                if val < 0:
                    raise HTTPException(status_code=400, detail="initial_offset must be non-negative")
            except ValueError:
                raise HTTPException(status_code=400, detail="Invalid initial_offset value")
        await commit_offset(payload.group, topic, p_id, val)
        
    return {"status": "registered", "group": payload.group, "topic": topic}

@router.post("/topics/{topic}/commit")
async def commit_offset_endpoint(topic: str, payload: CommitOffsetRequest):
    if payload.partition < 0 or payload.offset < 0:
        raise HTTPException(status_code=400, detail="partition and offset must be non-negative")
    await commit_offset(payload.group, topic, payload.partition, payload.offset)
    return {"status": "committed"}

@router.post("/topics/{topic}/consumer/commit")
async def commit_offset_endpoint_legacy(topic: str, payload: CommitOffsetRequest):
    if payload.partition < 0 or payload.offset < 0:
        raise HTTPException(status_code=400, detail="partition and offset must be non-negative")
    await commit_offset(payload.group, topic, payload.partition, payload.offset)
    return {"status": "committed"}

@router.get("/groups", response_model=List[ConsumerGroupInfo])
async def get_consumer_groups(request: Request):
    topics = request.app.state.topics
    all_db_offsets = await get_all_offsets()
    
    group_topics = {}
    for row in all_db_offsets:
        group = row["group_name"]
        topic = row["topic"]
        partition = row["partition_id"]
        offset = row["offset"]
        
        key = (group, topic)
        if key not in group_topics:
            group_topics[key] = {}
        group_topics[key][partition] = offset
        
    result = []
    for (group, topic), partition_offsets in group_topics.items():
        if topic not in topics:
            continue
            
        topic_obj = topics[topic]
        total_current = 0
        total_latest = 0
        
        for p_id, p in topic_obj.partitions.items():
            current = partition_offsets.get(p_id, 0)
            latest = p.get_high_watermark()
            total_current += current
            total_latest += latest
            
        lag = total_latest - total_current
        result.append(
            ConsumerGroupInfo(
                group=group,
                topic=topic,
                current_offset=total_current,
                latest_offset=total_latest,
                lag=lag
            )
        )
        
    return result

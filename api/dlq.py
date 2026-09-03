from fastapi import APIRouter, Request, HTTPException, Query
from typing import List, Optional
from models.schemas import DeadLetterRouteRequest, DeadLetterRedriveRequest
from core.dlq import route_to_dlq, redrive_dlq_messages, get_dlq_topic_name
from core.offsets import get_dead_letters, delete_dead_letter
from core.metrics import metrics

router = APIRouter()

@router.post("/topics/{topic}/dlq/route")
async def route_dlq_endpoint(request: Request, topic: str, payload: DeadLetterRouteRequest):
    topics = request.app.state.topics
    if topic not in topics:
        raise HTTPException(status_code=404, detail="Topic not found")
        
    result = await route_to_dlq(
        topics=topics,
        original_topic=topic,
        original_partition=payload.partition,
        original_offset=payload.offset,
        key=payload.key,
        value=payload.value,
        headers=payload.headers,
        reason=payload.reason,
        retry_count=payload.retry_count
    )
    metrics.record_dlq(count=1)
    return result

@router.get("/topics/{topic}/dlq/messages")
async def get_dlq_messages(
    request: Request,
    topic: str,
    offset: int = Query(default=0, ge=0),
    limit: int = Query(default=50, ge=1, le=500)
):
    topics = request.app.state.topics
    dlq_topic_name = get_dlq_topic_name(topic)
    
    if dlq_topic_name not in topics:
        return {"topic": dlq_topic_name, "messages": [], "total": 0}
        
    dlq_topic = topics[dlq_topic_name]
    dlq_part = dlq_topic.get_partition(0)
    if not dlq_part:
        return {"topic": dlq_topic_name, "messages": [], "total": 0}
        
    records = await dlq_part.read(offset=offset, max_count=limit)
    return {
        "dlq_topic": dlq_topic_name,
        "partition": 0,
        "messages": records,
        "high_watermark": dlq_part.get_high_watermark()
    }

@router.post("/topics/{topic}/dlq/redrive")
async def redrive_dlq_endpoint(request: Request, topic: str, payload: DeadLetterRedriveRequest):
    topics = request.app.state.topics
    dlq_topic_name = get_dlq_topic_name(topic)
    
    if dlq_topic_name not in topics:
        raise HTTPException(status_code=404, detail="No DLQ topic exists for this topic")
        
    result = await redrive_dlq_messages(topics, dlq_topic_name, max_count=payload.max_count)
    return result

@router.get("/dlq/audit")
async def get_dlq_audit(topic: Optional[str] = None, limit: int = Query(default=100, ge=1, le=1000)):
    records = await get_dead_letters(topic=topic, limit=limit)
    return {"count": len(records), "records": records}

@router.delete("/dlq/audit/{record_id}")
async def delete_dlq_audit(record_id: int):
    deleted = await delete_dead_letter(record_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Audit record not found")
    return {"status": "deleted", "id": record_id}

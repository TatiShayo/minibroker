import asyncio
from fastapi import APIRouter, Request, HTTPException, Query
from typing import List, Optional

from models.schemas import (
    TopicCreate,
    TopicExpand,
    TopicInfo,
    PartitionInfo,
    StatsResponse,
    RetentionCleanupResponse
)
from core.topic import Topic
from storage.persistence import save_topic_metadata, is_safe_topic_name, delete_topic_storage
from core.metrics import metrics
from config import RETENTION_HOURS, RETENTION_MAX_BYTES

router = APIRouter()

def _build_topic_info(topic_obj: Topic) -> TopicInfo:
    partitions_info = []
    total_messages = 0
    total_bytes = 0
    for p_id, p in topic_obj.partitions.items():
        messages_count = p.get_offset_count()
        total_messages += messages_count
        partition_size = p.get_size_bytes()
        total_bytes += partition_size
        partitions_info.append(
            PartitionInfo(
                id=p_id,
                messages=messages_count,
                size=partition_size,
                earliest_offset=p.get_earliest_offset(),
                latest_offset=p.get_high_watermark(),
                leader=p.get_leader()
            )
        )
    return TopicInfo(
        name=topic_obj.name,
        partitions=partitions_info,
        total_messages=total_messages,
        total_size_bytes=total_bytes
    )

@router.post("/topics", response_model=TopicInfo)
async def create_topic(request: Request, payload: TopicCreate):
    topics = request.app.state.topics
    topic_name = payload.name
    
    if not is_safe_topic_name(topic_name):
        raise HTTPException(status_code=400, detail="Invalid or unsafe topic name")
        
    if topic_name in topics:
        raise HTTPException(status_code=400, detail="Topic already exists")
    
    # Save metadata
    await save_topic_metadata(topic_name, payload.partitions)
    
    # Instantiate Topic and start it up
    topic_obj = Topic(topic_name, payload.partitions)
    await topic_obj.startup()
    
    # Register in application state
    topics[topic_name] = topic_obj
    
    return _build_topic_info(topic_obj)

@router.get("/topics", response_model=List[TopicInfo])
async def list_topics(request: Request):
    topics = request.app.state.topics
    return [_build_topic_info(t) for t in topics.values()]

@router.get("/topics/{topic}", response_model=TopicInfo)
async def get_topic(request: Request, topic: str):
    topics = request.app.state.topics
    if topic not in topics:
        raise HTTPException(status_code=404, detail="Topic not found")
    return _build_topic_info(topics[topic])

@router.post("/topics/{topic}/partitions", response_model=TopicInfo)
async def expand_partitions(request: Request, topic: str, payload: TopicExpand):
    topics = request.app.state.topics
    if topic not in topics:
        raise HTTPException(status_code=404, detail="Topic not found")
        
    topic_obj = topics[topic]
    try:
        await topic_obj.add_partitions(payload.partitions)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
        
    return _build_topic_info(topic_obj)

@router.delete("/topics/{topic}")
async def delete_topic(request: Request, topic: str):
    topics = request.app.state.topics
    if topic not in topics:
        raise HTTPException(status_code=404, detail="Topic not found")
    
    topic_obj = topics.pop(topic)
    await delete_topic_storage(topic_obj.name)
    return {"status": "deleted"}

@router.post("/topics/{topic}/cleanup", response_model=RetentionCleanupResponse)
async def trigger_topic_cleanup(
    request: Request,
    topic: str,
    retention_hours: float = Query(default=RETENTION_HOURS, ge=0.01),
    max_bytes: int = Query(default=RETENTION_MAX_BYTES, ge=1024)
):
    topics = request.app.state.topics
    if topic not in topics:
        raise HTTPException(status_code=404, detail="Topic not found")
        
    topic_obj = topics[topic]
    cleaned = await topic_obj.cleanup(retention_hours=retention_hours, max_bytes=max_bytes)
    metrics.record_dropped(cleaned)
    return RetentionCleanupResponse(status="cleaned", cleaned_segments=cleaned)

@router.post("/admin/cleanup", response_model=RetentionCleanupResponse)
async def trigger_all_cleanup(
    request: Request,
    retention_hours: float = Query(default=RETENTION_HOURS, ge=0.01),
    max_bytes: int = Query(default=RETENTION_MAX_BYTES, ge=1024)
):
    topics = request.app.state.topics
    tasks = [t.cleanup(retention_hours=retention_hours, max_bytes=max_bytes) for t in topics.values()]
    results = await asyncio.gather(*tasks) if tasks else []
    total_cleaned = sum(results)
    metrics.record_dropped(total_cleaned)
    return RetentionCleanupResponse(status="cleaned", cleaned_segments=total_cleaned)

@router.get("/stats", response_model=StatsResponse)
async def get_stats(request: Request):
    topics = request.app.state.topics
    summary = metrics.get_summary(topics)
    return StatsResponse(
        topics=summary["topics_count"],
        partitions=summary["partitions_count"],
        total_messages=summary["total_retained_messages"],
        disk_usage_bytes=summary["total_disk_usage_bytes"],
        uptime_seconds=summary["uptime_seconds"],
        throughput_msg_sec=summary["throughput_msg_sec"],
        messages_produced_total=summary["messages_produced_total"],
        messages_consumed_total=summary["messages_consumed_total"],
        dlq_messages_total=summary["dlq_messages_total"],
        dropped_messages_total=summary["dropped_messages_total"]
    )

@router.get("/metrics")
async def get_metrics_endpoint(request: Request):
    topics = request.app.state.topics
    return metrics.get_summary(topics)

import os
import shutil
import asyncio
from fastapi import APIRouter, Request, HTTPException
from typing import List

from models.schemas import TopicCreate, TopicInfo, PartitionInfo, StatsResponse
from core.topic import Topic
from storage.persistence import save_topic_metadata, is_safe_topic_name

router = APIRouter()

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
    
    # Construct response
    partitions_info = []
    total_messages = 0
    for p_id, p in topic_obj.partitions.items():
        messages_count = p.get_offset_count()
        total_messages += messages_count
        partition_size = sum(seg.get_size() for seg in p.log.segments)
        partitions_info.append(
            PartitionInfo(id=p_id, messages=messages_count, size=partition_size)
        )
        
    return TopicInfo(
        name=topic_name,
        partitions=partitions_info,
        total_messages=total_messages
    )

@router.get("/topics", response_model=List[TopicInfo])
async def list_topics(request: Request):
    topics = request.app.state.topics
    result = []
    
    for topic_name, topic_obj in topics.items():
        partitions_info = []
        total_messages = 0
        for p_id, p in topic_obj.partitions.items():
            messages_count = p.get_offset_count()
            total_messages += messages_count
            partition_size = sum(seg.get_size() for seg in p.log.segments)
            partitions_info.append(
                PartitionInfo(id=p_id, messages=messages_count, size=partition_size)
            )
        result.append(
            TopicInfo(
                name=topic_name,
                partitions=partitions_info,
                total_messages=total_messages
            )
        )
    return result

@router.delete("/topics/{topic}")
async def delete_topic(request: Request, topic: str):
    topics = request.app.state.topics
    if topic not in topics:
        raise HTTPException(status_code=404, detail="Topic not found")
    
    topic_obj = topics.pop(topic)
    
    # Delete directory
    if os.path.exists(topic_obj.topic_dir):
        await asyncio.to_thread(shutil.rmtree, topic_obj.topic_dir)
        
    return {"status": "deleted"}

@router.get("/stats", response_model=StatsResponse)
async def get_stats(request: Request):
    topics = request.app.state.topics
    
    num_topics = len(topics)
    num_partitions = 0
    total_messages = 0
    disk_usage_bytes = 0
    
    for topic_obj in topics.values():
        for p in topic_obj.get_all_partitions():
            num_partitions += 1
            total_messages += p.get_offset_count()
            disk_usage_bytes += sum(seg.get_size() for seg in p.log.segments)
            
    return StatsResponse(
        topics=num_topics,
        partitions=num_partitions,
        total_messages=total_messages,
        disk_usage_bytes=disk_usage_bytes
    )

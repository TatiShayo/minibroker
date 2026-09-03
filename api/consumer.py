import json
from fastapi import APIRouter, Request, HTTPException, Query
from typing import List, Optional, Union

from models.schemas import (
    MessageRecord,
    MessageConsumeResponse,
    ConsumerRegisterRequest,
    ConsumerRegisterResponse,
    ConsumerHeartbeatRequest,
    ConsumerLeaveRequest,
    CommitOffsetRequest,
    BatchCommitOffsetRequest,
    OffsetResetRequest,
    ConsumerGroupInfo,
    PartitionLagInfo,
    ConsumerGroupMemberInfo
)
from core.offsets import (
    get_offset,
    commit_offset,
    commit_offsets_batch,
    get_all_offsets,
    delete_group,
    get_group_offsets
)
from core.metrics import metrics

router = APIRouter()

@router.get("/topics/{topic}/messages", response_model=MessageConsumeResponse)
async def consume_messages(
    request: Request,
    topic: str,
    partition: int = 0,
    offset: Optional[int] = None,
    limit: int = 100,
    group: Optional[str] = None,
    auto_commit: bool = True,
    key_filter: Optional[str] = Query(default=None, alias="key"),
    header_filter_json: Optional[str] = Query(default=None, alias="header_filter"),
    drop_expired: bool = False
):
    if partition < 0:
        raise HTTPException(status_code=404, detail=f"Partition {partition} not found")
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

    parsed_header_filter = None
    if header_filter_json:
        try:
            parsed_header_filter = json.loads(header_filter_json)
            if not isinstance(parsed_header_filter, dict):
                raise ValueError()
        except Exception:
            raise HTTPException(status_code=400, detail="header_filter must be a valid JSON object")

    # Determine starting offset
    current_offset = None
    if group:
        current_offset = await get_offset(group, topic, partition)
        
    if current_offset is None:
        current_offset = offset if offset is not None else 0

    # Read messages
    messages = await partition_obj.read(
        offset=current_offset,
        max_count=limit,
        key_filter=key_filter,
        header_filter=parsed_header_filter,
        drop_expired=drop_expired
    )
    
    records = [
        MessageRecord(
            offset=msg["offset"],
            timestamp=msg["timestamp"],
            key=msg.get("key"),
            value=msg.get("value"),
            headers=msg.get("headers")
        ) for msg in messages
    ]
    
    if messages:
        next_offset = messages[-1]["offset"] + 1
    else:
        next_offset = current_offset
        
    # Auto-commit if group is provided, messages read, and auto_commit enabled
    if group and messages and auto_commit:
        await commit_offset(group, topic, partition, next_offset)
        
    high_watermark = partition_obj.get_high_watermark()
    has_more = next_offset < high_watermark
    
    metrics.record_consumed(topic, count=len(records))

    return MessageConsumeResponse(
        topic=topic,
        partition=partition,
        messages=records,
        next_offset=next_offset,
        has_more=has_more
    )

@router.post("/topics/{topic}/consumer/register", response_model=ConsumerRegisterResponse)
async def register_consumer(request: Request, topic: str, payload: ConsumerRegisterRequest):
    topics = request.app.state.topics
    if topic not in topics:
        raise HTTPException(status_code=404, detail="Topic not found")
        
    topic_obj = topics[topic]
    total_partitions = len(topic_obj.partitions)
    coordinator = request.app.state.coordinator
    
    # Initialize group offsets for all partitions if not already set
    for p_id, p in topic_obj.partitions.items():
        existing = await get_offset(payload.group, topic, p_id)
        if existing is None:
            if payload.initial_offset == "earliest":
                val = p.get_earliest_offset()
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

    assigned_partitions = list(range(total_partitions))
    if payload.consumer_id:
        assigned_partitions = await coordinator.join_group(
            group_id=payload.group,
            topic=topic,
            consumer_id=payload.consumer_id,
            total_partitions=total_partitions,
            strategy=payload.strategy or "range"
        )
        
    return ConsumerRegisterResponse(
        status="registered",
        group=payload.group,
        topic=topic,
        consumer_id=payload.consumer_id,
        assigned_partitions=assigned_partitions
    )

@router.post("/topics/{topic}/consumer/heartbeat")
async def consumer_heartbeat(request: Request, topic: str, payload: ConsumerHeartbeatRequest):
    coordinator = request.app.state.coordinator
    success = await coordinator.heartbeat(payload.group, topic, payload.consumer_id)
    if not success:
        raise HTTPException(status_code=404, detail="Consumer not found in group coordinator")
    return {"status": "heartbeat_ok"}

@router.post("/topics/{topic}/consumer/leave")
async def consumer_leave(request: Request, topic: str, payload: ConsumerLeaveRequest):
    topics = request.app.state.topics
    topic_obj = topics.get(topic)
    total_parts = len(topic_obj.partitions) if topic_obj else 1
    coordinator = request.app.state.coordinator
    
    success = await coordinator.leave_group(payload.group, topic, payload.consumer_id, total_parts)
    return {"status": "left" if success else "not_found"}

@router.post("/topics/{topic}/commit")
async def commit_offset_endpoint(topic: str, payload: CommitOffsetRequest):
    if payload.partition < 0 or payload.offset < 0:
        raise HTTPException(status_code=400, detail="partition and offset must be non-negative")
    await commit_offset(payload.group, topic, payload.partition, payload.offset, payload.metadata or "")
    return {"status": "committed"}

@router.post("/topics/{topic}/commit/batch")
async def commit_offsets_batch_endpoint(topic: str, payload: BatchCommitOffsetRequest):
    for p_id, off in payload.offsets.items():
        if p_id < 0 or off < 0:
            raise HTTPException(status_code=400, detail="All partitions and offsets must be non-negative")
    await commit_offsets_batch(payload.group, topic, payload.offsets)
    return {"status": "committed_batch", "count": len(payload.offsets)}

@router.post("/topics/{topic}/consumer/commit")
async def commit_offset_endpoint_legacy(topic: str, payload: CommitOffsetRequest):
    if payload.partition < 0 or payload.offset < 0:
        raise HTTPException(status_code=400, detail="partition and offset must be non-negative")
    await commit_offset(payload.group, topic, payload.partition, payload.offset, payload.metadata or "")
    return {"status": "committed"}

@router.post("/topics/{topic}/consumer/reset")
async def reset_offsets_endpoint(request: Request, topic: str, payload: OffsetResetRequest):
    topics = request.app.state.topics
    if topic not in topics:
        raise HTTPException(status_code=404, detail="Topic not found")
        
    topic_obj = topics[topic]
    partitions_to_reset = [payload.partition] if payload.partition is not None else list(topic_obj.partitions.keys())
    
    for p_id in partitions_to_reset:
        p = topic_obj.get_partition(p_id)
        if not p:
            continue
        if payload.offset == "earliest":
            val = p.get_earliest_offset()
        elif payload.offset == "latest":
            val = p.get_high_watermark()
        else:
            try:
                val = int(payload.offset)
                if val < 0:
                    raise HTTPException(status_code=400, detail="Offset must be non-negative")
            except ValueError:
                raise HTTPException(status_code=400, detail="Invalid offset specification")
                
        await commit_offset(payload.group, topic, p_id, val)
        
    return {"status": "reset", "group": payload.group, "topic": topic, "partitions": partitions_to_reset}

@router.get("/groups", response_model=List[ConsumerGroupInfo])
async def get_consumer_groups(request: Request):
    topics = request.app.state.topics
    all_db_offsets = await get_all_offsets()
    coordinator = request.app.state.coordinator
    
    group_topics = {}
    for row in all_db_offsets:
        group = row["group_name"]
        t = row["topic"]
        partition = row["partition_id"]
        off = row["offset"]
        
        key = (group, t)
        if key not in group_topics:
            group_topics[key] = {}
        group_topics[key][partition] = off
        
    result = []
    for (group, t), partition_offsets in group_topics.items():
        if t not in topics:
            continue
            
        topic_obj = topics[t]
        total_current = 0
        total_latest = 0
        part_details = []
        
        for p_id, p in topic_obj.partitions.items():
            current = partition_offsets.get(p_id, 0)
            latest = p.get_high_watermark()
            part_lag = max(0, latest - current)
            total_current += current
            total_latest += latest
            part_details.append(
                PartitionLagInfo(
                    partition=p_id,
                    current_offset=current,
                    latest_offset=latest,
                    lag=part_lag
                )
            )
            
        lag = max(0, total_latest - total_current)
        
        members_dicts = []
        assignment_map = await coordinator.get_assignments(group, t)
        for c_id, parts in assignment_map.items():
            m = await coordinator.get_member(group, t, c_id)
            if m:
                members_dicts.append(ConsumerGroupMemberInfo(**m.to_dict()))

        result.append(
            ConsumerGroupInfo(
                group=group,
                topic=t,
                current_offset=total_current,
                latest_offset=total_latest,
                lag=lag,
                partition_details=part_details,
                members=members_dicts
            )
        )
        
    return result

@router.delete("/groups/{group}")
async def delete_consumer_group(group: str, topic: Optional[str] = None):
    deleted = await delete_group(group, topic)
    return {"status": "deleted", "group": group, "rows_deleted": deleted}

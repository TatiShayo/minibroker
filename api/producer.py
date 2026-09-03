from fastapi import APIRouter, Request, HTTPException
from models.schemas import MessageProduce, MessageProduceResponse, BatchProduceRequest, BatchProduceResponse
from core.metrics import metrics

router = APIRouter()

@router.post("/topics/{topic}/messages", response_model=MessageProduceResponse)
async def produce_message(request: Request, topic: str, payload: MessageProduce):
    topics = request.app.state.topics
    if topic not in topics:
        raise HTTPException(status_code=404, detail="Topic not found")
        
    topic_obj = topics[topic]
    
    # Determine target partition: explicit or routed via key
    if payload.partition is not None:
        part_idx = payload.partition
        if part_idx not in topic_obj.partitions:
            raise HTTPException(status_code=400, detail=f"Partition {part_idx} does not exist in topic {topic}")
    else:
        part_idx = topic_obj.partition(payload.key)
        
    partition_obj = topic_obj.get_partition(part_idx)
    if not partition_obj:
        raise HTTPException(status_code=500, detail=f"Partition {part_idx} not found inside topic")
        
    result = await partition_obj.append(
        key=payload.key,
        value=payload.value,
        headers=payload.headers,
        timestamp=payload.timestamp
    )
    
    metrics.record_produced(topic, count=1)
    
    return MessageProduceResponse(
        topic=topic,
        partition=part_idx,
        offset=result["offset"],
        timestamp=result["timestamp"]
    )

@router.post("/topics/{topic}/messages/batch", response_model=BatchProduceResponse)
async def produce_message_batch(request: Request, topic: str, payload: BatchProduceRequest):
    if len(payload.messages) > 5000:
        raise HTTPException(status_code=400, detail="Batch size exceeds maximum limit of 5000 messages")
        
    topics = request.app.state.topics
    if topic not in topics:
        raise HTTPException(status_code=404, detail="Topic not found")
        
    topic_obj = topics[topic]
    batch_dicts = [
        {
            "key": m.key,
            "value": m.value,
            "headers": m.headers,
            "partition": m.partition,
            "timestamp": m.timestamp
        }
        for m in payload.messages
    ]
    
    try:
        results = await topic_obj.produce_batch(batch_dicts)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
        
    metrics.record_produced(topic, count=len(results))
    
    return BatchProduceResponse(
        topic=topic,
        results=[
            MessageProduceResponse(
                topic=r["topic"],
                partition=r["partition"],
                offset=r["offset"],
                timestamp=r["timestamp"]
            )
            for r in results
        ]
    )

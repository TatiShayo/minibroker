from fastapi import APIRouter, Request, HTTPException
from models.schemas import MessageProduce, MessageProduceResponse, BatchProduceRequest, BatchProduceResponse

router = APIRouter()

@router.post("/topics/{topic}/messages", response_model=MessageProduceResponse)
async def produce_message(request: Request, topic: str, payload: MessageProduce):
    topics = request.app.state.topics
    if topic not in topics:
        raise HTTPException(status_code=404, detail="Topic not found")
        
    topic_obj = topics[topic]
    
    # Route message to a partition based on key or round-robin
    part_idx = topic_obj.partition(payload.key)
    partition_obj = topic_obj.get_partition(part_idx)
    
    if not partition_obj:
        raise HTTPException(status_code=500, detail="Partition not found inside topic")
        
    # Append message
    result = await partition_obj.append(
        key=payload.key,
        value=payload.value,
        headers=payload.headers
    )
    
    return MessageProduceResponse(
        topic=topic,
        partition=part_idx,
        offset=result["offset"],
        timestamp=result["timestamp"]
    )

@router.post("/topics/{topic}/messages/batch", response_model=BatchProduceResponse)
async def produce_message_batch(request: Request, topic: str, payload: BatchProduceRequest):
    if len(payload.messages) > 1000:
        raise HTTPException(status_code=400, detail="Batch size exceeds maximum limit of 1000 messages")
        
    topics = request.app.state.topics
    if topic not in topics:
        raise HTTPException(status_code=404, detail="Topic not found")
        
    topic_obj = topics[topic]
    results = []
    
    for msg in payload.messages:
        part_idx = topic_obj.partition(msg.key)
        partition_obj = topic_obj.get_partition(part_idx)
        if not partition_obj:
            raise HTTPException(status_code=500, detail=f"Partition {part_idx} not found")
            
        result = await partition_obj.append(
            key=msg.key,
            value=msg.value,
            headers=msg.headers
        )
        results.append(
            MessageProduceResponse(
                topic=topic,
                partition=part_idx,
                offset=result["offset"],
                timestamp=result["timestamp"]
            )
        )
        
    return BatchProduceResponse(
        topic=topic,
        results=results
    )

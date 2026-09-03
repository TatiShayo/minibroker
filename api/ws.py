import asyncio
import json
from fastapi import APIRouter, WebSocket, WebSocketDisconnect, Query
from typing import Optional

router = APIRouter()

@router.websocket("/ws/topics/{topic}/subscribe")
async def websocket_topic_subscribe(
    websocket: WebSocket,
    topic: str,
    partition: Optional[int] = Query(default=None),
    from_offset: Optional[int] = Query(default=None)
):
    await websocket.accept()
    topics = websocket.app.state.topics
    
    if topic not in topics:
        await websocket.send_json({"error": f"Topic '{topic}' not found"})
        await websocket.close(code=1008)
        return
        
    topic_obj = topics[topic]
    partitions_to_watch = [partition] if partition is not None else list(topic_obj.partitions.keys())
    
    # Validate partitions exist
    for p_id in partitions_to_watch:
        if p_id not in topic_obj.partitions:
            await websocket.send_json({"error": f"Partition {p_id} does not exist"})
            await websocket.close(code=1008)
            return

    queue: asyncio.Queue = asyncio.Queue(maxsize=1000)

    # 1. Historical replay if from_offset provided
    if from_offset is not None:
        for p_id in partitions_to_watch:
            p_obj = topic_obj.partitions[p_id]
            historical = await p_obj.read(offset=from_offset, max_count=500)
            for rec in historical:
                await websocket.send_json({
                    "event": "message",
                    "topic": topic,
                    "partition": p_id,
                    "message": rec
                })

    # 2. Subscribe queue to live partitions
    for p_id in partitions_to_watch:
        topic_obj.partitions[p_id].subscribe(queue)

    try:
        while True:
            # Check for incoming client messages / ping / disconnect
            # or pop from subscriber queue with a timeout
            try:
                record = await asyncio.wait_for(queue.get(), timeout=1.0)
                await websocket.send_json({
                    "event": "message",
                    "topic": topic,
                    "message": record
                })
            except asyncio.TimeoutError:
                # Heartbeat ping
                try:
                    await websocket.send_json({"event": "ping"})
                except Exception:
                    break
    except (WebSocketDisconnect, Exception):
        pass
    finally:
        # Clean up subscriptions
        for p_id in partitions_to_watch:
            if p_id in topic_obj.partitions:
                topic_obj.partitions[p_id].unsubscribe(queue)

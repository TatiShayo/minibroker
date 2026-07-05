from typing import Any, Dict, List, Optional, Union
from pydantic import BaseModel, Field

class TopicCreate(BaseModel):
    name: str
    partitions: int = Field(default=1, ge=1)

class PartitionInfo(BaseModel):
    id: int
    messages: int
    size: int

class TopicInfo(BaseModel):
    name: str
    partitions: List[PartitionInfo]
    total_messages: int

class TopicListResponse(BaseModel):
    topics: List[TopicInfo]

class MessageProduce(BaseModel):
    key: Optional[str] = None
    value: Any
    headers: Optional[Dict[str, Any]] = None

class MessageProduceResponse(BaseModel):
    topic: str
    partition: int
    offset: int
    timestamp: int

class BatchProduceRequest(BaseModel):
    messages: List[MessageProduce]

class BatchProduceResponse(BaseModel):
    topic: str
    results: List[MessageProduceResponse]

class MessageRecord(BaseModel):
    offset: int
    timestamp: int
    key: Optional[str] = None
    value: Any
    headers: Optional[Dict[str, Any]] = None

class MessageConsumeResponse(BaseModel):
    topic: str
    partition: int
    messages: List[MessageRecord]
    next_offset: int
    has_more: bool

class ConsumerRegisterRequest(BaseModel):
    group: str
    initial_offset: Union[str, int] = Field(default="latest")

class ConsumerGroupInfo(BaseModel):
    group: str
    topic: str
    current_offset: int
    latest_offset: int
    lag: int

class StatsResponse(BaseModel):
    topics: int
    partitions: int
    total_messages: int
    disk_usage_bytes: int

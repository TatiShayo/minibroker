from typing import Any, Dict, List, Optional, Union
from pydantic import BaseModel, Field

class TopicCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=255, description="Topic name")
    partitions: int = Field(default=1, ge=1, le=1024, description="Number of partitions")

class TopicExpand(BaseModel):
    partitions: int = Field(..., ge=1, le=1024, description="New total number of partitions")

class PartitionInfo(BaseModel):
    id: int
    messages: int
    size: int
    earliest_offset: int = 0
    latest_offset: int = 0
    leader: int = 0

class TopicInfo(BaseModel):
    name: str
    partitions: List[PartitionInfo]
    total_messages: int
    total_size_bytes: int = 0

class TopicListResponse(BaseModel):
    topics: List[TopicInfo]

class MessageProduce(BaseModel):
    key: Optional[str] = None
    value: Any = None
    headers: Optional[Dict[str, Any]] = None
    partition: Optional[int] = None
    timestamp: Optional[int] = None

class MessageProduceResponse(BaseModel):
    topic: str
    partition: int
    offset: int
    timestamp: int

class BatchProduceRequest(BaseModel):
    messages: List[MessageProduce] = Field(..., min_length=1, max_length=5000)

class BatchProduceResponse(BaseModel):
    topic: str
    results: List[MessageProduceResponse]

class MessageRecord(BaseModel):
    offset: int
    timestamp: int
    key: Optional[str] = None
    value: Any = None
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
    consumer_id: Optional[str] = None
    strategy: Optional[str] = "range"

class ConsumerRegisterResponse(BaseModel):
    status: str
    group: str
    topic: str
    consumer_id: Optional[str] = None
    assigned_partitions: List[int] = []

class ConsumerHeartbeatRequest(BaseModel):
    group: str
    consumer_id: str

class ConsumerLeaveRequest(BaseModel):
    group: str
    consumer_id: str

class CommitOffsetRequest(BaseModel):
    group: str
    partition: int = Field(..., ge=0)
    offset: int = Field(..., ge=0)
    metadata: Optional[str] = ""

class BatchCommitOffsetRequest(BaseModel):
    group: str
    offsets: Dict[int, int]

class OffsetResetRequest(BaseModel):
    group: str
    partition: Optional[int] = None
    offset: Union[str, int] = Field(default="earliest")

class PartitionLagInfo(BaseModel):
    partition: int
    current_offset: int
    latest_offset: int
    lag: int

class ConsumerGroupMemberInfo(BaseModel):
    consumer_id: str
    group_id: str
    topic: str
    client_host: str = ""
    assigned_partitions: List[int] = []
    last_heartbeat: float = 0.0

class ConsumerGroupInfo(BaseModel):
    group: str
    topic: str
    current_offset: int
    latest_offset: int
    lag: int
    partition_details: Optional[List[PartitionLagInfo]] = None
    members: Optional[List[ConsumerGroupMemberInfo]] = None

class DeadLetterRouteRequest(BaseModel):
    partition: int = 0
    offset: int = 0
    key: Optional[str] = None
    value: Any = None
    headers: Optional[Dict[str, Any]] = None
    reason: str = "processing_failed"
    retry_count: int = 1

class DeadLetterRedriveRequest(BaseModel):
    max_count: int = Field(default=100, ge=1, le=1000)

class StatsResponse(BaseModel):
    topics: int
    partitions: int
    total_messages: int
    disk_usage_bytes: int
    uptime_seconds: float = 0.0
    throughput_msg_sec: float = 0.0
    messages_produced_total: int = 0
    messages_consumed_total: int = 0
    dlq_messages_total: int = 0
    dropped_messages_total: int = 0

class RetentionCleanupResponse(BaseModel):
    status: str
    cleaned_segments: int

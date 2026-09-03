import time
import asyncio
from typing import Dict, Any, Optional

class MetricsCollector:
    """Tracks global and granular broker throughput, message rates, lag, and drops."""
    
    def __init__(self):
        self.start_time: float = time.time()
        self.messages_produced: int = 0
        self.bytes_produced: int = 0
        self.messages_consumed: int = 0
        self.dlq_messages: int = 0
        self.dropped_messages: int = 0
        self._topic_stats: Dict[str, Dict[str, int]] = {}
        self._lock = asyncio.Lock()

    def record_produced(self, topic: str, count: int = 1, bytes_count: int = 0):
        self.messages_produced += count
        self.bytes_produced += bytes_count
        if topic not in self._topic_stats:
            self._topic_stats[topic] = {"produced": 0, "consumed": 0, "bytes": 0}
        self._topic_stats[topic]["produced"] += count
        self._topic_stats[topic]["bytes"] += bytes_count

    def record_consumed(self, topic: str, count: int = 1):
        self.messages_consumed += count
        if topic not in self._topic_stats:
            self._topic_stats[topic] = {"produced": 0, "consumed": 0, "bytes": 0}
        self._topic_stats[topic]["consumed"] += count

    def record_dlq(self, count: int = 1):
        self.dlq_messages += count

    def record_dropped(self, count: int = 1):
        self.dropped_messages += count

    def get_summary(self, topics_map: Optional[dict] = None) -> dict:
        uptime = max(0.001, time.time() - self.start_time)
        
        total_topics = len(topics_map) if topics_map is not None else 0
        total_partitions = 0
        total_messages_on_disk = 0
        total_disk_bytes = 0

        if topics_map:
            for t_obj in topics_map.values():
                for p in t_obj.get_all_partitions():
                    total_partitions += 1
                    total_messages_on_disk += p.get_offset_count()
                    total_disk_bytes += p.get_size_bytes()

        return {
            "uptime_seconds": round(uptime, 2),
            "throughput_msg_sec": round(self.messages_produced / uptime, 2),
            "messages_produced_total": self.messages_produced,
            "bytes_produced_total": self.bytes_produced,
            "messages_consumed_total": self.messages_consumed,
            "dlq_messages_total": self.dlq_messages,
            "dropped_messages_total": self.dropped_messages,
            "topics_count": total_topics,
            "partitions_count": total_partitions,
            "total_retained_messages": total_messages_on_disk,
            "total_disk_usage_bytes": total_disk_bytes,
            "topic_breakdown": dict(self._topic_stats)
        }

metrics = MetricsCollector()

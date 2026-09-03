import time
import asyncio
from typing import Dict, List, Optional, Set
from core.rebalance import assign_partitions

class ConsumerMember:
    """Represents an active consumer instance inside a consumer group."""
    def __init__(self, consumer_id: str, group_id: str, topic: str, client_host: str = ""):
        self.consumer_id = consumer_id
        self.group_id = group_id
        self.topic = topic
        self.client_host = client_host
        self.assigned_partitions: List[int] = []
        self.last_heartbeat: float = time.time()

    def touch_heartbeat(self):
        self.last_heartbeat = time.time()

    def to_dict(self) -> dict:
        return {
            "consumer_id": self.consumer_id,
            "group_id": self.group_id,
            "topic": self.topic,
            "client_host": self.client_host,
            "assigned_partitions": list(self.assigned_partitions),
            "last_heartbeat": self.last_heartbeat
        }

class ConsumerGroupCoordinator:
    """Coordinates consumer group membership, heartbeats, and partition rebalancing."""
    
    def __init__(self):
        # Key: (group_id, topic) -> dict[consumer_id, ConsumerMember]
        self._groups: Dict[tuple[str, str], Dict[str, ConsumerMember]] = {}
        self._strategy: Dict[tuple[str, str], str] = {}
        self._lock = asyncio.Lock()

    async def join_group(
        self,
        group_id: str,
        topic: str,
        consumer_id: str,
        total_partitions: int,
        client_host: str = "",
        strategy: str = "range"
    ) -> List[int]:
        """Register a consumer in a group and trigger dynamic partition rebalance."""
        async with self._lock:
            key = (group_id, topic)
            if key not in self._groups:
                self._groups[key] = {}
                self._strategy[key] = strategy

            member = ConsumerMember(consumer_id, group_id, topic, client_host)
            self._groups[key][consumer_id] = member
            self._rebalance(group_id, topic, total_partitions)
            return list(member.assigned_partitions)

    async def leave_group(self, group_id: str, topic: str, consumer_id: str, total_partitions: int) -> bool:
        """Deregister consumer and rebalance remaining partitions across active members."""
        async with self._lock:
            key = (group_id, topic)
            if key in self._groups and consumer_id in self._groups[key]:
                del self._groups[key][consumer_id]
                if self._groups[key]:
                    self._rebalance(group_id, topic, total_partitions)
                else:
                    del self._groups[key]
                    self._strategy.pop(key, None)
                return True
            return False

    async def heartbeat(self, group_id: str, topic: str, consumer_id: str) -> bool:
        """Update heartbeat timestamp for active consumer."""
        async with self._lock:
            key = (group_id, topic)
            if key in self._groups and consumer_id in self._groups[key]:
                self._groups[key][consumer_id].touch_heartbeat()
                return True
            return False

    async def get_assignments(self, group_id: str, topic: str) -> Dict[str, List[int]]:
        """Get partition assignment map for consumer group."""
        async with self._lock:
            key = (group_id, topic)
            if key not in self._groups:
                return {}
            return {c_id: list(m.assigned_partitions) for c_id, m in self._groups[key].items()}

    async def get_member(self, group_id: str, topic: str, consumer_id: str) -> Optional[ConsumerMember]:
        """Get individual consumer member."""
        async with self._lock:
            key = (group_id, topic)
            return self._groups.get(key, {}).get(consumer_id)

    async def reap_stale_consumers(self, timeout_seconds: float, get_topic_partitions_fn) -> int:
        """Remove consumers exceeding heartbeat timeout and rebalance affected groups."""
        async with self._lock:
            now = time.time()
            reaped_count = 0
            keys_to_rebalance = []

            for key, members in list(self._groups.items()):
                group_id, topic = key
                dead_consumers = []
                for c_id, member in members.items():
                    if (now - member.last_heartbeat) > timeout_seconds:
                        dead_consumers.append(c_id)

                for c_id in dead_consumers:
                    del members[c_id]
                    reaped_count += 1

                if dead_consumers:
                    if members:
                        keys_to_rebalance.append((group_id, topic))
                    else:
                        del self._groups[key]
                        self._strategy.pop(key, None)

            for group_id, topic in keys_to_rebalance:
                total_parts = get_topic_partitions_fn(topic)
                if total_parts is not None:
                    self._rebalance(group_id, topic, total_parts)

            return reaped_count

    def _rebalance(self, group_id: str, topic: str, total_partitions: int):
        """Execute partition assignment algorithm on current group members."""
        key = (group_id, topic)
        members = self._groups.get(key)
        if not members:
            return

        strategy = self._strategy.get(key, "range")
        partitions = list(range(total_partitions))
        consumer_ids = list(members.keys())
        current_map = {c_id: m.assigned_partitions for c_id, m in members.items()}

        new_assignment = assign_partitions(
            strategy=strategy,
            partitions=partitions,
            consumers=consumer_ids,
            current_assignment=current_map
        )

        for c_id, parts in new_assignment.items():
            if c_id in members:
                members[c_id].assigned_partitions = parts

    async def get_all_members(self, group_id: Optional[str] = None) -> List[dict]:
        """List all active group members across topics."""
        async with self._lock:
            results = []
            for (g_id, topic), members in self._groups.items():
                if group_id is None or group_id == g_id:
                    for member in members.values():
                        results.append(member.to_dict())
            return results

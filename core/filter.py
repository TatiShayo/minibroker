import time
from typing import Any, Dict, Optional

def is_message_expired(msg: dict, current_time_ms: Optional[int] = None) -> bool:
    """Check if message has expired according to header TTL or expires-at timestamp."""
    if current_time_ms is None:
        current_time_ms = int(time.time() * 1000)
        
    headers = msg.get("headers") or {}
    
    # Check explicit expiration timestamp
    if "x-expires-at" in headers:
        try:
            expires_at = int(headers["x-expires-at"])
            if current_time_ms >= expires_at:
                return True
        except (ValueError, TypeError):
            pass
            
    # Check TTL from message creation timestamp
    if "x-ttl-ms" in headers:
        try:
            ttl_ms = int(headers["x-ttl-ms"])
            msg_timestamp = int(msg.get("timestamp", 0))
            if current_time_ms >= (msg_timestamp + ttl_ms):
                return True
        except (ValueError, TypeError):
            pass
            
    return False

def matches_filter(
    msg: dict,
    key_filter: Optional[str] = None,
    header_filter: Optional[Dict[str, Any]] = None,
    since_timestamp: Optional[int] = None,
    until_timestamp: Optional[int] = None,
    drop_expired: bool = True,
    current_time_ms: Optional[int] = None
) -> bool:
    """Evaluate whether a message matches specified filter criteria."""
    if drop_expired and is_message_expired(msg, current_time_ms):
        return False

    msg_ts = msg.get("timestamp", 0)
    if since_timestamp is not None and msg_ts < since_timestamp:
        return False
    if until_timestamp is not None and msg_ts > until_timestamp:
        return False

    if key_filter is not None:
        msg_key = msg.get("key")
        if msg_key != key_filter:
            return False

    if header_filter:
        msg_headers = msg.get("headers") or {}
        for k, v in header_filter.items():
            if k not in msg_headers:
                return False
            if msg_headers[k] != v:
                return False

    return True

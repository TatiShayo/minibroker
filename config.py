import os

# Base directory for storage
DEFAULT_DATA_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "data"))
DATA_DIR = os.environ.get("MINIBROKER_DATA_DIR", DEFAULT_DATA_DIR)

# Storage limits
SEGMENT_MAX_SIZE = int(os.environ.get("MINIBROKER_SEGMENT_MAX_SIZE", 1_048_576))  # 1MB default
INDEX_INTERVAL = int(os.environ.get("MINIBROKER_INDEX_INTERVAL", 1000))  # index every 1000th message
RETENTION_HOURS = float(os.environ.get("MINIBROKER_RETENTION_HOURS", 72.0))  # 72 hours
RETENTION_MAX_BYTES = int(os.environ.get("MINIBROKER_RETENTION_MAX_BYTES", 1_073_741_824))  # 1GB default per partition
DEFAULT_PARTITIONS = int(os.environ.get("MINIBROKER_DEFAULT_PARTITIONS", 1))

# Dead Letter Queue (DLQ) Configuration
DLQ_MAX_RETRIES = int(os.environ.get("MINIBROKER_DLQ_MAX_RETRIES", 3))
DLQ_TOPIC_SUFFIX = os.environ.get("MINIBROKER_DLQ_TOPIC_SUFFIX", ".DLQ")

# Consumer Group Rebalancing & Heartbeats
CONSUMER_HEARTBEAT_TIMEOUT_SECONDS = float(os.environ.get("MINIBROKER_CONSUMER_HEARTBEAT_TIMEOUT", 30.0))
REBALANCE_STRATEGY = os.environ.get("MINIBROKER_REBALANCE_STRATEGY", "range")  # "range", "round_robin", "sticky"

# I/O Durability
SYNC_WRITES = os.environ.get("MINIBROKER_SYNC_WRITES", "false").lower() in ("true", "1", "yes")

# Server settings
HOST = os.environ.get("MINIBROKER_HOST", "0.0.0.0")
PORT = int(os.environ.get("MINIBROKER_PORT", 8000))

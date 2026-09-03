import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from core.offsets import init_db
from storage.persistence import load_all_topics
from core.topic import Topic
from core.consumer_group import ConsumerGroupCoordinator
from config import CONSUMER_HEARTBEAT_TIMEOUT_SECONDS, RETENTION_HOURS, RETENTION_MAX_BYTES

from api.admin import router as admin_router
from api.producer import router as producer_router
from api.consumer import router as consumer_router
from api.dlq import router as dlq_router
from api.ws import router as ws_router

async def _consumer_reaper_task(app: FastAPI):
    """Background task to detect dead consumers and trigger partition rebalancing."""
    while True:
        try:
            await asyncio.sleep(5.0)
            coordinator: ConsumerGroupCoordinator = getattr(app.state, "coordinator", None)
            topics_map = getattr(app.state, "topics", {})
            if coordinator and topics_map:
                def get_topic_parts(t_name):
                    t = topics_map.get(t_name)
                    return len(t.partitions) if t else None
                await coordinator.reap_stale_consumers(
                    timeout_seconds=CONSUMER_HEARTBEAT_TIMEOUT_SECONDS,
                    get_topic_partitions_fn=get_topic_parts
                )
        except asyncio.CancelledError:
            break
        except Exception:
            pass

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Initialize SQLite database
    await init_db()
    
    # Initialize topics and coordinator state
    app.state.topics = {}
    app.state.coordinator = ConsumerGroupCoordinator()
    
    # Load topic metadata from disk
    topics_metadata = await load_all_topics()
    
    # Instantiate Topic objects
    for name, partitions_count in topics_metadata.items():
        app.state.topics[name] = Topic(name, partitions_count)
        
    # Start up all topics in parallel
    if app.state.topics:
        await asyncio.gather(*(topic.startup() for topic in app.state.topics.values()))
        
    # Launch background consumer heartbeat reaper
    reaper_handle = asyncio.create_task(_consumer_reaper_task(app))
    
    yield
    
    # Clean shutdown
    reaper_handle.cancel()
    try:
        await reaper_handle
    except asyncio.CancelledError:
        pass

app = FastAPI(
    title="MiniBroker",
    description="High-performance async append-only message broker with partitions, consumer groups, and DLQ",
    version="1.0.0",
    lifespan=lifespan
)

# Enable CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(admin_router)
app.include_router(producer_router)
app.include_router(consumer_router)
app.include_router(dlq_router)
app.include_router(ws_router)

@app.get("/health")
def health():
    return {"status": "healthy"}

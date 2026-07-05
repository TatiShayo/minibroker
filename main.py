from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import asyncio

from core.offsets import init_db
from storage.persistence import load_all_topics
from core.topic import Topic
from api.admin import router as admin_router
from api.producer import router as producer_router
from api.consumer import router as consumer_router

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Initialize SQLite database
    await init_db()
    
    # Initialize topics state
    app.state.topics = {}
    
    # Load topic metadata
    topics_metadata = await load_all_topics()
    
    # Instantiate Topic objects
    for name, partitions_count in topics_metadata.items():
        app.state.topics[name] = Topic(name, partitions_count)
        
    # Start up all topics in parallel
    if app.state.topics:
        await asyncio.gather(*(topic.startup() for topic in app.state.topics.values()))
        
    yield

app = FastAPI(lifespan=lifespan)

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

@app.get("/health")
def health():
    return {"status": "healthy"}

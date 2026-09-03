import sys
import tempfile
import shutil
import pytest
import os
import asyncio
from contextlib import asynccontextmanager
from httpx import AsyncClient, ASGITransport

# Create temporary directory for isolated test execution
temp_data_dir = tempfile.mkdtemp()

# Modify config before modules import it
import config
config.DATA_DIR = temp_data_dir
config.SYNC_WRITES = False

# Override database path in core.offsets
import core.offsets
core.offsets.DB_PATH = os.path.join(temp_data_dir, "offsets.db")

from main import app

@pytest.fixture(scope="session", autouse=True)
def cleanup_temp_dir():
    yield
    try:
        shutil.rmtree(temp_data_dir)
    except Exception:
        pass

@pytest.fixture(autouse=True)
def clean_temp_data():
    # Clean the temp directory before each test
    for item in os.listdir(temp_data_dir):
        path = os.path.join(temp_data_dir, item)
        try:
            if os.path.isdir(path):
                shutil.rmtree(path)
            else:
                os.remove(path)
        except Exception:
            pass
    yield

@pytest.fixture
async def client():
    async with app.router.lifespan_context(app):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://testserver") as ac:
            yield ac

@pytest.fixture
def client_factory():
    @asynccontextmanager
    async def _factory():
        async with app.router.lifespan_context(app):
            async with AsyncClient(transport=ASGITransport(app=app), base_url="http://testserver") as ac:
                yield ac
    return _factory

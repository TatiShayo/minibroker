import sys
import tempfile
import shutil
import pytest
import os
import asyncio

# Create temporary directory for isolated test execution
temp_data_dir = tempfile.mkdtemp()

# Modify config before any modules import it
import config
config.DATA_DIR = temp_data_dir

# Override the database path in core.offsets
import core.offsets
core.offsets.DB_PATH = os.path.join(temp_data_dir, "offsets.db")

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

# Configure pytest-asyncio event loop
@pytest.fixture(scope="session")
def event_loop():
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
    yield loop
    loop.close()

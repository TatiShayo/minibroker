import pytest
import asyncio
from starlette.testclient import TestClient
from main import app
from core.topic import Topic
from core.offsets import init_db

@pytest.fixture(scope="module", autouse=True)
def init_test_db():
    asyncio.run(init_db())

def test_websocket_nonexistent_topic():
    with TestClient(app) as client:
        with client.websocket_connect("/ws/topics/nonexistent/subscribe") as ws:
            data = ws.receive_json()
            assert "error" in data
            assert "not found" in data["error"]

def test_websocket_realtime_streaming():
    with TestClient(app) as client:
        # Create topic first via REST
        resp = client.post("/topics", json={"name": "ws-live-topic", "partitions": 1})
        assert resp.status_code == 200
        
        # Connect WebSocket
        with client.websocket_connect("/ws/topics/ws-live-topic/subscribe?partition=0") as ws:
            # Produce a message via REST
            prod_resp = client.post("/topics/ws-live-topic/messages", json={
                "key": "stream-key",
                "value": {"event": "user_signup", "id": 42}
            })
            assert prod_resp.status_code == 200
            
            # Receive streamed message over WebSocket
            received = ws.receive_json()
            assert received.get("event") in ("message", "ping")
            if received.get("event") == "ping":
                received = ws.receive_json()
                
            assert received["event"] == "message"
            assert received["topic"] == "ws-live-topic"
            msg = received["message"]
            assert msg["key"] == "stream-key"
            assert msg["value"] == {"event": "user_signup", "id": 42}

def test_websocket_historical_replay():
    with TestClient(app) as client:
        topic = "ws-replay-topic"
        client.post("/topics", json={"name": topic, "partitions": 1})
        
        # Pre-populate 3 messages
        client.post(f"/topics/{topic}/messages", json={"value": "msg-0"})
        client.post(f"/topics/{topic}/messages", json={"value": "msg-1"})
        client.post(f"/topics/{topic}/messages", json={"value": "msg-2"})
        
        # Connect with from_offset=1
        with client.websocket_connect(f"/ws/topics/{topic}/subscribe?partition=0&from_offset=1") as ws:
            # First replayed message should be offset 1
            msg1 = ws.receive_json()
            assert msg1["event"] == "message"
            assert msg1["message"]["offset"] == 1
            assert msg1["message"]["value"] == "msg-1"
            
            # Second replayed message should be offset 2
            msg2 = ws.receive_json()
            assert msg2["event"] == "message"
            assert msg2["message"]["offset"] == 2
            assert msg2["message"]["value"] == "msg-2"

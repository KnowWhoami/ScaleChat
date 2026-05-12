from contextlib import asynccontextmanager
from typing import Annotated
import json
import os
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


from fastapi import (
    FastAPI,
    Query,
    WebSocket,
    WebSocketDisconnect,
    WebSocketException,
    status,
)
from contextlib import asynccontextmanager
from confluent_kafka import Producer, Consumer, KafkaError
import redis

from .jwt_auth import oauth2_scheme, get_username_from_token
from .models import Message, MessageRequest, Request
from .channel_requests import send_channel_request

SERVER_URL = os.getenv("SERVER_URL", "websocket_server_1:80")

# Kafka configuration
TOPIC = "messages"
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka-1:9092")
PRODUCER_CONFIG = {
    "bootstrap.servers": KAFKA_BROKER,
    "client.id": "websocket-message-producer",
}
producer = Producer(PRODUCER_CONFIG)

# Redis configuration
redis_instance = redis.Redis(host="redis", port=6379, db=0, decode_responses=True)

"""
Message sent by the client
{
    "message_id": str,
    "channel_id": str,
    "timestamp": str,
    "username": str,
    "message": str,
}
"""
# todo: in the future maybe would need to create a Request class, to allow different tipes of requests: message, history of a channel…
# for now, the only requests the client will send are messages.





@asynccontextmanager
async def lifespan(app: FastAPI):
    # startup

    yield
    # shutdown
    producer.flush()  # before shutdown, the producer needs to send the pending messages


app = FastAPI()


class ConnectionManager:
    def __init__(self):
        """
        active_connections: dics of the websocket connections. The username is the key and the WebSocket the value.
        """
        self.active_connections: dict[str, WebSocket] = {}

    async def connect(self, username: str, websocket: WebSocket):
        await websocket.accept()
        logger.info(f"[CONNECT] {username} connected (websocket id={id(websocket)})")
        self.active_connections[username] = websocket
        redis_instance.hset("active_connections", username, SERVER_URL)
        return True

    def disconnect(self, username, websocket: WebSocket):
        logger.info(f"[DISCONNECT] called for {username} (websocket id={id(websocket)}), current={id(self.active_connections.get(username))}")
        if username in self.active_connections and self.active_connections[username] is websocket:
            del self.active_connections[username]
            if redis_instance.hget("active_connections", username) == SERVER_URL:
                redis_instance.hdel("active_connections", username)
            logger.info(f"[DISCONNECT] {username} removed from active connections")


manager = ConnectionManager()


async def send_message_to_server(message_str: str, websocket: WebSocket):
    try:
        # Parse the data
        data = json.loads(message_str)
        message = Message(**data)

        # todo: maybe send the MessageRequest in kafka instead of Message.
        # Send the message to Kafka
        producer.produce(
            TOPIC, key=str(message.channel_id), value=message.model_dump_json()
        )
        producer.poll(1)

        # Send acknowledgment to the client
        await websocket.send_text(
            json.dumps({"status": "sent", "message_id": message.message_id})
        )

    except json.JSONDecodeError:
        await websocket.send_text(json.dumps({"error": "Invalid JSON format for message request"}))

    except ValueError as e:
        await websocket.send_text(
            json.dumps({"error": f"Invalid message format: {str(e)}"})
        )

    except Exception as e:
        await websocket.send_text(
            json.dumps({"error": f"Failed to process message, error: {str(e)}"})
        )



@app.get("/")
async def root():
    return {"hello_world": "Hello World!",
            "websocket_url": SERVER_URL
            }


@app.websocket("/ws")
async def websocket_endpoint(
    *,
    websocket: WebSocket,
    token: Annotated[str, Query()],
):
    # Check if the token is valid
    try:
        username = get_username_from_token(token)
        logger.info(f"[WS] Token valid for user: {username}")
    except Exception as error:
        logger.warning(f"[WS] Token validation failed: {type(error).__name__}: {error}")
        raise WebSocketException(code=status.WS_1008_POLICY_VIOLATION)

    # Store the connection in the manager
    connected = await manager.connect(username, websocket)

    if not connected:
        return

    try:
        while True:
            data_str = await websocket.receive_text()

            try:
                data = json.loads(data_str)
                request = Request(**data)

                if request.type == 0:
                    await send_message_to_server(request.data, websocket)

                elif request.type == 1:
                    await send_channel_request(request.data, username, websocket, redis_instance)


            except json.JSONDecodeError:
                await websocket.send_text(json.dumps({"error": "Invalid JSON format"}))


    except WebSocketDisconnect:
        logger.info(f"[WS] {username} disconnected normally")
        manager.disconnect(username, websocket)
    except Exception as e:
        logger.error(f"[WS] Unexpected exception for {username}: {type(e).__name__}: {e}", exc_info=True)
        manager.disconnect(username, websocket)
        raise WebSocketException(code=status.WS_1008_POLICY_VIOLATION)


@app.post("/message")
async def send_message_to_client(message_request: MessageRequest):
    """
    This endpoint is used internally, the message server sends the message to this endpoint
    Then the message is sent to the client via websocket.
    """
    message = message_request.message
    username = message_request.username
    print(f"Sending message to {username}, message: {message.message}")

    # Check if the user is connected
    if username not in manager.active_connections:
        return {"error": "User not connected"}

    if username == message.username:
        return {"error": f"Invalid username, {username} is the sender"}

    try:
        # Send the message to the client
        websocket = manager.active_connections[username]
        await websocket.send_text(message.model_dump_json())

        return {"status": "sent", "message_id": message.message_id}
    except Exception as e:
        return {"status": "error", "reason": f"Unexpected error: {str(e)}"}

@app.get("/active-connections")
async def get_active_connections():
    return {"active_connections": list(manager.active_connections.keys())}
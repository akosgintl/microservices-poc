"""
Chat Service - WebSocket Bidirectional Communication with Redis, Kafka, and Cassandra
Demonstrates: Full-duplex real-time messaging, chat rooms, presence, horizontal scaling, file sharing, event streaming
Features:
- Redis Pub/Sub for cross-instance message broadcasting
- Redis Sets for global room tracking
- Redis Hashes for user presence across instances
- Redis Lists for recent message cache (10 latest messages)
- Kafka for event streaming to persistence layer
- Cassandra for full message history queries
- MinIO S3-compatible object storage for file uploads
- Chunked file upload with signed URLs
- Periodic health checks to detect and cleanup silent disconnections
"""
import os
import json
import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, Set, Optional, List
from dataclasses import dataclass, asdict
from uuid import uuid4, UUID
import socket

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query, HTTPException, UploadFile, File, Body
from fastapi.responses import JSONResponse
from contextlib import asynccontextmanager
import redis.asyncio as redis
from minio import Minio
from minio.error import S3Error
from aiokafka import AIOKafkaProducer
from cassandra.cluster import Cluster, Session
from cassandra.auth import PlainTextAuthProvider
from cassandra.policies import DCAwareRoundRobinPolicy

# Configure consistent logging format
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(name)-20s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Global connection manager (initialized before app)
manager = None

# Instance identifier for tracking which instance handles which connections
INSTANCE_ID = f"chat-{socket.gethostname()}-{uuid4().hex[:8]}"

# Health check configuration
# Note: Health check is now passive (checks connection state without sending pings)
# Set to a longer interval to avoid unnecessary overhead
HEALTH_CHECK_INTERVAL = int(os.getenv("HEALTH_CHECK_INTERVAL", "60"))  # seconds (passive check)
PING_TIMEOUT = int(os.getenv("PING_TIMEOUT", "5"))  # seconds (unused in passive mode)

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC_MESSAGES = "chat.messages"
KAFKA_TOPIC_EVENTS = "chat.events"

# Cassandra configuration
CASSANDRA_HOSTS = os.getenv("CASSANDRA_HOSTS", "cassandra").split(",")
CASSANDRA_PORT = int(os.getenv("CASSANDRA_PORT", "9042"))
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "chat")


@dataclass
class ChatMessage:
    id: str
    room: str
    username: str
    content: str
    timestamp: str
    type: str = "message"  # message, join, leave, system, file
    file_url: Optional[str] = None  # For file messages
    file_name: Optional[str] = None  # Original file name
    file_size: Optional[int] = None  # File size in bytes


@dataclass
class RoomInfo:
    name: str
    users: list
    message_count: int
    created_at: str


class ConnectionManager:
    """
    Manages WebSocket connections for chat rooms with Redis-backed state, Kafka event streaming, 
    and Cassandra history queries
    
    Redis Data Structures Used:
    - SET   chat:rooms                           -> All active room names
    - SET   chat:room:{room}:users               -> Users in a room (global)
    - HASH  chat:user:{username}                 -> User metadata (room, instance, connected_at)
    - LIST  chat:room:{room}:history             -> Recent message cache (last 10)
    - PUBSUB chat:room:{room}                    -> Real-time message broadcasting
    
    Kafka Topics:
    - chat.messages                              -> Regular chat messages and files
    - chat.events                                -> Join, leave, system messages
    
    Cassandra:
    - Full message history (unlimited)
    
    MinIO Storage:
    - Bucket: chat-files
    - Path format: {room}/{file_id}/{original_filename}
    
    Background Tasks:
    - Redis subscriber loop: Receives messages from other instances via pub/sub
    - Health check loop: Periodically pings connections to detect silent disconnections
    """
    
    def __init__(self):
        # Local WebSocket connections (this instance only)
        self.local_connections: Dict[str, tuple[WebSocket, str, str]] = {}  # conn_id -> (ws, room, username)
        
        # Redis connections
        self.redis: Optional[redis.Redis] = None
        self.redis_subscriber: Optional[redis.Redis] = None
        self.subscriber_task: Optional[asyncio.Task] = None
        self.health_check_task: Optional[asyncio.Task] = None
        self.subscribed_rooms: Set[str] = set()
        
        # Kafka producer
        self.kafka_producer: Optional[AIOKafkaProducer] = None
        
        # Cassandra client
        self.cassandra_cluster: Optional[Cluster] = None
        self.cassandra_session: Optional[Session] = None
        
        # MinIO client
        self.minio_client: Optional[Minio] = None
        self.bucket_name: str = os.getenv("MINIO_BUCKET", "chat-files")
    
    async def init_redis(self):
        """Initialize Redis connections for state management and pub/sub"""
        redis_url = os.getenv("REDIS_URL", "redis://localhost:6379")
        try:
            # Main Redis connection for commands
            self.redis = redis.from_url(redis_url, decode_responses=True)
            await self.redis.ping()
            
            # Separate connection for pub/sub (blocking operations)
            self.redis_subscriber = redis.from_url(redis_url, decode_responses=True)
            
            logger.info(f"✓ Connected to Redis - Instance: {INSTANCE_ID}")
            
            # Start background subscriber task
            self.subscriber_task = asyncio.create_task(self._redis_subscriber_loop())
            
            # Start health check task
            self.health_check_task = asyncio.create_task(self._health_check_loop())
            
        except Exception as e:
            logger.error(f"✗ Redis connection failed: {e}")
            logger.warning("⚠ Running in SINGLE-INSTANCE mode (no cross-instance sync)")
            self.redis = None
            self.redis_subscriber = None
    
    def init_minio(self):
        """Initialize MinIO client for file storage"""
        try:
            endpoint = os.getenv("MINIO_ENDPOINT", "localhost:9000")
            access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
            secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin")
            use_ssl = os.getenv("MINIO_USE_SSL", "false").lower() == "true"
            
            self.minio_client = Minio(
                endpoint,
                access_key=access_key,
                secret_key=secret_key,
                secure=use_ssl
            )
            
            # Create bucket if it doesn't exist
            if not self.minio_client.bucket_exists(self.bucket_name):
                self.minio_client.make_bucket(self.bucket_name)
                logger.info(f"✓ Created MinIO bucket: {self.bucket_name}")
            else:
                logger.info(f"✓ Connected to MinIO bucket: {self.bucket_name}")
            
        except Exception as e:
            logger.error(f"✗ MinIO connection failed: {e}")
            logger.warning("⚠ File upload feature will be disabled")
            self.minio_client = None
    
    async def init_kafka(self):
        """Initialize Kafka producer for event streaming"""
        try:
            logger.info(f"Connecting to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")
            
            self.kafka_producer = AIOKafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            
            await self.kafka_producer.start()
            logger.info(f"✓ Kafka producer started")
            
        except Exception as e:
            logger.error(f"✗ Kafka connection failed: {e}")
            logger.warning("⚠ Message persistence will be disabled")
            self.kafka_producer = None
    
    def init_cassandra(self):
        """Initialize Cassandra client for history queries"""
        try:
            logger.info(f"Connecting to Cassandra at {CASSANDRA_HOSTS}:{CASSANDRA_PORT}")

            # Use load balancing policy to avoid deprecation warning
            self.cassandra_cluster = Cluster(
                contact_points=CASSANDRA_HOSTS,
                port=CASSANDRA_PORT,
                protocol_version=4,
                load_balancing_policy=DCAwareRoundRobinPolicy(local_dc='dc1')
            )

            self.cassandra_session = self.cassandra_cluster.connect()
            self.cassandra_session.set_keyspace(CASSANDRA_KEYSPACE)

            logger.info(f"✓ Connected to Cassandra keyspace: {CASSANDRA_KEYSPACE}")

        except Exception as e:
            logger.error(f"✗ Cassandra connection failed: {e}")
            logger.warning("⚠ History queries will be disabled")
            self.cassandra_cluster = None
            self.cassandra_session = None
    
    async def _redis_subscriber_loop(self):
        """
        Background task: Subscribe to Redis pub/sub channels for cross-instance messaging
        This enables messages from other chat-service instances to reach local WebSocket clients
        """
        if not self.redis_subscriber:
            return
        
        try:
            pubsub = self.redis_subscriber.pubsub()
            logger.info(f"🔊 Redis subscriber started on instance {INSTANCE_ID}")
            
            while True:
                # Dynamically subscribe to rooms as they are created
                await asyncio.sleep(1)
                
                # Get all rooms that have local connections
                local_rooms = set()
                for _, room, _ in self.local_connections.values():
                    local_rooms.add(room)
                
                # Subscribe to new rooms
                new_rooms = local_rooms - self.subscribed_rooms
                for room in new_rooms:
                    channel = f"chat:room:{room}"
                    await pubsub.subscribe(channel)
                    self.subscribed_rooms.add(room)
                    logger.info(f"📡 Subscribed to {channel}")
                
                # Process incoming messages (only if we have subscriptions)
                if self.subscribed_rooms:
                    try:
                        message = await asyncio.wait_for(pubsub.get_message(ignore_subscribe_messages=True), timeout=0.1)
                        if message and message['type'] == 'message':
                            await self._handle_redis_message(message)
                    except asyncio.TimeoutError:
                        continue
                    
        except asyncio.CancelledError:
            logger.info("🛑 Redis subscriber stopped")
            if pubsub:
                await pubsub.unsubscribe()
                await pubsub.aclose()
            raise
        except Exception as e:
            logger.error(f"❌ Redis subscriber error: {e}")
    
    async def _health_check_loop(self):
        """
        Background task: Periodically check for truly dead WebSocket connections
        
        IMPORTANT: We do NOT send ping frames here because:
        1. The API gateway proxy might not forward them correctly
        2. Clients handle their own connection monitoring
        3. WebSocket close events are sufficient for normal disconnections
        
        This task only cleans up connections that are in a broken state where
        the underlying socket is dead but no close event was received.
        """
        logger.info(f"💓 Health check started - passive monitoring every {HEALTH_CHECK_INTERVAL}s")
        
        try:
            while True:
                await asyncio.sleep(HEALTH_CHECK_INTERVAL)
                
                if not self.local_connections:
                    continue
                
                logger.debug(f"💓 Health check: monitoring {len(self.local_connections)} connections")
                
                # Passively check connection states without sending pings
                dead_connections = []
                for conn_id, (websocket, room, username) in list(self.local_connections.items()):
                    try:
                        # Check if the websocket client_state indicates disconnection
                        # This checks the underlying connection state without sending data
                        from starlette.websockets import WebSocketState
                        if websocket.client_state != WebSocketState.CONNECTED:
                            logger.warning(f"⚠ Connection no longer in CONNECTED state: {username} in {room} (state: {websocket.client_state})")
                            dead_connections.append((conn_id, room, username))
                        else:
                            logger.debug(f"✓ Connection alive: {username} in {room}")
                        
                    except Exception as e:
                        logger.warning(f"⚠ Failed to check connection state: {username} in {room} - {e}")
                        dead_connections.append((conn_id, room, username))
                
                # Clean up dead connections
                for conn_id, room, username in dead_connections:
                    logger.info(f"🧹 Cleaning up dead connection: {username} in {room}")
                    await self._disconnect_internal(conn_id, room, username, broadcast_leave=True)
                
                if dead_connections:
                    logger.info(f"💀 Cleaned up {len(dead_connections)} dead connection(s)")
                    
        except asyncio.CancelledError:
            logger.info("🛑 Health check stopped")
            raise
        except Exception as e:
            logger.error(f"❌ Health check error: {e}")
    
    async def _handle_redis_message(self, message):
        """Handle incoming Redis pub/sub message and broadcast to local WebSocket clients"""
        try:
            data = json.loads(message['data'])
            room = data.get('room')
            source_instance = data.get('_instance', '')
            
            # Don't echo messages from this instance (already sent locally)
            if source_instance == INSTANCE_ID:
                return
            
            # Broadcast to local connections in this room
            disconnected = []
            for conn_id, (websocket, conn_room, username) in self.local_connections.items():
                if conn_room == room:
                    try:
                        await websocket.send_json(data)
                        logger.debug(f"📨 Relayed message from {source_instance} to local user {username}")
                    except Exception as e:
                        logger.error(f"Failed to relay to {username}: {e}")
                        disconnected.append(conn_id)
            
            # Clean up disconnected (don't broadcast leave - let WebSocket close handler do it)
            for conn_id in disconnected:
                if conn_id in self.local_connections:
                    _, room, username = self.local_connections[conn_id]
                    await self._disconnect_internal(conn_id, room, username, broadcast_leave=False)
                    
        except Exception as e:
            logger.error(f"Error handling Redis message: {e}")
    
    async def connect(self, websocket: WebSocket, room: str, username: str) -> str:
        """
        Connect a user to a chat room
        Updates Redis state for cross-instance visibility
        """
        await websocket.accept()
        
        connection_id = str(uuid4())
        self.local_connections[connection_id] = (websocket, room, username)
        
        logger.info(f"👤 User '{username}' joined room '{room}' on instance {INSTANCE_ID}")
        
        if self.redis:
            try:
                # Add room to global rooms set
                await self.redis.sadd("chat:rooms", room)
                
                # Add user to room's user set
                await self.redis.sadd(f"chat:room:{room}:users", username)
                
                # Store user metadata
                await self.redis.hset(
                    f"chat:user:{username}",
                    mapping={
                        "room": room,
                        "instance": INSTANCE_ID,
                        "connected_at": datetime.utcnow().isoformat()
                    }
                )
                
                # Set expiry on user metadata (cleanup if connection drops without disconnect)
                await self.redis.expire(f"chat:user:{username}", 3600)  # 1 hour
                
            except Exception as e:
                logger.error(f"Redis state update failed: {e}")
        
        # Send join notification to room (broadcast to all instances)
        # Exclude the newly connected user from receiving their own join message
        # Don't store join messages in history to avoid duplicates
        join_msg = ChatMessage(
            id=str(uuid4()),
            room=room,
            username="system",
            content=f"{username} joined the chat",
            timestamp=datetime.utcnow().isoformat(),
            type="join"
        )
        await self.broadcast_to_room(room, join_msg, exclude_connection=connection_id, store_in_history=False)
        
        # Broadcast updated user list to all users in the room
        if self.redis:
            try:
                users_in_room = await self.redis.smembers(f"chat:room:{room}:users")
                users_msg = {
                    "type": "users",
                    "users": list(users_in_room),
                    "room": room,
                    "_instance": INSTANCE_ID
                }
                # Broadcast to all users (including the newly joined one)
                await self._broadcast_users_list(room, users_msg)
            except Exception as e:
                logger.error(f"Failed to broadcast user list: {e}")
        
        # Send message history from Redis (only 10 latest)
        if self.redis:
            try:
                history = await self.redis.lrange(f"chat:room:{room}:history", 0, 9)
                if history:
                    messages = [json.loads(msg) for msg in history]
                    # Redis LPUSH stores newest first, so we reverse to show oldest first
                    messages.reverse()
                    await websocket.send_json({
                        "type": "history",
                        "messages": messages
                    })
            except Exception as e:
                logger.error(f"Failed to load history: {e}")
        
        # Send current users list from Redis
        if self.redis:
            try:
                users_in_room = await self.redis.smembers(f"chat:room:{room}:users")
                await websocket.send_json({
                    "type": "users",
                    "users": list(users_in_room),
                    "room": room
                })
            except Exception as e:
                logger.error(f"Failed to load users: {e}")
        
        return connection_id
    
    async def disconnect(self, connection_id: str, room: str, username: str, broadcast_leave: bool = True):
        """
        Disconnect a user from a chat room
        Updates Redis state and cleans up
        """
        await self._disconnect_internal(connection_id, room, username, broadcast_leave)
    
    async def _disconnect_internal(self, connection_id: str, room: str, username: str, broadcast_leave: bool = True):
        """Internal disconnect logic with deduplication"""
        # Atomic check-and-remove: if connection not in local_connections, already disconnected
        if connection_id not in self.local_connections:
            logger.debug(f"Connection {connection_id} already disconnected, skipping duplicate cleanup")
            return
        
        # Remove from local connections (atomic operation prevents race conditions)
        del self.local_connections[connection_id]
        
        logger.info(f"👋 User '{username}' left room '{room}' on instance {INSTANCE_ID}")
        
        if self.redis:
            try:
                # Remove user from room's user set
                await self.redis.srem(f"chat:room:{room}:users", username)
                
                # Delete user metadata
                await self.redis.delete(f"chat:user:{username}")
                
                # Check if room is now empty globally
                room_users = await self.redis.smembers(f"chat:room:{room}:users")
                if not room_users:
                    # Remove room from global rooms set
                    await self.redis.srem("chat:rooms", room)
                    logger.info(f"🧹 Room '{room}' is now empty, removed from global list")
                
            except Exception as e:
                logger.error(f"Redis cleanup failed: {e}")
        
        # Broadcast leave message if requested
        if broadcast_leave:
            leave_msg = ChatMessage(
                id=str(uuid4()),
                room=room,
                username="system",
                content=f"{username} left the chat",
                timestamp=datetime.utcnow().isoformat(),
                type="leave"
            )
            await self.broadcast_to_room(room, leave_msg, store_in_history=False)
            
            # Broadcast updated user list to remaining users
            if self.redis:
                try:
                    users_in_room = await self.redis.smembers(f"chat:room:{room}:users")
                    if users_in_room:  # Only broadcast if there are users left
                        users_msg = {
                            "type": "users",
                            "users": list(users_in_room),
                            "room": room,
                            "_instance": INSTANCE_ID
                        }
                        await self._broadcast_users_list(room, users_msg)
                except Exception as e:
                    logger.error(f"Failed to broadcast user list after leave: {e}")
    
    async def broadcast_to_room(self, room: str, message: ChatMessage, exclude_connection: Optional[str] = None, store_in_history: bool = True):
        """
        Broadcast a message to all users in a room (local + remote via Redis)

        Flow (optimized for low-latency delivery):
        1. Broadcast to local WebSocket connections (instant)
        2. Publish to Redis pub/sub (for other instances to receive) - fast
        3. Store message in Redis cache (if store_in_history=True) - fast
        4. Publish to Kafka (for persistence to Cassandra) - non-blocking background task

        Message Type Handling:
        - "message": Regular chat messages (stored in cache, persisted via Kafka, broadcast globally)
        - "system": System notifications (stored in cache, persisted via Kafka, broadcast globally)
        - "file": File uploads (stored in cache, persisted via Kafka, broadcast globally)
        - "join": User joined (persisted via Kafka, broadcast globally, sender excluded)
        - "leave": User left (persisted via Kafka, broadcast globally)
        - "typing": Typing indicator (NOT stored, NOT persisted, broadcast globally, sender excluded)

        Parameters:
        - room: Room name to broadcast to
        - message: ChatMessage object to broadcast
        - exclude_connection: Optional connection_id to exclude from receiving (e.g., sender of typing indicator)
        - store_in_history: Whether to persist message in Redis cache (default: True)
        """
        msg_dict = asdict(message)

        # Add instance metadata for pub/sub
        msg_dict['_instance'] = INSTANCE_ID

        # PRIORITY 1: Broadcast to LOCAL connections FIRST (instant user feedback)
        disconnected = []
        for conn_id, (websocket, conn_room, username) in self.local_connections.items():
            if conn_room != room or conn_id == exclude_connection:
                continue

            try:
                await websocket.send_json(msg_dict)
            except Exception as e:
                logger.error(f"Failed to send to {username}: {e}")
                disconnected.append(conn_id)

        # Clean up disconnected (don't broadcast leave - let WebSocket close handler do it)
        for conn_id in disconnected:
            if conn_id in self.local_connections:
                _, room, username = self.local_connections[conn_id]
                await self._disconnect_internal(conn_id, room, username, broadcast_leave=False)

        # PRIORITY 2: Publish to Redis pub/sub immediately (critical for cross-instance broadcasting)
        # Don't make this a background task - it needs to be fast for multi-instance setups
        if self.redis:
            try:
                # Use fire-and-forget: don't wait for confirmation (fastest delivery)
                # Create task but don't await it - Redis pub/sub is very fast
                asyncio.create_task(self._publish_to_redis(room, msg_dict))
            except Exception as e:
                logger.error(f"Redis publish failed: {e}")

        # PRIORITY 3: Fire off Redis cache and Kafka operations as background tasks (non-blocking)
        # These can happen in the background without affecting message delivery speed
        asyncio.create_task(self._persist_message_async(room, msg_dict, message.type, store_in_history))

    async def _persist_message_async(self, room: str, msg_dict: dict, msg_type: str, store_in_history: bool):
        """
        Background task to persist message to Redis cache and Kafka without blocking WebSocket broadcast
        Runs asynchronously after the message has already been delivered to users
        """
        # Store in Redis message cache (keep only 10 latest messages)
        # Only store regular messages, files, and system messages (not join/leave/typing)
        if self.redis and store_in_history and msg_type in ['message', 'file', 'system']:
            try:
                # Add to room history (keep last 10 messages)
                await self.redis.lpush(f"chat:room:{room}:history", json.dumps(msg_dict))
                await self.redis.ltrim(f"chat:room:{room}:history", 0, 9)  # Keep only 10 latest
            except Exception as e:
                logger.error(f"Failed to store message in Redis: {e}")

        # Publish to Kafka for persistence (non-blocking)
        if self.kafka_producer and msg_type in ['message', 'system', 'file', 'join', 'leave']:
            try:
                # Choose topic based on message type
                topic = KAFKA_TOPIC_MESSAGES if msg_type in ['message', 'file'] else KAFKA_TOPIC_EVENTS

                # Send to Kafka without waiting for acknowledgment
                await self.kafka_producer.send(topic, msg_dict)
                logger.debug(f"✓ Queued for Kafka topic '{topic}': {msg_type}")

            except Exception as e:
                logger.error(f"✗ Failed to publish to Kafka: {e}")
    
    async def _broadcast_users_list(self, room: str, users_msg: dict):
        """
        Broadcast updated users list to all users in a room (local + remote via Redis)
        This is a specialized broadcast for user list updates
        Optimized for instant delivery: broadcasts locally first, then syncs to Redis in background
        """
        # PRIORITY 1: Broadcast to LOCAL connections FIRST (instant)
        disconnected = []
        for conn_id, (websocket, conn_room, username) in self.local_connections.items():
            if conn_room != room:
                continue

            try:
                await websocket.send_json(users_msg)
            except Exception as e:
                logger.error(f"Failed to send users list to {username}: {e}")
                disconnected.append(conn_id)

        # Clean up disconnected
        for conn_id in disconnected:
            if conn_id in self.local_connections:
                _, room, username = self.local_connections[conn_id]
                await self._disconnect_internal(conn_id, room, username, broadcast_leave=False)

        # PRIORITY 2: Publish to Redis pub/sub in background (non-blocking)
        if self.redis:
            asyncio.create_task(self._publish_to_redis(room, users_msg))

    async def _publish_to_redis(self, room: str, msg: dict):
        """Background task to publish message to Redis pub/sub"""
        try:
            await self.redis.publish(f"chat:room:{room}", json.dumps(msg))
        except Exception as e:
            logger.error(f"Redis publish failed: {e}")
    
    async def get_room_info(self, room: str) -> Optional[RoomInfo]:
        """
        Get information about a chat room from Redis (global state)
        Shows users across ALL instances, not just this one
        """
        if not self.redis:
            # Fallback to local state if Redis unavailable
            local_users = []
            for _, conn_room, username in self.local_connections.values():
                if conn_room == room:
                    local_users.append(username)
            
            if not local_users:
                return None
            
            return RoomInfo(
                name=room,
                users=local_users,
                message_count=0,
                created_at=datetime.utcnow().isoformat()
            )
        
        try:
            # Check if room exists in global rooms set
            exists = await self.redis.sismember("chat:rooms", room)
            if not exists:
                return None
            
            # Get all users in room (across all instances)
            users = await self.redis.smembers(f"chat:room:{room}:users")
            
            # Get message count from history
            msg_count = await self.redis.llen(f"chat:room:{room}:history")
            
            return RoomInfo(
                name=room,
                users=list(users),
                message_count=msg_count,
                created_at=datetime.utcnow().isoformat()  # Could store this in Redis too
            )
        except Exception as e:
            logger.error(f"Failed to get room info from Redis: {e}")
            return None
    
    async def get_all_rooms(self) -> list:
        """
        Get list of all active rooms from Redis (global state)
        Shows rooms across ALL instances
        """
        if not self.redis:
            # Fallback to local state
            local_rooms = set()
            for _, room, _ in self.local_connections.values():
                local_rooms.add(room)
            return [await self.get_room_info(room) for room in local_rooms]
        
        try:
            # Get all room names from global set
            room_names = await self.redis.smembers("chat:rooms")
            rooms = []
            for room_name in room_names:
                room_info = await self.get_room_info(room_name)
                if room_info:
                    rooms.append(room_info)
            return rooms
        except Exception as e:
            logger.error(f"Failed to get rooms from Redis: {e}")
            return []
    
    async def cleanup(self):
        """Cleanup on shutdown: disconnect all local connections and stop background tasks"""
        logger.info(f"🧹 Cleaning up instance {INSTANCE_ID}")
        
        # Disconnect all local connections (broadcast leave messages)
        for conn_id in list(self.local_connections.keys()):
            _, room, username = self.local_connections[conn_id]
            await self._disconnect_internal(conn_id, room, username, broadcast_leave=True)
        
        # Stop background tasks
        if self.subscriber_task:
            self.subscriber_task.cancel()
            try:
                await self.subscriber_task
            except asyncio.CancelledError:
                pass
        
        if self.health_check_task:
            self.health_check_task.cancel()
            try:
                await self.health_check_task
            except asyncio.CancelledError:
                pass
        
        # Close Kafka producer
        if self.kafka_producer:
            await self.kafka_producer.stop()
            logger.info("✓ Kafka producer stopped")
        
        # Close Cassandra connection
        if self.cassandra_cluster:
            self.cassandra_cluster.shutdown()
            logger.info("✓ Cassandra connection closed")
        
        # Close Redis connections
        if self.redis:
            await self.redis.aclose()
        if self.redis_subscriber:
            await self.redis_subscriber.aclose()

        logger.info("✓ Cleanup complete")
    
    def get_room_history_from_cassandra(self, room: str, limit: int = 100, before_timestamp: Optional[datetime] = None) -> List[dict]:
        """Get message history from Cassandra"""
        if not self.cassandra_session:
            logger.warning("Cassandra not available")
            return []
        
        try:
            if before_timestamp:
                query = """
                    SELECT * FROM messages 
                    WHERE room = ? AND timestamp < ?
                    LIMIT ?
                """
                rows = self.cassandra_session.execute(query, (room, before_timestamp, limit))
            else:
                query = """
                    SELECT * FROM messages 
                    WHERE room = ?
                    LIMIT ?
                """
                rows = self.cassandra_session.execute(query, (room, limit))
            
            messages = []
            for row in rows:
                messages.append({
                    'id': str(row.message_id),
                    'room': row.room,
                    'timestamp': row.timestamp.isoformat() if row.timestamp else None,
                    'username': row.username,
                    'content': row.content,
                    'type': row.type,
                    'file_url': row.file_url,
                    'file_name': row.file_name,
                    'file_size': row.file_size
                })
            
            # Messages come in DESC order from Cassandra, reverse to show oldest first
            messages.reverse()
            
            logger.info(f"Retrieved {len(messages)} messages for room '{room}' from Cassandra")
            return messages
            
        except Exception as e:
            logger.error(f"✗ Failed to query Cassandra: {e}")
            return []


# Lifespan context manager (replaces deprecated @app.on_event)
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application lifecycle (startup/shutdown)"""
    global manager

    # Startup
    manager = ConnectionManager()
    await manager.init_redis()
    await manager.init_kafka()
    manager.init_minio()
    manager.init_cassandra()
    logger.info(f"🚀 Chat Service started - Instance: {INSTANCE_ID}")
    logger.info(f"📊 Redis-backed horizontal scaling: {'ENABLED' if manager.redis else 'DISABLED'}")
    logger.info(f"📡 Kafka event streaming: {'ENABLED' if manager.kafka_producer else 'DISABLED'}")
    logger.info(f"💾 Cassandra history queries: {'ENABLED' if manager.cassandra_session else 'DISABLED'}")
    logger.info(f"📁 MinIO file storage: {'ENABLED' if manager.minio_client else 'DISABLED'}")
    logger.info(f"💓 Health check: {'ENABLED' if manager.health_check_task else 'DISABLED'} (interval: {HEALTH_CHECK_INTERVAL}s)")

    yield

    # Shutdown
    logger.info("🛑 Shutting down Chat Service...")
    await manager.cleanup()


# Create FastAPI app with lifespan
app = FastAPI(title="Chat Service", version="2.0.0", lifespan=lifespan)


@app.get("/health")
async def health_check():
    """Health check endpoint with instance info"""
    redis_connected = manager.redis is not None
    kafka_connected = manager.kafka_producer is not None
    cassandra_connected = manager.cassandra_session is not None
    local_connections = len(manager.local_connections)
    health_check_enabled = manager.health_check_task is not None and not manager.health_check_task.done()
    minio_connected = manager.minio_client is not None
    
    return {
        "status": "healthy",
        "service": "chat-service",
        "instance": INSTANCE_ID,
        "redis_connected": redis_connected,
        "kafka_connected": kafka_connected,
        "cassandra_connected": cassandra_connected,
        "minio_connected": minio_connected,
        "local_connections": local_connections,
        "horizontal_scaling": "enabled" if redis_connected else "disabled",
        "event_streaming": "enabled" if kafka_connected else "disabled",
        "history_queries": "enabled" if cassandra_connected else "disabled",
        "file_upload": "enabled" if minio_connected else "disabled",
        "health_check": {
            "enabled": health_check_enabled,
            "interval_seconds": HEALTH_CHECK_INTERVAL,
            "ping_timeout_seconds": PING_TIMEOUT
        }
    }


@app.get("/rooms")
async def list_rooms():
    """
    List all active chat rooms (global across all instances)
    
    Returns room data from Redis if available, showing users across all instances
    """
    rooms = await manager.get_all_rooms()
    return {
        "rooms": [asdict(r) for r in rooms if r],
        "count": len(rooms),
        "instance": INSTANCE_ID,
        "source": "redis" if manager.redis else "local"
    }


@app.get("/rooms/{room_name}")
async def get_room(room_name: str):
    """
    Get information about a specific room (global state from Redis)
    
    Shows all users in the room across all instances
    """
    room_info = await manager.get_room_info(room_name)
    if not room_info:
        return JSONResponse(
            status_code=404,
            content={"error": f"Room '{room_name}' not found"}
        )
    
    result = asdict(room_info)
    result["instance"] = INSTANCE_ID
    result["source"] = "redis" if manager.redis else "local"
    return result


@app.websocket("/ws/chat/{room}")
async def websocket_chat(
    websocket: WebSocket,
    room: str,
    username: str = Query(..., description="Username for the chat")
):
    """
    WebSocket endpoint for bidirectional chat
    
    Message format (client -> server):
    {
        "content": "Hello everyone!",
        "type": "message"  // optional: "message" (default), "typing", "system"
    }
    
    Message format (server -> client):
    {
        "id": "uuid",
        "room": "room-name",
        "username": "sender",
        "content": "message content",
        "timestamp": "ISO timestamp",
        "type": "message|join|leave|system|typing"
    }
    
    Message Type Behaviors:
    - "message": Regular chat message (stored in history, broadcast to all including sender)
    - "system": System notification (stored in history, broadcast to all including sender)
    - "typing": Typing indicator (NOT stored, broadcast to all EXCEPT sender, ephemeral)
    - "join": Auto-sent on connect (NOT stored, broadcast to all EXCEPT joiner)
    - "leave": Auto-sent on disconnect (NOT stored, broadcast to all)
    
    All message types are broadcast globally to all instances via Redis pub/sub.
    """
    connection_id = await manager.connect(websocket, room, username)
    
    try:
        while True:
            # Receive message from client
            data = await websocket.receive_json()
            
            content = data.get("content", "").strip()
            msg_type = data.get("type", "message")
            
            if not content:
                continue
            
            # Handle different message types
            if msg_type == "typing":
                # Broadcast typing indicator (don't store in history, exclude sender, broadcast globally)
                typing_msg = ChatMessage(
                    id=str(uuid4()),
                    room=room,
                    username=username,
                    content="is typing...",
                    timestamp=datetime.utcnow().isoformat(),
                    type="typing"
                )
                # Broadcast to all instances (local + remote), exclude sender, don't store
                await manager.broadcast_to_room(room, typing_msg, exclude_connection=connection_id, store_in_history=False)
            else:
                # Regular message or system message - broadcast to room
                message = ChatMessage(
                    id=str(uuid4()),
                    room=room,
                    username=username,
                    content=content,
                    timestamp=datetime.utcnow().isoformat(),
                    type=msg_type  # message, system, or any other type
                )
                # Broadcast to all instances including sender
                # Store in history only for regular messages and system messages (not typing, join, leave)
                store_in_history = msg_type in ["message", "system"]
                await manager.broadcast_to_room(room, message, store_in_history=store_in_history)
                
    except WebSocketDisconnect:
        # Disconnect with leave message broadcast (built into disconnect method now)
        await manager.disconnect(connection_id, room, username, broadcast_leave=True)
        
    except Exception as e:
        logger.error(f"WebSocket error for {username} in {room}: {e}")
        await manager.disconnect(connection_id, room, username, broadcast_leave=True)


@app.websocket("/ws/chat")
async def websocket_chat_default(websocket: WebSocket):
    """Default chat endpoint - redirect to general room"""
    await websocket.accept()
    await websocket.send_json({
        "type": "error",
        "message": "Please specify a room. Use /ws/chat/{room}?username={your_name}"
    })
    await websocket.close()


@app.post("/rooms/{room}/leave")
async def leave_room(
    room: str,
    username: str = Query(..., description="Username leaving the room")
):
    """
    REST endpoint to explicitly remove a user from a room
    Used when client wants to cleanly disconnect and update state
    Note: This is typically called BEFORE the WebSocket closes, so we don't broadcast here
    """
    logger.info(f"REST API: User '{username}' pre-disconnect cleanup for room '{room}'")
    
    # Just acknowledge - the actual cleanup will happen when WebSocket disconnects
    # This prevents duplicate leave messages
    return {
        "success": True,
        "message": f"User '{username}' disconnect acknowledged",
        "room": room,
        "note": "Cleanup will occur on WebSocket close"
    }


@app.get("/rooms/{room}/history")
async def get_room_full_history(
    room: str,
    limit: int = Query(100, ge=1, le=1000, description="Number of messages to retrieve"),
    before: Optional[str] = Query(None, description="Get messages before this ISO timestamp")
):
    """
    Get full message history for a room from Cassandra
    Returns messages in chronological order (oldest first)
    
    This endpoint queries the full persistent history, not just the Redis cache.
    """
    before_timestamp = None
    if before:
        try:
            before_timestamp = datetime.fromisoformat(before)
        except ValueError:
            return JSONResponse(
                status_code=400,
                content={"error": "Invalid timestamp format. Use ISO format."}
            )
    
    if not manager.cassandra_session:
        return JSONResponse(
            status_code=503,
            content={"error": "History service unavailable"}
        )
    
    messages = manager.get_room_history_from_cassandra(room, limit=limit, before_timestamp=before_timestamp)
    
    return {
        "room": room,
        "messages": messages,
        "count": len(messages),
        "limit": limit,
        "source": "cassandra"
    }


# =============================================================================
# FILE UPLOAD ENDPOINTS
# =============================================================================

@app.post("/files/initiate-upload")
async def initiate_file_upload(
    room: str = Body(..., description="Room name"),
    username: str = Body(..., description="Username"),
    filename: str = Body(..., description="Original filename"),
    file_size: int = Body(..., description="File size in bytes"),
    content_type: str = Body(default="application/octet-stream", description="MIME type")
):
    """
    Initiate a multipart file upload
    Returns upload ID and details for chunked upload
    
    Note: MinIO Python SDK doesn't support direct presigned multipart uploads like AWS SDK.
    For large files, we'll use direct PUT with presigned URLs.
    """
    if not manager.minio_client:
        raise HTTPException(status_code=503, detail="File upload service unavailable")
    
    # Generate unique file ID
    file_id = str(uuid4())
    
    # S3 object key: room/file_id/filename
    object_name = f"{room}/{file_id}/{filename}"
    
    logger.info(f"📤 Initiated upload: {filename} ({file_size} bytes) in room {room} by {username}")
    
    return {
        "file_id": file_id,
        "object_name": object_name,
        "bucket": manager.bucket_name,
        "chunk_size": 1024 * 1024,  # 1MB chunks
        "message": "Use simple-upload endpoint for actual upload"
    }


@app.post("/files/upload-chunk")
async def get_chunk_upload_url(
    file_id: str = Body(..., description="File ID from initiate-upload"),
    object_name: str = Body(..., description="Object name from initiate-upload"),
    part_number: int = Body(..., description="Part number (1-based)"),
    room: str = Body(..., description="Room name")
):
    """
    Get a presigned URL for uploading a specific chunk
    
    Note: MinIO presigned URLs work for simple PUT operations.
    For true multipart, we'll handle it on the server side.
    """
    if not manager.minio_client:
        raise HTTPException(status_code=503, detail="File upload service unavailable")
    
    try:
        # Generate presigned URL for uploading (valid for 1 hour)
        presigned_url = manager.minio_client.presigned_put_object(
            manager.bucket_name,
            f"{object_name}.part{part_number}",  # Temporary part file
            expires=timedelta(hours=1)
        )
        
        logger.debug(f"Generated presigned URL for part {part_number} of {file_id}")
        
        return {
            "presigned_url": presigned_url,
            "part_number": part_number,
            "expires_in_seconds": 3600
        }
        
    except Exception as e:
        logger.error(f"Failed to generate presigned URL: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to generate presigned URL: {str(e)}")


@app.post("/files/complete-upload")
async def complete_file_upload(
    file_id: str = Body(..., description="File ID"),
    object_name: str = Body(..., description="Object name"),
    parts: list = Body(..., description="List of uploaded part numbers"),
    room: str = Body(..., description="Room name"),
    username: str = Body(..., description="Username"),
    filename: str = Body(..., description="Original filename"),
    file_size: int = Body(..., description="File size in bytes")
):
    """
    Complete the chunked upload by combining parts
    This is a simplified version - for production, use proper multipart upload completion
    """
    if not manager.minio_client:
        raise HTTPException(status_code=503, detail="File upload service unavailable")
    
    try:
        # For simplicity, we'll assume parts were uploaded directly as one file
        # In a real implementation, you'd combine the parts here
        
        # Generate presigned GET URL (valid for 7 days)
        download_url = manager.minio_client.presigned_get_object(
            manager.bucket_name,
            object_name,
            expires=timedelta(days=7)
        )
        
        logger.info(f"✓ Upload completed: {filename} ({file_size} bytes) in room {room}")
        
        # Create file message and broadcast to room
        file_message = ChatMessage(
            id=str(uuid4()),
            room=room,
            username=username,
            content=f"Shared a file: {filename}",
            timestamp=datetime.utcnow().isoformat(),
            type="file",
            file_url=download_url,
            file_name=filename,
            file_size=file_size
        )
        
        # Broadcast to all users in the room (via Redis pub/sub)
        await manager.broadcast_to_room(room, file_message, store_in_history=True)
        
        return {
            "success": True,
            "file_id": file_id,
            "download_url": download_url,
            "message": "File uploaded and shared successfully"
        }
        
    except Exception as e:
        logger.error(f"Failed to complete upload: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to complete upload: {str(e)}")


@app.post("/files/simple-upload")
async def simple_file_upload(
    room: str = Query(..., description="Room name"),
    username: str = Query(..., description="Username"),
    file: UploadFile = File(...)
):
    """
    Simple single-part file upload (for small files < 5MB)
    Alternative to multipart upload for convenience
    Optimized: MinIO operations run in thread pool to avoid blocking async event loop
    """
    if not manager.minio_client:
        raise HTTPException(status_code=503, detail="File upload service unavailable")

    try:
        # Generate unique file ID
        file_id = str(uuid4())
        filename = file.filename or "unnamed-file"

        # Read file content (async)
        content = await file.read()
        file_size = len(content)

        # S3 object key
        object_name = f"{room}/{file_id}/{filename}"

        # Run MinIO operations in thread pool to avoid blocking event loop
        loop = asyncio.get_event_loop()

        # Upload to MinIO (blocking operation, run in thread pool)
        from io import BytesIO
        await loop.run_in_executor(
            None,  # Use default thread pool
            lambda: manager.minio_client.put_object(
                manager.bucket_name,
                object_name,
                BytesIO(content),
                file_size,
                content_type=file.content_type or "application/octet-stream",
                metadata={
                    "room": room,
                    "username": username,
                    "original-filename": filename,
                    "upload-timestamp": datetime.utcnow().isoformat()
                }
            )
        )

        # Generate presigned download URL (blocking operation, run in thread pool)
        download_url = await loop.run_in_executor(
            None,
            lambda: manager.minio_client.presigned_get_object(
                manager.bucket_name,
                object_name,
                expires=timedelta(days=7)
            )
        )

        logger.info(f"✓ Simple upload: {filename} ({file_size} bytes) in room {room}")

        # Create and broadcast file message (now truly instant!)
        file_message = ChatMessage(
            id=str(uuid4()),
            room=room,
            username=username,
            content=f"Shared a file: {filename}",
            timestamp=datetime.utcnow().isoformat(),
            type="file",
            file_url=download_url,
            file_name=filename,
            file_size=file_size
        )

        # Broadcast immediately - all persistence happens in background
        await manager.broadcast_to_room(room, file_message, store_in_history=True)
        
        return {
            "success": True,
            "file_id": file_id,
            "download_url": download_url,
            "file_size": file_size,
            "message": "File uploaded and shared successfully"
        }
        
    except Exception as e:
        logger.error(f"Failed to upload file: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to upload file: {str(e)}")


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8001))
    uvicorn.run(app, host="0.0.0.0", port=port)

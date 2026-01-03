"""
Message Persistence Service - Kafka Consumer + Cassandra Writer
Consumes chat messages from Kafka and persists them to Cassandra for full history
Features:
- Kafka consumer for chat.messages and chat.events topics
- Cassandra database for unlimited message history
- FastAPI for health checks and query endpoints
- Automatic schema initialization
"""
import os
import json
import asyncio
import logging
from datetime import datetime
from typing import Optional, List, Dict
from uuid import UUID
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
from aiokafka import AIOKafkaConsumer
from cassandra.cluster import Cluster, Session
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import SimpleStatement, ConsistencyLevel
import socket

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
CASSANDRA_HOSTS = os.getenv("CASSANDRA_HOSTS", "cassandra").split(",")
CASSANDRA_PORT = int(os.getenv("CASSANDRA_PORT", "9042"))
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "chat")
INSTANCE_ID = f"persistence-{socket.gethostname()}"

# Kafka topics
TOPICS = ["chat.messages", "chat.events"]

# =============================================================================
# Cassandra Connection
# =============================================================================

class CassandraClient:
    """Cassandra client for message persistence"""
    
    def __init__(self):
        self.cluster: Optional[Cluster] = None
        self.session: Optional[Session] = None
        self.connected = False
    
    def connect(self):
        """Connect to Cassandra and initialize schema"""
        try:
            logger.info(f"Connecting to Cassandra at {CASSANDRA_HOSTS}:{CASSANDRA_PORT}")
            
            self.cluster = Cluster(
                contact_points=CASSANDRA_HOSTS,
                port=CASSANDRA_PORT,
                protocol_version=4
            )
            
            # First connect without keyspace
            self.session = self.cluster.connect()
            logger.info("✓ Connected to Cassandra")
            
            # Create keyspace if not exists
            self._init_schema()
            
            # Use the keyspace
            self.session.set_keyspace(CASSANDRA_KEYSPACE)
            logger.info(f"✓ Using keyspace: {CASSANDRA_KEYSPACE}")
            
            self.connected = True
            
        except Exception as e:
            logger.error(f"✗ Failed to connect to Cassandra: {e}")
            self.connected = False
            raise
    
    def _init_schema(self):
        """Initialize Cassandra schema (keyspace and tables)"""
        try:
            # Create keyspace
            self.session.execute(f"""
                CREATE KEYSPACE IF NOT EXISTS {CASSANDRA_KEYSPACE}
                WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
            """)
            logger.info(f"✓ Keyspace '{CASSANDRA_KEYSPACE}' ready")
            
            # Use keyspace
            self.session.set_keyspace(CASSANDRA_KEYSPACE)
            
            # Create messages table
            # Partition by room, cluster by timestamp descending (newest first)
            self.session.execute("""
                CREATE TABLE IF NOT EXISTS messages (
                    room text,
                    message_id uuid,
                    timestamp timestamp,
                    username text,
                    content text,
                    type text,
                    file_url text,
                    file_name text,
                    file_size bigint,
                    PRIMARY KEY ((room), timestamp, message_id)
                ) WITH CLUSTERING ORDER BY (timestamp DESC, message_id DESC)
            """)
            logger.info("✓ Table 'messages' ready")
            
        except Exception as e:
            logger.error(f"✗ Schema initialization failed: {e}")
            raise
    
    def insert_message(self, message_data: dict):
        """Insert a message into Cassandra"""
        if not self.session:
            logger.error("Cannot insert: Cassandra not connected")
            return
        
        try:
            # Parse timestamp
            timestamp = datetime.fromisoformat(message_data.get('timestamp', datetime.utcnow().isoformat()))
            
            # Parse UUID
            message_id = UUID(message_data.get('id'))
            
            query = """
                INSERT INTO messages (room, message_id, timestamp, username, content, type, file_url, file_name, file_size)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            
            prepared = self.session.prepare(query)
            
            self.session.execute(prepared, (
                message_data.get('room'),
                message_id,
                timestamp,
                message_data.get('username'),
                message_data.get('content'),
                message_data.get('type', 'message'),
                message_data.get('file_url'),
                message_data.get('file_name'),
                message_data.get('file_size')
            ))
            
            logger.debug(f"✓ Inserted message {message_id} in room {message_data.get('room')}")
            
        except Exception as e:
            logger.error(f"✗ Failed to insert message: {e}")
            logger.error(f"Message data: {message_data}")
    
    def get_room_history(self, room: str, limit: int = 100, before_timestamp: Optional[datetime] = None) -> List[dict]:
        """Get message history for a room"""
        if not self.session:
            logger.error("Cannot query: Cassandra not connected")
            return []
        
        try:
            # Note: LIMIT cannot be a bound parameter in Cassandra CQL, must be literal
            if before_timestamp:
                query = f"""
                    SELECT * FROM messages 
                    WHERE room = %s AND timestamp < %s
                    LIMIT {limit}
                """
                rows = self.session.execute(query, (room, before_timestamp))
            else:
                query = f"""
                    SELECT * FROM messages 
                    WHERE room = %s
                    LIMIT {limit}
                """
                rows = self.session.execute(query, (room,))
            
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
            
            # Messages are already in DESC order from Cassandra
            # Reverse to show oldest first
            messages.reverse()
            
            logger.info(f"Retrieved {len(messages)} messages for room '{room}'")
            return messages
            
        except Exception as e:
            logger.error(f"✗ Failed to query messages: {e}")
            return []
    
    def get_room_count(self, room: str) -> int:
        """Get total message count for a room"""
        if not self.session:
            return 0
        
        try:
            query = "SELECT COUNT(*) FROM messages WHERE room = %s"
            row = self.session.execute(query, (room,)).one()
            return row.count if row else 0
        except Exception as e:
            logger.error(f"✗ Failed to count messages: {e}")
            return 0
    
    def close(self):
        """Close Cassandra connection"""
        if self.cluster:
            self.cluster.shutdown()
            logger.info("✓ Cassandra connection closed")


# Global Cassandra client
cassandra_client = CassandraClient()


# =============================================================================
# Kafka Consumer
# =============================================================================

class KafkaMessageConsumer:
    """Kafka consumer for chat messages"""
    
    def __init__(self, cassandra_client: CassandraClient):
        self.cassandra_client = cassandra_client
        self.consumer: Optional[AIOKafkaConsumer] = None
        self.running = False
        self.consumer_task: Optional[asyncio.Task] = None
    
    async def start(self):
        """Start Kafka consumer"""
        try:
            logger.info(f"Starting Kafka consumer: {KAFKA_BOOTSTRAP_SERVERS}")
            
            self.consumer = AIOKafkaConsumer(
                *TOPICS,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                group_id="message-persistence-service",
                auto_offset_reset="earliest",  # Start from beginning for new consumer
                enable_auto_commit=True,
                value_deserializer=lambda m: json.loads(m.decode('utf-8'))
            )
            
            await self.consumer.start()
            logger.info(f"✓ Kafka consumer started for topics: {TOPICS}")
            
            self.running = True
            self.consumer_task = asyncio.create_task(self._consume_loop())
            
        except Exception as e:
            logger.error(f"✗ Failed to start Kafka consumer: {e}")
            raise
    
    async def _consume_loop(self):
        """Main consumer loop"""
        logger.info("📨 Kafka consumer loop started")
        
        try:
            async for message in self.consumer:
                if not self.running:
                    break
                
                try:
                    data = message.value
                    topic = message.topic
                    
                    logger.debug(f"Received message from {topic}: {data.get('type')} in room {data.get('room')}")
                    
                    # Filter out messages we don't want to persist
                    msg_type = data.get('type', 'message')
                    
                    # Only persist: message, system, file, join, leave
                    # Skip: typing, users, history, error
                    if msg_type in ['message', 'system', 'file', 'join', 'leave']:
                        # Write to Cassandra
                        self.cassandra_client.insert_message(data)
                        logger.info(f"✓ Persisted {msg_type} from {data.get('username')} in {data.get('room')}")
                    else:
                        logger.debug(f"Skipping message type: {msg_type}")
                    
                except Exception as e:
                    logger.error(f"✗ Error processing message: {e}")
                    continue
                    
        except asyncio.CancelledError:
            logger.info("🛑 Kafka consumer loop cancelled")
            raise
        except Exception as e:
            logger.error(f"❌ Kafka consumer loop error: {e}")
    
    async def stop(self):
        """Stop Kafka consumer"""
        logger.info("Stopping Kafka consumer...")
        self.running = False
        
        if self.consumer_task:
            self.consumer_task.cancel()
            try:
                await self.consumer_task
            except asyncio.CancelledError:
                pass
        
        if self.consumer:
            await self.consumer.stop()
            logger.info("✓ Kafka consumer stopped")


# Global Kafka consumer
kafka_consumer: Optional[KafkaMessageConsumer] = None


# =============================================================================
# FastAPI App
# =============================================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown"""
    global kafka_consumer
    
    # Connect to Cassandra
    cassandra_client.connect()
    
    # Start Kafka consumer
    kafka_consumer = KafkaMessageConsumer(cassandra_client)
    await kafka_consumer.start()
    
    logger.info(f"🚀 Message Persistence Service started - Instance: {INSTANCE_ID}")
    yield
    
    # Shutdown
    if kafka_consumer:
        await kafka_consumer.stop()
    cassandra_client.close()
    logger.info("🛑 Message Persistence Service stopped")


app = FastAPI(
    title="Message Persistence Service",
    version="1.0.0",
    description="Kafka consumer that persists chat messages to Cassandra",
    lifespan=lifespan
)


# =============================================================================
# API Endpoints
# =============================================================================

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "service": "message-persistence-service",
        "instance": INSTANCE_ID,
        "cassandra_connected": cassandra_client.connected,
        "kafka_consumer_running": kafka_consumer.running if kafka_consumer else False,
        "topics": TOPICS
    }


class MessageResponse(BaseModel):
    id: str
    room: str
    timestamp: str
    username: str
    content: str
    type: str
    file_url: Optional[str] = None
    file_name: Optional[str] = None
    file_size: Optional[int] = None


@app.get("/api/rooms/{room}/history", response_model=List[MessageResponse])
async def get_room_history(
    room: str,
    limit: int = Query(100, ge=1, le=1000, description="Number of messages to retrieve"),
    before: Optional[str] = Query(None, description="Get messages before this ISO timestamp")
):
    """
    Get message history for a room from Cassandra
    Returns messages in chronological order (oldest first)
    """
    before_timestamp = None
    if before:
        try:
            before_timestamp = datetime.fromisoformat(before)
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid timestamp format. Use ISO format.")
    
    messages = cassandra_client.get_room_history(room, limit=limit, before_timestamp=before_timestamp)
    
    return messages


@app.get("/api/rooms/{room}/count")
async def get_room_message_count(room: str):
    """Get total message count for a room"""
    count = cassandra_client.get_room_count(room)
    return {
        "room": room,
        "message_count": count
    }


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8004))
    uvicorn.run(app, host="0.0.0.0", port=port)

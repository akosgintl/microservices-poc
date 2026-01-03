"""
Notification Service - Kafka Consumer + SSE Broadcaster
Demonstrates: Event-driven architecture, Server-Sent Events, message processing
"""
import os
import json
import asyncio
import logging
from datetime import datetime
from typing import Dict, Set, Optional, AsyncGenerator
from dataclasses import dataclass, asdict
from uuid import uuid4
from contextlib import asynccontextmanager

from fastapi import FastAPI, Query
from fastapi.responses import StreamingResponse
from aiokafka import AIOKafkaConsumer
import redis.asyncio as redis

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@dataclass
class Notification:
    id: str
    type: str
    title: str
    message: str
    data: dict
    timestamp: str
    user_id: Optional[str] = None


class NotificationStore:
    """In-memory notification storage"""
    
    def __init__(self, max_size: int = 1000):
        self.notifications: list = []
        self.max_size = max_size
    
    def add(self, notification: Notification):
        self.notifications.append(asdict(notification))
        if len(self.notifications) > self.max_size:
            self.notifications = self.notifications[-self.max_size:]
    
    def get_recent(self, count: int = 50, user_id: Optional[str] = None) -> list:
        notifications = self.notifications
        if user_id:
            notifications = [n for n in notifications if n.get("user_id") == user_id or n.get("user_id") is None]
        return notifications[-count:]


class SSEManager:
    """Manages Server-Sent Events connections"""
    
    def __init__(self):
        self.connections: Dict[str, Set[asyncio.Queue]] = {}  # channel -> queues
        self.user_connections: Dict[str, Set[asyncio.Queue]] = {}  # user_id -> queues
    
    def subscribe(self, channel: str = "notifications", user_id: Optional[str] = None) -> asyncio.Queue:
        """Subscribe to a channel and optionally filter by user"""
        queue = asyncio.Queue()
        
        if channel not in self.connections:
            self.connections[channel] = set()
        self.connections[channel].add(queue)
        
        if user_id:
            if user_id not in self.user_connections:
                self.user_connections[user_id] = set()
            self.user_connections[user_id].add(queue)
        
        logger.info(f"New SSE subscriber for channel '{channel}' (user: {user_id})")
        return queue
    
    def unsubscribe(self, queue: asyncio.Queue, channel: str = "notifications", user_id: Optional[str] = None):
        """Unsubscribe from a channel"""
        if channel in self.connections:
            self.connections[channel].discard(queue)
        
        if user_id and user_id in self.user_connections:
            self.user_connections[user_id].discard(queue)
        
        logger.info(f"SSE subscriber disconnected from channel '{channel}'")
    
    async def broadcast(self, notification: Notification, channel: str = "notifications"):
        """Broadcast notification to all subscribers"""
        # Broadcast to channel subscribers
        if channel in self.connections:
            notification_dict = asdict(notification)
            for queue in list(self.connections[channel]):
                try:
                    await queue.put(notification_dict)
                except Exception as e:
                    logger.error(f"Failed to send to queue: {e}")
        
        # Also send to user-specific subscribers
        if notification.user_id and notification.user_id in self.user_connections:
            notification_dict = asdict(notification)
            for queue in list(self.user_connections[notification.user_id]):
                try:
                    await queue.put(notification_dict)
                except Exception as e:
                    logger.error(f"Failed to send to user queue: {e}")


class KafkaConsumerService:
    """Kafka consumer for processing order events"""
    
    def __init__(self, sse_manager: SSEManager, store: NotificationStore):
        self.consumer: Optional[AIOKafkaConsumer] = None
        self.bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        self.sse_manager = sse_manager
        self.store = store
        self._running = False
        self._task: Optional[asyncio.Task] = None
    
    async def start(self):
        """Start consuming from Kafka"""
        topics = ["orders.created", "orders.updated"]
        
        try:
            self.consumer = AIOKafkaConsumer(
                *topics,
                bootstrap_servers=self.bootstrap_servers,
                group_id="notification-service",
                auto_offset_reset="latest",
                value_deserializer=lambda v: json.loads(v.decode('utf-8'))
            )
            await self.consumer.start()
            logger.info(f"Kafka consumer connected, subscribed to: {topics}")
            
            self._running = True
            self._task = asyncio.create_task(self._consume_loop())
            
        except Exception as e:
            logger.error(f"Failed to start Kafka consumer: {e}")
            self.consumer = None
    
    async def stop(self):
        """Stop the Kafka consumer"""
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        
        if self.consumer:
            await self.consumer.stop()
            logger.info("Kafka consumer stopped")
    
    async def _consume_loop(self):
        """Main consumption loop"""
        while self._running and self.consumer:
            try:
                async for message in self.consumer:
                    await self._process_message(message)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in consume loop: {e}")
                await asyncio.sleep(1)
    
    async def _process_message(self, message):
        """Process a Kafka message and create notification"""
        try:
            event = message.value
            event_type = event.get("event_type", "unknown")
            
            logger.info(f"Received event: {event_type} from topic {message.topic}")
            
            # Transform event to notification
            notification = self._create_notification(event)
            
            # Store notification
            self.store.add(notification)
            
            # Broadcast via SSE
            await self.sse_manager.broadcast(notification)
            
        except Exception as e:
            logger.error(f"Error processing message: {e}")
    
    def _create_notification(self, event: dict) -> Notification:
        """Transform an order event into a notification"""
        event_type = event.get("event_type", "unknown")
        order_id = event.get("order_id", "unknown")
        user_id = event.get("user_id")
        status = event.get("status", "unknown")
        total = event.get("total", 0)
        
        # Create appropriate notification based on event type
        if event_type == "order.created":
            title = "New Order Created"
            message = f"Order #{order_id[:8]} has been placed. Total: ${total:.2f}"
        elif event_type == "order.updated":
            title = "Order Status Updated"
            data = event.get("data", {})
            old_status = data.get("old_status", "unknown")
            new_status = data.get("new_status", status)
            message = f"Order #{order_id[:8]} status changed: {old_status} → {new_status}"
        elif event_type == "order.cancelled":
            title = "Order Cancelled"
            message = f"Order #{order_id[:8]} has been cancelled"
        else:
            title = "Order Event"
            message = f"Order #{order_id[:8]} - {event_type}"
        
        return Notification(
            id=str(uuid4()),
            type=event_type,
            title=title,
            message=message,
            data=event,
            timestamp=datetime.utcnow().isoformat(),
            user_id=user_id
        )


# Global instances
sse_manager = SSEManager()
notification_store = NotificationStore()
kafka_consumer = KafkaConsumerService(sse_manager, notification_store)
redis_client: Optional[redis.Redis] = None
redis_task: Optional[asyncio.Task] = None


async def redis_subscriber():
    """Subscribe to Redis for real-time notifications from Order Service"""
    global redis_client
    
    if not redis_client:
        logger.warning("Redis client not initialized, subscriber cannot start")
        return
    
    retry_delay = 5
    max_retry_delay = 60
    
    while True:
        pubsub = None
        try:
            pubsub = redis_client.pubsub()
            await pubsub.subscribe("notifications")
            
            logger.info("Redis subscriber started and listening on 'notifications' channel")
            
            async for message in pubsub.listen():
                if message["type"] == "message":
                    try:
                        event = json.loads(message["data"])
                        notification = kafka_consumer._create_notification(event)
                        notification_store.add(notification)
                        await sse_manager.broadcast(notification)
                        logger.info(f"Processed Redis notification: {notification.type}")
                    except Exception as e:
                        logger.error(f"Error processing Redis message: {e}")
                        
        except asyncio.CancelledError:
            logger.info("Redis subscriber cancelled")
            if pubsub:
                try:
                    await pubsub.unsubscribe("notifications")
                    await pubsub.close()
                except Exception:
                    pass
            break
        except Exception as e:
            logger.error(f"Redis subscriber error: {e}. Retrying in {retry_delay}s...")
            if pubsub:
                try:
                    await pubsub.close()
                except Exception:
                    pass
            
            await asyncio.sleep(retry_delay)
            retry_delay = min(retry_delay * 2, max_retry_delay)  # Exponential backoff


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown"""
    global redis_client, redis_task
    
    # Start Kafka consumer
    await kafka_consumer.start()
    
    # Initialize Redis client
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379")
    try:
        redis_client = redis.from_url(redis_url, decode_responses=True)
        await redis_client.ping()
        logger.info("Connected to Redis")
        
        # Start Redis subscriber task
        redis_task = asyncio.create_task(redis_subscriber())
    except Exception as e:
        logger.warning(f"Redis not available: {e}. Will operate without Redis pub/sub.")
        redis_client = None
        redis_task = None
    
    logger.info("Notification Service started")
    yield
    
    # Shutdown
    if redis_task:
        redis_task.cancel()
        try:
            await redis_task
        except asyncio.CancelledError:
            pass
    
    await kafka_consumer.stop()
    
    if redis_client:
        try:
            await redis_client.close()
            logger.info("Redis connection closed")
        except Exception as e:
            logger.error(f"Error closing Redis: {e}")


app = FastAPI(
    title="Notification Service",
    version="1.0.0",
    lifespan=lifespan
)


@app.get("/health")
async def health_check():
    kafka_status = "connected" if kafka_consumer.consumer else "disconnected"
    
    # Check Redis status
    redis_status = "disconnected"
    if redis_client:
        try:
            await redis_client.ping()
            redis_status = "connected"
        except Exception:
            redis_status = "error"
    
    return {
        "status": "healthy",
        "service": "notification-service",
        "kafka": kafka_status,
        "redis": redis_status,
        "active_sse_connections": sum(len(q) for q in sse_manager.connections.values())
    }


@app.get("/notifications")
async def get_notifications(
    count: int = Query(50, ge=1, le=200),
    user_id: Optional[str] = Query(None)
):
    """Get recent notifications"""
    notifications = notification_store.get_recent(count=count, user_id=user_id)
    return {"notifications": notifications, "count": len(notifications)}


@app.get("/events/notifications")
async def sse_notifications(
    user_id: Optional[str] = Query(None, description="Filter notifications for specific user")
):
    """
    Server-Sent Events endpoint for real-time notifications
    
    Event format:
    data: {"id": "...", "type": "order.created", "title": "...", "message": "...", ...}
    
    """
    async def event_generator() -> AsyncGenerator[str, None]:
        queue = sse_manager.subscribe(channel="notifications", user_id=user_id)
        
        try:
            # Send initial connection event
            yield f"event: connected\ndata: {json.dumps({'message': 'Connected to notification stream'})}\n\n"
            
            # Send recent notifications
            recent = notification_store.get_recent(count=10, user_id=user_id)
            for notification in recent:
                yield f"event: notification\ndata: {json.dumps(notification)}\n\n"
            
            # Stream new notifications
            while True:
                try:
                    notification = await asyncio.wait_for(queue.get(), timeout=30)
                    yield f"event: notification\ndata: {json.dumps(notification)}\n\n"
                except asyncio.TimeoutError:
                    # Send heartbeat to keep connection alive
                    yield f"event: heartbeat\ndata: {json.dumps({'timestamp': datetime.utcnow().isoformat()})}\n\n"
                    
        except asyncio.CancelledError:
            logger.info("SSE connection cancelled")
        finally:
            sse_manager.unsubscribe(queue, channel="notifications", user_id=user_id)
    
    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        }
    )


@app.get("/events/orders")
async def sse_order_events():
    """
    SSE endpoint specifically for order-related events
    """
    async def event_generator() -> AsyncGenerator[str, None]:
        queue = sse_manager.subscribe(channel="notifications")
        
        try:
            yield f"event: connected\ndata: {json.dumps({'message': 'Connected to order events stream'})}\n\n"
            
            while True:
                try:
                    notification = await asyncio.wait_for(queue.get(), timeout=30)
                    # Only send order-related events
                    if notification.get("type", "").startswith("order."):
                        yield f"event: {notification['type']}\ndata: {json.dumps(notification)}\n\n"
                except asyncio.TimeoutError:
                    yield f"event: heartbeat\ndata: {json.dumps({'timestamp': datetime.utcnow().isoformat()})}\n\n"
                    
        except asyncio.CancelledError:
            pass
        finally:
            sse_manager.unsubscribe(queue, channel="notifications")
    
    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        }
    )


@app.post("/notifications/test")
async def send_test_notification(
    title: str = Query("Test Notification"),
    message: str = Query("This is a test notification"),
    user_id: Optional[str] = Query(None)
):
    """Send a test notification (for debugging)"""
    notification = Notification(
        id=str(uuid4()),
        type="test",
        title=title,
        message=message,
        data={"test": True},
        timestamp=datetime.utcnow().isoformat(),
        user_id=user_id
    )
    
    notification_store.add(notification)
    await sse_manager.broadcast(notification)
    
    return {"status": "sent", "notification": asdict(notification)}


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8003))
    uvicorn.run(app, host="0.0.0.0", port=port)

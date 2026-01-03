"""
Order Service - REST API + Kafka Producer
Demonstrates: RESTful API design, event-driven architecture with Kafka
"""
import os
import json
import asyncio
import logging
from datetime import datetime
from typing import Dict, List, Optional
from uuid import uuid4
from enum import Enum
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, status, Query, BackgroundTasks
from pydantic import BaseModel, Field
from aiokafka import AIOKafkaProducer
import redis.asyncio as redis

# Configure consistent logging format
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(name)-20s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


# =============================================================================
# Models
# =============================================================================

class OrderStatus(str, Enum):
    PENDING = "pending"
    CONFIRMED = "confirmed"
    PROCESSING = "processing"
    SHIPPED = "shipped"
    DELIVERED = "delivered"
    CANCELLED = "cancelled"


class OrderItem(BaseModel):
    product_id: str = Field(default_factory=lambda: str(uuid4()))
    product: str
    quantity: int = Field(ge=1)
    price: float = Field(default=0.0, ge=0)


class CreateOrderRequest(BaseModel):
    user_id: str
    items: List[OrderItem]
    shipping_address: Optional[str] = None
    notes: Optional[str] = None


class UpdateOrderStatusRequest(BaseModel):
    status: OrderStatus
    notes: Optional[str] = None


class Order(BaseModel):
    id: str
    user_id: str
    items: List[OrderItem]
    status: OrderStatus
    total: float
    shipping_address: Optional[str]
    notes: Optional[str]
    created_at: str
    updated_at: str


class OrderEvent(BaseModel):
    event_type: str  # order.created, order.updated, order.cancelled
    order_id: str
    user_id: str
    status: str
    total: float
    timestamp: str
    data: dict


# =============================================================================
# Database & Kafka
# =============================================================================

class OrderDatabase:
    """In-memory order database"""
    
    def __init__(self):
        self.orders: Dict[str, dict] = {}
        self._seed_data()
    
    def _seed_data(self):
        """Seed with sample orders"""
        sample_orders = [
            {
                "user_id": "user-1",
                "items": [
                    {"product_id": "prod-1", "product": "Laptop", "quantity": 1, "price": 999.99},
                    {"product_id": "prod-2", "product": "Mouse", "quantity": 2, "price": 29.99}
                ],
                "status": OrderStatus.DELIVERED,
                "shipping_address": "123 Main St, City, Country"
            },
            {
                "user_id": "user-2",
                "items": [
                    {"product_id": "prod-3", "product": "Keyboard", "quantity": 1, "price": 149.99}
                ],
                "status": OrderStatus.PROCESSING,
                "shipping_address": "456 Oak Ave, Town, Country"
            }
        ]
        for order_data in sample_orders:
            self.create(order_data)
        logger.info(f"Seeded {len(sample_orders)} sample orders")
    
    def create(self, data: dict) -> dict:
        order_id = str(uuid4())
        now = datetime.utcnow().isoformat()
        
        items = data.get("items", [])
        total = sum(item.get("price", 0) * item.get("quantity", 1) for item in items)
        
        order = {
            "id": order_id,
            "user_id": data["user_id"],
            "items": items,
            "status": data.get("status", OrderStatus.PENDING),
            "total": total,
            "shipping_address": data.get("shipping_address"),
            "notes": data.get("notes"),
            "created_at": now,
            "updated_at": now,
        }
        self.orders[order_id] = order
        return order
    
    def get(self, order_id: str) -> Optional[dict]:
        return self.orders.get(order_id)
    
    def update(self, order_id: str, data: dict) -> Optional[dict]:
        if order_id not in self.orders:
            return None
        order = self.orders[order_id]
        for key, value in data.items():
            if value is not None:
                order[key] = value
        order["updated_at"] = datetime.utcnow().isoformat()
        return order
    
    def list_all(self, user_id: Optional[str] = None, status: Optional[OrderStatus] = None) -> List[dict]:
        orders = list(self.orders.values())
        if user_id:
            orders = [o for o in orders if o["user_id"] == user_id]
        if status:
            orders = [o for o in orders if o["status"] == status]
        return sorted(orders, key=lambda x: x["created_at"], reverse=True)


class KafkaProducerService:
    """Kafka producer for publishing order events"""
    
    def __init__(self):
        self.producer: Optional[AIOKafkaProducer] = None
        self.bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    
    async def start(self):
        """Start the Kafka producer"""
        try:
            self.producer = AIOKafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
            )
            await self.producer.start()
            logger.info(f"Kafka producer connected to {self.bootstrap_servers}")
        except Exception as e:
            logger.error(f"Failed to connect to Kafka: {e}")
            self.producer = None
    
    async def stop(self):
        """Stop the Kafka producer"""
        if self.producer:
            await self.producer.stop()
            logger.info("Kafka producer stopped")
    
    async def publish_event(self, topic: str, event: OrderEvent):
        """Publish an event to Kafka topic"""
        if not self.producer:
            logger.warning(f"Kafka not available. Event not published: {event.event_type}")
            return False
        
        try:
            await self.producer.send_and_wait(
                topic=topic,
                key=event.order_id,
                value=event.model_dump()
            )
            logger.info(f"Published event to {topic}: {event.event_type} for order {event.order_id}")
            return True
        except Exception as e:
            logger.error(f"Failed to publish event: {e}")
            return False


# Global instances
db = OrderDatabase()
kafka_producer = KafkaProducerService()
redis_client: Optional[redis.Redis] = None


async def publish_to_redis(channel: str, message: str):
    """Helper function to publish to Redis"""
    if redis_client:
        try:
            await redis_client.publish(channel, message)
            logger.info(f"Published to Redis channel '{channel}'")
        except Exception as e:
            logger.error(f"Failed to publish to Redis: {e}")


# =============================================================================
# FastAPI App
# =============================================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown events"""
    global redis_client
    
    # Startup
    await kafka_producer.start()
    
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379")
    try:
        redis_client = redis.from_url(redis_url, decode_responses=True)
        await redis_client.ping()
        logger.info("Connected to Redis")
    except Exception as e:
        logger.warning(f"Redis not available: {e}")
        redis_client = None
    
    logger.info("Order Service started")
    yield
    
    # Shutdown
    await kafka_producer.stop()
    if redis_client:
        await redis_client.aclose()


app = FastAPI(
    title="Order Service",
    version="1.0.0",
    lifespan=lifespan
)


# =============================================================================
# Endpoints
# =============================================================================

@app.get("/health")
async def health_check():
    kafka_status = "connected" if kafka_producer.producer else "disconnected"
    
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
        "service": "order-service",
        "kafka": kafka_status,
        "redis": redis_status
    }


@app.post("/orders", response_model=Order, status_code=status.HTTP_201_CREATED)
async def create_order(request: CreateOrderRequest, background_tasks: BackgroundTasks):
    """
    Create a new order
    
    This will:
    1. Save the order to database
    2. Publish 'orders.created' event to Kafka
    3. Notify via Redis pub/sub for SSE
    """
    order_data = {
        "user_id": request.user_id,
        "items": [item.model_dump() for item in request.items],
        "shipping_address": request.shipping_address,
        "notes": request.notes,
    }
    
    order = db.create(order_data)
    logger.info(f"Created order {order['id']} for user {order['user_id']}")
    
    # Publish event to Kafka (async in background)
    event = OrderEvent(
        event_type="order.created",
        order_id=order["id"],
        user_id=order["user_id"],
        status=order["status"],
        total=order["total"],
        timestamp=datetime.utcnow().isoformat(),
        data=order
    )
    background_tasks.add_task(kafka_producer.publish_event, "orders.created", event)
    
    # Publish to Redis for immediate SSE notification
    background_tasks.add_task(
        publish_to_redis,
        "notifications",
        json.dumps(event.model_dump())
    )
    
    return Order(**order)


@app.get("/orders", response_model=List[Order])
async def list_orders(
    user_id: Optional[str] = Query(None, description="Filter by user ID"),
    status: Optional[OrderStatus] = Query(None, description="Filter by status"),
    skip: int = Query(0, ge=0),
    limit: int = Query(10, ge=1, le=100)
):
    """List orders with optional filtering"""
    orders = db.list_all(user_id=user_id, status=status)
    return [Order(**o) for o in orders[skip:skip + limit]]


@app.get("/orders/{order_id}", response_model=Order)
async def get_order(order_id: str):
    """Get a specific order by ID"""
    order = db.get(order_id)
    if not order:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Order '{order_id}' not found"
        )
    return Order(**order)


@app.put("/orders/{order_id}/status", response_model=Order)
async def update_order_status(
    order_id: str,
    request: UpdateOrderStatusRequest,
    background_tasks: BackgroundTasks
):
    """
    Update order status
    
    This will publish 'orders.updated' event to Kafka
    """
    order = db.get(order_id)
    if not order:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Order '{order_id}' not found"
        )
    
    old_status = order["status"]
    update_data = {"status": request.status}
    if request.notes:
        update_data["notes"] = request.notes
    
    updated_order = db.update(order_id, update_data)
    logger.info(f"Updated order {order_id} status: {old_status} -> {request.status}")
    
    # Publish event to Kafka
    event = OrderEvent(
        event_type="order.updated",
        order_id=order_id,
        user_id=updated_order["user_id"],
        status=updated_order["status"],
        total=updated_order["total"],
        timestamp=datetime.utcnow().isoformat(),
        data={
            "old_status": old_status,
            "new_status": request.status,
            "notes": request.notes
        }
    )
    background_tasks.add_task(kafka_producer.publish_event, "orders.updated", event)
    
    # Publish to Redis for SSE
    background_tasks.add_task(
        publish_to_redis,
        "notifications",
        json.dumps(event.model_dump())
    )
    
    return Order(**updated_order)


@app.delete("/orders/{order_id}", status_code=status.HTTP_204_NO_CONTENT)
async def cancel_order(order_id: str, background_tasks: BackgroundTasks):
    """Cancel an order (sets status to cancelled)"""
    order = db.get(order_id)
    if not order:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Order '{order_id}' not found"
        )
    
    if order["status"] in [OrderStatus.SHIPPED, OrderStatus.DELIVERED]:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Cannot cancel order with status '{order['status']}'"
        )
    
    updated_order = db.update(order_id, {"status": OrderStatus.CANCELLED})
    
    # Publish cancellation event
    event = OrderEvent(
        event_type="order.cancelled",
        order_id=order_id,
        user_id=updated_order["user_id"],
        status=OrderStatus.CANCELLED,
        total=updated_order["total"],
        timestamp=datetime.utcnow().isoformat(),
        data=updated_order
    )
    background_tasks.add_task(kafka_producer.publish_event, "orders.updated", event)
    
    background_tasks.add_task(
        publish_to_redis,
        "notifications",
        json.dumps(event.model_dump())
    )


@app.get("/orders/stats/summary")
async def get_order_stats():
    """Get order statistics"""
    orders = db.list_all()
    
    status_counts = {}
    total_revenue = 0
    
    for order in orders:
        status = order["status"]
        status_counts[status] = status_counts.get(status, 0) + 1
        if status in [OrderStatus.DELIVERED, OrderStatus.SHIPPED]:
            total_revenue += order["total"]
    
    return {
        "total_orders": len(orders),
        "status_breakdown": status_counts,
        "total_revenue": round(total_revenue, 2)
    }


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8002))
    uvicorn.run(app, host="0.0.0.0", port=port)

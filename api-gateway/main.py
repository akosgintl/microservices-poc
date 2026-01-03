"""
API Gateway - Main Entry Point
Demonstrates: REST API, WebSocket proxy, SSE proxy, gRPC client integration
"""
import os
import json
import asyncio
import logging
from datetime import datetime
from typing import Optional, List
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect, Query, status, UploadFile, File
from fastapi.responses import StreamingResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field, EmailStr
import httpx
import grpc
from grpc import aio as grpc_aio
import redis.asyncio as redis

# gRPC generated code (we'll generate this at runtime for flexibility)
import sys
sys.path.insert(0, '/app/generated')

# Configure consistent logging format
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(name)-20s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


# =============================================================================
# Configuration
# =============================================================================

USER_SERVICE_GRPC = os.getenv("USER_SERVICE_GRPC", "user-service:50051")
ORDER_SERVICE_URL = os.getenv("ORDER_SERVICE_URL", "http://order-service:8002")
CHAT_SERVICE_WS = os.getenv("CHAT_SERVICE_WS", "ws://chat-service:8001")
NOTIFICATION_SERVICE_URL = os.getenv("NOTIFICATION_SERVICE_URL", "http://notification-service:8003")
MESSAGE_PERSISTENCE_SERVICE_URL = os.getenv("MESSAGE_PERSISTENCE_SERVICE_URL", "http://message-persistence-service:8004")
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379")


# =============================================================================
# Models
# =============================================================================

class CreateUserRequest(BaseModel):
    name: str = Field(..., min_length=2, max_length=100)
    email: EmailStr
    password: str = Field(default="password123", min_length=6)
    role: str = Field(default="user")


class UserResponse(BaseModel):
    id: str
    name: str
    email: str
    role: str
    is_active: bool
    created_at: str
    updated_at: str


class OrderItemRequest(BaseModel):
    product: str
    quantity: int = Field(ge=1)
    price: float = Field(default=0.0, ge=0)


class CreateOrderRequest(BaseModel):
    user_id: str
    items: List[OrderItemRequest]
    shipping_address: Optional[str] = None


class UpdateOrderStatusRequest(BaseModel):
    status: str
    notes: Optional[str] = None


# =============================================================================
# gRPC Client for User Service
# =============================================================================

class UserServiceClient:
    """gRPC client for User Service"""
    
    def __init__(self, address: str):
        self.address = address
        self.channel: Optional[grpc_aio.Channel] = None
        self.stub = None
    
    async def connect(self):
        """Establish gRPC connection"""
        try:
            self.channel = grpc_aio.insecure_channel(self.address)
            
            # Import generated code
            try:
                import user_pb2
                import user_pb2_grpc
                self.stub = user_pb2_grpc.UserServiceStub(self.channel)
                self.pb2 = user_pb2
                logger.info(f"gRPC client connected to {self.address}")
            except ImportError:
                logger.warning("gRPC generated code not found, using fallback")
                self.stub = None
                
        except Exception as e:
            logger.error(f"Failed to connect to gRPC: {e}")
            self.stub = None
    
    async def close(self):
        """Close gRPC connection"""
        if self.channel:
            await self.channel.close()
    
    async def get_user(self, user_id: str) -> Optional[dict]:
        """Get user by ID via gRPC"""
        if not self.stub:
            return None
        
        try:
            request = self.pb2.GetUserRequest(id=user_id)
            response = await self.stub.GetUser(request)
            return self._user_to_dict(response)
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.NOT_FOUND:
                return None
            raise
    
    async def create_user(self, name: str, email: str, password: str, role: str = "user") -> dict:
        """Create user via gRPC"""
        if not self.stub:
            raise HTTPException(status_code=503, detail="User service unavailable")
        
        try:
            request = self.pb2.CreateUserRequest(
                name=name,
                email=email,
                password=password,
                role=role
            )
            response = await self.stub.CreateUser(request)
            return self._user_to_dict(response)
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.ALREADY_EXISTS:
                raise HTTPException(status_code=409, detail=str(e.details()))
            raise
    
    async def list_users(self, page: int = 1, page_size: int = 10) -> dict:
        """List users via gRPC"""
        if not self.stub:
            return {"users": [], "total": 0, "page": page, "pages": 0}
        
        try:
            request = self.pb2.ListUsersRequest(page=page, page_size=page_size)
            response = await self.stub.ListUsers(request)
            return {
                "users": [self._user_to_dict(u) for u in response.users],
                "total": response.total,
                "page": response.page,
                "pages": response.pages
            }
        except grpc.RpcError as e:
            logger.error(f"gRPC ListUsers error: {e}")
            return {"users": [], "total": 0, "page": page, "pages": 0}
    
    async def delete_user(self, user_id: str) -> bool:
        """Delete user via gRPC"""
        if not self.stub:
            return False
        
        try:
            request = self.pb2.DeleteUserRequest(id=user_id)
            response = await self.stub.DeleteUser(request)
            return response.success
        except grpc.RpcError:
            return False
    
    def _user_to_dict(self, user) -> dict:
        """Convert protobuf User to dict"""
        return {
            "id": user.id,
            "name": user.name,
            "email": user.email,
            "role": user.role,
            "is_active": user.is_active,
            "created_at": user.created_at,
            "updated_at": user.updated_at
        }


# =============================================================================
# HTTP Client for Order Service
# =============================================================================

class OrderServiceClient:
    """HTTP client for Order Service"""
    
    def __init__(self, base_url: str):
        self.base_url = base_url.rstrip('/')
        self.client: Optional[httpx.AsyncClient] = None
    
    async def connect(self):
        self.client = httpx.AsyncClient(base_url=self.base_url, timeout=30.0)
        logger.info(f"HTTP client ready for {self.base_url}")
    
    async def close(self):
        if self.client:
            await self.client.aclose()
    
    async def create_order(self, data: dict) -> dict:
        response = await self.client.post("/orders", json=data)
        response.raise_for_status()
        return response.json()
    
    async def get_order(self, order_id: str) -> Optional[dict]:
        response = await self.client.get(f"/orders/{order_id}")
        if response.status_code == 404:
            return None
        response.raise_for_status()
        return response.json()
    
    async def list_orders(self, user_id: Optional[str] = None, status: Optional[str] = None) -> list:
        params = {}
        if user_id:
            params["user_id"] = user_id
        if status:
            params["status"] = status
        response = await self.client.get("/orders", params=params)
        response.raise_for_status()
        return response.json()
    
    async def update_status(self, order_id: str, status: str, notes: Optional[str] = None) -> dict:
        data = {"status": status}
        if notes:
            data["notes"] = notes
        response = await self.client.put(f"/orders/{order_id}/status", json=data)
        response.raise_for_status()
        return response.json()


# =============================================================================
# Global Clients
# =============================================================================

user_client = UserServiceClient(USER_SERVICE_GRPC)
order_client = OrderServiceClient(ORDER_SERVICE_URL)
redis_client: Optional[redis.Redis] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown"""
    global redis_client
    
    # Connect to gRPC User Service
    await user_client.connect()
    
    # Connect to Order Service
    await order_client.connect()
    
    # Connect to Redis
    try:
        redis_client = redis.from_url(REDIS_URL, decode_responses=True)
        await redis_client.ping()
        logger.info("Connected to Redis")
    except Exception as e:
        logger.warning(f"Redis not available: {e}")
        redis_client = None
    
    logger.info("API Gateway started")
    yield
    
    # Shutdown
    await user_client.close()
    await order_client.close()
    if redis_client:
        await redis_client.close()


app = FastAPI(
    title="Microservices API Gateway",
    version="1.0.0",
    description="Unified API Gateway demonstrating REST, gRPC, WebSocket, and SSE",
    lifespan=lifespan
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# =============================================================================
# Static Files (Web Client)
# =============================================================================

static_path = Path("/app/static")
if static_path.exists():
    app.mount("/static", StaticFiles(directory=str(static_path)), name="static")


# =============================================================================
# Health & Root
# =============================================================================

@app.get("/")
async def root():
    """Serve web client or redirect"""
    index_path = static_path / "index.html"
    if index_path.exists():
        return HTMLResponse(content=index_path.read_text())
    return {
        "service": "API Gateway",
        "version": "1.0.0",
        "endpoints": {
            "users": "/api/users",
            "orders": "/api/orders",
            "websocket_chat": "/ws/chat/{room}?username={name}",
            "sse_notifications": "/events/notifications",
            "docs": "/docs"
        }
    }


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "service": "api-gateway",
        "timestamp": datetime.utcnow().isoformat(),
        "dependencies": {
            "user_service_grpc": "connected" if user_client.stub else "disconnected",
            "order_service": "connected" if order_client.client else "disconnected",
            "redis": "connected" if redis_client else "disconnected",
            "message_persistence": MESSAGE_PERSISTENCE_SERVICE_URL
        }
    }


# =============================================================================
# User API (REST → gRPC)
# =============================================================================

@app.get("/api/users", response_model=dict)
async def list_users(
    page: int = Query(1, ge=1),
    page_size: int = Query(10, ge=1, le=100)
):
    """List users (REST → gRPC)"""
    return await user_client.list_users(page=page, page_size=page_size)


@app.get("/api/users/{user_id}", response_model=UserResponse)
async def get_user(user_id: str):
    """Get user by ID (REST → gRPC)"""
    user = await user_client.get_user(user_id)
    if not user:
        raise HTTPException(status_code=404, detail=f"User '{user_id}' not found")
    return user


@app.post("/api/users", response_model=UserResponse, status_code=status.HTTP_201_CREATED)
async def create_user(request: CreateUserRequest):
    """Create new user (REST → gRPC)"""
    return await user_client.create_user(
        name=request.name,
        email=request.email,
        password=request.password,
        role=request.role
    )


@app.delete("/api/users/{user_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_user(user_id: str):
    """Delete user (REST → gRPC)"""
    success = await user_client.delete_user(user_id)
    if not success:
        raise HTTPException(status_code=404, detail=f"User '{user_id}' not found")


# =============================================================================
# Order API (REST → REST)
# =============================================================================

@app.get("/api/orders")
async def list_orders(
    user_id: Optional[str] = Query(None),
    status: Optional[str] = Query(None)
):
    """List orders"""
    return await order_client.list_orders(user_id=user_id, status=status)


@app.get("/api/orders/{order_id}")
async def get_order(order_id: str):
    """Get order by ID"""
    order = await order_client.get_order(order_id)
    if not order:
        raise HTTPException(status_code=404, detail=f"Order '{order_id}' not found")
    return order


@app.post("/api/orders", status_code=status.HTTP_201_CREATED)
async def create_order(request: CreateOrderRequest):
    """Create new order"""
    data = {
        "user_id": request.user_id,
        "items": [item.model_dump() for item in request.items],
        "shipping_address": request.shipping_address
    }
    return await order_client.create_order(data)


@app.put("/api/orders/{order_id}/status")
async def update_order_status(order_id: str, request: UpdateOrderStatusRequest):
    """Update order status"""
    return await order_client.update_status(order_id, request.status, request.notes)


# =============================================================================
# WebSocket Chat (Proxy to Chat Service)
# =============================================================================

@app.websocket("/ws/chat/{room}")
async def websocket_chat_proxy(
    websocket: WebSocket,
    room: str,
    username: str = Query(..., description="Username")
):
    """
    WebSocket endpoint for bidirectional chat
    Proxies to the Chat Service or handles directly
    
    Configured with long timeouts to prevent premature disconnections
    """
    await websocket.accept()
    
    # Connect to Chat Service WebSocket
    chat_ws_url = f"{CHAT_SERVICE_WS}/ws/chat/{room}?username={username}"
    
    import websockets
    
    try:
        # Connect with increased timeouts and ping settings
        async with websockets.connect(
            chat_ws_url,
            ping_interval=None,  # Disable automatic ping (let chat service handle it)
            ping_timeout=None,   # No ping timeout
            close_timeout=10,    # Wait up to 10s for clean close
            max_size=10 * 1024 * 1024  # 10MB max message size (for file uploads)
        ) as chat_ws:
            # Event to signal when either side disconnects
            disconnect_event = asyncio.Event()
            
            async def forward_to_client():
                try:
                    async for message in chat_ws:
                        if disconnect_event.is_set():
                            break
                        try:
                            await websocket.send_text(message)
                        except RuntimeError:
                            # WebSocket already closed
                            break
                except websockets.exceptions.ConnectionClosed:
                    # Chat service closed connection
                    pass
                except Exception as e:
                    logger.debug(f"Client forwarding stopped: {e}")
                finally:
                    disconnect_event.set()
            
            async def forward_to_server():
                try:
                    while not disconnect_event.is_set():
                        data = await websocket.receive_text()
                        if disconnect_event.is_set():
                            break
                        try:
                            await chat_ws.send(data)
                        except websockets.exceptions.ConnectionClosed:
                            # Chat service closed connection
                            break
                except WebSocketDisconnect:
                    # Client disconnected normally
                    pass
                except RuntimeError:
                    # WebSocket already closed
                    pass
                except Exception as e:
                    logger.debug(f"Server forwarding stopped: {e}")
                finally:
                    disconnect_event.set()
            
            # Run both directions concurrently
            tasks = [
                asyncio.create_task(forward_to_client()),
                asyncio.create_task(forward_to_server())
            ]
            
            # Wait for either task to complete (disconnect)
            done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            
            # Signal disconnection and cancel remaining tasks
            disconnect_event.set()
            for task in pending:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
            
            # Close client websocket gracefully
            try:
                await websocket.close()
            except Exception:
                pass
            
    except Exception as e:
        logger.error(f"WebSocket proxy error: {e}")
        try:
            await websocket.send_json({"type": "error", "message": f"Failed to connect to chat: {e}"})
            await websocket.close()
        except Exception:
            pass


# =============================================================================
# SSE Notifications (Proxy to Notification Service)
# =============================================================================

@app.get("/events/notifications")
async def sse_notifications(
    user_id: Optional[str] = Query(None, description="Filter by user ID")
):
    """
    Server-Sent Events for real-time notifications
    Proxies to Notification Service
    """
    async def event_generator():
        notification_url = f"{NOTIFICATION_SERVICE_URL}/events/notifications"
        if user_id:
            notification_url += f"?user_id={user_id}"
        
        try:
            async with httpx.AsyncClient() as client:
                async with client.stream("GET", notification_url, timeout=None) as response:
                    async for line in response.aiter_lines():
                        if line:
                            yield f"{line}\n"
                        else:
                            yield "\n"
        except Exception as e:
            logger.error(f"SSE proxy error: {e}")
            yield f"event: error\ndata: {json.dumps({'error': str(e)})}\n\n"
    
    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no"
        }
    )


@app.get("/events/orders")
async def sse_order_events():
    """SSE for order-specific events"""
    async def event_generator():
        notification_url = f"{NOTIFICATION_SERVICE_URL}/events/orders"
        
        try:
            async with httpx.AsyncClient() as client:
                async with client.stream("GET", notification_url, timeout=None) as response:
                    async for line in response.aiter_lines():
                        if line:
                            yield f"{line}\n"
                        else:
                            yield "\n"
        except Exception as e:
            logger.error(f"SSE proxy error: {e}")
            yield f"event: error\ndata: {json.dumps({'error': str(e)})}\n\n"
    
    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no"
        }
    )


# =============================================================================
# Chat API - Leave Room
# =============================================================================

@app.post("/api/chat/rooms/{room}/leave")
async def leave_chat_room(
    room: str,
    username: str = Query(..., description="Username leaving the room")
):
    """
    Explicitly remove a user from a chat room
    Called when user disconnects to ensure state consistency
    """
    chat_url = CHAT_SERVICE_WS.replace("ws://", "http://").replace("wss://", "https://")
    
    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{chat_url}/rooms/{room}/leave",
                params={"username": username},
                timeout=5.0
            )
            
            if response.status_code == 200:
                result = response.json()
                logger.info(f"User '{username}' left room '{room}': {result.get('message')}")
                return result
            else:
                logger.warning(f"Failed to remove user '{username}' from room '{room}': {response.status_code}")
                return JSONResponse(
                    status_code=response.status_code,
                    content={"success": False, "message": "Failed to leave room"}
                )
                
    except Exception as e:
        logger.error(f"Error calling chat service leave endpoint: {e}")
        return JSONResponse(
            status_code=500,
            content={"success": False, "message": str(e)}
        )


@app.get("/api/chat/rooms")
async def list_chat_rooms():
    """
    List all active chat rooms
    Proxy to chat service to get room information including participants
    """
    chat_url = CHAT_SERVICE_WS.replace("ws://", "http://").replace("wss://", "https://")
    
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(f"{chat_url}/rooms", timeout=5.0)
            response.raise_for_status()
            return response.json()
    except Exception as e:
        logger.error(f"Error fetching chat rooms: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch rooms: {str(e)}")


@app.get("/api/chat/rooms/{room}")
async def get_chat_room_info(room: str):
    """
    Get information about a specific chat room including list of participants
    Proxy to chat service
    """
    chat_url = CHAT_SERVICE_WS.replace("ws://", "http://").replace("wss://", "https://")
    
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(f"{chat_url}/rooms/{room}", timeout=5.0)
            
            if response.status_code == 404:
                raise HTTPException(status_code=404, detail=f"Room '{room}' not found")
            
            response.raise_for_status()
            return response.json()
    except httpx.HTTPStatusError as e:
        if e.response.status_code == 404:
            raise HTTPException(status_code=404, detail=f"Room '{room}' not found")
        raise HTTPException(status_code=500, detail=f"Failed to fetch room info: {str(e)}")
    except Exception as e:
        logger.error(f"Error fetching room info: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch room info: {str(e)}")


@app.get("/api/chat/rooms/{room}/history")
async def get_chat_room_history(
    room: str,
    limit: int = Query(100, ge=1, le=1000, description="Number of messages to retrieve"),
    before: Optional[str] = Query(None, description="Get messages before this ISO timestamp")
):
    """
    Get full message history for a room from Cassandra (via chat service)
    Returns messages in chronological order (oldest first)
    """
    chat_url = CHAT_SERVICE_WS.replace("ws://", "http://").replace("wss://", "https://")
    
    try:
        params = {"limit": limit}
        if before:
            params["before"] = before
        
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{chat_url}/rooms/{room}/history",
                params=params,
                timeout=10.0
            )
            
            if response.status_code == 503:
                raise HTTPException(status_code=503, detail="History service unavailable")
            
            response.raise_for_status()
            return response.json()
    except httpx.HTTPStatusError as e:
        if e.response.status_code == 503:
            raise HTTPException(status_code=503, detail="History service unavailable")
        raise HTTPException(status_code=500, detail=f"Failed to fetch history: {str(e)}")
    except Exception as e:
        logger.error(f"Error fetching history: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch history: {str(e)}")


# =============================================================================
# Message Persistence API (REST → REST proxy to persistence service)
# =============================================================================

@app.get("/api/persistence/rooms/{room}/history")
async def get_persistence_history(
    room: str,
    limit: int = Query(100, ge=1, le=1000, description="Number of messages to retrieve"),
    before: Optional[str] = Query(None, description="Get messages before this ISO timestamp")
):
    """
    Get message history directly from persistence service (Cassandra)
    Proxy to message-persistence-service
    Returns messages in chronological order (oldest first)
    """
    try:
        params = {"limit": limit}
        if before:
            params["before"] = before
        
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{MESSAGE_PERSISTENCE_SERVICE_URL}/api/rooms/{room}/history",
                params=params,
                timeout=10.0
            )
            
            if response.status_code == 404:
                raise HTTPException(status_code=404, detail=f"Room '{room}' not found")
            
            response.raise_for_status()
            return response.json()
    except httpx.HTTPStatusError as e:
        if e.response.status_code == 404:
            raise HTTPException(status_code=404, detail=f"Room '{room}' not found")
        raise HTTPException(status_code=500, detail=f"Failed to fetch history: {str(e)}")
    except Exception as e:
        logger.error(f"Error fetching persistence history: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch history: {str(e)}")


@app.get("/api/persistence/rooms/{room}/count")
async def get_persistence_count(room: str):
    """
    Get total message count for a room from persistence service
    Proxy to message-persistence-service
    """
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{MESSAGE_PERSISTENCE_SERVICE_URL}/api/rooms/{room}/count",
                timeout=10.0
            )
            
            response.raise_for_status()
            return response.json()
    except httpx.HTTPStatusError as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch message count: {str(e)}")
    except Exception as e:
        logger.error(f"Error fetching message count: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch message count: {str(e)}")


# =============================================================================
# File Upload API (Proxy to Chat Service)
# =============================================================================

@app.post("/api/chat/files/initiate-upload")
async def initiate_file_upload(request: dict):
    """Proxy to chat service: Initiate file upload"""
    chat_url = CHAT_SERVICE_WS.replace("ws://", "http://").replace("wss://", "https://")
    
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(
                f"{chat_url}/files/initiate-upload",
                json=request
            )
            response.raise_for_status()
            return response.json()
    except Exception as e:
        logger.error(f"Failed to initiate upload: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/chat/files/upload-chunk")
async def get_chunk_upload_url(request: dict):
    """Proxy to chat service: Get presigned URL for chunk upload"""
    chat_url = CHAT_SERVICE_WS.replace("ws://", "http://").replace("wss://", "https://")
    
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(
                f"{chat_url}/files/upload-chunk",
                json=request
            )
            response.raise_for_status()
            return response.json()
    except Exception as e:
        logger.error(f"Failed to get chunk upload URL: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/chat/files/complete-upload")
async def complete_file_upload(request: dict):
    """Proxy to chat service: Complete multipart upload"""
    chat_url = CHAT_SERVICE_WS.replace("ws://", "http://").replace("wss://", "https://")
    
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(
                f"{chat_url}/files/complete-upload",
                json=request
            )
            response.raise_for_status()
            return response.json()
    except Exception as e:
        logger.error(f"Failed to complete upload: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/chat/files/simple-upload")
async def simple_file_upload(
    room: str = Query(...),
    username: str = Query(...),
    file: UploadFile = File(...)
):
    """Proxy to chat service: Simple file upload"""
    chat_url = CHAT_SERVICE_WS.replace("ws://", "http://").replace("wss://", "https://")
    
    try:
        # Read file content
        file_content = await file.read()
        
        async with httpx.AsyncClient(timeout=60.0) as client:
            # Create proper multipart form data
            files = {
                "file": (file.filename, file_content, file.content_type or "application/octet-stream")
            }
            response = await client.post(
                f"{chat_url}/files/simple-upload",
                params={"room": room, "username": username},
                files=files
            )
            response.raise_for_status()
            return response.json()
    except Exception as e:
        logger.error(f"Failed to upload file: {e}")
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)

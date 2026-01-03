"""
User Service - gRPC Implementation
Demonstrates: Unary RPC, Server Streaming, Authentication
"""
import os
import asyncio
import logging
from datetime import datetime
from uuid import uuid4
from concurrent import futures
from typing import Optional, Dict, Any
import hashlib
import secrets

import grpc
from grpc import aio as grpc_aio

# Generated protobuf imports (generated at container build time)
import user_pb2
import user_pb2_grpc

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def hash_password(password: str) -> str:
    """Simple password hashing (use bcrypt in production)"""
    return hashlib.sha256(password.encode()).hexdigest()


def generate_token() -> str:
    """Generate a simple auth token"""
    return secrets.token_urlsafe(32)


class UserDatabase:
    """In-memory user database (use real DB in production)"""
    
    def __init__(self):
        self.users: Dict[str, Dict[str, Any]] = {}
        self.tokens: Dict[str, str] = {}  # token -> user_id
        self._seed_data()
    
    def _seed_data(self):
        """Seed with sample users"""
        sample_users = [
            {"name": "Alice Johnson", "email": "alice@example.com", "password": "password123", "role": "admin"},
            {"name": "Bob Smith", "email": "bob@example.com", "password": "password123", "role": "user"},
            {"name": "Charlie Brown", "email": "charlie@example.com", "password": "password123", "role": "user"},
            {"name": "Diana Prince", "email": "diana@example.com", "password": "password123", "role": "moderator"},
            {"name": "Eve Wilson", "email": "eve@example.com", "password": "password123", "role": "user"},
        ]
        for user_data in sample_users:
            self.create(user_data)
        logger.info(f"Seeded {len(sample_users)} sample users")
    
    def create(self, data: Dict[str, Any]) -> Dict[str, Any]:
        user_id = str(uuid4())
        now = datetime.utcnow().isoformat()
        user = {
            "id": user_id,
            "name": data["name"],
            "email": data["email"],
            "password_hash": hash_password(data.get("password", "default")),
            "role": data.get("role", "user"),
            "created_at": now,
            "updated_at": now,
            "is_active": True,
        }
        self.users[user_id] = user
        return user
    
    def get(self, user_id: str) -> Optional[Dict[str, Any]]:
        return self.users.get(user_id)
    
    def get_by_email(self, email: str) -> Optional[Dict[str, Any]]:
        for user in self.users.values():
            if user["email"] == email:
                return user
        return None

    def get_by_name(self, name: str) -> Optional[Dict[str, Any]]:
        for user in self.users.values():
            if user["name"] == name:
                return user
        return None

    def update(self, user_id: str, data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        if user_id not in self.users:
            return None
        user = self.users[user_id]
        for key, value in data.items():
            if value is not None and key in user:
                user[key] = value
        user["updated_at"] = datetime.utcnow().isoformat()
        return user
    
    def delete(self, user_id: str) -> bool:
        if user_id in self.users:
            del self.users[user_id]
            return True
        return False
    
    def list_all(self, role_filter: Optional[str] = None, active_only: bool = False) -> list:
        users = list(self.users.values())
        if role_filter:
            users = [u for u in users if u["role"] == role_filter]
        if active_only:
            users = [u for u in users if u["is_active"]]
        return users
    
    def authenticate(self, email: str, password: str) -> Optional[Dict[str, Any]]:
        user = self.get_by_email(email)
        if user and user["password_hash"] == hash_password(password):
            return user
        return None


# Global database instance
db = UserDatabase()


def user_to_proto(user: Dict[str, Any]) -> user_pb2.User:
    """Convert user dict to protobuf message"""
    return user_pb2.User(
        id=user["id"],
        name=user["name"],
        email=user["email"],
        role=user["role"],
        created_at=user["created_at"],
        updated_at=user["updated_at"],
        is_active=user["is_active"],
    )


class UserServiceServicer(user_pb2_grpc.UserServiceServicer):
    """gRPC User Service Implementation"""
    
    async def GetUser(self, request: user_pb2.GetUserRequest, context: grpc.aio.ServicerContext) -> user_pb2.User:
        """Unary RPC - Get single user by ID"""
        logger.info(f"GetUser called with id={request.id}")
        
        user = db.get(request.id)
        if not user:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(f"User with id '{request.id}' not found")
            return user_pb2.User()
        
        return user_to_proto(user)

    async def GetUserByName(self, request: user_pb2.GetUserByNameRequest, context: grpc.aio.ServicerContext) -> user_pb2.User:
        """Unary RPC - Get single user by Name"""
        logger.info(f"GetUserByName called with name={request.name}")
        
        user = db.get_by_name(request.name)
        if not user:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(f"User with name '{request.name}' not found")
            return user_pb2.User()
        
        return user_to_proto(user)

    async def CreateUser(self, request: user_pb2.CreateUserRequest, context: grpc.aio.ServicerContext) -> user_pb2.User:
        """Unary RPC - Create new user"""
        logger.info(f"CreateUser called for email={request.email}")
        
        # Check if email already exists
        if db.get_by_email(request.email):
            context.set_code(grpc.StatusCode.ALREADY_EXISTS)
            context.set_details(f"User with email '{request.email}' already exists")
            return user_pb2.User()
        
        user = db.create({
            "name": request.name,
            "email": request.email,
            "password": request.password,
            "role": request.role or "user",
        })
        
        logger.info(f"Created user with id={user['id']}")
        return user_to_proto(user)
    
    async def UpdateUser(self, request: user_pb2.UpdateUserRequest, context: grpc.aio.ServicerContext) -> user_pb2.User:
        """Unary RPC - Update existing user"""
        logger.info(f"UpdateUser called for id={request.id}")
        
        update_data = {}
        if request.HasField('name'):
            update_data['name'] = request.name
        if request.HasField('email'):
            update_data['email'] = request.email
        if request.HasField('role'):
            update_data['role'] = request.role
        if request.HasField('is_active'):
            update_data['is_active'] = request.is_active
        
        user = db.update(request.id, update_data)
        if not user:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(f"User with id '{request.id}' not found")
            return user_pb2.User()
        
        return user_to_proto(user)
    
    async def DeleteUser(self, request: user_pb2.DeleteUserRequest, context: grpc.aio.ServicerContext) -> user_pb2.DeleteUserResponse:
        """Unary RPC - Delete user"""
        logger.info(f"DeleteUser called for id={request.id}")
        
        success = db.delete(request.id)
        return user_pb2.DeleteUserResponse(
            success=success,
            message="User deleted successfully" if success else f"User '{request.id}' not found"
        )
    
    async def ListUsers(self, request: user_pb2.ListUsersRequest, context: grpc.aio.ServicerContext) -> user_pb2.ListUsersResponse:
        """Unary RPC - List users with pagination"""
        logger.info(f"ListUsers called page={request.page}, page_size={request.page_size}")
        
        role_filter = request.role_filter if request.HasField('role_filter') else None
        active_only = request.active_only if request.HasField('active_only') else False
        
        all_users = db.list_all(role_filter=role_filter, active_only=active_only)
        total = len(all_users)
        
        page = max(1, request.page)
        page_size = min(100, max(1, request.page_size or 10))
        pages = (total + page_size - 1) // page_size
        
        start = (page - 1) * page_size
        end = start + page_size
        paginated_users = all_users[start:end]
        
        return user_pb2.ListUsersResponse(
            users=[user_to_proto(u) for u in paginated_users],
            total=total,
            page=page,
            pages=pages,
        )
    
    async def StreamUsers(self, request: user_pb2.StreamUsersRequest, context: grpc.aio.ServicerContext):
        """Server streaming RPC - Stream all users"""
        logger.info("StreamUsers called - starting user stream")
        
        role_filter = request.role_filter if request.HasField('role_filter') else None
        active_only = request.active_only if request.HasField('active_only') else False
        
        users = db.list_all(role_filter=role_filter, active_only=active_only)
        
        for user in users:
            if context.cancelled():
                logger.info("StreamUsers cancelled by client")
                break
            
            yield user_to_proto(user)
            await asyncio.sleep(0.1)  # Simulate some processing delay
        
        logger.info(f"StreamUsers completed - streamed {len(users)} users")
    
    async def Authenticate(self, request: user_pb2.AuthRequest, context: grpc.aio.ServicerContext) -> user_pb2.AuthResponse:
        """Unary RPC - Authenticate user"""
        logger.info(f"Authenticate called for email={request.email}")
        
        user = db.authenticate(request.email, request.password)
        if not user:
            return user_pb2.AuthResponse(
                success=False,
                error="Invalid email or password"
            )
        
        token = generate_token()
        db.tokens[token] = user["id"]
        
        return user_pb2.AuthResponse(
            success=True,
            token=token,
            user=user_to_proto(user)
        )


async def serve():
    """Start the gRPC server"""
    port = os.getenv("GRPC_PORT", "50051")
    server = grpc_aio.server(futures.ThreadPoolExecutor(max_workers=10))
    
    user_pb2_grpc.add_UserServiceServicer_to_server(UserServiceServicer(), server)
    
    listen_addr = f"[::]:{port}"
    server.add_insecure_port(listen_addr)
    
    logger.info(f"Starting gRPC User Service on {listen_addr}")
    await server.start()
    
    logger.info("User Service is ready to accept requests")
    await server.wait_for_termination()


if __name__ == "__main__":
    asyncio.run(serve())

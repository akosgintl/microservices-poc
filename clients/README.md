# Microservices PoC - Client Applications

This directory contains **Python client applications** that demonstrate all communication protocols and features implemented in the microservices proof-of-concept.

## 📋 Table of Contents

- [Overview](#overview)
- [Files in This Directory](#files-in-this-directory)
- [Installation](#installation)
- [Client Applications](#client-applications)
- [Features Demonstrated](#features-demonstrated)
- [Quick Start](#quick-start)
- [Advanced Usage](#advanced-usage)
- [Troubleshooting](#troubleshooting)

---

## 🎯 Overview

These client scripts provide **interactive demonstrations** of:

- ✅ **REST API** - HTTP requests to User and Order services
- ✅ **gRPC** - High-performance RPC with Protocol Buffers
- ✅ **WebSocket** - Bidirectional real-time chat
- ✅ **SSE** - Server-Sent Events for real-time notifications
- ✅ **File Sharing** - MinIO S3-compatible storage
- ✅ **Message Persistence** - Kafka + Cassandra architecture
- ✅ **Horizontal Scaling** - Redis-powered distributed state
- ✅ **Health Monitoring** - Automatic connection health checks

---

## 📁 Files in This Directory

### Client Scripts

| File | Purpose | Protocol |
|------|---------|----------|
| `rest_client.py` | Demonstrates HTTP REST API calls to User and Order services | REST/HTTP |
| `grpc_client.py` | Demonstrates gRPC communication with Protocol Buffers | gRPC |
| `websocket_client.py` | Interactive bidirectional real-time chat client | WebSocket |
| `sse_client.py` | Server-Sent Events listener for real-time notifications | SSE |
| `main.py` | Simple entry point script (placeholder) | - |

### Configuration Files

| File | Purpose |
|------|---------|
| `requirements.txt` | Python package dependencies with specific versions |
| `pyproject.toml` | Modern Python project configuration (PEP 518) |
| `.gitignore` | Git ignore patterns for client-specific temporary files |

### Dependency Management

This project supports **two dependency management approaches**:

1. **pip + requirements.txt** (traditional):
   ```bash
   pip install -r requirements.txt
   ```

2. **uv + pyproject.toml** (modern):
   ```bash
   uv sync
   ```

Both methods install the same packages with compatible versions.

---

## 📦 Installation

### Prerequisites

- **Python 3.11+** (3.11, 3.12, or 3.13 recommended)
- **Docker Compose** (for running the microservices)
- **pip** or **uv** package manager

### Install Dependencies

**Option 1: Using pip + requirements.txt (traditional)**

```bash
# Navigate to the clients directory
cd clients

# Install required packages
pip install -r requirements.txt

# Or use a virtual environment (recommended)
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

**Option 2: Using uv + pyproject.toml (modern, faster)**

```bash
# Navigate to the clients directory
cd clients

# Install dependencies with uv
uv sync

# Or activate environment and install
uv venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
uv pip install -e .
```

### Verify Installation

```bash
# Check installations
python -c "import httpx, websockets, aiohttp, grpc; print('✓ All dependencies installed')"
```

### Installed Dependencies

The following packages are installed (see `requirements.txt` or `pyproject.toml`):

- **httpx** (0.26.0) - Async HTTP client for REST API calls
- **websockets** (12.0) - WebSocket protocol implementation
- **aiohttp** (3.9.1) - Async HTTP with SSE support
- **grpcio** (1.60.0) - gRPC runtime library
- **grpcio-tools** (1.60.0) - Protocol buffer compiler tools
- **protobuf** (4.25.2) - Protocol buffer runtime
- **cassandra-driver** (3.29.1) - Cassandra database driver (for direct DB access)
- **kafka-python** (2.0.2) - Kafka Python client (for event monitoring)
- **redis** (5.0.1) - Redis client (for cache inspection)

---

## 🖥️ Client Applications

### 1. REST API Client (`rest_client.py`)

**Purpose:** Demonstrates HTTP REST API calls, REST → gRPC translation, order management, and health checks

**Features:**
- Health checks for all microservices
- User CRUD operations (REST → gRPC internal communication)
- Order management (create, update, query statistics)
- Chat room listing
- Chat history queries from Cassandra
- Message persistence service health monitoring

**Usage:**

```bash
# Run the client (automated demo)
python rest_client.py

# What it demonstrates:
# [1] Health Check - All Services
# [2] List Users (REST → gRPC)
# [3] Create User (REST → gRPC)
# [4] List Orders
# [5] Order Statistics
# [6] Create Order (triggers Kafka event)
# [7] Update Order Status (triggers Kafka event)
# [8] Get Specific Order
# [9] List Active Chat Rooms
# [10] Query Chat History (from Cassandra)
# [11] Message Persistence Service Health
```

**Key Observations:**
- REST to gRPC internal communication
- Event-driven architecture (Kafka)
- Three-tier persistence (Redis/Kafka/Cassandra)
- Comprehensive service health monitoring
- Order creation triggers real-time notifications via SSE

**Example Output:**
```
[1] Health Check - All Services
    Gateway Status: healthy
    Backend Services:
      ✅ user-service: healthy
      ✅ order-service: healthy
      ✅ chat-service: healthy

[6] Create Order (REST → Kafka)
    Created order: 550e8400...
    Total: $459.97
    Status: pending
    → Kafka event 'orders.created' published!
    → Redis pub/sub notification sent!
```

---

### 2. WebSocket Chat Client (`websocket_client.py`)

**Purpose:** Interactive bidirectional real-time chat with file sharing and participant tracking

**Features:**
- Real-time bidirectional messaging
- Participant list with join/leave notifications
- File sharing notifications (📁 icon)
- Health check pings (💓 icon)
- Message history from Redis cache (last 10 messages)
- Cross-instance communication via Redis pub/sub
- Interactive and demo modes

**Usage:**

```bash
# Interactive mode
python websocket_client.py --room general --user alice

# Connect to specific chat instance (for scaling tests)
python websocket_client.py --room general --user bob --port 8011

# Automated demo
python websocket_client.py --demo

# See help
python websocket_client.py --help
```

**Interactive Commands:**
- Type messages to send to all users in the room
- `/users` - Show current participants
- `/help` - Show available commands
- `quit` - Exit the chat

**Command-Line Arguments:**
- `--room, -r` - Chat room name (default: "general")
- `--user, -u` - Your username (default: "user1")
- `--server, -s` - Server URL (default: "ws://localhost:8000")
- `--port, -p` - Direct chat service port (for scaling tests)
- `--demo, -d` - Run automated demo mode

**Example Output:**
```
✓ Connected to chat room 'general'

Commands:
  - Type messages and press Enter to send
  - Type 'quit' to exit
  - Type '/users' to show participants

--- Recent Messages (from Redis cache) ---
  [2026-01-03 10:30:15] alice: Hello everyone!
  [2026-01-03 10:30:20] bob: 📁 example.pdf (1.2 MB)
--- End of History ---

[👥 Participants (3): alice, bob, charlie]

[➕ charlie joined the chat]
  [charlie]: Hey team!
```

---

### 3. SSE Client (`sse_client.py`)

**Purpose:** Server-Sent Events listener for real-time server-push notifications and order tracking

**Features:**
- Real-time server-to-client push notifications
- Automatic reconnection on disconnect
- Event filtering by user
- Order lifecycle tracking
- Dual-path delivery (Kafka + Redis pub/sub)
- Heartbeat monitoring
- Automated demo mode

**Usage:**

```bash
# Listen to all notifications
python sse_client.py

# Filter by user
python sse_client.py --user user-123

# Listen to order events only
python sse_client.py --endpoint /events/orders

# Automated demo (creates orders automatically)
python sse_client.py --demo

# See help
python sse_client.py --help
```

**Command-Line Arguments:**
- `--server, -s` - Server URL (default: "http://localhost:8000")
- `--user, -u` - Filter notifications by user ID
- `--endpoint, -e` - SSE endpoint (`/events/notifications` or `/events/orders`)
- `--demo, -d` - Run demo mode (auto-creates orders)

**Available Endpoints:**
- `/events/notifications` - All notifications (default)
- `/events/orders` - Order-specific events only

**Example Output:**
```
✓ Connected to notification stream
Waiting for events...

────────────────────────────────────────────────────────────
[10:30:45] 🔔 NOTIFICATION #1
────────────────────────────────────────────────────────────
  Type:    order.created
  Title:   New Order Created
  Message: Order #550e8400 has been placed. Total: $459.97
  User:    user-1
  Order:   550e8400...
────────────────────────────────────────────────────────────

────────────────────────────────────────────────────────────
[10:31:20] 🔄 ORDER EVENT: ORDER.UPDATED
────────────────────────────────────────────────────────────
  Title:   Order Status Updated
  Message: Order #550e8400 status changed: pending → confirmed
  Change:  pending → confirmed
────────────────────────────────────────────────────────────
```

---

### 4. gRPC Client (`grpc_client.py`)

**Purpose:** Direct gRPC communication demonstrating Protocol Buffers, RPC patterns, streaming, and authentication

**Features:**
- Automatic proto file generation from `.proto` definitions
- Protocol Buffers for efficient serialization
- Unary RPC (request-response)
- Server streaming RPC
- Authentication with JWT tokens
- Strong typing with auto-generated code
- ~7x faster than REST for internal APIs

**Usage:**

```bash
# Run all tests
python grpc_client.py

# Run specific test
python grpc_client.py --test list      # List users
python grpc_client.py --test create    # Create user
python grpc_client.py --test stream    # Server streaming
python grpc_client.py --test auth      # Test authentication

# Connect to different server
USER_SERVICE_GRPC=localhost:50051 python grpc_client.py

# See help
python grpc_client.py --help
```

**Available Tests:**
- `all` - Run all tests (default)
- `list` - List users with pagination
- `create` - Create a new user
- `get` - Get user by ID
- `stream` - Server streaming RPC demo
- `auth` - Test authentication with JWT
- `delete` - Delete user

**Command-Line Arguments:**
- `--test, -t` - Which test to run (choices: all, list, create, get, stream, auth, update, delete)

**Environment Variables:**
- `USER_SERVICE_GRPC` - gRPC server address (default: "localhost:50051")

**Example Output:**
```
✓ gRPC code generated successfully

[1] Connecting to gRPC server at localhost:50051...
✓ Connected to User Service

============================================================
[2] Unary RPC: ListUsers
============================================================

📊 Total users: 42
📄 Page 1 of 9

Users:
  1. Alice Johnson
     Email: alice@example.com
     Role: admin
     Active: ✅
     Created: 2026-01-01 09:00:00

============================================================
[5] Server Streaming RPC: StreamUsers
============================================================

📡 Starting user stream...
   (Receiving users one at a time via gRPC streaming)

[Stream 1] Alice Johnson
   Email: alice@example.com
   Role: admin
```

---

### 5. Main Entry Point (`main.py`)

**Purpose:** Simple placeholder entry point for the clients package

**Content:**
```python
def main():
    print("Hello from clients!")

if __name__ == "__main__":
    main()
```

**Note:** This is a minimal placeholder. Use the specific client scripts above for actual demonstrations.

---

## 🎯 Features Demonstrated

### 1. File Sharing (MinIO S3)

**Implemented in:** WebSocket client
**Documentation:** [`../docs/FILE_UPLOAD_SUMMARY.md`](../docs/FILE_UPLOAD_SUMMARY.md)

- S3-compatible object storage
- Presigned URLs for secure access
- Real-time file sharing notifications
- Cross-instance synchronization

### 2. Message Persistence (Kafka + Cassandra)

**Implemented in:** REST client, WebSocket client
**Documentation:** [`../docs/KAFKA_CASSANDRA_IMPLEMENTATION_SUMMARY.md`](../docs/KAFKA_CASSANDRA_IMPLEMENTATION_SUMMARY.md)

**Three-tier architecture:**
- **Redis Cache**: 10 most recent messages (< 1ms latency)
- **Kafka Streaming**: Reliable event pipeline
- **Cassandra Storage**: Unlimited persistent history

### 3. Participants Display

**Implemented in:** WebSocket client
**Documentation:** [`../docs/PARTICIPANTS_DISPLAY_FEATURE.md`](../docs/PARTICIPANTS_DISPLAY_FEATURE.md)

- Real-time participant list
- Join/leave notifications
- Cross-instance support via Redis

### 4. Health Monitoring

**Implemented in:** All clients
**Documentation:** [`../docs/HEALTH_CHECK_QUICKSTART.md`](../docs/HEALTH_CHECK_QUICKSTART.md)

- Periodic connection pings (every 30s)
- Dead connection detection
- Automatic cleanup
- Cross-instance notifications

---

## 🚀 Quick Start

### Step 1: Start the Microservices

```bash
# From the project root
docker compose up -d

# Verify all services are running
docker compose ps

# Check health
curl http://localhost:8000/health
```

### Step 2: Run a Client

```bash
# From the clients directory
cd clients

# Try the REST client (easiest to start with)
python rest_client.py

# Try the WebSocket chat
python websocket_client.py --room demo --user alice

# Try SSE notifications
python sse_client.py --demo
```

### Step 3: Test Horizontal Scaling

```bash
# Scale chat service to 3 instances
docker compose up -d --scale chat-service=3

# Open 3 terminals and connect to different instances
# Terminal 1
python websocket_client.py --room general --user alice --port 8011

# Terminal 2
python websocket_client.py --room general --user bob --port 8012

# Terminal 3
python websocket_client.py --room general --user charlie --port 8013

# Messages flow between instances via Redis pub/sub!
```

---

## 🔧 Advanced Usage

### Testing Kafka + Cassandra Persistence

```bash
# 1. Send 20+ messages via WebSocket
python websocket_client.py --room test --user tester

# 2. Query full history from Cassandra
python rest_client.py
# (Look for section [10] Query Chat History)

# 3. Verify Redis cache is limited to 10 messages
docker exec -it redis redis-cli
> LLEN chat:room:test:history
# Should output: 10
```

### Monitoring Redis Activity

```bash
# Terminal 1: Monitor Redis commands
docker exec -it redis redis-cli MONITOR

# Terminal 2: Send chat messages
python websocket_client.py --room monitor-test --user alice

# Observe Redis pub/sub, set operations, list operations in Terminal 1
```

### Testing File Uploads

File uploads are done via the web client (`http://localhost:8000`), but you can verify file messages in the WebSocket client:

```bash
# Connect to chat
python websocket_client.py --room files --user alice

# In web browser:
# 1. Open http://localhost:8000
# 2. Connect to 'files' room
# 3. Upload a file
# 4. See notification in Python client: "📁 Shared filename.pdf (1.2 MB)"
```

### Testing Order Events → SSE Notifications

```bash
# Terminal 1: Start SSE listener
python sse_client.py

# Terminal 2: Create orders
python rest_client.py
# OR manually:
curl -X POST http://localhost:8000/api/orders \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": "user-1",
    "items": [{"product": "Keyboard", "quantity": 1, "price": 79.99}]
  }'

# See instant notification in Terminal 1!
```

---

## 🐛 Troubleshooting

### Services Not Running

```bash
# Check service status
docker compose ps

# View logs
docker compose logs -f api-gateway
docker compose logs -f chat-service

# Restart services
docker compose restart

# Full reset
docker compose down -v
docker compose up --build
```

### Connection Refused

**Problem:** Client can't connect to service

**Solutions:**
```bash
# Verify service is listening
curl http://localhost:8000/health        # API Gateway
curl http://localhost:8002/health        # Order Service
curl http://localhost:8003/health        # Notification Service
curl http://localhost:8004/health        # Message Persistence

# Check gRPC service
grpcurl -plaintext localhost:50051 list

# Verify ports are not in use
netstat -ano | findstr :8000   # Windows
lsof -i :8000                  # Mac/Linux
```

### gRPC Code Generation Fails

**Problem:** `grpc_client.py` fails to generate proto files

**Solution:**
```bash
# Ensure grpcio-tools is installed
pip install grpcio-tools

# Manually generate proto files
cd user-service
python -m grpc_tools.protoc \
  -I./protos \
  --python_out=./generated \
  --pyi_out=./generated \
  --grpc_python_out=./generated \
  ./protos/user.proto
```

### WebSocket Disconnecting Immediately

**Problem:** WebSocket connects then immediately disconnects

**Solution:**
```bash
# Check chat service logs
docker compose logs chat-service | grep -E "error|fail|crash"

# Verify Redis is running
docker compose ps redis

# Rebuild chat service
docker compose build chat-service
docker compose up -d chat-service
```

**See also:** [`../docs/WEBSOCKET_DISCONNECT_FIX_SUMMARY.md`](../docs/WEBSOCKET_DISCONNECT_FIX_SUMMARY.md)

### No Message History

**Problem:** Chat history is empty when connecting

**Solutions:**
```bash
# Check Redis connection
docker exec -it redis redis-cli
> LRANGE chat:room:general:history 0 -1

# Check Cassandra
docker exec -it cassandra cqlsh
> USE chat;
> SELECT COUNT(*) FROM messages WHERE room = 'general';

# Check message persistence service
curl http://localhost:8004/health
docker compose logs message-persistence-service
```

### SSE Client Not Receiving Events

**Problem:** SSE client connects but receives no notifications

**Solutions:**
```bash
# 1. Check notification service
curl http://localhost:8003/health

# 2. Verify Kafka is running
docker compose ps kafka

# 3. Check if events are being published
docker compose logs notification-service | grep -E "event|notification"

# 4. Test with curl
curl -N http://localhost:8003/events/notifications

# 5. Create a test order to trigger event
curl -X POST http://localhost:8000/api/orders \
  -H "Content-Type: application/json" \
  -d '{"user_id":"test","items":[{"product":"Test","quantity":1,"price":9.99}]}'
```

---

## 📚 Additional Resources

### Documentation

- **Main README:** [`../README.md`](../README.md) - Full architecture overview
- **File Upload:** [`../docs/FILE_UPLOAD.md`](../docs/FILE_UPLOAD.md)
- **Kafka + Cassandra:** [`../docs/KAFKA_CASSANDRA_PERSISTENCE.md`](../docs/KAFKA_CASSANDRA_PERSISTENCE.md)
- **Health Checks:** [`../docs/HEALTH_CHECK.md`](../docs/HEALTH_CHECK.md)
- **Redis Scaling:** [`../docs/REDIS_SCALING.md`](../docs/REDIS_SCALING.md)

### Protocol Comparison

| Protocol | Best For | Latency | Use Case |
|----------|----------|---------|----------|
| **REST** | Public APIs, CRUD | ~50ms | User/Order management |
| **gRPC** | Internal services | ~7ms | High-performance inter-service |
| **WebSocket** | Bidirectional real-time | ~5ms | Chat, gaming, collaboration |
| **SSE** | Server-push notifications | ~10ms | Live feeds, notifications |
| **Kafka** | Event-driven architecture | Variable | Async event processing |
| **Redis Pub/Sub** | Real-time messaging | ~1ms | Fast ephemeral messages |

### Testing Matrix

| Feature | REST | gRPC | WebSocket | SSE |
|---------|------|------|-----------|-----|
| User CRUD | ✅ | ✅ | ❌ | ❌ |
| Order CRUD | ✅ | ❌ | ❌ | ❌ |
| Chat | ❌ | ❌ | ✅ | ❌ |
| File Sharing | ❌ | ❌ | ✅ | ❌ |
| Notifications | ❌ | ❌ | ❌ | ✅ |
| Health Checks | ✅ | ✅ | ✅ | ✅ |

---

## 🎓 Learning Outcomes

After using these clients, you should understand:

✅ **Protocol Selection**
- When to use REST vs gRPC vs WebSocket vs SSE
- Trade-offs between protocols

✅ **Event-Driven Architecture**
- Kafka for reliable event streaming
- Redis pub/sub for fast ephemeral messaging
- Separation of concerns

✅ **Horizontal Scaling**
- Stateless services (Order Service)
- Distributed state with Redis (Chat Service)
- Consumer groups with Kafka

✅ **Message Persistence**
- Three-tier architecture (Redis/Kafka/Cassandra)
- Hot vs cold storage trade-offs
- Time-series data modeling

✅ **Production Patterns**
- Health checks and monitoring
- Graceful degradation
- Error handling and retry logic
- Cross-service communication

---

## 📝 License

MIT License - See main project LICENSE file

---

## 🤝 Contributing

Improvements and suggestions are welcome! These are educational demo clients, so feel free to:
- Add new test scenarios
- Improve error handling
- Add more command-line options
- Create additional client examples

---

**Created:** January 2026  
**Last Updated:** January 2026  
**Author:** Microservices Communication PoC  
**Questions?** See main project README or open an issue

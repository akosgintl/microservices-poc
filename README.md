# Microservices Communication Protocols PoC

A comprehensive, production-ready proof-of-concept demonstrating modern microservices communication patterns: **REST**, **gRPC**, **WebSocket**, **SSE (Server-Sent Events)**, and **Kafka** event-driven architecture with **two-tier NGINX load balancing**, **HTTPS encryption**, horizontal scaling capabilities, and **S3-compatible file storage**.

## 🎯 What You'll Learn

This PoC demonstrates enterprise-level patterns for:
- **Synchronous Communication**: REST APIs and gRPC for request-response patterns
- **Asynchronous Communication**: Kafka for event-driven architectures
- **Real-time Communication**: WebSocket for bidirectional streaming, SSE for server-push
- **Load Balancing**: NGINX Layer 7 load balancer with session affinity
- **File Storage**: MinIO S3-compatible object storage with presigned URLs
- **Message Persistence**: Three-tier architecture (Redis/Kafka/Cassandra)
- **Horizontal Scaling**: Redis-powered distributed state management
- **Microservices Patterns**: Service mesh, API gateway, event sourcing, CQRS principles

✨ **Highlights:**
- 🔄 **Two-Tier Load Balancing** - nginx-fe (frontend) + nginx-be (backend) with auto-discovery
- 🔒 **HTTPS Everywhere** - SSL/TLS encryption with self-signed certificates (production-ready)
- 🎯 **Unified Management UI** - Redis, Kafka, MinIO accessible via single HTTPS endpoint
- 🚀 **Horizontally Scalable** - All services scale independently with automatic load distribution
- 📦 **Containerized** - Full Docker Compose orchestration
- 🔄 **Event-Driven** - Kafka with KRaft mode (no ZooKeeper)
- 📁 **File Sharing** - MinIO object storage for file uploads/downloads
- 💾 **Message Persistence** - Three-tier (Redis/Kafka/Cassandra) for optimal performance
- 🌐 **Production Patterns** - Health checks, failover, rate limiting, security headers
- 🧪 **Fully Testable** - Web UI + Python clients included

## Architecture Overview

### Two-Tier Load Balancing Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              EXTERNAL CLIENTS                               │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────────┐ │
│  │ REST Client  │  │  WebSocket   │  │  SSE Client  │  │   Web Browser    │ │
│  │   (HTTP)     │  │   Client     │  │  (EventSrc)  │  │   (All modes)    │ │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘  └────────┬─────────┘ │
└─────────┼─────────────────┼─────────────────┼───────────────────┼───────────┘
          │                 │                 │                   │
          └─────────────────┴─────────────────┴───────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│              TIER 1: nginx-fe (Frontend Load Balancer)                     │
│                     HTTPS :443 / HTTP :80                                  │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  • SSL/TLS termination          • Rate limiting (100 req/s)         │   │
│  │  • Layer 7 HTTP/WebSocket/SSE   • Security headers (HSTS, CSP)      │   │
│  │  • Least-conn (REST/SSE)        • Unified UI proxy:                 │   │
│  │  • IP-hash (WebSocket)          •   /redis-ui/ → Redis UI           │   │
│  │  • Auto-discovery               •   /kafka-ui/ → Kafka UI           │   │
│  │  • Health checks & failover     •   /minio/ → MinIO Console         │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
          ┌─────────────────────────┼─────────────────────────┐
          │                         │                         │
          ▼                         ▼                         ▼
┌──────────────────┐      ┌──────────────────┐      ┌──────────────────┐
│  API GATEWAY     │      │  API GATEWAY     │      │  API GATEWAY     │
│   Instance 1     │      │   Instance 2     │      │   Instance 3     │
│     :8000        │      │     :8000        │      │     :8000        │
│  (Internal)      │      │  (Internal)      │      │  (Internal)      │
└──────────────────┘      └──────────────────┘      └──────────────────┘
          │                         │                         │
          └─────────────────────────┴─────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│              TIER 2: nginx-be (Backend Load Balancer)                      │
│                        Internal Only                                        │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  • HTTP (Layer 7): chat, order, notification, persistence           │   │
│  │  • gRPC (Layer 4): user service                                     │   │
│  │  • Least-conn algorithm        • Auto-discovery                     │   │
│  │  • Health checks & failover    • Connection pooling (keepalive 32)  │   │
│  │  • Ports: 8001-8004, 50051     • No rate limiting (internal)        │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────┘
          │              │              │              │              │
          ▼              ▼              ▼              ▼              ▼
┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐
│ USER SERVICE │  │ CHAT SERVICE │  │ORDER SERVICE │  │NOTIFICATION  │  │ MESSAGE      │
│   (gRPC)     │  │ (WebSocket)  │  │(REST + Kafka)│  │   SERVICE    │  │ PERSISTENCE  │
│   :50051     │  │   :8001      │  │   :8002      │  │   :8003      │  │   :8004      │
└──────────────┘  └──────────────┘  └──────────────┘  └──────────────┘  └──────────────┘
        │                   │                   │                │               │
        │                   └──────────┬────────┴────────────────┴───────────────┘
        │                              ▼
        │           ┌──────────────────────────────────┐
        │           │   INFRASTRUCTURE LAYER           │
        │           │  ┌─────────────┐ ┌────────────┐  │         ┌──────────────────┐
        │           │  │   KAFKA     │ │   REDIS    │  │         │      MINIO       │
        │           │  │   :9092     │ │   :6379    │  │         │ (S3-compatible)  │
        │           │  └─────────────┘ └────────────┘  │         │  :9000, :9001    │
        │           │  ┌─────────────┐                 │         └──────────────────┘
        │           │  │  CASSANDRA  │                 │
        │           │  │   :9042     │                 │
        │           │  └─────────────┘                 │
        │           └──────────────────────────────────┘
        └────────────────────────────────────────────────┘
```

## 💾 Three-Tier Message Persistence Architecture

The chat system implements a **three-tier persistence strategy** for optimal performance and reliability:

### 1. Redis Cache (Hot Storage)
- **Stores**: 10 most recent messages per room
- **Purpose**: Fast initial message load on connect
- **Latency**: < 1ms
- **Expiry**: In-memory, ephemeral

### 2. Kafka Streaming (Event Pipeline)
- **Topics**: `chat.messages`, `chat.events`
- **Purpose**: Reliable event streaming to persistence layer
- **Throughput**: 100K+ messages/sec
- **Retention**: 7 days (configurable)

### 3. Cassandra (Cold Storage)
- **Stores**: Full unlimited message history
- **Purpose**: Long-term persistence, history queries
- **Latency**: 10-50ms (query dependent)
- **Scalability**: Linearly scalable

### Message Flow

```
User Message → Chat Service → ┬→ Redis (cache 10 latest)
                             ├→ Redis Pub/Sub (broadcast)
                             ├→ Kafka (stream) → Message Persistence Service → Cassandra (persist)
                             └→ Local WebSocket clients
```

### Benefits

✅ **Fast Recent Access**: Redis cache provides instant load of recent messages  
✅ **Full History**: Cassandra stores unlimited message history  
✅ **Reliable Delivery**: Kafka ensures messages are never lost  
✅ **Horizontal Scaling**: All components scale independently  
✅ **Separation of Concerns**: Cache, streaming, and persistence are decoupled  

## 📡 Communication Protocols Demonstrated

| Protocol | Service | Use Case | Pattern | When to Use |
|----------|---------|----------|---------|-------------|
| **REST** | API Gateway, Order Service | Public API, CRUD operations | Request-Response | Standard web APIs, CRUD |
| **gRPC** | User Service | Internal microservice communication | RPC with Protobuf | High-performance internal APIs |
| **WebSocket** | Chat Service | Real-time bidirectional chat | Full-duplex streaming | Real-time bidirectional communication |
| **SSE** | Notification Service | Server-push notifications | Unidirectional streaming | Server-to-client real-time updates |
| **Kafka** | Order → Notification, Chat → Persistence | Event-driven architecture | Pub/Sub messaging | Async event processing, decoupling |
| **Redis Pub/Sub** | Chat Service | Cross-instance messaging | In-memory messaging | Fast, ephemeral message broadcasting |
| **Cassandra** | Message Persistence Service | Full message history | NoSQL time-series | Unlimited storage, time-series queries |
| **S3 API** | MinIO | File storage & sharing | Object storage | File uploads, media storage, backups |

### Protocol Comparison

#### REST vs gRPC
- **REST**: Human-readable (JSON), browser-friendly, widely supported
- **gRPC**: Binary (Protobuf), 7x faster, strongly-typed, streaming support
- **Use REST for**: Public APIs, browser clients, third-party integrations
- **Use gRPC for**: Internal microservices, high-throughput, strict contracts

#### WebSocket vs SSE
- **WebSocket**: Bidirectional, full-duplex, custom protocols
- **SSE**: Unidirectional (server→client), auto-reconnect, HTTP-based
- **Use WebSocket for**: Chat, gaming, collaborative editing
- **Use SSE for**: Notifications, live feeds, monitoring dashboards

#### Kafka vs Redis Pub/Sub
- **Kafka**: Persistent, replay capability, exactly-once, high throughput
- **Redis Pub/Sub**: In-memory, fire-and-forget, sub-millisecond latency
- **Use Kafka for**: Event sourcing, audit logs, critical business events
- **Use Redis for**: Real-time chat, presence, ephemeral notifications

## 🚀 Quick Start

### Prerequisites

**Required:**
- **Docker Desktop** 24.x+ with Docker Compose v2.x
- **At least 6GB RAM** allocated to Docker
- **Ports available**: 8000-8004, 9000-9001, 9042, 9092, 6379, 50051

**Optional:**
- **Python 3.11+** for running client scripts locally
- **make** (GNU Make) for convenience commands
- **grpcurl** for testing gRPC endpoints
- **redis-cli** for Redis debugging

### 1. Start All Services

```bash
# Clone and navigate to project
cd microservices-poc

# Start all services
docker compose up --build

# Or run in detached mode
docker compose up --build -d

# View logs
docker compose logs -f
```

**Note**: First startup may take 60-90 seconds as Cassandra initializes.

### 2. Verify Services are Running

```bash
# Check all services are healthy
docker compose ps

# Quick health check
curl http://localhost:8000/health

# Or use the Makefile
make health
```

Expected output: All services should show "healthy" status.

### 3. Access the Web Client

Open your browser: **https://localhost** (HTTPS recommended) or **http://localhost**

**Browser Certificate Warning:**
Since we use a self-signed SSL certificate, your browser will show a security warning. This is expected and safe for development:
- Click "Advanced" → "Proceed to localhost (unsafe)"
- This certificate is auto-generated and used only for local HTTPS encryption

The web client provides an interactive UI to test all protocols:
- ✅ **REST API Testing** - Create users, manage orders
- 💬 **WebSocket Chat** - Real-time bidirectional messaging with file sharing
- 📁 **File Upload** - Share files with other users in chat rooms
- 🔔 **SSE Notifications** - Live order event stream
- 📊 **Real-time Monitoring** - Watch events flow through the system
- 👥 **Participant Display** - See who's in your chat room
- 🎨 **Modern UI** - Clean, responsive interface

**Try this:**
1. Open the web client in your browser
2. Create a new order in the "Orders" tab
3. Watch the notification appear instantly in the "Notifications" tab
4. Open the chat, connect to a room, and send messages
5. Try uploading a file and see it shared with other users in real-time

### 4. Access Management UIs (Optional)

All management UIs are accessible via HTTPS through nginx-fe:

```
Main App:        https://localhost
Redis UI:        https://localhost/redis-ui/
Kafka UI:        https://localhost/kafka-ui/
MinIO Console:   https://localhost/minio/  (user: minioadmin, pass: minioadmin)
```

**Benefits:**
- ✅ **Single URL** - No need to remember multiple ports
- ✅ **HTTPS Everywhere** - All traffic encrypted
- ✅ **Unified Access** - Same security context for all UIs

### 5. Run Python Client Scripts (Optional)

```bash
# Install client dependencies (if running locally)
pip install httpx websockets aiohttp grpcio grpcio-tools

# REST API client
python clients/rest_client.py

# WebSocket chat client
python clients/websocket_client.py

# SSE notifications client
python clients/sse_client.py

# gRPC client (requires proto compilation)
python clients/grpc_client.py

# Full demo script (runs all scenarios)
python scripts/demo.py
```

See [`clients/README.md`](clients/README.md) for detailed client documentation.

## 🏗️ Service Architecture

### Two-Tier Load Balancing

This POC implements a production-ready two-tier NGINX load balancing architecture for maximum scalability and separation of concerns.

### nginx-fe - Frontend Load Balancer (Ports 80, 443)

**Role**: Client-facing load balancer with SSL termination and UI proxy
**Tech**: NGINX 1.25 with SSL/TLS
**Patterns**: Load balancing, HTTPS encryption, session affinity, rate limiting, reverse proxy

The frontend load balancer provides:

**Features:**
- ✅ **SSL/TLS Encryption**: Auto-generated self-signed certificate (production-ready for real certs)
- ✅ **HTTP & HTTPS**: Dual support on ports 80 (HTTP) and 443 (HTTPS)
- ✅ **Load Distribution**: Routes traffic using least-connections algorithm
- ✅ **High Availability**: Automatic failover (max_fails=3, fail_timeout=30s)
- ✅ **Protocol Support**: REST, WebSocket (with sticky sessions), SSE
- ✅ **Rate Limiting**: 100 req/sec baseline, burst 200 (configurable)
- ✅ **Health Checks**: Monitors `/health` endpoint of all instances
- ✅ **Session Affinity**: IP-hash for WebSocket connections
- ✅ **Performance**: Keepalive connections, gzip compression, HTTP/2
- ✅ **Security**: HSTS headers, CSP, rate limiting, request size limits
- ✅ **Unified UI Proxy**: Access Redis UI, Kafka UI, MinIO Console via HTTPS

**Load Balancing Configuration:**

```nginx
# REST/SSE - Least connections
upstream api_gateway_cluster {
    least_conn;
    server api-gateway:8000 max_fails=3 fail_timeout=30s;
    keepalive 32;
}

# WebSocket - Session affinity
upstream api_gateway_websocket {
    ip_hash;  # Same client → same backend
    server api-gateway:8000 max_fails=3 fail_timeout=30s;
}
```

**Why Layer 7 (not Layer 4)?**
- ✅ Understands HTTP, WebSocket, SSE protocols
- ✅ Can route based on URL paths (/api/, /ws/, /events/)
- ✅ SSL/TLS termination
- ✅ Application-level health checks
- ✅ Session affinity for WebSocket

**Scaling:**
```bash
# Scale API Gateway instances
docker compose up -d --scale api-gateway=5

# nginx-fe automatically routes to all instances!
# No configuration change needed
```

See `nginx-fe/README.md` for detailed configuration.

### nginx-be - Backend Load Balancer (Internal)

**Role**: Internal load balancer for backend microservices
**Tech**: NGINX 1.25 with stream module for gRPC
**Patterns**: Service discovery, load balancing, health checking, protocol translation

The backend load balancer provides:

**Features:**
- ✅ **HTTP Load Balancing** (Layer 7): Chat, Order, Notification, Persistence services
- ✅ **gRPC Load Balancing** (Layer 4): User service via stream module
- ✅ **Auto-Discovery**: Automatically detects scaled backend instances via Docker DNS
- ✅ **Health Checks**: Monitors backend service health with automatic failover
- ✅ **Connection Pooling**: Keepalive connections to reduce overhead
- ✅ **Internal Only**: Not exposed to host, only accessible from API Gateway
- ✅ **No Rate Limiting**: Trusts internal traffic for maximum throughput

**Ports:**
- `8001` → Chat Service cluster (WebSocket)
- `8002` → Order Service cluster (REST)
- `8003` → Notification Service cluster (SSE)
- `8004` → Message Persistence Service cluster (HTTP)
- `50051` → User Service cluster (gRPC)
- `8090` → nginx-be health check

**Scaling:**
```bash
# Scale any backend service
docker compose up -d --scale chat-service=3
docker compose up -d --scale order-service=5

# nginx-be automatically routes to all instances!
# No configuration change needed
```

**Why Two Tiers?**
- **Separation of Concerns**: nginx-fe handles client security, nginx-be handles service routing
- **Independent Scaling**: Scale frontend and backend services independently
- **Simplified API Gateway**: No need to manage backend instance discovery
- **Better Observability**: Separate logs for client vs internal traffic
- **Security Isolation**: Backend services not directly accessible from outside

See `nginx-be/README.md` for detailed configuration.

### API Gateway (Port 8000)

**Role**: Main entry point and protocol aggregator (behind NGINX)  
**Tech**: FastAPI, Uvicorn  
**Patterns**: API Gateway, Backend-for-Frontend (BFF)

The gateway exposes multiple protocols and routes requests to backend services:
- REST endpoints → Order Service (HTTP) and User Service (gRPC)
- WebSocket → Chat Service (WebSocket passthrough)
- SSE → Notification Service (HTTP streaming)
- Static files → Web client (HTML/CSS/JS)

**Key Endpoints:**

| Method | Path | Description | Backend |
|--------|------|-------------|---------|
| GET | `/health` | Health check with status of all services | - |
| GET | `/` | Serve web client UI | Static files |
| **Users API** | | | |
| GET | `/api/users` | List all users | gRPC → User Service |
| POST | `/api/users` | Create new user | gRPC → User Service |
| **Orders API** | | | |
| GET | `/api/orders` | List orders (with filters) | REST → Order Service |
| POST | `/api/orders` | Create order (triggers Kafka event) | REST → Order Service |
| **Chat API** | | | |
| WS | `/ws/chat/{room}?username={user}` | Join chat room | WS → Chat Service |
| POST | `/api/chat/files/simple-upload` | Upload file | Chat Service |
| GET | `/api/chat/rooms/{room}/history` | Get message history | Chat Service → Cassandra |
| **Server-Sent Events** | | | |
| GET | `/events/notifications` | Real-time notification stream | SSE → Notification Service |
| GET | `/events/orders` | Real-time order event stream | SSE → Notification Service |

### User Service - gRPC (Port 50051)

**Role**: User management microservice  
**Tech**: gRPC, Protocol Buffers  
**Patterns**: Internal API, strongly-typed contracts, streaming RPC

**Why gRPC?**
- **Performance**: 7x faster than REST for internal APIs
- **Strong typing**: Auto-generated clients, compile-time safety
- **Streaming**: Support for bidirectional streaming (StreamUsers)
- **Polyglot**: Can be called from any language

**Proto Definition:**
```protobuf
service UserService {
    rpc GetUser(GetUserRequest) returns (User);
    rpc ListUsers(ListUsersRequest) returns (ListUsersResponse);
    rpc CreateUser(CreateUserRequest) returns (User);
    rpc DeleteUser(DeleteUserRequest) returns (DeleteUserResponse);
    rpc StreamUsers(StreamUsersRequest) returns (stream User);
}
```

### Chat Service - WebSocket (Port 8001)

**Role**: Real-time bidirectional chat with persistence  
**Tech**: FastAPI WebSocket, Redis, Kafka, Cassandra, MinIO  
**Patterns**: Pub/Sub, event streaming, horizontal scaling, three-tier storage

**Features:**
- ✅ **Multiple chat rooms** - Isolated conversations
- 👥 **User presence** - Join/leave notifications, participant display
- 📢 **Message broadcasting** - All users in room receive messages
- 💓 **Health checks** - Automatic connection monitoring
- 📁 **File sharing** - Upload/download files via MinIO S3
- 💾 **Redis cache** - 10 most recent messages for fast load
- 📡 **Kafka streaming** - All events streamed for persistence
- 📚 **Full history** - Query unlimited history from Cassandra
- 🚀 **Horizontal Scaling** - Run multiple instances with Redis coordination
- 🔄 **Redis Pub/Sub** - Cross-instance message broadcasting
- 🌍 **Distributed State** - Global room tracking across all instances

**Redis Data Structures:**

| Structure | Key Pattern | Purpose |
|-----------|------------|---------|
| **SET** | `chat:rooms` | All active rooms globally |
| **SET** | `chat:room:{room}:users` | Users in a specific room |
| **HASH** | `chat:user:{username}` | User metadata (room, instance, timestamp) |
| **LIST** | `chat:room:{room}:history` | Last 10 messages (FIFO) |
| **Pub/Sub** | `chat:room:{room}` | Real-time message broadcasting |

**File Upload:**
- **Storage**: MinIO (S3-compatible object storage)
- **Format**: `{room}/{file_id}/{filename}`
- **Access**: Presigned URLs (7-day expiration)
- **Max Size**: Configurable (100MB recommended)
- **Supported**: All file types

**Horizontal Scaling Demo:**
```bash
# Scale to 3 instances (ports 8011-8013)
docker compose up -d --scale chat-service=3

# Connect user Alice to instance 1
python clients/websocket_client.py --room general --user alice --port 8011

# Connect user Bob to instance 2 (different instance!)
python clients/websocket_client.py --room general --user bob --port 8012

# Messages flow between instances via Redis pub/sub!

# Monitor Redis activity
docker exec -it redis redis-cli MONITOR

# Check active rooms
docker exec -it redis redis-cli SMEMBERS chat:rooms

# See who's in a room
docker exec -it redis redis-cli SMEMBERS chat:room:general:users
```

### Order Service - REST + Kafka (Port 8002)

**Role**: Order management and event publishing  
**Tech**: FastAPI, aiokafka, Redis  
**Patterns**: Event sourcing, CQRS, domain events

**Why Kafka?**
- **Decoupling**: Order service doesn't know about notification service
- **Reliability**: Events are persisted, guaranteed delivery
- **Scalability**: Multiple consumers can process same events
- **Audit trail**: All business events are logged permanently
- **Replay**: Can reprocess historical events

**Event-Driven Flow:**
```
1. POST /orders
   ↓
2. Order saved to database
   ↓
3. Event published to Kafka topic: orders.created
   ↓
4. Background task publishes to Redis for immediate SSE
   ↓
5. Notification Service consumes Kafka event
   ↓
6. Notification broadcast via SSE to all connected clients
```

### Notification Service - Kafka Consumer + SSE (Port 8003)

**Role**: Event processing and real-time notification broadcasting  
**Tech**: FastAPI, aiokafka, Redis Pub/Sub, Server-Sent Events  
**Patterns**: Event consumer, message transformation, push notifications

**Why SSE (Server-Sent Events)?**
- **Unidirectional**: Server pushes updates to clients
- **Auto-reconnect**: Built-in reconnection logic in browsers
- **HTTP-based**: Works through firewalls, proxies, CDNs
- **EventSource API**: Native browser support
- **Lightweight**: Less overhead than WebSocket for one-way communication

**Dual-Path Delivery:**

1. **Kafka Path** (reliable, persistent):
   - Order Service → Kafka → Notification Service → SSE clients
   - Guaranteed delivery, survives restarts
   - ~10-50ms latency

2. **Redis Path** (fast, ephemeral):
   - Order Service → Redis Pub/Sub → Notification Service → SSE clients
   - Fire-and-forget, immediate delivery
   - ~1-5ms latency

### Message Persistence Service (Port 8004)

**Role**: Persistent storage of all chat messages and events  
**Tech**: FastAPI, aiokafka, Cassandra  
**Patterns**: Event sourcing, time-series storage, unlimited history

**Why Cassandra?**
- **Time-series optimized**: Perfect for append-only message history
- **Horizontal scalability**: Linear scale-out for storage and throughput
- **High write performance**: Optimized for write-heavy workloads
- **Partition by room**: Natural data distribution
- **No single point of failure**: Distributed architecture

**Data Model:**
```sql
CREATE TABLE chat.messages (
    room text,              -- Partition key
    message_id uuid,        -- Clustering key
    timestamp timestamp,    -- Clustering key (DESC order)
    username text,
    content text,
    type text,
    file_url text,
    file_name text,
    file_size bigint,
    PRIMARY KEY ((room), timestamp, message_id)
) WITH CLUSTERING ORDER BY (timestamp DESC, message_id DESC);
```

**Features:**
- ✅ **Unlimited Storage** - No message limits (vs 10 in Redis)
- 📊 **Time-series Queries** - Efficient range queries by timestamp
- 🔄 **Event Replay** - Full history for auditing, compliance
- 📈 **Pagination Support** - Query by limit and before timestamp
- 💾 **All Event Types** - Messages, files, joins, leaves, system events
- ⚡ **Fast Writes** - 10-50ms write latency via Kafka buffering

### Infrastructure Services

#### Apache Kafka 4.1.1 (Port 9092)
- **Mode**: KRaft (no ZooKeeper dependency)
- **Topics**: `chat.messages`, `chat.events`, `orders.created`, `orders.updated`, `notifications`
- **Replication**: Single broker (development mode)
- **Persistence**: Data stored in Docker volume
- **UI**: Kafka UI available at http://localhost:8080

#### Redis 7.x (Port 6379)
- **Role**: Cache, Pub/Sub, distributed state
- **Persistence**: AOF enabled
- **Used by**: Chat Service (horizontal scaling), Notification Service (pub/sub)
- **UI**: Redis UI available at http://localhost:5540

#### Cassandra 5.0 (Port 9042)
- **Role**: Long-term message persistence
- **Keyspace**: `chat`
- **Tables**: `messages` (time-series, partition by room)
- **Replication**: SimpleStrategy, factor 1 (development)

#### MinIO (Ports 9000, 9001)
- **Role**: S3-compatible object storage
- **Bucket**: `chat-files`
- **Access**: Presigned URLs (7-day expiration)
- **Console**: http://localhost:9001 (admin/minioadmin)

## 🧪 Testing Scenarios

### Scenario 0: NGINX Load Balancer Testing **NEW!**

**What it demonstrates**: Layer 7 load balancing, failover, session affinity

```bash
# 1. Start with load balancer (3 API Gateway instances)
docker compose up --build -d

# 2. Run comprehensive test suite
bash nginx/test-lb.sh

# 3. Test load distribution manually
echo "Sending 20 requests to see load distribution..."
for i in {1..20}; do
  curl -s http://localhost/health | jq -r '.service' 
done

# 4. Watch NGINX logs to see upstream routing
docker compose logs -f nginx &

# 5. Send requests and watch distribution
for i in {1..10}; do
  curl -s http://localhost/health
  sleep 1
done

# 6. Test failover
# List gateway instances
docker compose ps api-gateway

# Stop one instance
docker compose stop <instance-name-from-above>

# Requests still work! NGINX routes around failed instance
curl http://localhost/health

# Check NGINX logs - no routing to stopped instance
docker compose logs nginx | grep "upstream:" | tail -20

# 7. Restart instance
docker compose start <instance-name>

# 8. Scale up to 5 instances
make scale-gateway

# NGINX automatically picks up new instances!
docker compose ps api-gateway

# 9. Test WebSocket with session affinity
# Open two terminals and connect same user
# Terminal 1:
python clients/websocket_client.py --room test --user alice

# Terminal 2 (same IP = same backend instance):
python clients/websocket_client.py --room test --user bob

# Messages delivered correctly via sticky sessions!
```

**Behind the scenes:**
- NGINX uses `least_conn` algorithm for REST/SSE
- NGINX uses `ip_hash` for WebSocket (session affinity)
- Failed instances automatically removed from pool
- Health checks run every connection attempt
- Rate limiting protects against abuse

### Scenario 1: REST API → gRPC Internal Communication

**What it demonstrates**: API Gateway translating REST to gRPC

```bash
# 1. Create a user via REST API (through NGINX load balancer)
curl -X POST http://localhost/api/users \
  -H "Content-Type: application/json" \
  -d '{"name": "Alice Johnson", "email": "alice@example.com", "role": "customer"}' | jq

# Behind the scenes:
# - NGINX load balancer receives REST request
# - Routes to one of 3 API Gateway instances (least_conn)
# - API Gateway converts to gRPC call
# - User Service handles via Protocol Buffers
# - Response converted back to JSON
# - NGINX returns response to client

# 2. List all users
curl http://localhost/api/users | jq

# 3. Get specific user
USER_ID=$(curl -s http://localhost/api/users | jq -r '.[0].id')
curl http://localhost/api/users/$USER_ID | jq
```

### Scenario 2: WebSocket Chat with File Sharing and Persistence

**What it demonstrates**: Real-time bidirectional communication + Redis scaling + Kafka/Cassandra persistence

```bash
# Terminal 1 - Alice on instance 1 (port 8011)
python clients/websocket_client.py --room general --user alice --port 8011

# Terminal 2 - Bob on instance 2 (port 8012)  
python clients/websocket_client.py --room general --user bob --port 8012

# Type messages in either terminal:
# - Both users see messages in real-time (cross-instance!)
# - Messages cached in Redis (10 latest)
# - Messages streamed to Kafka
# - Messages persisted to Cassandra

# Test persistence:
# 1. Send 15+ messages
# 2. Check Redis: docker exec -it redis redis-cli LLEN chat:room:general:history
#    → Should show: 10 (limited cache)
# 3. Check Cassandra:
#    docker exec -it cassandra cqlsh -e "SELECT COUNT(*) FROM chat.messages WHERE room='general';"
#    → Should show: 15+ (full history)
# 4. Query history via API:
#    curl "http://localhost/api/chat/rooms/general/history?limit=100"
```

### Scenario 3: End-to-End Event-Driven Flow (Order → Kafka → SSE)

**What it demonstrates**: Event-driven architecture, decoupled services, real-time notifications

```bash
# Terminal 1 - Start SSE listener
python clients/sse_client.py

# Terminal 2 - Create an order
curl -X POST http://localhost/api/orders \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": "user-1",
    "items": [
      {"product": "MacBook Pro M3", "quantity": 1, "price": 2499.99}
    ],
    "shipping_address": "123 Main St, San Francisco, CA 94105"
  }' | jq

# Watch Terminal 1 - You'll see notification appear instantly!

# Behind the scenes:
# 1. POST /api/orders
#    ├─> Order Service saves order
#    ├─> Publish to Kafka topic: orders.created
#    └─> Publish to Redis: notifications (immediate delivery)
# 2. Notification Service (has 2 listeners):
#    ├─> Kafka Consumer: Receives event (reliable)
#    └─> Redis Subscriber: Receives event (fast)
# 3. Notification Service transforms event → Notification
# 4. SSE broadcast to all connected clients
```

### Scenario 4: File Upload and Sharing

**What it demonstrates**: MinIO S3 storage, presigned URLs, real-time file sharing

```bash
# 1. Open web client in two browser windows
# Window 1: Connect as "alice" to room "files"
# Window 2: Connect as "bob" to room "files"

# 2. In Window 1, upload a file via the file upload area
# - File stored in MinIO
# - Presigned URL generated (7-day expiration)
# - File message broadcasted via Redis pub/sub
# - Bob in Window 2 sees the file message with download button

# Or test via API:
curl -X POST "http://localhost:8001/files/simple-upload?room=test&username=tester" \
  -F "file=@test.pdf"

# Response includes download URL:
# {
#   "success": true,
#   "file_id": "uuid",
#   "download_url": "http://minio:9000/chat-files/...",
#   "file_size": 1234567
# }
```

### Scenario 5: Horizontal Scaling Test

**What it demonstrates**: Chat service scales linearly, Redis coordination

```bash
# 1. Scale chat service to 3 instances
docker compose up -d --scale chat-service=3

# 2. Verify instances running
docker compose ps chat-service

# 3. Connect users to different instances
# Terminal 1: Alice on instance 1
python clients/websocket_client.py --room lobby --user alice --port 8011

# Terminal 2: Bob on instance 2
python clients/websocket_client.py --room lobby --user bob --port 8012

# Terminal 3: Charlie on instance 3
python clients/websocket_client.py --room lobby --user charlie --port 8013

# 4. Send messages - all users see messages across all instances!

# 5. Monitor Redis
docker exec -it redis redis-cli MONITOR
# Watch as messages flow: PUBLISH, SUBSCRIBE, SMEMBERS, SADD, etc.
```

## ⚙️ Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| **Kafka** | | |
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:9092` | Kafka broker address |
| **Redis** | | |
| `REDIS_URL` | `redis://redis:6379` | Redis connection URL |
| **Cassandra** | | |
| `CASSANDRA_HOSTS` | `cassandra` | Cassandra contact points |
| `CASSANDRA_PORT` | `9042` | Cassandra CQL port |
| `CASSANDRA_KEYSPACE` | `chat` | Keyspace for messages |
| **MinIO** | | |
| `MINIO_ENDPOINT` | `minio:9000` | MinIO API endpoint |
| `MINIO_ACCESS_KEY` | `minioadmin` | MinIO access key |
| `MINIO_SECRET_KEY` | `minioadmin` | MinIO secret key |
| `MINIO_BUCKET` | `chat-files` | Bucket for file uploads |
| **Health Check** | | |
| `HEALTH_CHECK_INTERVAL` | `60` | Connection check interval (seconds) |
| **Service Discovery** | | |
| `USER_SERVICE_GRPC` | `nginx-be:50051` | gRPC address (via backend LB) |
| `ORDER_SERVICE_URL` | `http://nginx-be:8002` | REST address (via backend LB) |
| `NOTIFICATION_SERVICE_URL` | `http://nginx-be:8003` | SSE address (via backend LB) |
| `CHAT_SERVICE_URL` | `http://nginx-be:8001` | WebSocket address (via backend LB) |
| `MESSAGE_PERSISTENCE_SERVICE_URL` | `http://nginx-be:8004` | Persistence API (via backend LB) |

### Port Mapping

#### External Access (Published to Host)

| Service | Port | Protocol | Access URL | Notes |
|---------|------|----------|------------|-------|
| **nginx-fe** | **80** | HTTP | http://localhost | Main HTTP entry point |
| **nginx-fe** | **443** | HTTPS | **https://localhost** | **Main HTTPS entry point (recommended)** |
| Kafka UI | 8080 | HTTP | Direct: http://localhost:8080<br>**Proxy: https://localhost/kafka-ui/** | **Use proxy (recommended)** |
| Redis UI | 5540 | HTTP | Direct: http://localhost:5540<br>**Proxy: https://localhost/redis-ui/** | **Use proxy (recommended)** |
| MinIO Console | 9001 | HTTP | Direct: http://localhost:9001<br>**Proxy: https://localhost/minio/** | **Use proxy (recommended)** |
| MinIO API | 9000 | S3 API | http://localhost:9000 | Direct S3 API access |
| Cassandra | 9042 | CQL | localhost:9042 | Direct database access |
| Kafka | 9092 | Kafka | localhost:9092 | Direct broker access |
| Redis | 6379 | Redis | localhost:6379 | Direct cache access |

#### Internal Services (Not Exposed to Host)

| Service | Port | Protocol | Access | Notes |
|---------|------|----------|--------|-------|
| **nginx-be** | 8001-8004, 50051 | HTTP/gRPC | Internal only | Backend load balancer |
| API Gateway | 8000 | HTTP/WS/SSE | Via nginx-fe | 3 instances by default |
| User Service | 50051 | gRPC | Via nginx-be | Accessed through backend LB |
| Chat Service | 8001 | WebSocket | Via nginx-be | Scalable instances |
| Order Service | 8002 | HTTP | Via nginx-be | Scalable instances |
| Notification Service | 8003 | HTTP/SSE | Via nginx-be | Scalable instances |
| Message Persistence | 8004 | HTTP | Via nginx-be | Scalable instances |

**Access Pattern:**
```
External Client → nginx-fe (80/443) → API Gateway → nginx-be → Backend Services
```

**Important Changes:**
- **HTTPS is now the primary access method** - Use `https://localhost` instead of `http://localhost`
- **Management UIs accessible via HTTPS proxy** - Single unified access point
- **All client traffic goes through nginx-fe** - Not directly to API Gateway
- **Backend services accessed via nginx-be** - API Gateway uses backend load balancer

### Horizontal Scaling

```bash
# Scale any service
docker compose up -d --scale <service-name>=<count>

# Examples:
docker compose up -d --scale api-gateway=5      # 5 gateway instances (behind NGINX)
docker compose up -d --scale chat-service=3     # 3 chat instances
docker compose up -d --scale order-service=3    # 3 order instances  
docker compose up -d --scale notification-service=2  # 2 notification consumers

# Using Makefile
make scale-gateway   # Scales API Gateway to 5
make scale-chat      # Scales chat to 3

# Check scaling status
docker compose ps
make status
```

**Which services can scale horizontally?**
- ✅ **API Gateway** - nginx-fe load balances across all instances automatically
- ✅ **Chat Service** - nginx-be load balances with Redis coordination
- ✅ **Order Service** - nginx-be load balances, stateless design
- ✅ **Notification Service** - nginx-be load balances, Kafka consumer groups
- ✅ **Message Persistence Service** - nginx-be load balances via Kafka partitions
- ✅ **User Service (gRPC)** - nginx-be stream module handles gRPC load balancing
- ❌ **nginx-fe** - Single frontend LB (can use external LB for HA)
- ❌ **nginx-be** - Single backend LB (sufficient for internal traffic)

## 📊 Observability & Monitoring

### Service Health Checks

All services expose `/health` endpoints:

```bash
# Check all services
curl http://localhost:8000/health | jq   # API Gateway
curl http://localhost:8002/health | jq   # Order Service
curl http://localhost:8003/health | jq   # Notification Service
curl http://localhost:8004/health | jq   # Message Persistence
curl http://localhost:8011/health | jq   # Chat Service

# gRPC service
grpcurl -plaintext localhost:50051 list
```

### Kafka Monitoring

```bash
# Kafka UI (via nginx-fe proxy - recommended)
open https://localhost/kafka-ui/

# Or direct access
open http://localhost:8080

# List topics
docker exec kafka kafka-topics.sh \
  --bootstrap-server localhost:9092 --list

# Consume messages
docker exec kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic chat.messages \
  --from-beginning

# Check consumer group lag
docker exec kafka kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe --group notification-service
```

### Redis Monitoring

```bash
# Redis UI (via nginx-fe proxy - recommended)
open https://localhost/redis-ui/

# Or direct access
open http://localhost:5540

# Redis CLI
docker exec -it redis redis-cli

# Inside Redis CLI:
> MONITOR                     # Watch all commands in real-time
> INFO stats                  # Get statistics
> SMEMBERS chat:rooms         # List all rooms
> SMEMBERS chat:room:general:users  # Users in room
> LRANGE chat:room:general:history 0 9  # Recent messages
> HGETALL chat:user:alice     # User metadata
```

### Cassandra Monitoring

```bash
# Cassandra CLI
docker exec -it cassandra cqlsh

# Inside cqlsh:
> USE chat;
> SELECT COUNT(*) FROM messages WHERE room = 'general';
> SELECT * FROM messages WHERE room = 'general' LIMIT 10;
> DESCRIBE TABLE messages;
```

### MinIO Monitoring

```bash
# MinIO Console (via nginx-fe proxy - recommended)
open https://localhost/minio/
# Username: minioadmin
# Password: minioadmin

# Or direct access
open http://localhost:9001

# Check bucket
docker exec minio mc ls local/chat-files

# Check health
curl http://localhost:9000/minio/health/live
```

## 🔧 Troubleshooting

### Common Issues

#### 1. Services won't start

```bash
# Check Docker resources
# Ensure Docker has at least 6GB RAM allocated

# Check service logs
docker compose logs <service-name>

# Restart all services
docker compose down -v
docker compose up --build
```

#### 2. Kafka not starting

```bash
# Check Kafka logs
docker compose logs kafka | tail -50

# Verify Kafka is running
docker compose exec kafka kafka-broker-api-versions.sh \
  --bootstrap-server localhost:9092

# Clean restart
docker compose down -v
docker compose up --build kafka
```

#### 3. Cassandra connection issues

```bash
# Cassandra takes 60-90 seconds to initialize on first start
# Wait for this log message:
docker compose logs cassandra | grep "Starting listening for CQL clients"

# If taking too long, restart:
docker compose restart cassandra
```

#### 4. Redis connection issues

```bash
# Check Redis
docker exec redis redis-cli ping
# Expected: PONG

# Check Redis connections
docker exec redis redis-cli CLIENT LIST

# Restart Redis
docker compose restart redis
```

#### 5. WebSocket disconnecting immediately

**This was fixed!** If you still experience issues:

```bash
# 1. Check chat service logs
docker compose logs chat-service | grep -E "error|fail|crash"

# 2. Verify Redis is running
docker compose ps redis

# 3. Rebuild chat service
docker compose build chat-service
docker compose up -d chat-service

# 4. Verify the fix
# Open http://localhost:8000
# Try connecting to chat
# Connection should establish and remain stable
```

**Root cause was**: Missing `python-multipart` dependency causing service to crash on startup.

#### 6. File upload fails

```bash
# Verify MinIO is running
docker compose ps minio

# Check MinIO console
open http://localhost:9001

# Check chat service MinIO connection
curl http://localhost:8011/health | jq '.minio_connected'
# Should be: true

# Restart MinIO
docker compose restart minio
```

#### 7. No messages in Cassandra

```bash
# 1. Check message persistence service
docker compose ps message-persistence-service

# 2. Check logs for errors
docker compose logs message-persistence-service

# 3. Verify Kafka has messages
open http://localhost:8080  # Kafka UI

# 4. Restart persistence service
docker compose restart message-persistence-service
```

### Fresh Start

If all else fails:

```bash
# Stop everything and remove volumes
docker compose down -v

# Remove all containers, networks, volumes
docker system prune -a --volumes

# Rebuild from scratch
docker compose up --build

# Wait 60-90 seconds for Cassandra to initialize
# Check health
curl http://localhost:8000/health
```

## 🛠️ Technology Stack

| Component | Technology | Version | Purpose |
|-----------|------------|---------|---------|
| **API Gateway** | FastAPI | 0.109+ | REST/WebSocket/SSE endpoints |
| **gRPC Framework** | grpcio | 1.60+ | Internal service communication |
| **Message Broker** | Apache Kafka | 4.1.1 (KRaft) | Event streaming, pub/sub |
| **Cache/Pub-Sub** | Redis | 7.x | Distributed state, real-time messaging |
| **NoSQL Database** | Cassandra | 5.0 | Time-series message storage |
| **Object Storage** | MinIO | Latest | S3-compatible file storage |
| **Serialization** | Protocol Buffers | 3.x | gRPC message format |
| **Kafka Client** | aiokafka | 0.10+ | Async Kafka producer/consumer |
| **HTTP Client** | httpx | 0.26+ | Async HTTP requests |
| **WebSocket** | websockets | 12.x | Real-time bidirectional |
| **ASGI Server** | Uvicorn | 0.27+ | High-performance Python server |
| **Container** | Docker | 24.x | Containerization |
| **Orchestration** | Docker Compose | v2.x | Multi-container management |
| **Python** | CPython | 3.11+ | Runtime |

### Why These Technologies?

#### FastAPI
- **Async native**: Perfect for I/O-bound microservices
- **Auto docs**: OpenAPI/Swagger UI out of the box
- **Type safety**: Pydantic validation
- **Performance**: One of the fastest Python frameworks

#### Apache Kafka 4.1.1 (KRaft)
- **No ZooKeeper**: Simplified architecture, faster startup
- **Durability**: Events persisted to disk
- **Scalability**: Millions of events/second
- **Replay**: Can reprocess historical events

#### Redis 7.x
- **Speed**: Sub-millisecond latency
- **Versatile**: Cache, Pub/Sub, data structures
- **Horizontal scaling**: Ready for Redis Cluster

#### Cassandra 5.0
- **Time-series optimized**: Perfect for chat messages
- **Linear scalability**: Add nodes for more capacity
- **No SPOF**: Distributed, no master
- **High write throughput**: Append-only workloads

#### MinIO
- **S3-compatible**: Drop-in replacement for AWS S3
- **Self-hosted**: No cloud vendor lock-in
- **High performance**: Optimized for large files
- **Erasure coding**: Data protection

## 💡 Key Takeaways

After studying this PoC, you should understand:

✅ **When to use each protocol**
- REST for public APIs, CRUD
- gRPC for internal high-performance APIs
- WebSocket for bidirectional real-time
- SSE for unidirectional real-time (notifications)
- Kafka for event-driven, decoupled architecture
- S3 API for scalable object storage

✅ **How to scale microservices horizontally**
- Stateless services (Order Service)
- Distributed state with Redis (Chat Service)
- Consumer groups with Kafka (Notification Service)
- Cassandra clustering (Message Persistence)

✅ **Three-tier persistence architecture**
- Hot storage: Redis cache (10 messages, <1ms)
- Event pipeline: Kafka streaming (reliable, replay)
- Cold storage: Cassandra (unlimited, 10-50ms)

✅ **Event-driven architecture patterns**
- Event sourcing (all orders as events)
- CQRS principles (separate read/write models)
- Eventual consistency
- Dual-path delivery (Kafka + Redis)

✅ **Production-ready patterns**
- Health checks and graceful shutdown
- Error handling and retry logic
- Service discovery via Docker networking
- API Gateway pattern for protocol translation
- File storage with presigned URLs
- Message persistence with time-series DB

## 🎓 What's Next?

### Production Enhancements

To make this production-ready, consider adding:

1. **Authentication & Authorization**
   - JWT tokens for API Gateway
   - mTLS for gRPC
   - OAuth2/OpenID Connect

2. **Observability**
   - Distributed tracing (Jaeger, Zipkin)
   - Metrics (Prometheus + Grafana)
   - Centralized logging (ELK stack)

3. **Resilience**
   - Circuit breakers (Resilience4j)
   - Rate limiting
   - Retry policies with exponential backoff

4. **Service Mesh**
   - Istio or Linkerd for traffic management
   - Automatic mTLS
   - Advanced routing

5. **Database**
   - PostgreSQL for order persistence
   - MongoDB for user profiles
   - Event store for event sourcing

6. **Kafka Enhancements**
   - Multi-broker cluster (3+ nodes)
   - Schema registry (Avro schemas)
   - Kafka Connect for integrations

7. **Redis Enhancements**
   - Redis Cluster for HA
   - Redis Sentinel for failover
   - Proper persistence strategy

8. **Cassandra Enhancements**
   - Multi-node cluster (3+ nodes)
   - Replication factor 3
   - Proper compaction strategy

9. **Kubernetes Deployment**
   - Helm charts
   - Horizontal Pod Autoscaling
   - StatefulSets for Kafka/Redis/Cassandra

### Recommended Exercises

1. **Add a new service** (e.g., Payment Service) that consumes order events
2. **Implement authentication** with JWT tokens
3. **Add a database** (PostgreSQL) to replace in-memory storage
4. **Create Kubernetes manifests** and deploy to Minikube
5. **Add distributed tracing** to visualize request flow
6. **Implement the Saga pattern** for distributed transactions
7. **Add GraphQL** as an alternative to REST
8. **Enhance file upload** with virus scanning and thumbnails

## 📝 License

MIT License - Feel free to use this PoC for learning and experimentation.

## 🌟 Star This Repo

If you found this helpful, please give it a star! ⭐

---

**Created**: January 2026  
**Last Updated**: January 2026  
**Tech Stack**: Python 3.11 | FastAPI | gRPC | Kafka 4.1.1 | Redis 7 | Cassandra 5 | MinIO | Docker  
**Author**: Microservices Communication PoC

**Questions?** Open an issue or discussion on GitHub!

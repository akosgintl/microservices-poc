# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

This is a microservices proof-of-concept demonstrating modern communication protocols (REST, gRPC, WebSocket, SSE, Kafka) with **two-tier NGINX load balancing**, Redis coordination, and three-tier message persistence (Redis/Kafka/Cassandra).

**Architecture:**
- **nginx-fe** (port 80): External clients → API Gateway instances (frontend load balancer)
- **nginx-be** (internal): API Gateway → Backend services (backend load balancer)

This creates a fully load-balanced architecture where both frontend and backend traffic is distributed.

## Common Development Commands

### Service Management
```bash
# Start all services (3 API Gateway instances behind NGINX)
make up
# Or: docker compose up --build -d

# Start with logs
make up-logs

# View logs
make logs                    # All services
make logs-<service-name>     # Specific service (e.g., make logs-chat-service)

# Restart services
make restart                 # All
make restart-<service>       # Specific (e.g., make restart-api-gateway)

# Stop services
make down

# Clean restart (removes volumes)
make clean
docker compose up --build
```

### Health Checks
```bash
# Check via NGINX load balancer (main entry point)
make health
# Or: curl http://localhost/health | python -m json.tool

# Check individual services
make health-order-service
make health-chat-service
```

### Testing
```bash
# Install Python client dependencies
make install-clients
# Or: pip install -r clients/requirements.txt

# Test individual protocols
make test-rest      # REST API
make test-ws        # WebSocket chat
make test-sse       # Server-Sent Events
make test-grpc      # gRPC

# Use web client
# Open http://localhost (served via NGINX on port 80)
```

### Horizontal Scaling
```bash
# Scale API Gateway (NGINX auto-routes to all instances)
make scale-gateway
# Or: docker compose up -d --scale api-gateway=5

# Scale chat service (Redis coordinates state)
make scale-chat
# Or: docker compose up -d --scale chat-service=3

# Monitor NGINX load balancers
make nginx-fe-logs      # Frontend LB logs
make nginx-fe-status    # Frontend LB status
make nginx-be-logs      # Backend LB logs
make nginx-be-status    # Backend LB status

# Check scaling status
make status
docker compose ps
```

### Infrastructure Monitoring
```bash
# Kafka
make kafka-topics                          # List topics
make kafka-consume-chat.messages           # Consume from topic
# Kafka UI: https://localhost/kafka-ui/ (via nginx-fe)

# Redis
make redis-cli                             # Open Redis CLI
make chat-rooms                            # Show active rooms
make chat-users-<room>                     # Users in room (e.g., make chat-users-general)
make chat-redis-monitor                    # Monitor Redis commands
# Redis UI: https://localhost/redis-ui/ (via nginx-fe)

# Cassandra (message history)
make cassandra-cli                         # Open CQL shell
make cassandra-describe                    # Show schema

# MinIO (S3-compatible storage)
make minio-cli                             # Show MinIO info
# MinIO Console: https://localhost/minio/ (minioadmin/minioadmin, via nginx-fe)
```

### Troubleshooting
```bash
# View resource usage
make stats              # One-time snapshot
make stats-live         # Continuous updates

# Check service status
make ps
make status

# Fresh start (WARNING: removes all data)
docker compose down -v
docker compose up --build
```

## Architecture

### Service Communication Flow

```
Client → nginx-fe (port 80) [Frontend Load Balancer]
         ↓
    API Gateway instances (3x) [load balanced by nginx-fe]
         ↓
    nginx-be [Backend Load Balancer - internal only]
         ↓
    ├─→ User Service (gRPC :50051) [load balanced]
    ├─→ Chat Service (WebSocket :8001) [load balanced] → Redis + Kafka + MinIO
    ├─→ Order Service (REST :8002) [load balanced] → Kafka
    ├─→ Notification Service (SSE :8003) [load balanced] ← Kafka + Redis
    └─→ Message Persistence Service (:8004) [load balanced] ← Kafka → Cassandra
```

**Two-tier load balancing:**
1. **nginx-fe** distributes external traffic across API Gateway instances
2. **nginx-be** distributes internal traffic across backend service instances

### Key Patterns

**nginx-fe - Frontend Load Balancer** (Layer 7, port 80):
- Distributes client traffic to API Gateway instances
- `least_conn` algorithm for REST/SSE
- `ip_hash` for WebSocket (session affinity)
- Health checks on `/health` endpoint
- Rate limiting: 100 req/s baseline, burst 200
- Externally accessible

**nginx-be - Backend Load Balancer** (Layer 7 + Layer 4):
- Distributes API Gateway traffic to backend services
- `least_conn` algorithm for all HTTP services
- gRPC load balancing via stream module (Layer 4)
- Internal only - not exposed to host
- Listens on ports: 8001 (chat), 8002 (order), 8003 (notification), 8004 (persistence), 50051 (gRPC)
- Health check endpoint: 8090

**Protocol Translation** (API Gateway):
- REST → gRPC (User Service via nginx-be)
- WebSocket passthrough with proxy (Chat Service via nginx-be)
- SSE streaming proxy (Notification Service via nginx-be)
- All backend communication goes through nginx-be

**Three-Tier Message Persistence**:
1. **Hot cache** (Redis): 10 most recent messages per room, <1ms latency
2. **Event stream** (Kafka): Reliable delivery, replay capability, 7-day retention
3. **Cold storage** (Cassandra): Unlimited history, time-series queries, 10-50ms latency

**Horizontal Scaling**:
- **API Gateway**: nginx-fe routes across all instances automatically
- **Backend Services**: nginx-be routes across all scaled instances
- **Chat Service**: Redis pub/sub for cross-instance messaging, distributed state tracking
- **Order/Notification Services**: Stateless, can scale freely
- **Message Persistence**: Kafka consumer group balances partition load
- **User Service (gRPC)**: nginx-be handles gRPC load balancing via stream module

### gRPC Integration

**Proto files:** `user-service/protos/user.proto`

**Code generation** (automatic during Docker build):
```bash
# In service Dockerfile:
python -m grpc_tools.protoc \
  -I./protos \
  --python_out=. \
  --grpc_python_out=. \
  protos/user.proto
```

**Client usage** (in API Gateway):
```python
# API Gateway translates REST → gRPC
user_client = UserServiceClient(USER_SERVICE_GRPC)
await user_client.connect()
user = await user_client.get_user(user_id)
```

### Service-Specific Notes

**Chat Service** (`chat-service/main.py`):
- WebSocket connections require `username` query param: `/ws/chat/{room}?username={name}`
- File uploads via MinIO with presigned URLs (7-day expiration)
- Health check pings every 60s to keep connections alive
- Redis data structures:
  - `chat:rooms` (SET): All active rooms
  - `chat:room:{room}:users` (SET): Users in room
  - `chat:user:{username}` (HASH): User metadata
  - `chat:room:{room}:history` (LIST): Last 10 messages (FIFO)
  - Pub/Sub channel: `chat:room:{room}`

**Order Service** (`order-service/main.py`):
- Creates Kafka events on order creation/updates
- Topics: `orders.created`, `orders.updated`
- Dual-path delivery: Kafka (persistent) + Redis pub/sub (fast)

**Notification Service** (`notification-service/main.py`):
- Consumes from Kafka topics
- Broadcasts via SSE to connected clients
- Listens on both Kafka and Redis for fast delivery

**Message Persistence Service** (`message-persistence-service/main.py`):
- Consumes `chat.messages` and `chat.events` from Kafka
- Writes to Cassandra table: `chat.messages`
- Partition key: `room`, clustering key: `timestamp DESC, message_id DESC`
- API endpoints:
  - `GET /api/rooms/{room}/history?limit=100&before={iso_timestamp}`
  - `GET /api/rooms/{room}/count`

### Environment Variables

Key configurations in `docker-compose.yml`:

```yaml
# Kafka
KAFKA_BOOTSTRAP_SERVERS: kafka:9092

# Redis
REDIS_URL: redis://redis:6379

# Cassandra
CASSANDRA_HOSTS: cassandra
CASSANDRA_KEYSPACE: chat
CASSANDRA_PORT: 9042

# MinIO
MINIO_ENDPOINT: minio:9000
MINIO_ACCESS_KEY: minioadmin
MINIO_SECRET_KEY: minioadmin
MINIO_BUCKET: chat-files

# Backend Service Discovery (via nginx-be load balancer)
USER_SERVICE_GRPC: nginx-be:50051
CHAT_SERVICE_WS: ws://nginx-be:8001
ORDER_SERVICE_URL: http://nginx-be:8002
NOTIFICATION_SERVICE_URL: http://nginx-be:8003
MESSAGE_PERSISTENCE_SERVICE_URL: http://nginx-be:8004
```

**Important:** All API Gateway instances connect to backend services through nginx-be, not directly.

## Port Mapping

**External Access:**
- **80** - nginx-fe HTTP (main entry point)
- **443** - nginx-fe HTTPS (main entry point with SSL)
- 9000 - MinIO API (direct access)
- 9042 - Cassandra CQL
- 9092 - Kafka broker
- 6379 - Redis

**Management UIs (via nginx-fe HTTPS proxy):**
- https://localhost/redis-ui/ - Redis UI (proxied from redis-ui:5540)
- https://localhost/kafka-ui/ - Kafka UI (proxied from kafka-ui:8080)
- https://localhost/minio/ - MinIO Console (proxied from minio:9001)

**Direct UI Access (legacy, use proxied URLs instead):**
- 5540 - Redis UI (direct)
- 8080 - Kafka UI (direct)
- 9001 - MinIO Console (direct)

**Internal Load Balancer Ports** (nginx-be - not exposed to host):
- 8001 - Chat Service LB
- 8002 - Order Service LB
- 8003 - Notification Service LB
- 8004 - Message Persistence LB
- 50051 - User Service gRPC LB
- 8090 - nginx-be health check

**Internal Service Ports** (accessed via nginx-be):
- API Gateway: 8000 (accessed via nginx-fe)
- Chat Service: 8001 (scaled to 8010-8019, accessed via nginx-be)
- Order Service: 8002 (scaled to 8020-8029, accessed via nginx-be)
- Notification Service: 8003 (scaled to 8030-8039, accessed via nginx-be)
- Message Persistence: 8004 (scaled to 8040-8049, accessed via nginx-be)
- User Service: 50051 (gRPC, accessed via nginx-be)

**Traffic Flow:**
1. External clients → nginx-fe (port 80) → API Gateway instances
2. API Gateway → nginx-be (internal) → Backend services

## Code Patterns

### Adding a New REST Endpoint

1. If proxying to existing service, add route in `api-gateway/main.py`
2. Use `httpx.AsyncClient` for HTTP calls to backend
3. Follow existing patterns (see `/api/orders` endpoints)

### Adding a New gRPC Method

1. Update `user-service/protos/user.proto`
2. Implement in `user-service/main.py` (UserServiceImpl class)
3. Rebuild container to regenerate Python code
4. Update API Gateway client if exposing via REST

### Adding a Kafka Topic

1. Auto-created on first publish (see `KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"`)
2. Publish from producer service (Order/Chat)
3. Consume in Notification/Persistence service
4. Monitor: `make kafka-topics` or Kafka UI

### Horizontal Scaling Considerations

**Stateless services** (Order, Notification):
- No special handling needed
- Just scale: `docker compose up -d --scale order-service=N`

**Stateful services** (Chat):
- Use Redis for shared state
- Pub/Sub for cross-instance messaging
- Redis data structures must be instance-agnostic

**API Gateway scaling**:
- nginx-fe handles routing automatically
- No code changes needed
- Test with: `make scale-gateway`

**Backend service scaling**:
- nginx-be handles routing automatically
- Scale any backend service: `docker compose up -d --scale <service>=N`
- nginx-be auto-discovers scaled instances via Docker DNS

## Important Notes

- **First startup**: Cassandra takes 60-90 seconds to initialize
- **HTTPS**: Self-signed SSL certificate generated automatically (browsers will warn - this is expected)
- **Management UIs**: Access via nginx-fe proxy (https://localhost/redis-ui/, /kafka-ui/, /minio/)
- **WebSocket connections**: Go through nginx-fe with `ip_hash` for session affinity, then through nginx-be to chat service instances
- **File uploads**: Max 100MB (configurable via `client_max_body_size` in nginx.conf)
- **Kafka consumer groups**: Use unique group IDs per service to avoid conflicts
- **Redis persistence**: AOF enabled, data survives restarts
- **Cassandra replication**: Single node in dev (SimpleStrategy, RF=1)

## Testing Scenarios

See README.md for detailed testing scenarios, including:
- Scenario 0: NGINX load balancer testing
- Scenario 1: REST → gRPC communication
- Scenario 2: WebSocket chat with horizontal scaling
- Scenario 3: Event-driven flow (Order → Kafka → SSE)
- Scenario 4: File upload and sharing
- Scenario 5: Horizontal scaling test

## Common Issues

**Services won't start:**
- Check Docker has at least 6GB RAM allocated
- Wait 60-90s for Cassandra to initialize
- Check logs: `make logs-<service>`

**WebSocket disconnects:**
- Fixed in current version (missing `python-multipart` dependency)
- If issues persist, check NGINX logs: `make nginx-logs`

**Kafka not available:**
- Wait for health check: `docker compose logs kafka | grep "started"`
- Verify: `make kafka-topics`

**Redis connection errors:**
- Check Redis health: `docker exec redis redis-cli ping`
- Should return: `PONG`

.PHONY: up down logs logs-tail logs-tail-% stats stats-live build clean clean-volumes clean-all demo test-rest test-ws test-sse test-grpc install-clients health health-% kafka-topics kafka-consume-% redis-cli scale-% scale-chat scale-gateway test-chat-scaling chat-redis-monitor chat-rooms chat-users-% minio-cli cassandra-cli cassandra-describe kafka-ui redis-ui nginx-status nginx-logs help up-logs up-lb restart restart-% ps status

# Default target - show help
.DEFAULT_GOAL := help

# Start all services with load balancer (3 API Gateway instances)
up:
	docker compose up --build -d
	@echo "Services starting with NGINX load balancer..."
	@echo "Main entry point: http://localhost (port 80)"
	@echo "Wait a few seconds for services to be healthy..."

# Start with load balancing configuration and resource limits
up-lb:
	docker compose -f docker-compose.yml -f docker-compose.lb.yml up --build -d
	@echo "Services starting with enhanced load balancing..."
	@echo "API Gateway: 3 instances behind NGINX"
	@echo "Access: http://localhost"

# Start with logs visible
up-logs:
	docker compose up --build

# Restart all services
restart:
	docker compose restart

# Restart specific service
restart-%:
	docker compose restart $*

# Stop all services
down:
	docker compose down

# Show service status
ps:
	docker compose ps

# Show all services status with formatting
status:
	@echo "=== Service Status ==="
	@docker compose ps --format "table {{.Name}}\t{{.Status}}\t{{.Ports}}"

# View logs
logs:
	docker compose logs -f

# View specific service logs
logs-%:
	docker compose logs -f $*

# Rebuild all services
build:
	docker compose build --no-cache

# Clean up everything
clean:
	docker compose down -v --rmi local
	@echo "Cleaning up generated files..."
	@if [ -d "clients/generated" ]; then rm -rf clients/generated/; fi
	@echo "Clean complete!"

# Clean volumes only (preserve images)
clean-volumes:
	docker compose down -v
	@echo "Volumes cleaned!"

# Deep clean (remove everything including images)
clean-all:
	docker compose down -v --rmi all
	@if [ -d "clients/generated" ]; then rm -rf clients/generated/; fi
	@echo "Full cleanup complete!"


# Run the full demo
# demo:
# 	python scripts/demo.py
# Note: demo script not yet implemented

# Run individual client tests
test-rest:
	python clients/rest_client.py

test-ws:
	python clients/websocket_client.py --demo

test-sse:
	python clients/sse_client.py --demo

test-grpc:
	python clients/grpc_client.py

# Install client dependencies
install-clients:
	pip install -r clients/requirements.txt

# Check service health (via NGINX load balancer)
health:
	@echo "Checking health via NGINX load balancer (port 80)..."
	@curl -s http://localhost/health | python -m json.tool

# Check individual service health
health-%:
	@echo "Checking health for $*..."
	@case "$*" in \
		nginx) curl -s http://localhost/health | python -m json.tool ;; \
		order-service) curl -s http://localhost:8002/health | python -m json.tool ;; \
		notification-service) curl -s http://localhost:8003/health | python -m json.tool ;; \
		message-persistence-service) curl -s http://localhost:8004/health | python -m json.tool ;; \
		chat-service) curl -s http://localhost:8010/health | python -m json.tool ;; \
		*) echo "Unknown service: $*" ;; \
	esac

# View last N lines of logs (default 50)
logs-tail:
	docker compose logs --tail=50

# View last N lines of specific service logs
logs-tail-%:
	docker compose logs --tail=50 $*

# Show resource usage statistics
stats:
	docker stats --no-stream

# Show resource usage with continuous updates
stats-live:
	docker stats

# List Kafka topics
kafka-topics:
	docker compose exec kafka kafka-topics.sh --bootstrap-server localhost:9092 --list

# Consume from Kafka topic
kafka-consume-%:
	docker compose exec kafka kafka-console-consumer.sh \
		--bootstrap-server localhost:9092 \
		--topic $* \
		--from-beginning

# Redis CLI
redis-cli:
	docker compose exec redis redis-cli

# Scale a service
scale-%:
	docker compose up -d --scale $*=3

# Chat-specific commands for testing horizontal scaling
scale-chat:
	@echo "Scaling chat-service to 3 instances..."
	docker compose up -d --scale chat-service=3
	@echo "Chat service scaled! Instances running on ports 8010-8012"

# Scale API Gateway instances (behind NGINX)
scale-gateway:
	@echo "Scaling api-gateway to 5 instances..."
	docker compose up -d --scale api-gateway=5
	@echo "API Gateway scaled to 5 instances behind NGINX load balancer"
	@echo "NGINX will automatically distribute traffic across all instances"

# NGINX monitoring commands
nginx-fe-status:
	@echo "NGINX Frontend Status:"
	@curl -s http://localhost/nginx-status || echo "Status endpoint not accessible from outside container"
	@echo ""
	@echo "API Gateway instances:"
	@docker compose ps api-gateway

nginx-fe-logs:
	docker compose logs -f nginx-fe

nginx-be-status:
	@echo "NGINX Backend Status:"
	@docker compose exec nginx-be wget -qO- http://localhost:8090/health || echo "Failed to get status"
	@echo ""
	@echo "Backend service status:"
	@docker compose ps user-service chat-service order-service notification-service message-persistence-service

nginx-be-logs:
	docker compose logs -f nginx-be

# Backwards compatibility
nginx-status: nginx-fe-status
nginx-logs: nginx-fe-logs

# test-chat-scaling:
# 	@echo "Testing Redis-powered horizontal scaling..."
# 	python chat-service/test_scaling.py
# Note: test_scaling.py script not yet implemented

chat-redis-monitor:
	@echo "Monitoring Redis commands (Ctrl+C to stop)..."
	docker compose exec redis redis-cli MONITOR

chat-rooms:
	@echo "Current chat rooms in Redis:"
	@docker compose exec redis redis-cli SMEMBERS chat:rooms

chat-users-%:
	@echo "Users in room '$*':"
	@docker compose exec redis redis-cli SMEMBERS chat:room:$*:users

# MinIO commands
minio-cli:
	@echo "MinIO Console: http://localhost:9001"
	@echo "MinIO API: http://localhost:9000"
	@echo "Default credentials: minioadmin/minioadmin"

# Cassandra commands
cassandra-cli:
	docker compose exec cassandra cqlsh

cassandra-describe:
	@echo "Cassandra keyspace and tables:"
	docker compose exec cassandra cqlsh -e "DESCRIBE KEYSPACES; DESCRIBE TABLES;"

# UI Access commands (via nginx-fe HTTPS proxy)
ui-redis:
	@echo "Opening Redis UI via nginx-fe..."
	@echo "URL: https://localhost/redis-ui/"
	@echo "Note: Browser will warn about self-signed certificate - this is expected"

ui-kafka:
	@echo "Opening Kafka UI via nginx-fe..."
	@echo "URL: https://localhost/kafka-ui/"
	@echo "Note: Browser will warn about self-signed certificate - this is expected"

ui-minio:
	@echo "Opening MinIO Console via nginx-fe..."
	@echo "URL: https://localhost/minio/"
	@echo "Login: minioadmin / minioadmin"
	@echo "Note: Browser will warn about self-signed certificate - this is expected"

# Legacy direct access (use ui-* commands instead)
kafka-ui:
	@echo "Opening Kafka UI at http://localhost:8080 (direct access)"
	@echo "Recommended: Use 'make ui-kafka' for HTTPS access via nginx-fe"

redis-ui:
	@echo "Opening Redis UI at http://localhost:5540 (direct access)"
	@echo "Recommended: Use 'make ui-redis' for HTTPS access via nginx-fe"

# Help
help:
	@echo "Available commands:"
	@echo ""
	@echo "Service Management:"
	@echo "  make up              - Start all services with NGINX load balancer"
	@echo "  make up-lb           - Start with load balancing config (resource limits)"
	@echo "  make up-logs         - Start all services with logs"
	@echo "  make down            - Stop all services"
	@echo "  make restart         - Restart all services"
	@echo "  make restart-<svc>   - Restart specific service"
	@echo "  make ps              - Show service status"
	@echo "  make status          - Show formatted service status"
	@echo "  make logs            - View all logs"
	@echo "  make logs-<svc>      - View specific service logs"
	@echo "  make logs-tail       - View last 50 lines of all logs"
	@echo "  make logs-tail-<svc> - View last 50 lines of specific service"
	@echo "  make stats           - Show resource usage (one-time)"
	@echo "  make stats-live      - Show resource usage (live updates)"
	@echo "  make build           - Rebuild all services"
	@echo "  make clean           - Remove all containers, volumes, and local images"
	@echo "  make clean-volumes   - Remove volumes only (preserve images)"
	@echo "  make clean-all       - Deep clean (remove everything)"
	@echo "  make health          - Check API gateway health"
	@echo "  make health-<svc>    - Check specific service health"
	@echo ""
	@echo "Testing:"
	@echo "  make install-clients - Install Python client dependencies"
	@echo "  make test-rest       - Test REST API"
	@echo "  make test-ws         - Test WebSocket"
	@echo "  make test-sse        - Test SSE"
	@echo "  make test-grpc       - Test gRPC"
	@echo ""
	@echo "Load Balancing & Scaling:"
	@echo "  make scale-gateway   - Scale API Gateway to 5 instances"
	@echo "  make scale-chat      - Scale chat-service to 3 instances"
	@echo "  make scale-<svc>=N   - Scale any service to N instances"
	@echo "  make nginx-fe-status - Show NGINX frontend and gateway status"
	@echo "  make nginx-fe-logs   - View NGINX frontend logs"
	@echo "  make nginx-be-status - Show NGINX backend and service status"
	@echo "  make nginx-be-logs   - View NGINX backend logs"
	@echo "  make nginx-status    - Show NGINX frontend status (alias)"
	@echo "  make nginx-logs      - View NGINX frontend logs (alias)"
	@echo "  make chat-rooms      - Show all active rooms in Redis"
	@echo "  make chat-users-<room> - Show users in a specific room"
	@echo "  make chat-redis-monitor - Monitor Redis commands in real-time"
	@echo ""
	@echo "Infrastructure Tools:"
	@echo "  make kafka-topics    - List Kafka topics"
	@echo "  make kafka-consume-<topic> - Consume from Kafka topic"
	@echo "  make redis-cli       - Open Redis CLI"
	@echo "  make cassandra-cli   - Open Cassandra CQL shell"
	@echo "  make cassandra-describe - Describe Cassandra schema"
	@echo ""
	@echo "Management UIs (via HTTPS):"
	@echo "  make ui-redis        - Open Redis UI (https://localhost/redis-ui/)"
	@echo "  make ui-kafka        - Open Kafka UI (https://localhost/kafka-ui/)"
	@echo "  make ui-minio        - Open MinIO Console (https://localhost/minio/)"
	@echo "  make kafka-ui        - Show direct Kafka UI URL (legacy)"
	@echo "  make redis-ui        - Show direct Redis UI URL (legacy)"
	@echo "  make minio-cli       - Show MinIO console info"
	@echo ""
	@echo "Examples:"
	@echo "  make logs-chat-service       - View chat service logs"
	@echo "  make restart-api-gateway     - Restart API gateway"
	@echo "  make kafka-consume-orders.created - Consume order events"
	@echo "  make chat-users-general      - Show users in 'general' room"
	@echo "  make scale-order-service=5   - Scale order service to 5 instances"


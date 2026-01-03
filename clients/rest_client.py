#!/usr/bin/env python3
"""
REST API Client
Demonstrates: HTTP requests to Users and Orders APIs, health checks, chat history queries
"""
import asyncio
import json
from datetime import datetime

import httpx

API_BASE = "http://localhost:8000"


async def main():
    print("=" * 60)
    print("REST API Client Demo")
    print("=" * 60)
    
    async with httpx.AsyncClient(base_url=API_BASE, timeout=30.0) as client:
        # Health check (enhanced with service status)
        print("\n[1] Health Check - All Services")
        response = await client.get("/health")
        health_data = response.json()
        print(f"    Gateway Status: {health_data.get('status', 'unknown')}")
        
        # Show backend service health
        services = health_data.get("services", {})
        if services:
            print("    Backend Services:")
            for service, info in services.items():
                status_icon = "✅" if info.get("status") == "healthy" else "❌"
                print(f"      {status_icon} {service}: {info.get('status', 'unknown')}")
        
        # List users (REST → gRPC)
        print("\n[2] List Users (REST → gRPC)")
        response = await client.get("/api/users")
        data = response.json()
        print(f"    Found {data['total']} users:")
        for user in data['users'][:3]:
            print(f"      - {user['name']} ({user['email']}) - {user['role']}")
        
        # Create user (REST → gRPC)
        print("\n[3] Create User (REST → gRPC)")
        new_user = {
            "name": f"Test User {datetime.now().strftime('%H%M%S')}",
            "email": f"test{datetime.now().timestamp():.0f}@example.com",
            "password": "password123",
            "role": "user"
        }
        response = await client.post("/api/users", json=new_user)
        if response.status_code == 201:
            user = response.json()
            print(f"    Created: {user['name']} (ID: {user['id'][:8]}...)")
            created_user_id = user['id']
        else:
            print(f"    Error: {response.status_code} - {response.text}")
            created_user_id = None
        
        # List orders
        print("\n[4] List Orders")
        response = await client.get("/api/orders")
        orders = response.json()
        print(f"    Found {len(orders)} orders:")
        for order in orders[:3]:
            print(f"      - {order['id'][:8]}... | {order['status']} | ${order['total']}")
        
        # Get order statistics
        print("\n[5] Order Statistics")
        response = await client.get("/api/orders/stats/summary")
        if response.status_code == 200:
            stats = response.json()
            print(f"    Total Orders: {stats.get('total', 0)}")
            print(f"    Total Revenue: ${stats.get('total_revenue', 0):.2f}")
            print(f"    Average Order Value: ${stats.get('average_order_value', 0):.2f}")
            status_breakdown = stats.get('by_status', {})
            if status_breakdown:
                print("    By Status:")
                for status, count in status_breakdown.items():
                    print(f"      - {status}: {count}")
        
        # Create order (triggers Kafka event)
        print("\n[6] Create Order (REST → Kafka)")
        new_order = {
            "user_id": created_user_id or "user-1",
            "items": [
                {"product": "Keyboard", "quantity": 2, "price": 79.99},
                {"product": "Monitor", "quantity": 1, "price": 299.99}
            ],
            "shipping_address": "456 Test Ave, Demo City"
        }
        response = await client.post("/api/orders", json=new_order)
        if response.status_code == 201:
            order = response.json()
            print(f"    Created order: {order['id'][:8]}...")
            print(f"    Total: ${order['total']}")
            print(f"    Status: {order['status']}")
            print("    → Kafka event 'orders.created' published!")
            print("    → Redis pub/sub notification sent!")
            created_order_id = order['id']
        else:
            print(f"    Error: {response.status_code}")
            created_order_id = None
        
        # Update order status (triggers Kafka event)
        if created_order_id:
            print("\n[7] Update Order Status (REST → Kafka)")
            update = {"status": "confirmed", "notes": "Payment received"}
            response = await client.put(f"/api/orders/{created_order_id}/status", json=update)
            if response.status_code == 200:
                order = response.json()
                print(f"    Updated: {order['status']}")
                print("    → Kafka event 'orders.updated' published!")
        
        # Get specific order
        if created_order_id:
            print("\n[8] Get Specific Order")
            response = await client.get(f"/api/orders/{created_order_id}")
            order = response.json()
            print(f"    Order ID: {order['id']}")
            print(f"    User ID: {order['user_id']}")
            print(f"    Items: {len(order['items'])}")
            print(f"    Status: {order['status']}")
            print(f"    Total: ${order['total']}")
        
        # Query chat rooms
        print("\n[9] List Active Chat Rooms")
        response = await client.get("/api/chat/rooms")
        if response.status_code == 200:
            rooms_data = response.json()
            
            # Handle both list and dict responses
            if isinstance(rooms_data, dict):
                # If it's a dict, it might be {"rooms": [...]} or {"room_name": {...}, ...}
                if 'rooms' in rooms_data:
                    rooms = rooms_data['rooms']
                else:
                    # Dict of rooms, convert to list
                    rooms = list(rooms_data.values())
            else:
                rooms = rooms_data
            
            print(f"    Found {len(rooms)} active rooms:")
            for room in list(rooms)[:5]:  # Convert to list and slice
                if isinstance(room, dict):
                    room_name = room.get('name', 'unknown')
                    user_count = len(room.get('users', []))
                    print(f"      - {room_name}: {user_count} users")
                else:
                    print(f"      - {room}")
        else:
            print(f"    No active rooms or error: {response.status_code}")
        
        # Query chat history (Cassandra)
        print("\n[10] Query Chat History (from Cassandra)")
        test_room = "general"
        response = await client.get(f"/api/chat/rooms/{test_room}/history?limit=5")
        if response.status_code == 200:
            history = response.json()
            messages = history.get("messages", [])
            total = history.get("total", 0)
            print(f"    Room: {test_room}")
            print(f"    Total messages in Cassandra: {total}")
            print(f"    Latest 5 messages:")
            for msg in messages:
                timestamp = msg.get("timestamp", "")[:19]
                username = msg.get("username", "unknown")
                msg_type = msg.get("type", "message")
                if msg_type == "file":
                    file_name = msg.get("file_name", "file")
                    print(f"      [{timestamp}] {username}: 📁 {file_name}")
                else:
                    content = msg.get("content", "")[:50]
                    print(f"      [{timestamp}] {username}: {content}")
        else:
            print(f"    No history available or error: {response.status_code}")
        
        # Test message persistence service health
        print("\n[11] Message Persistence Service Health")
        try:
            response = await client.get("http://localhost:8004/health", timeout=5.0)
            if response.status_code == 200:
                health = response.json()
                print(f"    Status: {health.get('status', 'unknown')}")
                print(f"    Kafka Connected: {health.get('kafka_connected', False)}")
                print(f"    Cassandra Connected: {health.get('cassandra_connected', False)}")
                print(f"    Messages Processed: {health.get('messages_processed', 0)}")
        except Exception as e:
            print(f"    ⚠️  Service not available: {str(e)[:50]}")
    
    print("\n" * 2 + "=" * 60)
    print("REST API Demo Complete!")
    print("=" * 60)
    print("\nKey Observations:")
    print("  ✅ REST → gRPC internal communication")
    print("  ✅ Event-driven architecture with Kafka")
    print("  ✅ Real-time notifications via Redis pub/sub")
    print("  ✅ Three-tier message persistence (Redis/Kafka/Cassandra)")
    print("  ✅ Comprehensive health checks")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())

#!/usr/bin/env python3
"""
Server-Sent Events (SSE) Client
Demonstrates: Unidirectional server-push notifications with real-time order tracking
"""
import asyncio
import json
import argparse
from datetime import datetime

import aiohttp


async def sse_client(endpoint: str = "/events/notifications", server_url: str = "http://localhost:8000", user_id: str = None):
    """Connect to SSE endpoint and receive notifications"""
    
    url = f"{server_url}{endpoint}"
    if user_id:
        url += f"?user_id={user_id}"
    
    print(f"\n{'=' * 60}")
    print(f"Server-Sent Events (SSE) Client")
    print(f"{'=' * 60}")
    print(f"Connecting to: {url}")
    print(f"Endpoint: {endpoint}")
    if user_id:
        print(f"Filtering by user: {user_id}")
    print(f"Press Ctrl+C to stop listening")
    print(f"{'=' * 60}\n")
    
    event_count = 0
    heartbeat_count = 0
    notification_count = 0
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                if response.status != 200:
                    print(f"[Error] HTTP {response.status}")
                    return
                
                print("✓ Connected to notification stream")
                print("Waiting for events...\n")
                
                buffer = ""
                async for chunk in response.content.iter_any():
                    buffer += chunk.decode('utf-8')
                    
                    # Process complete events (double newline separated)
                    while "\n\n" in buffer:
                        event_data, buffer = buffer.split("\n\n", 1)
                        
                        if not event_data.strip():
                            continue
                        
                        # Parse SSE format
                        event_type = "message"
                        data = None
                        event_id = None
                        
                        for line in event_data.split("\n"):
                            if line.startswith("event:"):
                                event_type = line[6:].strip()
                            elif line.startswith("data:"):
                                try:
                                    data = json.loads(line[5:].strip())
                                except json.JSONDecodeError:
                                    data = line[5:].strip()
                            elif line.startswith("id:"):
                                event_id = line[3:].strip()
                        
                        event_count += 1
                        timestamp = datetime.now().strftime("%H:%M:%S")
                        
                        if event_type == "connected":
                            print(f"[{timestamp}] 🔗 {data.get('message', 'Connected')}")
                            print(f"           Connection ID: {event_id}\n")
                            
                        elif event_type == "heartbeat":
                            heartbeat_count += 1
                            # Show every 5th heartbeat to reduce noise
                            if heartbeat_count % 5 == 0:
                                print(f"[{timestamp}] 💓 Heartbeat ({heartbeat_count} total)")
                            
                        elif event_type == "notification":
                            notification_count += 1
                            print(f"\n{'─' * 60}")
                            print(f"[{timestamp}] 🔔 NOTIFICATION #{notification_count}")
                            print(f"{'─' * 60}")
                            print(f"  Type:    {data.get('type', 'unknown')}")
                            print(f"  Title:   {data.get('title', 'N/A')}")
                            print(f"  Message: {data.get('message', 'N/A')}")
                            
                            # Additional details based on notification type
                            if data.get('user_id'):
                                print(f"  User:    {data.get('user_id')}")
                            if data.get('order_id'):
                                print(f"  Order:   {data.get('order_id')[:8]}...")
                            if data.get('status'):
                                print(f"  Status:  {data.get('status')}")
                            if data.get('timestamp'):
                                print(f"  Time:    {data.get('timestamp')[:19]}")
                            
                            print(f"{'─' * 60}\n")
                            
                        elif event_type.startswith("order."):
                            notification_count += 1
                            # Order-specific event styling
                            icon = "📦" if event_type == "order.created" else "🔄"
                            print(f"\n{'─' * 60}")
                            print(f"[{timestamp}] {icon} ORDER EVENT: {event_type.upper()}")
                            print(f"{'─' * 60}")
                            print(f"  Title:   {data.get('title', 'N/A')}")
                            print(f"  Message: {data.get('message', 'N/A')}")
                            
                            if data.get('order_id'):
                                print(f"  Order:   {data.get('order_id')[:8]}...")
                            if data.get('user_id'):
                                print(f"  User:    {data.get('user_id')}")
                            if data.get('total'):
                                print(f"  Total:   ${data.get('total'):.2f}")
                            if data.get('old_status') and data.get('new_status'):
                                print(f"  Change:  {data.get('old_status')} → {data.get('new_status')}")
                            
                            print(f"{'─' * 60}\n")
                            
                        else:
                            print(f"[{timestamp}] 📨 Event: {event_type}")
                            if data:
                                data_preview = json.dumps(data, indent=2)[:200]
                                print(f"    Data: {data_preview}...")
                            print()
        
    except aiohttp.ClientError as e:
        print(f"\n[Connection Error] {e}")
        print("Make sure services are running: docker compose up")
    except asyncio.CancelledError:
        print("\n\n[Cancelled]")
    except KeyboardInterrupt:
        print("\n\n[Stopped by user]")
    
    print(f"\n{'=' * 60}")
    print(f"Session Summary:")
    print(f"  Total Events: {event_count}")
    print(f"  Notifications: {notification_count}")
    print(f"  Heartbeats: {heartbeat_count}")
    print(f"{'=' * 60}")


async def demo_mode(server_url: str = "http://localhost:8000"):
    """Run demo showing SSE with automatic order creation"""
    
    print(f"\n{'=' * 60}")
    print(f"SSE Demo Mode - Will create orders to trigger events")
    print(f"{'=' * 60}\n")
    
    async def create_orders():
        """Create orders to generate SSE events"""
        await asyncio.sleep(3)  # Wait for SSE connection
        
        async with aiohttp.ClientSession() as session:
            for i in range(3):
                order_data = {
                    "user_id": f"demo-user-{i}",
                    "items": [
                        {
                            "product": f"Demo Product {i+1}", 
                            "quantity": i+1, 
                            "price": 99.99 * (i+1)
                        }
                    ],
                    "shipping_address": f"{100+i} Demo Street, Test City"
                }
                
                print(f"\n[Demo] Creating order {i+1}/3...")
                try:
                    async with session.post(
                        f"{server_url}/api/orders",
                        json=order_data,
                        headers={"Content-Type": "application/json"}
                    ) as response:
                        if response.status == 201:
                            order = await response.json()
                            print(f"[Demo] ✓ Order created: {order['id'][:8]}...")
                            print(f"[Demo]   Total: ${order['total']:.2f}")
                            print(f"[Demo]   Status: {order['status']}")
                        else:
                            print(f"[Demo] ✗ Failed to create order: {response.status}")
                except Exception as e:
                    print(f"[Demo] ✗ Error: {str(e)[:50]}")
                
                await asyncio.sleep(3)  # Wait between orders
            
            print("\n[Demo] All orders created. Waiting for final notifications...")
            await asyncio.sleep(5)
    
    # Run SSE listener and order creator concurrently
    sse_task = asyncio.create_task(sse_client(server_url=server_url))
    order_task = asyncio.create_task(create_orders())
    
    try:
        await asyncio.gather(order_task)
        await asyncio.sleep(5)  # Wait for events
        sse_task.cancel()
        await sse_task
    except asyncio.CancelledError:
        pass


def main():
    parser = argparse.ArgumentParser(
        description="SSE Notifications Client - Real-time server-push events",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Listen to all notifications
  python sse_client.py
  
  # Filter notifications by user
  python sse_client.py --user user-123
  
  # Listen to order events only
  python sse_client.py --endpoint /events/orders
  
  # Run automated demo (creates orders)
  python sse_client.py --demo

Available Endpoints:
  /events/notifications  - All notifications (default)
  /events/orders         - Order-specific events only

Features:
  - Real-time server-to-client notifications
  - Automatic reconnection on disconnect
  - Event filtering by user
  - Order lifecycle tracking
  - Kafka + Redis pub/sub event delivery
        """
    )
    parser.add_argument("--server", "-s", default="http://localhost:8000", help="Server URL (API Gateway)")
    parser.add_argument("--user", "-u", default=None, help="Filter by user ID")
    parser.add_argument("--endpoint", "-e", default="/events/notifications", 
                       choices=["/events/notifications", "/events/orders"],
                       help="SSE endpoint to connect to")
    parser.add_argument("--demo", "-d", action="store_true", help="Run demo mode (auto-creates orders)")
    
    args = parser.parse_args()
    
    if args.demo:
        asyncio.run(demo_mode(args.server))
    else:
        asyncio.run(sse_client(args.endpoint, args.server, args.user))


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
WebSocket Chat Client
Demonstrates: Bidirectional real-time communication, file sharing, participants display
"""
import asyncio
import json
import sys
import argparse
import os
from datetime import datetime
from pathlib import Path

import websockets


async def chat_client(room: str, username: str, server_url: str = "ws://localhost:8000"):
    """Connect to chat room and enable bidirectional messaging"""
    
    uri = f"{server_url}/ws/chat/{room}?username={username}"
    print(f"\n{'=' * 60}")
    print(f"WebSocket Chat Client")
    print(f"{'=' * 60}")
    print(f"Connecting to room '{room}' as '{username}'...")
    print(f"URI: {uri}")
    print(f"{'=' * 60}\n")
    
    participants = []
    ping_count = 0
    
    try:
        async with websockets.connect(uri) as websocket:
            print(f"✓ Connected to chat room '{room}'")
            print("\nCommands:")
            print("  - Type messages and press Enter to send")
            print("  - Type 'quit' to exit")
            print("  - Type '/users' to show participants")
            print("  - Type '/help' for more commands\n")
            
            async def receive_messages():
                """Receive and display messages from server"""
                nonlocal participants, ping_count
                
                try:
                    async for message in websocket:
                        data = json.loads(message)
                        msg_type = data.get("type")
                        
                        if msg_type == "history":
                            print("\n--- Recent Messages (from Redis cache) ---")
                            messages = data.get("messages", [])
                            if messages:
                                for msg in messages[-10:]:  # Show last 10
                                    timestamp = msg.get("timestamp", "")[:19]
                                    msg_username = msg.get("username", "unknown")
                                    content = msg.get("content", "")
                                    msg_type_inner = msg.get("type", "message")
                                    
                                    if msg_type_inner == "file":
                                        file_name = msg.get("file_name", "file")
                                        file_size = msg.get("file_size", 0)
                                        print(f"  [{timestamp}] {msg_username}: 📁 {file_name} ({_format_file_size(file_size)})")
                                    else:
                                        print(f"  [{timestamp}] {msg_username}: {content}")
                            else:
                                print("  (No message history)")
                            print("--- End of History ---\n")
                            
                        elif msg_type == "users":
                            participants = data.get("users", [])
                            print(f"[👥 Participants ({len(participants)}): {', '.join(participants)}]\n")
                            
                        elif msg_type == "join":
                            print(f"[➕ {data.get('content', 'User joined')}]")
                            
                        elif msg_type == "leave":
                            print(f"[➖ {data.get('content', 'User left')}]")
                            
                        elif msg_type == "system":
                            print(f"[ℹ️  {data.get('content', '')}]")
                            
                        elif msg_type == "typing":
                            # Could show typing indicator
                            pass
                            
                        elif msg_type == "ping":
                            ping_count += 1
                            ping_time = data.get("timestamp", "")
                            print(f"[💓 Health check ping #{ping_count} at {ping_time[:19]}]")
                            
                        elif msg_type == "file":
                            # File message
                            timestamp = data.get("timestamp", "")[:19]
                            sender = data.get("username", "unknown")
                            file_name = data.get("file_name", "file")
                            file_size = data.get("file_size", 0)
                            file_url = data.get("file_url", "")
                            
                            if sender == username:
                                print(f"  [You]: 📁 Uploaded {file_name} ({_format_file_size(file_size)})")
                            else:
                                print(f"  [{sender}]: 📁 Shared {file_name} ({_format_file_size(file_size)})")
                            
                            if file_url:
                                print(f"       URL: {file_url[:80]}...")
                            
                        else:
                            # Regular message
                            timestamp = data.get("timestamp", "")[:19]
                            sender = data.get("username", "unknown")
                            content = data.get("content", "")
                            
                            if sender == username:
                                print(f"  [You]: {content}")
                            else:
                                print(f"  [{sender}]: {content}")
                                
                except websockets.ConnectionClosed:
                    print("\n[Connection closed by server]")
            
            async def send_messages():
                """Read input and send messages"""
                loop = asyncio.get_event_loop()
                
                while True:
                    try:
                        # Read input in executor to not block
                        message = await loop.run_in_executor(
                            None, 
                            lambda: input("")
                        )
                        
                        if message.lower() == "quit":
                            print("\n[Leaving chat...]")
                            return
                        
                        # Handle commands
                        if message.startswith("/"):
                            if message.lower() == "/users":
                                if participants:
                                    print(f"\n👥 Current participants ({len(participants)}):")
                                    for i, user in enumerate(participants, 1):
                                        marker = " (you)" if user == username else ""
                                        print(f"  {i}. {user}{marker}")
                                    print()
                                else:
                                    print("\n[No participants list received yet]\n")
                            elif message.lower() == "/help":
                                print("\n📚 Available commands:")
                                print("  /users    - Show current participants")
                                print("  /help     - Show this help message")
                                print("  quit      - Exit the chat")
                                print("  <message> - Send a regular message\n")
                            else:
                                print(f"\n[Unknown command: {message}. Type '/help' for available commands]\n")
                            continue
                        
                        if message.strip():
                            await websocket.send(json.dumps({
                                "content": message,
                                "type": "message"
                            }))
                            
                    except EOFError:
                        return
            
            # Run both tasks concurrently
            receive_task = asyncio.create_task(receive_messages())
            send_task = asyncio.create_task(send_messages())
            
            # Wait for either task to complete
            done, pending = await asyncio.wait(
                [receive_task, send_task],
                return_when=asyncio.FIRST_COMPLETED
            )
            
            # Cancel pending tasks
            for task in pending:
                task.cancel()
                
    except websockets.exceptions.ConnectionClosed as e:
        print(f"\n[Connection closed: {e}]")
    except ConnectionRefusedError:
        print(f"\n[Error: Could not connect to {server_url}]")
        print("Make sure the services are running: docker compose up")
    except Exception as e:
        print(f"\n[Error: {e}]")
    
    print("\n✓ Chat session ended")


async def demo_mode(room: str = "demo", server_url: str = "ws://localhost:8000"):
    """Run automated demo showing WebSocket functionality"""
    
    print(f"\n{'=' * 60}")
    print(f"WebSocket Demo Mode")
    print(f"{'=' * 60}")
    
    uri = f"{server_url}/ws/chat/{room}?username=demo_bot"
    
    try:
        async with websockets.connect(uri) as websocket:
            print("✓ Connected as 'demo_bot'")
            
            # Receive initial messages
            for _ in range(3):
                try:
                    message = await asyncio.wait_for(websocket.recv(), timeout=2.0)
                    data = json.loads(message)
                    print(f"  Received: {data.get('type', 'message')}")
                except asyncio.TimeoutError:
                    break
            
            # Send test messages
            test_messages = [
                "Hello from the WebSocket client!",
                "This demonstrates bidirectional communication.",
                "Messages are broadcast to all users in the room.",
            ]
            
            for msg in test_messages:
                print(f"\n  Sending: {msg}")
                await websocket.send(json.dumps({"content": msg, "type": "message"}))
                await asyncio.sleep(1)
                
                # Receive echo/broadcast
                try:
                    response = await asyncio.wait_for(websocket.recv(), timeout=2.0)
                    data = json.loads(response)
                    print(f"  Echo: {data.get('content', '')[:50]}...")
                except asyncio.TimeoutError:
                    pass
            
            print("\n✓ Demo complete!")
            
    except Exception as e:
        print(f"\n[Error: {e}]")
        print("Make sure services are running: docker compose up")


def _format_file_size(size_bytes: int) -> str:
    """Format file size in human-readable format"""
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size_bytes < 1024.0:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.1f} TB"


def main():
    parser = argparse.ArgumentParser(
        description="WebSocket Chat Client - Real-time bidirectional chat with file sharing",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Connect to default room 'general' as 'user1'
  python websocket_client.py
  
  # Connect to specific room and username
  python websocket_client.py --room tech-talk --user alice
  
  # Connect to chat service instance (for scaling tests)
  python websocket_client.py --port 8011 --user alice
  
  # Run automated demo
  python websocket_client.py --demo

Features:
  - Real-time bidirectional messaging
  - Participant list display
  - File sharing notifications
  - Health check pings
  - Join/leave notifications
  - Message history from Redis cache
        """
    )
    parser.add_argument("--room", "-r", default="general", help="Chat room name")
    parser.add_argument("--user", "-u", default="user1", help="Username")
    parser.add_argument("--server", "-s", default="ws://localhost:8000", help="Server URL (API Gateway)")
    parser.add_argument("--port", "-p", type=int, help="Chat service port (for direct connection or scaling tests)")
    parser.add_argument("--demo", "-d", action="store_true", help="Run demo mode")
    
    args = parser.parse_args()
    
    # If port specified, connect directly to chat service
    server_url = args.server
    if args.port:
        server_url = f"ws://localhost:{args.port}"
    
    if args.demo:
        asyncio.run(demo_mode(args.room, server_url))
    else:
        asyncio.run(chat_client(args.room, args.user, server_url))


if __name__ == "__main__":
    main()

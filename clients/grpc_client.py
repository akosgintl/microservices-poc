#!/usr/bin/env python3
"""
gRPC Client for User Service
Demonstrates: Direct gRPC communication with Protocol Buffers, authentication, streaming
"""
import asyncio
import sys
import os
import argparse

# Add generated code to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'user-service', 'generated'))

import grpc
from grpc import aio as grpc_aio


def print_section(title: str):
    """Print a formatted section header"""
    print(f"\n{'=' * 60}")
    print(f"{title}")
    print(f"{'=' * 60}\n")


async def main(test_mode: str = "all"):
    """Demonstrate gRPC communication patterns"""
    
    print_section("gRPC Client Demo - User Service")
    
    # First, we need to generate the proto files
    print("[Setup] Generating gRPC code from proto files...")
    
    proto_dir = os.path.join(os.path.dirname(__file__), '..', 'user-service', 'protos')
    output_dir = os.path.join(os.path.dirname(__file__), 'generated')
    os.makedirs(output_dir, exist_ok=True)
    
    # Generate Python code from proto
    import subprocess
    result = subprocess.run([
        sys.executable, '-m', 'grpc_tools.protoc',
        f'-I{proto_dir}',
        f'--python_out={output_dir}',
        f'--pyi_out={output_dir}',
        f'--grpc_python_out={output_dir}',
        os.path.join(proto_dir, 'user.proto')
    ], capture_output=True, text=True)
    
    if result.returncode != 0:
        print(f"[Error] Failed to generate gRPC code: {result.stderr}")
        print("\n⚠️  Make sure grpcio-tools is installed:")
        print("    pip install grpcio-tools")
        return
    
    print("✓ gRPC code generated successfully\n")
    
    # Import generated code
    sys.path.insert(0, output_dir)
    import user_pb2
    import user_pb2_grpc
    
    # Connect to gRPC server
    server_address = os.getenv("USER_SERVICE_GRPC", "localhost:50051")
    print(f"[1] Connecting to gRPC server at {server_address}...")
    
    created_user_id = None
    
    try:
        async with grpc_aio.insecure_channel(server_address) as channel:
            stub = user_pb2_grpc.UserServiceStub(channel)
            
            # Wait for channel to be ready
            try:
                await asyncio.wait_for(
                    channel.channel_ready(),
                    timeout=5.0
                )
                print("✓ Connected to User Service\n")
            except asyncio.TimeoutError:
                print("✗ Connection timeout after 5 seconds")
                print("\n⚠️  Make sure User Service is running:")
                print("    docker compose up user-service")
                return
            
            # ===========================================
            # Unary RPC: List Users
            # ===========================================
            if test_mode in ["all", "list"]:
                print_section("[2] Unary RPC: ListUsers")
                request = user_pb2.ListUsersRequest(page=1, page_size=5)
                
                try:
                    response = await stub.ListUsers(request)
                    
                    print(f"📊 Total users: {response.total}")
                    print(f"📄 Page {response.page} of {response.pages}")
                    print(f"\nUsers:")
                    for idx, user in enumerate(response.users, 1):
                        status = "✅" if user.is_active else "❌"
                        print(f"  {idx}. {user.name}")
                        print(f"     Email: {user.email}")
                        print(f"     Role: {user.role}")
                        print(f"     Active: {status}")
                        print(f"     Created: {user.created_at[:19]}")
                        print()
                except grpc.RpcError as e:
                    print(f"✗ Error: {e.code()} - {e.details()}")
            
            # ===========================================
            # Unary RPC: Create User
            # ===========================================
            if test_mode in ["all", "create"]:
                print_section("[3] Unary RPC: CreateUser")
                import time
                timestamp = int(time.time())
                request = user_pb2.CreateUserRequest(
                    name=f"gRPC Test User {timestamp}",
                    email=f"grpc_test_{timestamp}@example.com",
                    password="SecurePass123!",
                    role="user"
                )
                
                try:
                    user = await stub.CreateUser(request)
                    print(f"✓ Successfully created user!")
                    print(f"   ID: {user.id}")
                    print(f"   Name: {user.name}")
                    print(f"   Email: {user.email}")
                    print(f"   Role: {user.role}")
                    print(f"   Created: {user.created_at[:19]}")
                    created_user_id = user.id
                except grpc.RpcError as e:
                    print(f"✗ Error: {e.code()} - {e.details()}")
            
            # ===========================================
            # Unary RPC: Get User
            # ===========================================
            if test_mode in ["all", "get"] and created_user_id:
                print_section("[4] Unary RPC: GetUser")
                request = user_pb2.GetUserRequest(id=created_user_id)
                
                try:
                    user = await stub.GetUser(request)
                    print(f"✓ Retrieved user details:")
                    print(f"   Name: {user.name}")
                    print(f"   Email: {user.email}")
                    print(f"   Role: {user.role}")
                    print(f"   Active: {'Yes' if user.is_active else 'No'}")
                    print(f"   Created: {user.created_at[:19]}")
                    print(f"   Updated: {user.updated_at[:19] if user.updated_at else 'N/A'}")
                except grpc.RpcError as e:
                    print(f"✗ Error: {e.code()} - {e.details()}")
            
            # ===========================================
            # Server Streaming RPC: StreamUsers
            # ===========================================
            if test_mode in ["all", "stream"]:
                print_section("[5] Server Streaming RPC: StreamUsers")
                request = user_pb2.StreamUsersRequest(active_only=True)
                
                print("📡 Starting user stream...")
                print("   (Receiving users one at a time via gRPC streaming)\n")
                
                user_count = 0
                try:
                    async for user in stub.StreamUsers(request):
                        user_count += 1
                        print(f"[Stream {user_count}] {user.name}")
                        print(f"   Email: {user.email}")
                        print(f"   Role: {user.role}")
                        print()
                        
                        if user_count >= 5:
                            print("   ... (limiting to first 5 users)")
                            break
                    
                    print(f"✓ Streamed {user_count} users successfully")
                    
                except grpc.RpcError as e:
                    print(f"✗ Streaming error: {e.code()} - {e.details()}")
            
            # ===========================================
            # Unary RPC: Authenticate
            # ===========================================
            if test_mode in ["all", "auth"]:
                print_section("[6] Unary RPC: Authenticate")
                
                # Try with default user (should exist)
                request = user_pb2.AuthRequest(
                    email="alice@example.com",
                    password="password123"
                )
                
                try:
                    response = await stub.Authenticate(request)
                    if response.success:
                        print(f"✓ Authentication successful!")
                        print(f"   User: {response.user.name}")
                        print(f"   Email: {response.user.email}")
                        print(f"   Role: {response.user.role}")
                        print(f"   Token: {response.token[:30]}...{response.token[-10:]}")
                    else:
                        print(f"✗ Authentication failed: {response.error}")
                except grpc.RpcError as e:
                    print(f"✗ RPC Error: {e.code()} - {e.details()}")
                
                # Try with wrong password
                print("\n  Testing with wrong password...")
                request = user_pb2.AuthRequest(
                    email="alice@example.com",
                    password="wrongpassword"
                )
                
                try:
                    response = await stub.Authenticate(request)
                    if not response.success:
                        print(f"  ✓ Correctly rejected: {response.error}")
                except grpc.RpcError as e:
                    print(f"  ✗ RPC Error: {e.code()} - {e.details()}")
            
            # ===========================================
            # Unary RPC: Update User (if supported)
            # ===========================================
            if test_mode in ["all", "update"] and created_user_id:
                print_section("[7] Unary RPC: UpdateUser (if available)")
                print("   Skipping - UpdateUser not implemented in current service")
            
            # ===========================================
            # Unary RPC: Delete User
            # ===========================================
            if test_mode in ["all", "delete"] and created_user_id:
                print_section("[8] Unary RPC: DeleteUser")
                request = user_pb2.DeleteUserRequest(id=created_user_id)
                
                try:
                    response = await stub.DeleteUser(request)
                    if response.success:
                        print(f"✓ {response.message}")
                        print(f"   User {created_user_id[:8]}... has been deleted")
                    else:
                        print(f"✗ {response.message}")
                except grpc.RpcError as e:
                    print(f"✗ Error: {e.code()} - {e.details()}")
            
    except asyncio.TimeoutError:
        print("\n[Error] Connection timeout. Is the User Service running?")
        print("\n⚠️  Start services with: docker compose up")
    except grpc.RpcError as e:
        print(f"\n[Error] gRPC error: {e.code()} - {e.details()}")
    except Exception as e:
        print(f"\n[Error] {e}")
        import traceback
        traceback.print_exc()
    
    print_section("gRPC Demo Complete!")
    
    print("Key Observations:")
    print("  ✅ Protocol Buffers for efficient serialization")
    print("  ✅ Strong typing with auto-generated code")
    print("  ✅ Unary RPC for request-response patterns")
    print("  ✅ Server streaming for large datasets")
    print("  ✅ ~7x faster than REST for internal APIs")
    print("  ✅ Language-agnostic (can call from Java, Go, etc.)")
    print()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="gRPC Client for User Service - Demonstrates Protocol Buffers and RPC patterns",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run all tests
  python grpc_client.py
  
  # Run specific test
  python grpc_client.py --test list
  python grpc_client.py --test create
  python grpc_client.py --test stream
  
  # Connect to different server
  USER_SERVICE_GRPC=localhost:50051 python grpc_client.py

Available Tests:
  all     - Run all tests (default)
  list    - List users
  create  - Create a user
  get     - Get user details
  stream  - Stream users (server streaming)
  auth    - Test authentication
  delete  - Delete user

Features:
  - Unary RPC (single request/response)
  - Server streaming RPC (stream of responses)
  - Authentication with JWT tokens
  - Strong typing with Protocol Buffers
  - Auto-generated client code
        """
    )
    parser.add_argument(
        "--test", "-t", 
        choices=["all", "list", "create", "get", "stream", "auth", "update", "delete"],
        default="all",
        help="Which test to run"
    )
    
    args = parser.parse_args()
    
    asyncio.run(main(args.test))

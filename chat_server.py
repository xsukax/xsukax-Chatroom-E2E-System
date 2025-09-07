#!/usr/bin/env python3
import asyncio
import websockets
import json
import threading
import time
import secrets
import string
from datetime import datetime, timedelta
import os
import ipaddress
import re
from collections import defaultdict, deque

class ChatServer:
    def __init__(self):
        self.clients = {}  # websocket -> user_data
        self.user_counter = 1
        self.banned_ips = set()
        self.admin_password = ""
        self.public_keys = {}  # username -> public_key
        self.usernames = set()  # track taken usernames
        self.user_message_times = defaultdict(deque)  # username -> deque of message timestamps
        self.load_banned_users()
        self.generate_admin_password()
        self.start_password_rotation()

    def generate_admin_password(self):
        """Generate new admin password and save to file"""
        self.admin_password = ''.join(secrets.choice(string.ascii_letters + string.digits) for _ in range(12))
        try:
            with open('admin.txt', 'w') as f:
                f.write(f"{self.admin_password}\n")
            print(f"[{datetime.now().strftime('%H:%M:%S')}] New admin password generated: {self.admin_password}")
        except Exception as e:
            print(f"Error writing admin password: {e}")

    def start_password_rotation(self):
        """Start background thread for hourly password rotation"""
        def rotate_password():
            while True:
                time.sleep(3600)  # 1 hour
                self.generate_admin_password()
        
        thread = threading.Thread(target=rotate_password, daemon=True)
        thread.start()

    async def start_heartbeat_checker(self):
        """Start background task for connection heartbeat"""
        while True:
            await asyncio.sleep(30)  # Check every 30 seconds
            await self.ping_all_clients()

    async def ping_all_clients(self):
        """Send ping to all clients to keep connections alive"""
        if not self.clients:
            return
        
        disconnected = []
        for websocket in list(self.clients.keys()):
            try:
                await websocket.ping()
            except (websockets.exceptions.ConnectionClosed, Exception):
                disconnected.append(websocket)
        
        # Clean up disconnected clients
        for ws in disconnected:
            if ws in self.clients:
                await self.unregister_client(ws)

    def check_flood_protection(self, username, is_admin):
        """Check if user is flooding (30 messages per minute) - admins are exempt"""
        if is_admin:
            return False  # Admins are not subject to flood protection
        
        now = datetime.now()
        user_times = self.user_message_times[username]
        
        # Remove messages older than 1 minute
        while user_times and now - user_times[0] > timedelta(minutes=1):
            user_times.popleft()
        
        # Check if user has sent 30+ messages in the last minute
        if len(user_times) >= 30:
            return True  # User is flooding
        
        # Add current message time
        user_times.append(now)
        return False

    def load_banned_users(self):
        """Load banned users from file"""
        try:
            if os.path.exists('banned.txt'):
                with open('banned.txt', 'r') as f:
                    for line in f:
                        line = line.strip()
                        if line.startswith('IP:'):
                            self.banned_ips.add(line[3:])
        except Exception as e:
            print(f"Error loading banned users: {e}")

    def save_banned_users(self):
        """Save banned users to file"""
        try:
            with open('banned.txt', 'w') as f:
                for ip in self.banned_ips:
                    f.write(f"IP:{ip}\n")
        except Exception as e:
            print(f"Error saving banned users: {e}")

    def get_client_ip(self, websocket):
        """Extract client IP address"""
        try:
            return websocket.remote_address[0]
        except:
            return "unknown"

    def is_banned(self, websocket):
        """Check if client is banned"""
        client_ip = self.get_client_ip(websocket)
        return client_ip in self.banned_ips

    def validate_username(self, username):
        """Validate username format and availability"""
        if not username:
            return False, "Username cannot be empty"
        
        # Allow letters, numbers, underscore, hyphen
        if not re.match(r'^[a-zA-Z0-9_-]+$', username):
            return False, "Username can only contain letters, numbers, underscore, and hyphen"
        
        if len(username) < 2 or len(username) > 20:
            return False, "Username must be between 2 and 20 characters"
        
        if username.lower() in [u.lower() for u in self.usernames]:
            return False, "Username is already taken"
        
        return True, "Valid"

    def generate_auto_username(self):
        """Generate automatic username like xsukax0001"""
        while True:
            username = f"xsukax{self.user_counter:04d}"
            if username not in self.usernames:
                return username
            self.user_counter += 1

    def get_users_list(self):
        """Get list of current users with their public keys"""
        users = []
        for ws, data in self.clients.items():
            user_info = {
                'username': data['username'],
                'ip': data['ip'],
                'is_admin': data['is_admin'],
                'joined_at': data['joined_at']
            }
            if data['username'] in self.public_keys:
                user_info['public_key'] = self.public_keys[data['username']]
            users.append(user_info)
        return users

    async def broadcast_users_list(self):
        """Broadcast updated users list to all clients"""
        users_list = self.get_users_list()
        message = {
            'type': 'users_list',
            'users': users_list
        }
        await self.broadcast_message(message)

    async def register_client(self, websocket, custom_username=None):
        """Register new client"""
        client_ip = self.get_client_ip(websocket)
        
        # Check if banned
        if self.is_banned(websocket):
            await websocket.send(json.dumps({
                'type': 'error',
                'message': 'You are banned from this server'
            }))
            return None

        # Determine username
        if custom_username:
            valid, message = self.validate_username(custom_username)
            if not valid:
                await websocket.send(json.dumps({
                    'type': 'error',
                    'message': f'Invalid username: {message}'
                }))
                return None
            username = custom_username
        else:
            username = self.generate_auto_username()
            self.user_counter += 1

        self.usernames.add(username)
        
        user_data = {
            'username': username,
            'ip': client_ip,
            'is_admin': False,
            'joined_at': datetime.now().isoformat(),
            'last_ping': time.time()
        }
        
        self.clients[websocket] = user_data

        # Send welcome message
        await websocket.send(json.dumps({
            'type': 'welcome',
            'username': username,
            'message': f'Connected as {username}'
        }))

        # Broadcast user joined
        await self.broadcast_message({
            'type': 'user_joined',
            'username': username,
            'message': f'{username} joined the chat',
            'timestamp': datetime.now().isoformat()
        }, exclude=websocket)

        # Send updated users list
        await self.broadcast_users_list()

        print(f"[{datetime.now().strftime('%H:%M:%S')}] {username} ({client_ip}) joined")
        return user_data

    async def handle_username_change(self, websocket, new_username):
        """Handle username change request"""
        if websocket not in self.clients:
            return
            
        user_data = self.clients[websocket]
        old_username = user_data['username']
        
        # Validate new username
        valid, message = self.validate_username(new_username)
        if not valid:
            await websocket.send(json.dumps({
                'type': 'error',
                'message': f'Cannot change username: {message}'
            }))
            return

        # Update username
        self.usernames.discard(old_username)
        self.usernames.add(new_username)
        user_data['username'] = new_username
        
        # Update public key mapping
        if old_username in self.public_keys:
            self.public_keys[new_username] = self.public_keys[old_username]
            del self.public_keys[old_username]

        # Update flood protection tracking
        if old_username in self.user_message_times:
            self.user_message_times[new_username] = self.user_message_times[old_username]
            del self.user_message_times[old_username]

        # Send confirmation to user
        await websocket.send(json.dumps({
            'type': 'username_changed',
            'old_username': old_username,
            'new_username': new_username,
            'message': f'Username changed to {new_username}'
        }))

        # Broadcast username change
        await self.broadcast_message({
            'type': 'user_renamed',
            'old_username': old_username,
            'new_username': new_username,
            'message': f'{old_username} changed username to {new_username}',
            'timestamp': datetime.now().isoformat()
        }, exclude=websocket)

        # Send updated users list
        await self.broadcast_users_list()

        print(f"[{datetime.now().strftime('%H:%M:%S')}] {old_username} changed username to {new_username}")

    async def unregister_client(self, websocket):
        """Unregister client"""
        if websocket in self.clients:
            user_data = self.clients[websocket]
            username = user_data['username']
            
            # Remove username and public key
            self.usernames.discard(username)
            if username in self.public_keys:
                del self.public_keys[username]
            
            # Clean up flood protection tracking
            if username in self.user_message_times:
                del self.user_message_times[username]
            
            del self.clients[websocket]
            
            # Broadcast user left
            await self.broadcast_message({
                'type': 'user_left',
                'username': username,
                'message': f'{username} left the chat',
                'timestamp': datetime.now().isoformat()
            })
            
            # Send updated users list
            await self.broadcast_users_list()
            
            print(f"[{datetime.now().strftime('%H:%M:%S')}] {username} left")

    async def broadcast_message(self, message, exclude=None):
        """Broadcast message to all connected clients"""
        if not self.clients:
            return
        
        disconnected = []
        for websocket in list(self.clients.keys()):
            if websocket != exclude:
                try:
                    await websocket.send(json.dumps(message))
                except (websockets.exceptions.ConnectionClosed, Exception) as e:
                    print(f"Error sending message to client: {e}")
                    disconnected.append(websocket)
        
        # Clean up disconnected clients
        for ws in disconnected:
            if ws in self.clients:
                await self.unregister_client(ws)

    async def send_private_message(self, sender_ws, recipient_username, encrypted_message):
        """Send private encrypted message to specific user"""
        sender_data = self.clients[sender_ws]
        for ws, data in self.clients.items():
            if data['username'] == recipient_username:
                try:
                    await ws.send(json.dumps({
                        'type': 'private_message',
                        'from_username': sender_data['username'],
                        'encrypted_content': encrypted_message,
                        'timestamp': datetime.now().isoformat(),
                        'is_admin': sender_data['is_admin']
                    }))
                    return True
                except (websockets.exceptions.ConnectionClosed, Exception) as e:
                    print(f"Error sending private message: {e}")
                    await self.unregister_client(ws)
                    return False
        return False

    async def handle_ping(self, websocket):
        """Handle client ping message"""
        if websocket in self.clients:
            self.clients[websocket]['last_ping'] = time.time()
            try:
                await websocket.send(json.dumps({
                    'type': 'pong',
                    'timestamp': datetime.now().isoformat()
                }))
            except (websockets.exceptions.ConnectionClosed, Exception):
                pass

    async def handle_public_key_register(self, websocket, public_key):
        """Handle public key registration"""
        if websocket not in self.clients:
            return
            
        user_data = self.clients[websocket]
        self.public_keys[user_data['username']] = public_key
        
        await websocket.send(json.dumps({
            'type': 'key_registered',
            'message': 'Public key registered successfully'
        }))
        
        # Broadcast updated users list with public keys
        await self.broadcast_users_list()
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Public key registered for {user_data['username']}")

    async def handle_admin_command(self, websocket, password):
        """Handle admin authentication"""
        if websocket not in self.clients:
            return
            
        user_data = self.clients[websocket]
        
        if password == self.admin_password:
            user_data['is_admin'] = True
            await websocket.send(json.dumps({
                'type': 'admin_success',
                'message': 'Admin privileges granted'
            }))
            await self.broadcast_users_list()  # Update admin status in users list
            print(f"[{datetime.now().strftime('%H:%M:%S')}] {user_data['username']} became admin")
        else:
            await websocket.send(json.dumps({
                'type': 'error',
                'message': 'Invalid admin password'
            }))

    async def handle_user_info_command(self, websocket, target_username):
        """Handle user info request (admin only)"""
        if websocket not in self.clients:
            return
            
        user_data = self.clients[websocket]
        
        if not user_data['is_admin']:
            await websocket.send(json.dumps({
                'type': 'error',
                'message': 'Admin privileges required'
            }))
            return

        # Find target user by username
        for ws, data in self.clients.items():
            if data['username'] == target_username:
                await websocket.send(json.dumps({
                    'type': 'user_info',
                    'target': target_username,
                    'info': {
                        'username': data['username'],
                        'ip': data['ip'],
                        'is_admin': data['is_admin'],
                        'joined_at': data['joined_at']
                    }
                }))
                return
        
        await websocket.send(json.dumps({
            'type': 'error',
            'message': f'User {target_username} not found'
        }))

    async def handle_kick_command(self, websocket, target_username):
        """Handle kick command (admin only)"""
        if websocket not in self.clients:
            return
            
        user_data = self.clients[websocket]
        
        if not user_data['is_admin']:
            await websocket.send(json.dumps({
                'type': 'error',
                'message': 'Admin privileges required'
            }))
            return

        # Find target user by username
        target_ws = None
        target_data = None
        for ws, data in self.clients.items():
            if data['username'] == target_username:
                target_ws = ws
                target_data = data
                break
        
        if target_ws:
            await target_ws.send(json.dumps({
                'type': 'kicked',
                'message': f'You have been kicked by {user_data["username"]}'
            }))
            
            await self.broadcast_message({
                'type': 'user_kicked',
                'message': f'{target_data["username"]} was kicked by {user_data["username"]}',
                'timestamp': datetime.now().isoformat()
            }, exclude=target_ws)
            
            await target_ws.close()
            print(f"[{datetime.now().strftime('%H:%M:%S')}] {target_data['username']} kicked by {user_data['username']}")
        else:
            await websocket.send(json.dumps({
                'type': 'error',
                'message': f'User {target_username} not found'
            }))

    async def handle_ban_command(self, websocket, target_username):
        """Handle ban command (admin only) - ban by IP only"""
        if websocket not in self.clients:
            return
            
        user_data = self.clients[websocket]
        
        if not user_data['is_admin']:
            await websocket.send(json.dumps({
                'type': 'error',
                'message': 'Admin privileges required'
            }))
            return

        # Ban by IP
        for ws, data in list(self.clients.items()):
            if data['username'] == target_username:
                self.banned_ips.add(data['ip'])
                await ws.send(json.dumps({
                    'type': 'banned',
                    'message': f'You have been banned by {user_data["username"]}'
                }))
                await ws.close()
                self.save_banned_users()
                
                await self.broadcast_message({
                    'type': 'user_banned',
                    'message': f'{data["username"]} was banned by {user_data["username"]}',
                    'timestamp': datetime.now().isoformat()
                })
                print(f"[{datetime.now().strftime('%H:%M:%S')}] {data['username']} banned by {user_data['username']}")
                
                await websocket.send(json.dumps({
                    'type': 'ban_success',
                    'message': f'{target_username} has been banned'
                }))
                return
        
        await websocket.send(json.dumps({
            'type': 'error',
            'message': f'User {target_username} not found'
        }))

    async def handle_message(self, websocket, message_data):
        """Handle incoming message"""
        if websocket not in self.clients:
            return
            
        user_data = self.clients[websocket]
        content = message_data.get('content', '').strip()
        message_type = message_data.get('message_type', 'text')
        
        # Handle different message types
        if message_type == 'ping':
            await self.handle_ping(websocket)
            return

        elif message_type == 'register':
            custom_username = message_data.get('username', '').strip()
            if custom_username:
                # Attempt to register with custom username
                valid, msg = self.validate_username(custom_username)
                if not valid:
                    await websocket.send(json.dumps({
                        'type': 'error',
                        'message': f'Invalid username: {msg}'
                    }))
                    return
            return

        elif message_type == 'register_key':
            public_key = message_data.get('public_key')
            if public_key:
                await self.handle_public_key_register(websocket, public_key)
            return

        elif message_type == 'private':
            recipient_username = message_data.get('recipient_username')
            encrypted_content = message_data.get('encrypted_content')
            if recipient_username and encrypted_content:
                # Check flood protection for private messages too
                if self.check_flood_protection(user_data['username'], user_data['is_admin']):
                    await websocket.send(json.dumps({
                        'type': 'error',
                        'message': 'Flood protection: You are sending messages too quickly. Please wait before sending more.'
                    }))
                    return
                
                success = await self.send_private_message(websocket, recipient_username, encrypted_content)
                if not success:
                    await websocket.send(json.dumps({
                        'type': 'error',
                        'message': 'User not found or offline'
                    }))
            return

        if not content:
            return

        # Handle commands
        if content.startswith('/changeuname '):
            new_username = content[13:].strip()
            if not new_username:
                await websocket.send(json.dumps({
                    'type': 'error',
                    'message': 'Usage: /changeuname <new_username>'
                }))
                return
            await self.handle_username_change(websocket, new_username)
            return

        elif content.startswith('/admin '):
            password = content[7:]
            await self.handle_admin_command(websocket, password)
            return
        
        elif content.startswith('/userinfo '):
            target = content[10:]
            await self.handle_user_info_command(websocket, target)
            return
        
        elif content.startswith('/kick '):
            target = content[6:]
            await self.handle_kick_command(websocket, target)
            return
        
        elif content.startswith('/ban '):
            target = content[5:]
            await self.handle_ban_command(websocket, target)
            return
        
        elif content == '/help':
            help_text = "Commands: /admin <password>, /changeuname <new_username>, /kick <username>, /ban <username>, /userinfo <username>"
            await websocket.send(json.dumps({
                'type': 'help',
                'message': help_text
            }))
            return

        # Check flood protection for regular messages
        if self.check_flood_protection(user_data['username'], user_data['is_admin']):
            await websocket.send(json.dumps({
                'type': 'error',
                'message': 'Flood protection: You are sending messages too quickly. Please wait before sending more.'
            }))
            return

        # Regular message (unencrypted for main room)
        message = {
            'type': 'message',
            'username': user_data['username'],
            'content': content,
            'timestamp': datetime.now().isoformat(),
            'is_admin': user_data['is_admin']
        }
        
        await self.broadcast_message(message)

    async def handle_client(self, websocket):
        """Handle client connection with enhanced error handling"""
        print(f"[{datetime.now().strftime('%H:%M:%S')}] New client connecting from {self.get_client_ip(websocket)}")
        
        user_data = None
        try:
            async for message in websocket:
                try:
                    data = json.loads(message)
                    message_type = data.get('message_type', 'text')
                    
                    if message_type == 'register' and not user_data:
                        # Initial registration
                        custom_username = data.get('username', '').strip()
                        user_data = await self.register_client(websocket, custom_username if custom_username else None)
                        if not user_data:
                            return
                    elif user_data:
                        await self.handle_message(websocket, data)
                    else:
                        await websocket.send(json.dumps({
                            'type': 'error',
                            'message': 'Must register first'
                        }))
                except json.JSONDecodeError:
                    await websocket.send(json.dumps({
                        'type': 'error',
                        'message': 'Invalid message format'
                    }))
                except Exception as e:
                    print(f"Error handling message: {e}")
        except websockets.exceptions.ConnectionClosed:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Client disconnected normally")
        except Exception as e:
            print(f"Error in client handler: {e}")
        finally:
            if user_data and websocket in self.clients:
                await self.unregister_client(websocket)

async def main():
    print("Starting xsukax Chat System on port 3333...")
    print("Admin password will rotate every hour")
    print("Supports both ws:// and wss:// connections")
    print("Private messages are end-to-end encrypted")
    print("Enhanced connection stability with heartbeat")
    print("Flood protection: 30 messages per minute for users (admins exempt)")
    
    server = ChatServer()
    
    print(f"Server started. Initial admin password: {server.admin_password}")
    print("Server listening on ws://localhost:3333")
    print("Press Ctrl+C to stop the server")
    
    # Start heartbeat checker as asyncio task
    asyncio.create_task(server.start_heartbeat_checker())
    
    # Enhanced server settings for better connection stability
    async with websockets.serve(
        server.handle_client, 
        "0.0.0.0", 
        3333,
        ping_interval=20,  # Send ping every 20 seconds
        ping_timeout=10,   # Wait 10 seconds for pong
        close_timeout=10,  # Wait 10 seconds for close
        max_size=10**6,    # 1MB max message size
        max_queue=32       # Max queued messages
    ):
        await asyncio.Future()  # Run forever

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nServer shutting down...")
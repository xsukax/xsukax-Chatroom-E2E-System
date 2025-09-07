#!/usr/bin/env python3
import asyncio
import websockets
import json
import threading
import time
import secrets
import string
import sqlite3
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
        self.user_rooms = defaultdict(set)  # username -> set of room names
        self.room_users = defaultdict(set)  # room_name -> set of usernames
        self.init_database()
        self.load_banned_users()
        self.generate_admin_password()
        self.start_password_rotation()

    def init_database(self):
        """Initialize SQLite database for room management"""
        self.db_path = 'chat_rooms.db'
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Create rooms table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS rooms (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE NOT NULL,
                created_by TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                is_active BOOLEAN DEFAULT 1
            )
        ''')
        
        # Create user_rooms table for tracking memberships
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS user_rooms (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT NOT NULL,
                room_name TEXT NOT NULL,
                joined_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(username, room_name)
            )
        ''')
        
        # Create default main room if not exists
        cursor.execute('INSERT OR IGNORE INTO rooms (name, created_by) VALUES (?, ?)', 
                      ('main', 'system'))
        
        conn.commit()
        conn.close()
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Database initialized")

    def get_db_connection(self):
        """Get database connection"""
        return sqlite3.connect(self.db_path)

    def create_room(self, room_name, created_by):
        """Create a new room (admin only)"""
        try:
            conn = self.get_db_connection()
            cursor = conn.cursor()
            cursor.execute('INSERT INTO rooms (name, created_by) VALUES (?, ?)', 
                          (room_name, created_by))
            conn.commit()
            conn.close()
            return True, f"Room '{room_name}' created successfully"
        except sqlite3.IntegrityError:
            return False, f"Room '{room_name}' already exists"
        except Exception as e:
            return False, f"Error creating room: {str(e)}"

    def delete_room(self, room_name, admin_username):
        """Delete a room (admin only)"""
        if room_name == 'main':
            return False, "Cannot delete the main room"
        
        try:
            conn = self.get_db_connection()
            cursor = conn.cursor()
            
            # Check if room exists
            cursor.execute('SELECT id FROM rooms WHERE name = ? AND is_active = 1', (room_name,))
            if not cursor.fetchone():
                conn.close()
                return False, f"Room '{room_name}' does not exist"
            
            # Deactivate room instead of deleting (for audit trail)
            cursor.execute('UPDATE rooms SET is_active = 0 WHERE name = ?', (room_name,))
            
            # Remove all user memberships
            cursor.execute('DELETE FROM user_rooms WHERE room_name = ?', (room_name,))
            
            conn.commit()
            conn.close()
            
            # Remove users from room in memory
            if room_name in self.room_users:
                for username in list(self.room_users[room_name]):
                    self.user_rooms[username].discard(room_name)
                del self.room_users[room_name]
            
            return True, f"Room '{room_name}' deleted successfully"
        except Exception as e:
            return False, f"Error deleting room: {str(e)}"

    def get_active_rooms(self):
        """Get list of active rooms"""
        try:
            conn = self.get_db_connection()
            cursor = conn.cursor()
            cursor.execute('SELECT name, created_by, created_at FROM rooms WHERE is_active = 1 ORDER BY name')
            rooms = cursor.fetchall()
            conn.close()
            return [{'name': r[0], 'created_by': r[1], 'created_at': r[2]} for r in rooms]
        except Exception as e:
            print(f"Error getting rooms: {e}")
            return []

    def join_room(self, username, room_name):
        """Add user to room"""
        try:
            # Check if room exists
            conn = self.get_db_connection()
            cursor = conn.cursor()
            cursor.execute('SELECT name FROM rooms WHERE name = ? AND is_active = 1', (room_name,))
            if not cursor.fetchone():
                conn.close()
                return False, f"Room '{room_name}' does not exist"
            
            # Add to database
            cursor.execute('INSERT OR IGNORE INTO user_rooms (username, room_name) VALUES (?, ?)', 
                          (username, room_name))
            conn.commit()
            conn.close()
            
            # Add to memory
            self.user_rooms[username].add(room_name)
            self.room_users[room_name].add(username)
            
            return True, f"Joined room '{room_name}'"
        except Exception as e:
            return False, f"Error joining room: {str(e)}"

    def leave_room(self, username, room_name):
        """Remove user from room"""
        if room_name == 'main':
            return False, "Cannot leave the main room"
        
        try:
            conn = self.get_db_connection()
            cursor = conn.cursor()
            cursor.execute('DELETE FROM user_rooms WHERE username = ? AND room_name = ?', 
                          (username, room_name))
            conn.commit()
            conn.close()
            
            # Remove from memory
            self.user_rooms[username].discard(room_name)
            self.room_users[room_name].discard(username)
            
            return True, f"Left room '{room_name}'"
        except Exception as e:
            return False, f"Error leaving room: {str(e)}"

    def get_user_rooms(self, username):
        """Get list of rooms user is in"""
        return list(self.user_rooms[username])

    def get_room_users(self, room_name):
        """Get list of users in room"""
        return list(self.room_users[room_name])

    def get_room_users_detailed(self, room_name):
        """Get detailed user list for a specific room"""
        users = []
        if room_name in self.room_users:
            for username in self.room_users[room_name]:
                # Find the user's websocket and data
                for ws, data in self.clients.items():
                    if data['username'] == username:
                        user_info = {
                            'username': data['username'],
                            'ip': data['ip'],
                            'is_admin': data['is_admin'],
                            'joined_at': data['joined_at']
                        }
                        if data['username'] in self.public_keys:
                            user_info['public_key'] = self.public_keys[data['username']]
                        users.append(user_info)
                        break
        return users

    def load_user_rooms_from_db(self, username):
        """Load user's rooms from database on reconnect"""
        try:
            conn = self.get_db_connection()
            cursor = conn.cursor()
            cursor.execute('''
                SELECT ur.room_name FROM user_rooms ur
                JOIN rooms r ON ur.room_name = r.name
                WHERE ur.username = ? AND r.is_active = 1
            ''', (username,))
            rooms = [row[0] for row in cursor.fetchall()]
            conn.close()
            
            # Update memory
            self.user_rooms[username] = set(rooms)
            for room_name in rooms:
                self.room_users[room_name].add(username)
            
            return rooms
        except Exception as e:
            print(f"Error loading user rooms: {e}")
            return []

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

    def validate_room_name(self, room_name):
        """Validate room name format"""
        if not room_name:
            return False, "Room name cannot be empty"
        
        # Remove # prefix if present
        if room_name.startswith('#'):
            room_name = room_name[1:]
        
        # Allow letters, numbers, underscore, hyphen
        if not re.match(r'^[a-zA-Z0-9_-]+$', room_name):
            return False, "Room name can only contain letters, numbers, underscore, and hyphen"
        
        if len(room_name) < 2 or len(room_name) > 20:
            return False, "Room name must be between 2 and 20 characters"
        
        return True, room_name

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

    async def broadcast_room_users_list(self, room_name):
        """Broadcast updated room users list to users in that room"""
        room_users_list = self.get_room_users_detailed(room_name)
        message = {
            'type': 'room_users_list',
            'room_name': room_name,
            'users': room_users_list
        }
        await self.broadcast_to_room(room_name, message)

    async def broadcast_rooms_list(self):
        """Broadcast updated rooms list to all clients"""
        rooms_list = self.get_active_rooms()
        message = {
            'type': 'rooms_list',
            'rooms': rooms_list
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

        # Load user's rooms from database and join main room
        user_rooms = self.load_user_rooms_from_db(username)
        if 'main' not in user_rooms:
            self.join_room(username, 'main')

        # Send welcome message
        await websocket.send(json.dumps({
            'type': 'welcome',
            'username': username,
            'message': f'Connected as {username}',
            'rooms': self.get_user_rooms(username)
        }))

        # Broadcast user joined to main room
        await self.broadcast_to_room('main', {
            'type': 'user_joined',
            'username': username,
            'message': f'{username} joined the chat',
            'timestamp': datetime.now().isoformat()
        }, exclude=websocket)

        # Send updated lists
        await self.broadcast_users_list()
        await self.broadcast_rooms_list()
        
        # Update room user lists for all rooms the user is in
        for room_name in self.get_user_rooms(username):
            await self.broadcast_room_users_list(room_name)

        print(f"[{datetime.now().strftime('%H:%M:%S')}] {username} ({client_ip}) joined")
        return user_data

    async def unregister_client(self, websocket):
        """Unregister client"""
        if websocket in self.clients:
            user_data = self.clients[websocket]
            username = user_data['username']
            
            # Remove from all rooms in memory
            user_rooms = list(self.user_rooms[username])
            for room_name in user_rooms:
                self.room_users[room_name].discard(username)
                # Broadcast user left to each room
                await self.broadcast_to_room(room_name, {
                    'type': 'user_left',
                    'username': username,
                    'message': f'{username} left {room_name}',
                    'timestamp': datetime.now().isoformat()
                })
                # Update room user lists
                await self.broadcast_room_users_list(room_name)
            
            # Clean up
            self.usernames.discard(username)
            if username in self.public_keys:
                del self.public_keys[username]
            if username in self.user_message_times:
                del self.user_message_times[username]
            if username in self.user_rooms:
                del self.user_rooms[username]
            
            del self.clients[websocket]
            
            # Send updated lists
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

    async def broadcast_to_room(self, room_name, message, exclude=None):
        """Broadcast message to all users in a specific room"""
        if room_name not in self.room_users:
            return
        
        disconnected = []
        for websocket, user_data in list(self.clients.items()):
            if websocket != exclude and user_data['username'] in self.room_users[room_name]:
                try:
                    message_with_room = message.copy()
                    message_with_room['room'] = room_name
                    await websocket.send(json.dumps(message_with_room))
                except (websockets.exceptions.ConnectionClosed, Exception) as e:
                    print(f"Error sending message to client: {e}")
                    disconnected.append(websocket)
        
        # Clean up disconnected clients
        for ws in disconnected:
            if ws in self.clients:
                await self.unregister_client(ws)

    async def handle_room_command(self, websocket, command, args):
        """Handle room-related commands"""
        user_data = self.clients[websocket]
        username = user_data['username']
        is_admin = user_data['is_admin']
        
        if command == 'join':
            if not args:
                await websocket.send(json.dumps({
                    'type': 'error',
                    'message': 'Usage: /join #room-name'
                }))
                return
            
            valid, room_name = self.validate_room_name(args)
            if not valid:
                await websocket.send(json.dumps({
                    'type': 'error',
                    'message': f'Invalid room name: {room_name}'
                }))
                return
            
            success, message = self.join_room(username, room_name)
            if success:
                await websocket.send(json.dumps({
                    'type': 'room_joined',
                    'room_name': room_name,
                    'message': message
                }))
                # Broadcast to room that user joined
                await self.broadcast_to_room(room_name, {
                    'type': 'user_joined_room',
                    'username': username,
                    'message': f'{username} joined the room',
                    'timestamp': datetime.now().isoformat()
                }, exclude=websocket)
                # Update room user lists
                await self.broadcast_room_users_list(room_name)
            else:
                await websocket.send(json.dumps({
                    'type': 'error',
                    'message': message
                }))
        
        elif command == 'left':
            # Get current room from context (you'll need to track this)
            current_rooms = self.get_user_rooms(username)
            if len(current_rooms) <= 1:  # Only main room
                await websocket.send(json.dumps({
                    'type': 'error',
                    'message': 'You are only in the main room and cannot leave it'
                }))
                return
            
            # For simplicity, leave the last joined room (excluding main)
            room_to_leave = None
            for room in current_rooms:
                if room != 'main':
                    room_to_leave = room
                    break
            
            if room_to_leave:
                success, message = self.leave_room(username, room_to_leave)
                if success:
                    await websocket.send(json.dumps({
                        'type': 'room_left',
                        'room_name': room_to_leave,
                        'message': message
                    }))
                    # Broadcast to room that user left
                    await self.broadcast_to_room(room_to_leave, {
                        'type': 'user_left_room',
                        'username': username,
                        'message': f'{username} left the room',
                        'timestamp': datetime.now().isoformat()
                    })
                    # Update room user lists
                    await self.broadcast_room_users_list(room_to_leave)
                else:
                    await websocket.send(json.dumps({
                        'type': 'error',
                        'message': message
                    }))
        
        elif command == 'createroom':
            if not is_admin:
                await websocket.send(json.dumps({
                    'type': 'error',
                    'message': 'Admin privileges required to create rooms'
                }))
                return
            
            if not args:
                await websocket.send(json.dumps({
                    'type': 'error',
                    'message': 'Usage: /createroom room-name'
                }))
                return
            
            valid, room_name = self.validate_room_name(args)
            if not valid:
                await websocket.send(json.dumps({
                    'type': 'error',
                    'message': f'Invalid room name: {room_name}'
                }))
                return
            
            success, message = self.create_room(room_name, username)
            await websocket.send(json.dumps({
                'type': 'room_created' if success else 'error',
                'message': message
            }))
            
            if success:
                await self.broadcast_rooms_list()
        
        elif command == 'deleteroom':
            if not is_admin:
                await websocket.send(json.dumps({
                    'type': 'error',
                    'message': 'Admin privileges required to delete rooms'
                }))
                return
            
            if not args:
                await websocket.send(json.dumps({
                    'type': 'error',
                    'message': 'Usage: /deleteroom room-name'
                }))
                return
            
            valid, room_name = self.validate_room_name(args)
            if not valid:
                await websocket.send(json.dumps({
                    'type': 'error',
                    'message': f'Invalid room name: {room_name}'
                }))
                return
            
            success, message = self.delete_room(room_name, username)
            await websocket.send(json.dumps({
                'type': 'room_deleted' if success else 'error',
                'message': message
            }))
            
            if success:
                # Notify all users in the deleted room
                await self.broadcast_to_room(room_name, {
                    'type': 'room_deleted',
                    'room_name': room_name,
                    'message': f'Room {room_name} has been deleted by {username}',
                    'timestamp': datetime.now().isoformat()
                })
                await self.broadcast_rooms_list()

    async def handle_message(self, websocket, message_data):
        """Handle incoming message"""
        if websocket not in self.clients:
            return
            
        user_data = self.clients[websocket]
        content = message_data.get('content', '').strip()
        message_type = message_data.get('message_type', 'text')
        target_room = message_data.get('room', 'main')
        
        # Handle different message types
        if message_type == 'ping':
            await self.handle_ping(websocket)
            return

        elif message_type == 'register':
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

        elif message_type == 'get_rooms':
            rooms_list = self.get_active_rooms()
            await websocket.send(json.dumps({
                'type': 'rooms_list',
                'rooms': rooms_list
            }))
            return

        elif message_type == 'get_room_users':
            room_name = message_data.get('room_name', 'main')
            room_users_list = self.get_room_users_detailed(room_name)
            await websocket.send(json.dumps({
                'type': 'room_users_list',
                'room_name': room_name,
                'users': room_users_list
            }))
            return

        elif message_type == 'join_room':
            room_name = message_data.get('room_name')
            if room_name:
                success, message = self.join_room(user_data['username'], room_name)
                if success:
                    await websocket.send(json.dumps({
                        'type': 'room_joined',
                        'room_name': room_name,
                        'message': message
                    }))
                    await self.broadcast_to_room(room_name, {
                        'type': 'user_joined_room',
                        'username': user_data['username'],
                        'message': f'{user_data["username"]} joined the room',
                        'timestamp': datetime.now().isoformat()
                    }, exclude=websocket)
                    await self.broadcast_room_users_list(room_name)
                else:
                    await websocket.send(json.dumps({
                        'type': 'error',
                        'message': message
                    }))
            return

        elif message_type == 'leave_room':
            room_name = message_data.get('room_name')
            if room_name:
                success, message = self.leave_room(user_data['username'], room_name)
                if success:
                    await websocket.send(json.dumps({
                        'type': 'room_left',
                        'room_name': room_name,
                        'message': message
                    }))
                    await self.broadcast_to_room(room_name, {
                        'type': 'user_left_room',
                        'username': user_data['username'],
                        'message': f'{user_data["username"]} left the room',
                        'timestamp': datetime.now().isoformat()
                    })
                    await self.broadcast_room_users_list(room_name)
                else:
                    await websocket.send(json.dumps({
                        'type': 'error',
                        'message': message
                    }))
            return

        if not content:
            return

        # Handle commands
        if content.startswith('/'):
            command_parts = content[1:].split(' ', 1)
            command = command_parts[0]
            args = command_parts[1] if len(command_parts) > 1 else ''
            
            # Room commands
            if command in ['join', 'left', 'createroom', 'deleteroom']:
                await self.handle_room_command(websocket, command, args)
                return
            
            # Other existing commands
            elif command == 'changeuname':
                if not args:
                    await websocket.send(json.dumps({
                        'type': 'error',
                        'message': 'Usage: /changeuname <new_username>'
                    }))
                    return
                await self.handle_username_change(websocket, args)
                return

            elif command == 'admin':
                await self.handle_admin_command(websocket, args)
                return
            
            elif command == 'userinfo':
                await self.handle_user_info_command(websocket, args)
                return
            
            elif command == 'kick':
                await self.handle_kick_command(websocket, args)
                return
            
            elif command == 'ban':
                await self.handle_ban_command(websocket, args)
                return
            
            elif command == 'help':
                help_text = "Commands: /admin <password>, /changeuname <new_username>, /kick <username>, /ban <username>, /userinfo <username>, /join #room-name, /left, /createroom <n> (admin), /deleteroom <n> (admin)"
                await websocket.send(json.dumps({
                    'type': 'help',
                    'message': help_text
                }))
                return

        # Check if user is in target room
        if target_room not in self.user_rooms[user_data['username']]:
            await websocket.send(json.dumps({
                'type': 'error',
                'message': f'You are not in room {target_room}'
            }))
            return

        # Check flood protection for regular messages
        if self.check_flood_protection(user_data['username'], user_data['is_admin']):
            await websocket.send(json.dumps({
                'type': 'error',
                'message': 'Flood protection: You are sending messages too quickly. Please wait before sending more.'
            }))
            return

        # Regular message to room
        message = {
            'type': 'message',
            'username': user_data['username'],
            'content': content,
            'timestamp': datetime.now().isoformat(),
            'is_admin': user_data['is_admin']
        }
        
        await self.broadcast_to_room(target_room, message)

    # ... (include all other existing methods like handle_ping, handle_public_key_register, etc.)
    
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
        
        # Update room user lists for all rooms the user is in
        for room_name in self.get_user_rooms(user_data['username']):
            await self.broadcast_room_users_list(room_name)
        
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Public key registered for {user_data['username']}")

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

        # Update username in database
        try:
            conn = self.get_db_connection()
            cursor = conn.cursor()
            cursor.execute('UPDATE user_rooms SET username = ? WHERE username = ?', 
                          (new_username, old_username))
            conn.commit()
            conn.close()
        except Exception as e:
            print(f"Error updating username in database: {e}")

        # Update username in memory
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

        # Update room memberships
        if old_username in self.user_rooms:
            user_rooms = self.user_rooms[old_username]
            self.user_rooms[new_username] = user_rooms
            del self.user_rooms[old_username]
            
            # Update room_users mapping
            for room_name in user_rooms:
                if old_username in self.room_users[room_name]:
                    self.room_users[room_name].discard(old_username)
                    self.room_users[room_name].add(new_username)

        # Send confirmation to user
        await websocket.send(json.dumps({
            'type': 'username_changed',
            'old_username': old_username,
            'new_username': new_username,
            'message': f'Username changed to {new_username}'
        }))

        # Broadcast username change to all rooms user is in
        for room_name in self.user_rooms[new_username]:
            await self.broadcast_to_room(room_name, {
                'type': 'user_renamed',
                'old_username': old_username,
                'new_username': new_username,
                'message': f'{old_username} changed username to {new_username}',
                'timestamp': datetime.now().isoformat()
            }, exclude=websocket)
            # Update room user lists
            await self.broadcast_room_users_list(room_name)

        # Send updated users list
        await self.broadcast_users_list()

        print(f"[{datetime.now().strftime('%H:%M:%S')}] {old_username} changed username to {new_username}")

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
            await self.broadcast_users_list()
            
            # Update room user lists for all rooms the user is in
            for room_name in self.get_user_rooms(user_data['username']):
                await self.broadcast_room_users_list(room_name)
            
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
                        'joined_at': data['joined_at'],
                        'rooms': self.get_user_rooms(data['username'])
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
            
            # Broadcast to all rooms user was in
            for room_name in self.user_rooms[target_data['username']]:
                await self.broadcast_to_room(room_name, {
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
                
                # Broadcast to all rooms user was in
                for room_name in self.user_rooms[data['username']]:
                    await self.broadcast_to_room(room_name, {
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
    print("Starting xsukax Chat System with Room Support on port 3333...")
    print("Admin password will rotate every hour")
    print("Supports both ws:// and wss:// connections")
    print("Private messages are end-to-end encrypted")
    print("Enhanced connection stability with heartbeat")
    print("Flood protection: 30 messages per minute for users (admins exempt)")
    print("Room functionality with SQLite database storage")
    print("Room-specific user lists for better organization")
    
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

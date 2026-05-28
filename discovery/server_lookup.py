"""
discovery/server_lookup.py
Relay server communication for WAN device discovery
Handles registration, online status, and user lookup
"""

import socket
import json
import threading
import time
import uuid
import requests
from typing import Callable, Optional, Dict, List
from dataclasses import dataclass, asdict
from enum import Enum

class UserStatus(Enum):
    ONLINE = "online"
    AWAY = "away"
    OFFLINE = "offline"
    BUSY = "busy"

@dataclass
class RegisteredUser:
    """Information about a registered user"""
    user_id: str
    username: str
    display_name: str
    status: UserStatus
    last_seen: float
    public_key: Optional[str] = None
    avatar_url: Optional[str] = None
    
    def to_dict(self):
        return {
            'user_id': self.user_id,
            'username': self.username,
            'display_name': self.display_name,
            'status': self.status.value,
            'last_seen': self.last_seen,
            'public_key': self.public_key,
            'avatar_url': self.avatar_url
        }
    
    @classmethod
    def from_dict(cls, data: dict):
        return cls(
            user_id=data['user_id'],
            username=data['username'],
            display_name=data['display_name'],
            status=UserStatus(data['status']),
            last_seen=data['last_seen'],
            public_key=data.get('public_key'),
            avatar_url=data.get('avatar_url')
        )

@dataclass
class SharingCode:
    """Temporary sharing code for quick connections"""
    code: str
    user_id: str
    username: str
    expires_at: float
    max_uses: int = 1
    used_count: int = 0
    
    def is_valid(self) -> bool:
        return time.time() < self.expires_at and self.used_count < self.max_uses

class ServerLookup:
    """
    Relay server communication for WAN discovery
    """
    
    def __init__(self, server_url: str = "http://localhost:5000",
                 on_user_status_change: Callable = None,
                 on_sharing_code_received: Callable = None):
        """
        Initialize server lookup
        
        Args:
            server_url: URL of relay server
            on_user_status_change: Callback when user comes online/offline
            on_sharing_code_received: Callback when sharing code is generated
        """
        self.server_url = server_url.rstrip('/')
        self.on_user_status_change = on_user_status_change
        self.on_sharing_code_received = on_sharing_code_received
        
        # Local device info
        self.device_id = str(uuid.uuid4())
        self.device_name = socket.gethostname()
        self.local_ip = self._get_local_ip()
        
        # Current user (set after registration)
        self.current_user = None
        self.auth_token = None
        
        # Known users cache
        self.online_users: Dict[str, RegisteredUser] = {}
        self.all_users: Dict[str, RegisteredUser] = {}
        self.lock = threading.Lock()
        
        # Active sharing codes
        self.active_codes: Dict[str, SharingCode] = {}
        
        # Background threads
        self.is_running = False
        self.heartbeat_thread = None
        self.pull_thread = None
        
        # Configuration
        self.HEARTBEAT_INTERVAL = 30  # Send heartbeat every 30 seconds
        self.PULL_INTERVAL = 5  # Pull updates every 5 seconds
        
    def register(self, username: str, display_name: str = None, 
                 email: str = None, password: str = None) -> bool:
        """
        Register this device with relay server
        
        Args:
            username: Unique username
            display_name: Display name (defaults to username)
            email: Optional email for account recovery
            password: Optional password for authentication
            
        Returns:
            True if registration successful
        """
        display_name = display_name or username
        
        payload = {
            'device_id': self.device_id,
            'device_name': self.device_name,
            'username': username,
            'display_name': display_name,
            'email': email,
            'password': password,
            'local_ip': self.local_ip,
            'platform': self._get_platform()
        }
        
        try:
            response = requests.post(
                f"{self.server_url}/api/register",
                json=payload,
                timeout=10
            )
            
            if response.status_code == 200:
                data = response.json()
                
                # FIX: Parse new server response format
                if data.get('success') and data.get('user'):
                    user_data = data['user']
                    self.auth_token = user_data.get('token')
                    self.current_user = RegisteredUser(
                        user_id=user_data.get('user_id', str(uuid.uuid4())),
                        username=user_data.get('username', username),
                        display_name=user_data.get('display_name', display_name),
                        status=UserStatus.ONLINE,
                        last_seen=time.time()
                    )
                    print(f"[ServerLookup] ✅ Registered as {display_name}")
                    return True
                else:
                    # Try login if already registered
                    print(f"[ServerLookup] User might already exist, trying login...")
                    return self.login(username, password)
            else:
                print(f"[ServerLookup] ❌ Registration failed: {response.text}")
                return False
                
        except Exception as e:
            print(f"[ServerLookup] ❌ Registration error: {e}")
            return False
            
    def login(self, username: str, password: str = None) -> bool:
        """
        Login to existing account
        
        Args:
            username: Username
            password: Optional password
            
        Returns:
            True if login successful
        """
        payload = {
            'device_id': self.device_id,
            'device_name': self.device_name,
            'username': username,
            'password': password,
            'local_ip': self.local_ip
        }
        
        try:
            response = requests.post(
                f"{self.server_url}/api/login",
                json=payload,
                timeout=10
            )
            
            if response.status_code == 200:
                data = response.json()
                
                # FIX: Parse new server response format
                if data.get('success') and data.get('user'):
                    user_data = data['user']
                    self.auth_token = user_data.get('token')
                    self.current_user = RegisteredUser(
                        user_id=user_data.get('user_id', str(uuid.uuid4())),
                        username=user_data.get('username', username),
                        display_name=user_data.get('display_name', username),
                        status=UserStatus.ONLINE,
                        last_seen=time.time()
                    )
                    print(f"[ServerLookup] ✅ Logged in as {self.current_user.display_name}")
                    return True
                else:
                    print(f"[ServerLookup] ❌ Login failed: Unexpected response format")
                    return False
            elif response.status_code == 404:
                print(f"[ServerLookup] ❌ User not found: {username}")
                return False
            else:
                print(f"[ServerLookup] ❌ Login failed: {response.text}")
                return False
                
        except Exception as e:
            print(f"[ServerLookup] ❌ Login error: {e}")
            return False
            
    def start(self):
        """Start background heartbeat and pull threads"""
        if self.is_running:
            return
            
        if not self.current_user:
            print("[ServerLookup] ⚠️ Not logged in. Call register() or login() first.")
            return
            
        self.is_running = True
        
        # Start threads
        self.heartbeat_thread = threading.Thread(target=self._heartbeat_loop, daemon=True)
        self.pull_thread = threading.Thread(target=self._pull_loop, daemon=True)
        
        self.heartbeat_thread.start()
        self.pull_thread.start()
        
        print("[ServerLookup] ✅ Started background service")
        
    def stop(self):
        """Stop background service"""
        self.is_running = False
        
        # Send offline notification
        if self.current_user:
            self._send_status(UserStatus.OFFLINE)
            
        # Wait for threads
        for thread in [self.heartbeat_thread, self.pull_thread]:
            if thread and thread.is_alive():
                thread.join(timeout=2.0)
                
        print("[ServerLookup] Stopped")
        
    def _heartbeat_loop(self):
        """Send periodic heartbeat to server"""
        while self.is_running:
            try:
                self._send_heartbeat()
                time.sleep(self.HEARTBEAT_INTERVAL)
            except Exception as e:
                print(f"[ServerLookup] Heartbeat error: {e}")
                time.sleep(5)
                
    def _send_heartbeat(self):
        """Send heartbeat to keep online status"""
        if not self.auth_token:
            return
            
        payload = {
            'device_id': self.device_id,
            'status': UserStatus.ONLINE.value,
            'local_ip': self.local_ip
        }
        
        headers = {'Authorization': f'Bearer {self.auth_token}'}
        
        try:
            response = requests.post(
                f"{self.server_url}/api/heartbeat",
                json=payload,
                headers=headers,
                timeout=5
            )
            
            if response.status_code != 200:
                print(f"[ServerLookup] ⚠️ Heartbeat failed: {response.status_code}")
                
        except Exception as e:
            print(f"[ServerLookup] ⚠️ Heartbeat error: {e}")
            
    def _send_status(self, status: UserStatus):
        """Update user status on server"""
        if not self.auth_token:
            return
            
        payload = {
            'device_id': self.device_id,
            'status': status.value
        }
        
        headers = {'Authorization': f'Bearer {self.auth_token}'}
        
        try:
            requests.post(
                f"{self.server_url}/api/user/status",
                json=payload,
                headers=headers,
                timeout=5
            )
        except:
            pass
            
    def _pull_loop(self):
        """Pull updates from server"""
        while self.is_running:
            try:
                self._pull_updates()
                time.sleep(self.PULL_INTERVAL)
            except Exception as e:
                print(f"[ServerLookup] Pull error: {e}")
                time.sleep(5)
                
    def _pull_updates(self):
        """Get online users and pending notifications"""
        if not self.auth_token:
            return
            
        headers = {'Authorization': f'Bearer {self.auth_token}'}
        
        try:
            # FIX: Correct URL path
            response = requests.get(
                f"{self.server_url}/api/users/online",
                headers=headers,
                timeout=5
            )
            
            if response.status_code == 200:
                data = response.json()
                self._update_online_users(data.get('users', []))
                
            # Check for pending transfers
            response = requests.get(
                f"{self.server_url}/api/pending_transfers",
                headers=headers,
                timeout=5
            )
            
            if response.status_code == 200:
                data = response.json()
                self._handle_pending_transfers(data.get('transfers', []))
                
        except Exception as e:
            pass
            
    def _update_online_users(self, users_data: list):
        """Update local cache of online users"""
        with self.lock:
            current_online_ids = set()
            
            for user_data in users_data:
                user = RegisteredUser.from_dict(user_data)
                
                # Skip self
                if user.user_id == self.current_user.user_id:
                    continue
                    
                current_online_ids.add(user.user_id)
                
                # Check if newly online
                if user.user_id not in self.online_users:
                    self.online_users[user.user_id] = user
                    if self.on_user_status_change:
                        self.on_user_status_change(user, True)  # Came online
                        
                # Update cache
                self.all_users[user.user_id] = user
                
            # Check for users who went offline
            for user_id in list(self.online_users.keys()):
                if user_id not in current_online_ids:
                    offline_user = self.online_users[user_id]
                    del self.online_users[user_id]
                    
                    if self.on_user_status_change:
                        self.on_user_status_change(offline_user, False)  # Went offline
                        
    def _handle_pending_transfers(self, transfers: list):
        """Handle incoming transfer notifications"""
        for transfer in transfers:
            print(f"[ServerLookup] 📨 Incoming transfer from {transfer['from_user']}")
            # This will be handled by the main controller
            
    def get_online_users(self) -> List[RegisteredUser]:
        """Get list of currently online users"""
        with self.lock:
            return list(self.online_users.values())
            
    def get_user(self, user_id: str) -> Optional[RegisteredUser]:
        """Get user by ID"""
        with self.lock:
            return self.all_users.get(user_id)
            
    def find_user_by_username(self, username: str) -> Optional[RegisteredUser]:
        """Find user by username"""
        with self.lock:
            for user in self.all_users.values():
                if user.username.lower() == username.lower():
                    return user
        return None
        
    def generate_sharing_code(self, duration_seconds: int = 3600, 
                              max_uses: int = 1) -> Optional[str]:
        """
        Generate a temporary sharing code
        
        Args:
            duration_seconds: How long code is valid (default 1 hour)
            max_uses: Maximum times code can be used
            
        Returns:
            Sharing code string (e.g., "X7K9-M2NP")
        """
        if not self.auth_token:
            print("[ServerLookup] ❌ Not authenticated")
            return None
            
        payload = {
            'user_id': self.current_user.user_id,
            'duration': duration_seconds,
            'max_uses': max_uses
        }
        
        headers = {'Authorization': f'Bearer {self.auth_token}'}
        
        try:
            response = requests.post(
                f"{self.server_url}/api/code/generate",
                json=payload,
                headers=headers,
                timeout=10
            )
            
            if response.status_code == 200:
                data = response.json()
                code = data['code']
                
                # Store locally
                self.active_codes[code] = SharingCode(
                    code=code,
                    user_id=self.current_user.user_id,
                    username=self.current_user.username,
                    expires_at=time.time() + duration_seconds,
                    max_uses=max_uses
                )
                
                if self.on_sharing_code_received:
                    self.on_sharing_code_received(code)
                    
                print(f"[ServerLookup] ✅ Generated code: {code}")
                return code
                
        except Exception as e:
            print(f"[ServerLookup] ❌ Generate code error: {e}")
            
        return None
        
    def lookup_code(self, code: str) -> Optional[RegisteredUser]:
        """
        Look up user by sharing code
        
        Args:
            code: Sharing code (e.g., "X7K9-M2NP")
            
        Returns:
            RegisteredUser if code is valid
        """
        # Clean code (remove spaces, uppercase)
        code = code.upper().replace(' ', '').replace('-', '')
        
        # Check local cache first
        if code in self.active_codes:
            sharing_code = self.active_codes[code]
            if sharing_code.is_valid():
                return self.get_user(sharing_code.user_id)
                
        # Query server
        try:
            response = requests.get(
                f"{self.server_url}/api/code/{code}",
                timeout=5
            )
            
            if response.status_code == 200:
                data = response.json()
                return RegisteredUser.from_dict(data['user'])
                
        except Exception as e:
            print(f"[ServerLookup] ❌ Code lookup error: {e}")
            
        return None
        
    def create_transfer_link(self, transfer_id: str) -> Optional[str]:
        """
        Create a shareable link for a transfer
        
        Args:
            transfer_id: ID of the transfer
            
        Returns:
            Shareable URL
        """
        if not self.auth_token:
            return None
            
        payload = {
            'transfer_id': transfer_id,
            'sender_id': self.current_user.user_id
        }
        
        headers = {'Authorization': f'Bearer {self.auth_token}'}
        
        try:
            response = requests.post(
                f"{self.server_url}/api/transfer/create",
                json=payload,
                headers=headers,
                timeout=10
            )
            
            if response.status_code == 200:
                data = response.json()
                return data.get('transfer_id')
                
        except Exception as e:
            print(f"[ServerLookup] ❌ Create transfer error: {e}")
            
        return None
        
    def send_transfer_request(self, target_user_id: str, 
                              file_metadata: dict) -> bool:
        """
        Send transfer request to another user
        
        Args:
            target_user_id: Recipient user ID
            file_metadata: Info about file being sent
            
        Returns:
            True if request sent successfully
        """
        if not self.auth_token:
            return False
            
        payload = {
            'from_user_id': self.current_user.user_id,
            'from_username': self.current_user.username,
            'to_user_id': target_user_id,
            'file_metadata': file_metadata
        }
        
        headers = {'Authorization': f'Bearer {self.auth_token}'}
        
        try:
            response = requests.post(
                f"{self.server_url}/api/transfer_request",
                json=payload,
                headers=headers,
                timeout=10
            )
            
            return response.status_code == 200
            
        except Exception as e:
            print(f"[ServerLookup] ❌ Transfer request error: {e}")
            return False
            
    def set_status(self, status: UserStatus):
        """Manually set user status"""
        self._send_status(status)
        
    def _get_local_ip(self) -> str:
        """Get local IP address"""
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(('8.8.8.8', 80))
            ip = s.getsockname()[0]
            s.close()
            return ip
        except:
            return '127.0.0.1'
            
    def _get_platform(self) -> str:
        """Get platform name"""
        import platform
        return platform.system()
        
    def is_authenticated(self) -> bool:
        """Check if user is authenticated"""
        return self.current_user is not None and self.auth_token is not None


# ============================================
# TEST FUNCTION
# ============================================

def test_server_lookup():
    """Test server lookup functionality"""
    
    def on_status_change(user: RegisteredUser, is_online: bool):
        status = "🟢 ONLINE" if is_online else "🔴 OFFLINE"
        print(f"\n{status}: {user.display_name} (@{user.username})")
        
    def on_code_generated(code: str):
        print(f"\n🔑 Sharing Code Generated: {code}")
        print(f"   Share this code with others to connect!")
        
    # Create lookup client
    lookup = ServerLookup(
        server_url="http://localhost:5000",
        on_user_status_change=on_status_change,
        on_sharing_code_received=on_code_generated
    )
    
    print("\n🔐 Server Lookup Test")
    print("=" * 50)
    
    # Try to register or login
    username = input("Username: ").strip()
    
    if not username:
        username = f"User-{uuid.uuid4().hex[:6]}"
        print(f"Generated username: {username}")
        
    # Register
    if lookup.register(username=username, display_name=username):
        lookup.start()
        
        print("\n✅ Connected to relay server!")
        print("\nCommands:")
        print("  /online - Show online users")
        print("  /code - Generate sharing code")
        print("  /lookup <code> - Look up user by code")
        print("  /away - Set status to AWAY")
        print("  /quit - Exit\n")
        
        try:
            while True:
                cmd = input().strip().lower()
                
                if cmd == "/online":
                    users = lookup.get_online_users()
                    if users:
                        print(f"\n👥 Online Users ({len(users)}):")
                        for u in users:
                            print(f"   • {u.display_name} (@{u.username}) - {u.status.value}")
                    else:
                        print("\n   No users online")
                        
                elif cmd == "/code":
                    code = lookup.generate_sharing_code(duration_seconds=300)
                    
                elif cmd.startswith("/lookup"):
                    parts = cmd.split()
                    if len(parts) > 1:
                        user = lookup.lookup_code(parts[1])
                        if user:
                            print(f"\n✅ Found: {user.display_name} (@{user.username})")
                        else:
                            print("\n❌ Invalid or expired code")
                            
                elif cmd == "/away":
                    lookup.set_status(UserStatus.AWAY)
                    print("Status set to AWAY")
                    
                elif cmd == "/quit":
                    break
                    
        except KeyboardInterrupt:
            pass
            
        lookup.stop()
        
    else:
        print("❌ Failed to connect to server")
        print("   Make sure relay server is running!")


if __name__ == "__main__":
    test_server_lookup()
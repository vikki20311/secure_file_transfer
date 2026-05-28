"""
server/user_registry.py
User management and authentication system for relay server
Handles user accounts, sessions, and online presence
"""

import os
import json
import time
import uuid
import hashlib
import secrets
import threading
import sqlite3
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, field
from enum import Enum
from datetime import datetime, timedelta

class UserStatus(Enum):
    ONLINE = "online"
    AWAY = "away"
    BUSY = "busy"
    OFFLINE = "offline"
    INVISIBLE = "invisible"

class UserRole(Enum):
    USER = "user"
    ADMIN = "admin"
    GUEST = "guest"

@dataclass
class User:
    """User account information"""
    user_id: str
    username: str
    display_name: str
    email: Optional[str] = None
    password_hash: Optional[str] = None
    role: UserRole = UserRole.USER
    status: UserStatus = UserStatus.OFFLINE
    avatar_url: Optional[str] = None
    public_key: Optional[str] = None
    created_at: float = 0.0
    last_login: float = 0.0
    last_seen: float = 0.0
    
    def to_dict(self, include_private: bool = False) -> dict:
        """Convert to dictionary"""
        data = {
            'user_id': self.user_id,
            'username': self.username,
            'display_name': self.display_name,
            'email': self.email if include_private else None,
            'role': self.role.value,
            'status': self.status.value,
            'avatar_url': self.avatar_url,
            'public_key': self.public_key,
            'created_at': self.created_at,
            'last_login': self.last_login,
            'last_seen': self.last_seen
        }
        
        if not include_private:
            data.pop('email', None)
            
        return {k: v for k, v in data.items() if v is not None}
        
    @classmethod
    def from_dict(cls, data: dict) -> 'User':
        """Create from dictionary"""
        return cls(
            user_id=data.get('user_id', ''),
            username=data.get('username', ''),
            display_name=data.get('display_name', ''),
            email=data.get('email'),
            password_hash=data.get('password_hash'),
            role=UserRole(data.get('role', 'user')),
            status=UserStatus(data.get('status', 'offline')),
            avatar_url=data.get('avatar_url'),
            public_key=data.get('public_key'),
            created_at=data.get('created_at', time.time()),
            last_login=data.get('last_login', 0),
            last_seen=data.get('last_seen', 0)
        )

@dataclass
class Session:
    """Active user session"""
    session_id: str
    user_id: str
    device_id: str
    device_name: str
    auth_token: str
    ip_address: str
    created_at: float
    expires_at: float
    last_activity: float
    
    def is_expired(self) -> bool:
        """Check if session is expired"""
        return time.time() > self.expires_at
        
    def get_remaining_time(self) -> float:
        """Get remaining session time"""
        return max(0, self.expires_at - time.time())

@dataclass
class Contact:
    """User contact/friend relationship"""
    user_id: str
    contact_id: str
    nickname: Optional[str] = None
    is_favorite: bool = False
    is_blocked: bool = False
    created_at: float = 0.0

class UserDatabase:
    """Database operations for user management"""
    
    def __init__(self, db_path: str = "relay_server.db"):
        self.db_path = db_path
        self.lock = threading.Lock()
        self._init_tables()
        
    def _init_tables(self):
        """Initialize database tables"""
        with self.lock:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Users table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS users (
                    user_id TEXT PRIMARY KEY,
                    username TEXT UNIQUE NOT NULL,
                    display_name TEXT NOT NULL,
                    email TEXT UNIQUE,
                    password_hash TEXT,
                    role TEXT DEFAULT 'user',
                    status TEXT DEFAULT 'offline',
                    avatar_url TEXT,
                    public_key TEXT,
                    created_at REAL,
                    last_login REAL,
                    last_seen REAL
                )
            ''')
            
            # Sessions table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS sessions (
                    session_id TEXT PRIMARY KEY,
                    user_id TEXT NOT NULL,
                    device_id TEXT NOT NULL,
                    device_name TEXT,
                    auth_token TEXT UNIQUE NOT NULL,
                    ip_address TEXT,
                    created_at REAL,
                    expires_at REAL,
                    last_activity REAL,
                    FOREIGN KEY (user_id) REFERENCES users(user_id)
                )
            ''')
            
            # Contacts table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS contacts (
                    user_id TEXT NOT NULL,
                    contact_id TEXT NOT NULL,
                    nickname TEXT,
                    is_favorite INTEGER DEFAULT 0,
                    is_blocked INTEGER DEFAULT 0,
                    created_at REAL,
                    PRIMARY KEY (user_id, contact_id),
                    FOREIGN KEY (user_id) REFERENCES users(user_id),
                    FOREIGN KEY (contact_id) REFERENCES users(user_id)
                )
            ''')
            
            # Devices table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS user_devices (
                    device_id TEXT PRIMARY KEY,
                    user_id TEXT NOT NULL,
                    device_name TEXT,
                    device_type TEXT,
                    last_used REAL,
                    is_trusted INTEGER DEFAULT 0,
                    FOREIGN KEY (user_id) REFERENCES users(user_id)
                )
            ''')
            
            conn.commit()
            conn.close()
            
    def create_user(self, username: str, display_name: str,
                    password: str = None, email: str = None) -> Optional[User]:
        """Create a new user"""
        user_id = str(uuid.uuid4())
        password_hash = self._hash_password(password) if password else None
        
        user = User(
            user_id=user_id,
            username=username.lower(),
            display_name=display_name,
            email=email.lower() if email else None,
            password_hash=password_hash,
            created_at=time.time()
        )
        
        try:
            with self.lock:
                conn = sqlite3.connect(self.db_path)
                cursor = conn.cursor()
                
                cursor.execute('''
                    INSERT INTO users 
                    (user_id, username, display_name, email, password_hash, 
                     role, status, avatar_url, public_key, created_at, last_login, last_seen)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    user.user_id, user.username, user.display_name, user.email,
                    user.password_hash, user.role.value, user.status.value,
                    user.avatar_url, user.public_key, user.created_at,
                    user.last_login, user.last_seen
                ))
                
                conn.commit()
                conn.close()
                
                return user
                
        except sqlite3.IntegrityError:
            return None
            
    def get_user(self, user_id: str = None, username: str = None,
                 email: str = None) -> Optional[User]:
        """Get user by ID, username, or email"""
        try:
            with self.lock:
                conn = sqlite3.connect(self.db_path)
                cursor = conn.cursor()
                
                if user_id:
                    cursor.execute('SELECT * FROM users WHERE user_id = ?', (user_id,))
                elif username:
                    cursor.execute('SELECT * FROM users WHERE username = ?', (username.lower(),))
                elif email:
                    cursor.execute('SELECT * FROM users WHERE email = ?', (email.lower(),))
                else:
                    return None
                    
                row = cursor.fetchone()
                conn.close()
                
                if row:
                    return User(
                        user_id=row[0], username=row[1], display_name=row[2],
                        email=row[3], password_hash=row[4], role=UserRole(row[5]),
                        status=UserStatus(row[6]), avatar_url=row[7], public_key=row[8],
                        created_at=row[9], last_login=row[10], last_seen=row[11]
                    )
        except:
            pass
            
        return None
        
    def update_user(self, user: User) -> bool:
        """Update user information"""
        try:
            with self.lock:
                conn = sqlite3.connect(self.db_path)
                cursor = conn.cursor()
                
                cursor.execute('''
                    UPDATE users SET
                        display_name = ?,
                        email = ?,
                        role = ?,
                        status = ?,
                        avatar_url = ?,
                        public_key = ?,
                        last_login = ?,
                        last_seen = ?
                    WHERE user_id = ?
                ''', (
                    user.display_name, user.email, user.role.value,
                    user.status.value, user.avatar_url, user.public_key,
                    user.last_login, user.last_seen, user.user_id
                ))
                
                conn.commit()
                conn.close()
                return True
        except:
            return False
            
    def update_status(self, user_id: str, status: UserStatus):
        """Update user online status"""
        try:
            with self.lock:
                conn = sqlite3.connect(self.db_path)
                cursor = conn.cursor()
                
                cursor.execute('''
                    UPDATE users SET status = ?, last_seen = ? WHERE user_id = ?
                ''', (status.value, time.time(), user_id))
                
                conn.commit()
                conn.close()
        except:
            pass
            
    def authenticate_user(self, username: str, password: str) -> Optional[User]:
        """Authenticate user with password"""
        user = self.get_user(username=username)
        
        if user and user.password_hash:
            if self._verify_password(password, user.password_hash):
                user.last_login = time.time()
                user.last_seen = time.time()
                user.status = UserStatus.ONLINE
                self.update_user(user)
                return user
                
        return None
        
    def _hash_password(self, password: str) -> str:
        """Hash password with salt"""
        salt = secrets.token_hex(16)
        hash_obj = hashlib.pbkdf2_hmac(
            'sha256',
            password.encode('utf-8'),
            salt.encode('utf-8'),
            100000
        )
        return f"{salt}${hash_obj.hex()}"
        
    def _verify_password(self, password: str, password_hash: str) -> bool:
        """Verify password against hash"""
        try:
            salt, hash_value = password_hash.split('$')
            verify_hash = hashlib.pbkdf2_hmac(
                'sha256',
                password.encode('utf-8'),
                salt.encode('utf-8'),
                100000
            )
            return verify_hash.hex() == hash_value
        except:
            return False
            
    def create_session(self, user_id: str, device_id: str,
                       device_name: str, ip_address: str,
                       duration: int = 86400) -> Optional[Session]:
        """Create a new session"""
        session_id = str(uuid.uuid4())
        auth_token = secrets.token_hex(32)
        
        session = Session(
            session_id=session_id,
            user_id=user_id,
            device_id=device_id,
            device_name=device_name,
            auth_token=auth_token,
            ip_address=ip_address,
            created_at=time.time(),
            expires_at=time.time() + duration,
            last_activity=time.time()
        )
        
        try:
            with self.lock:
                conn = sqlite3.connect(self.db_path)
                cursor = conn.cursor()
                
                cursor.execute('''
                    INSERT INTO sessions 
                    (session_id, user_id, device_id, device_name, auth_token,
                     ip_address, created_at, expires_at, last_activity)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    session.session_id, session.user_id, session.device_id,
                    session.device_name, session.auth_token, session.ip_address,
                    session.created_at, session.expires_at, session.last_activity
                ))
                
                conn.commit()
                conn.close()
                
                return session
                
        except:
            return None
            
    def get_session(self, auth_token: str) -> Optional[Session]:
        """Get session by auth token"""
        try:
            with self.lock:
                conn = sqlite3.connect(self.db_path)
                cursor = conn.cursor()
                
                cursor.execute(
                    'SELECT * FROM sessions WHERE auth_token = ?',
                    (auth_token,)
                )
                row = cursor.fetchone()
                conn.close()
                
                if row:
                    return Session(
                        session_id=row[0], user_id=row[1], device_id=row[2],
                        device_name=row[3], auth_token=row[4], ip_address=row[5],
                        created_at=row[6], expires_at=row[7], last_activity=row[8]
                    )
        except:
            pass
            
        return None
        
    def update_session_activity(self, auth_token: str):
        """Update session last activity"""
        try:
            with self.lock:
                conn = sqlite3.connect(self.db_path)
                cursor = conn.cursor()
                
                cursor.execute('''
                    UPDATE sessions SET last_activity = ? WHERE auth_token = ?
                ''', (time.time(), auth_token))
                
                conn.commit()
                conn.close()
        except:
            pass
            
    def delete_session(self, auth_token: str):
        """Delete a session"""
        try:
            with self.lock:
                conn = sqlite3.connect(self.db_path)
                cursor = conn.cursor()
                
                cursor.execute('DELETE FROM sessions WHERE auth_token = ?', (auth_token,))
                
                conn.commit()
                conn.close()
        except:
            pass
            
    def cleanup_expired_sessions(self):
        """Remove expired sessions"""
        try:
            with self.lock:
                conn = sqlite3.connect(self.db_path)
                cursor = conn.cursor()
                
                cursor.execute('DELETE FROM sessions WHERE expires_at < ?', (time.time(),))
                
                conn.commit()
                conn.close()
        except:
            pass
            
    def add_contact(self, user_id: str, contact_id: str,
                    nickname: str = None) -> bool:
        """Add a contact"""
        try:
            with self.lock:
                conn = sqlite3.connect(self.db_path)
                cursor = conn.cursor()
                
                cursor.execute('''
                    INSERT OR REPLACE INTO contacts 
                    (user_id, contact_id, nickname, created_at)
                    VALUES (?, ?, ?, ?)
                ''', (user_id, contact_id, nickname, time.time()))
                
                conn.commit()
                conn.close()
                return True
        except:
            return False
            
    def get_contacts(self, user_id: str) -> List[Contact]:
        """Get user's contacts"""
        contacts = []
        
        try:
            with self.lock:
                conn = sqlite3.connect(self.db_path)
                cursor = conn.cursor()
                
                cursor.execute(
                    'SELECT * FROM contacts WHERE user_id = ? AND is_blocked = 0',
                    (user_id,)
                )
                
                for row in cursor.fetchall():
                    contacts.append(Contact(
                        user_id=row[0], contact_id=row[1], nickname=row[2],
                        is_favorite=bool(row[3]), is_blocked=bool(row[4]),
                        created_at=row[5]
                    ))
                    
                conn.close()
        except:
            pass
            
        return contacts
        
    def get_online_users(self, exclude_user_id: str = None) -> List[User]:
        """Get all online users"""
        users = []
        
        try:
            with self.lock:
                conn = sqlite3.connect(self.db_path)
                cursor = conn.cursor()
                
                query = "SELECT * FROM users WHERE status = 'online'"
                params = []
                
                if exclude_user_id:
                    query += " AND user_id != ?"
                    params.append(exclude_user_id)
                    
                cursor.execute(query, params)
                
                for row in cursor.fetchall():
                    users.append(User(
                        user_id=row[0], username=row[1], display_name=row[2],
                        email=row[3], password_hash=row[4], role=UserRole(row[5]),
                        status=UserStatus(row[6]), avatar_url=row[7], public_key=row[8],
                        created_at=row[9], last_login=row[10], last_seen=row[11]
                    ))
                    
                conn.close()
        except:
            pass
            
        return users

class UserRegistry:
    """
    Main user registry and authentication manager
    """
    
    def __init__(self, db_path: str = "relay_server.db"):
        self.db = UserDatabase(db_path)
        self.sessions: Dict[str, Session] = {}
        self.lock = threading.Lock()
        
        # Session timeout (24 hours)
        self.session_duration = 86400
        
        # Start cleanup thread
        self.cleanup_thread = threading.Thread(target=self._cleanup_loop, daemon=True)
        self.cleanup_thread.start()
        
    def register_user(self, username: str, display_name: str,
                      password: str = None, email: str = None) -> Optional[User]:
        """Register a new user"""
        # Validate username
        if len(username) < 3:
            return None
            
        if self.db.get_user(username=username):
            return None  # Username taken
            
        return self.db.create_user(username, display_name, password, email)
        
    def authenticate(self, username: str, password: str = None) -> Optional[User]:
        """Authenticate user"""
        if password:
            return self.db.authenticate_user(username, password)
        else:
            return self.db.get_user(username=username)
            
    def create_session(self, user: User, device_id: str,
                       device_name: str, ip_address: str) -> Optional[str]:
        """Create a session for authenticated user"""
        session = self.db.create_session(
            user.user_id, device_id, device_name, ip_address, self.session_duration
        )
        
        if session:
            with self.lock:
                self.sessions[session.auth_token] = session
                
            # Update user status
            user.status = UserStatus.ONLINE
            user.last_seen = time.time()
            self.db.update_user(user)
            
            return session.auth_token
            
        return None
        
    def validate_token(self, auth_token: str) -> Optional[User]:
        """Validate auth token and return user"""
        with self.lock:
            session = self.sessions.get(auth_token)
            
        if not session:
            session = self.db.get_session(auth_token)
            if session:
                with self.lock:
                    self.sessions[auth_token] = session
                    
        if session:
            if session.is_expired():
                self.revoke_token(auth_token)
                return None
                
            self.db.update_session_activity(auth_token)
            
            user = self.db.get_user(user_id=session.user_id)
            
            if user:
                user.last_seen = time.time()
                
            return user
            
        return None
        
    def revoke_token(self, auth_token: str):
        """Revoke an auth token"""
        with self.lock:
            session = self.sessions.pop(auth_token, None)
            
        if session:
            self.db.delete_session(auth_token)
            
            # Check if user has other active sessions
            user = self.db.get_user(user_id=session.user_id)
            
            if user:
                # Update to offline if no other sessions
                user.status = UserStatus.OFFLINE
                self.db.update_user(user)
                
    def update_user_status(self, user_id: str, status: UserStatus):
        """Update user status"""
        self.db.update_status(user_id, status)
        
    def get_online_users(self, exclude_user_id: str = None) -> List[dict]:
        """Get list of online users"""
        users = self.db.get_online_users(exclude_user_id)
        return [u.to_dict() for u in users]
        
    def get_user_info(self, user_id: str) -> Optional[dict]:
        """Get public user information"""
        user = self.db.get_user(user_id=user_id)
        return user.to_dict() if user else None
        
    def search_users(self, query: str, limit: int = 20) -> List[dict]:
        """Search for users"""
        results = []
        
        try:
            conn = sqlite3.connect(self.db.db_path)
            cursor = conn.cursor()
            
            search_term = f"%{query.lower()}%"
            
            cursor.execute('''
                SELECT * FROM users 
                WHERE username LIKE ? OR display_name LIKE ?
                LIMIT ?
            ''', (search_term, search_term, limit))
            
            for row in cursor.fetchall():
                user = User(
                    user_id=row[0], username=row[1], display_name=row[2],
                    email=row[3], password_hash=row[4], role=UserRole(row[5]),
                    status=UserStatus(row[6]), avatar_url=row[7], public_key=row[8],
                    created_at=row[9], last_login=row[10], last_seen=row[11]
                )
                results.append(user.to_dict())
                
            conn.close()
        except:
            pass
            
        return results
        
    def add_contact(self, user_id: str, contact_id: str,
                    nickname: str = None) -> bool:
        """Add a contact"""
        return self.db.add_contact(user_id, contact_id, nickname)
        
    def get_contacts(self, user_id: str) -> List[dict]:
        """Get user's contacts with online status"""
        contacts = self.db.get_contacts(user_id)
        result = []
        
        for contact in contacts:
            contact_user = self.db.get_user(user_id=contact.contact_id)
            
            if contact_user:
                info = contact_user.to_dict()
                info['nickname'] = contact.nickname
                info['is_favorite'] = contact.is_favorite
                result.append(info)
                
        return result
        
    def _cleanup_loop(self):
        """Periodic cleanup of expired sessions"""
        while True:
            time.sleep(3600)  # Run every hour
            
            self.db.cleanup_expired_sessions()
            
            # Clear expired from memory
            with self.lock:
                expired = [
                    token for token, session in self.sessions.items()
                    if session.is_expired()
                ]
                
                for token in expired:
                    self.sessions.pop(token, None)
                    
    def get_stats(self) -> dict:
        """Get registry statistics"""
        return {
            'active_sessions': len(self.sessions),
            'online_users': len(self.db.get_online_users())
        }


# ============================================
# TEST FUNCTION
# ============================================

def test_user_registry():
    """Test user registry"""
    print("👤 User Registry Test")
    print("=" * 50)
    
    # Create registry
    registry = UserRegistry("test_users.db")
    
    print("\n1️⃣ Testing user registration...")
    
    # Register test user
    user = registry.register_user(
        username="testuser",
        display_name="Test User",
        password="testpass123",
        email="test@example.com"
    )
    
    if user:
        print(f"   ✅ Registered: {user.display_name} (@{user.username})")
    else:
        print("   ❌ Registration failed")
        return
        
    print("\n2️⃣ Testing authentication...")
    
    auth_user = registry.authenticate("testuser", "testpass123")
    
    if auth_user:
        print(f"   ✅ Authenticated: {auth_user.display_name}")
    else:
        print("   ❌ Authentication failed")
        
    print("\n3️⃣ Testing session creation...")
    
    token = registry.create_session(
        auth_user,
        device_id="test-device-123",
        device_name="Test Laptop",
        ip_address="127.0.0.1"
    )
    
    if token:
        print(f"   ✅ Session created: {token[:16]}...")
    else:
        print("   ❌ Session creation failed")
        
    print("\n4️⃣ Testing token validation...")
    
    validated_user = registry.validate_token(token)
    
    if validated_user:
        print(f"   ✅ Token valid for: {validated_user.display_name}")
    else:
        print("   ❌ Token validation failed")
        
    print("\n5️⃣ Testing online users...")
    
    online = registry.get_online_users()
    print(f"   Online users: {len(online)}")
    for u in online:
        print(f"   • {u['display_name']} (@{u['username']})")
        
    print("\n6️⃣ Testing search...")
    
    results = registry.search_users("test")
    print(f"   Search results: {len(results)}")
    
    print("\n7️⃣ Testing stats...")
    
    stats = registry.get_stats()
    print(f"   Active sessions: {stats['active_sessions']}")
    print(f"   Online users: {stats['online_users']}")
    
    # Cleanup
    registry.revoke_token(token)
    
    # Remove test database
    import os
    if os.path.exists("test_users.db"):
        os.remove("test_users.db")
        
    print("\n✅ User Registry test complete!")


if __name__ == "__main__":
    test_user_registry()
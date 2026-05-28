"""
server/phone_auth.py
Phone number authentication with OTP verification
Integrates with Twilio for SMS delivery
"""

import os
import random
import time
import hashlib
import secrets
import sqlite3
import threading
from typing import Optional, Dict, Any, Tuple
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum

# Try to import Twilio
try:
    from twilio.rest import Client
    from twilio.base.exceptions import TwilioRestException
    TWILIO_AVAILABLE = True
except ImportError:
    TWILIO_AVAILABLE = False
    print("[PhoneAuth] ⚠️ Twilio not installed. Run: pip install twilio")
    print("[PhoneAuth] Using MOCK SMS mode (codes printed to console)")

class AuthStatus(Enum):
    PENDING = "pending"
    VERIFIED = "verified"
    EXPIRED = "expired"
    INVALID = "invalid"
    MAX_ATTEMPTS = "max_attempts"

@dataclass
class VerificationSession:
    """Active verification session"""
    session_id: str
    phone_number: str
    code: str
    created_at: float
    expires_at: float
    attempts: int = 0
    max_attempts: int = 3
    verified: bool = False
    
    def is_expired(self) -> bool:
        return time.time() > self.expires_at
    
    def can_attempt(self) -> bool:
        return self.attempts < self.max_attempts
    
    def verify(self, code: str) -> bool:
        self.attempts += 1
        
        if not self.can_attempt():
            return False
            
        if code == self.code:
            self.verified = True
            return True
            
        return False

@dataclass
class PalmsyncUser:
    """User account in Palmsync"""
    user_id: str
    phone_number: str
    display_name: str
    status: str = "offline"
    avatar_url: Optional[str] = None
    public_key: Optional[str] = None
    created_at: float = 0.0
    last_seen: float = 0.0
    device_tokens: list = None
    
    def to_dict(self, include_private: bool = False) -> dict:
        data = {
            'user_id': self.user_id,
            'phone_number': self.phone_number[-4:].rjust(len(self.phone_number), '*') if not include_private else self.phone_number,
            'display_name': self.display_name,
            'status': self.status,
            'avatar_url': self.avatar_url
        }
        return data

class PhoneAuthDatabase:
    """Database operations for phone authentication"""
    
    def __init__(self, db_path: str = "palmsync.db"):
        self.db_path = db_path
        self.lock = threading.Lock()
        self._init_tables()
        
    def _init_tables(self):
        """Initialize database tables"""
        with self.lock:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Users table (phone-based)
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS palmsync_users (
                    user_id TEXT PRIMARY KEY,
                    phone_number TEXT UNIQUE NOT NULL,
                    display_name TEXT,
                    status TEXT DEFAULT 'offline',
                    avatar_url TEXT,
                    public_key TEXT,
                    created_at REAL,
                    last_seen REAL,
                    device_tokens TEXT
                )
            ''')
            
            # Verification sessions table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS verification_sessions (
                    session_id TEXT PRIMARY KEY,
                    phone_number TEXT NOT NULL,
                    code_hash TEXT NOT NULL,
                    created_at REAL,
                    expires_at REAL,
                    attempts INTEGER DEFAULT 0,
                    verified INTEGER DEFAULT 0
                )
            ''')
            
            # Contacts table (who synced which contacts)
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS user_contacts (
                    user_id TEXT NOT NULL,
                    contact_phone TEXT NOT NULL,
                    contact_name TEXT,
                    last_synced REAL,
                    PRIMARY KEY (user_id, contact_phone)
                )
            ''')
            
            # Auth tokens table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS auth_tokens (
                    token TEXT PRIMARY KEY,
                    user_id TEXT NOT NULL,
                    device_id TEXT,
                    created_at REAL,
                    expires_at REAL,
                    last_used REAL,
                    FOREIGN KEY (user_id) REFERENCES palmsync_users(user_id)
                )
            ''')
            
            conn.commit()
            conn.close()
            
    def create_or_update_user(self, phone_number: str, display_name: str = None) -> PalmsyncUser:
        """Create or update user record"""
        import uuid
        
        with self.lock:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Check if user exists
            cursor.execute('SELECT user_id FROM palmsync_users WHERE phone_number = ?', (phone_number,))
            row = cursor.fetchone()
            
            if row:
                user_id = row[0]
                # Update last seen
                cursor.execute('''
                    UPDATE palmsync_users 
                    SET last_seen = ?, display_name = COALESCE(?, display_name)
                    WHERE user_id = ?
                ''', (time.time(), display_name, user_id))
            else:
                user_id = str(uuid.uuid4())
                display_name = display_name or f"User{phone_number[-4:]}"
                
                cursor.execute('''
                    INSERT INTO palmsync_users 
                    (user_id, phone_number, display_name, created_at, last_seen)
                    VALUES (?, ?, ?, ?, ?)
                ''', (user_id, phone_number, display_name, time.time(), time.time()))
                
            conn.commit()
            
            # Get full user record
            cursor.execute('SELECT * FROM palmsync_users WHERE user_id = ?', (user_id,))
            row = cursor.fetchone()
            conn.close()
            
            return PalmsyncUser(
                user_id=row[0],
                phone_number=row[1],
                display_name=row[2],
                status=row[3],
                avatar_url=row[4],
                public_key=row[5],
                created_at=row[6],
                last_seen=row[7]
            )
            
    def get_user_by_phone(self, phone_number: str) -> Optional[PalmsyncUser]:
        """Get user by phone number"""
        with self.lock:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('SELECT * FROM palmsync_users WHERE phone_number = ?', (phone_number,))
            row = cursor.fetchone()
            conn.close()
            
            if row:
                return PalmsyncUser(
                    user_id=row[0],
                    phone_number=row[1],
                    display_name=row[2],
                    status=row[3],
                    avatar_url=row[4],
                    public_key=row[5],
                    created_at=row[6],
                    last_seen=row[7]
                )
        return None
        
    def get_user_by_id(self, user_id: str) -> Optional[PalmsyncUser]:
        """Get user by ID"""
        with self.lock:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('SELECT * FROM palmsync_users WHERE user_id = ?', (user_id,))
            row = cursor.fetchone()
            conn.close()
            
            if row:
                return PalmsyncUser(
                    user_id=row[0],
                    phone_number=row[1],
                    display_name=row[2],
                    status=row[3],
                    avatar_url=row[4],
                    public_key=row[5],
                    created_at=row[6],
                    last_seen=row[7]
                )
        return None
        
    def save_verification_code(self, session_id: str, phone_number: str, 
                               code: str, expires_at: float) -> bool:
        """Save verification code"""
        with self.lock:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Hash code for security
            code_hash = hashlib.sha256(code.encode()).hexdigest()
            
            cursor.execute('''
                INSERT OR REPLACE INTO verification_sessions 
                (session_id, phone_number, code_hash, created_at, expires_at)
                VALUES (?, ?, ?, ?, ?)
            ''', (session_id, phone_number, code_hash, time.time(), expires_at))
            
            conn.commit()
            conn.close()
            return True
            
    def verify_code(self, session_id: str, phone_number: str, code: str) -> Tuple[bool, str]:
        """Verify OTP code"""
        with self.lock:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT code_hash, expires_at, attempts, verified 
                FROM verification_sessions 
                WHERE session_id = ? AND phone_number = ?
            ''', (session_id, phone_number))
            
            row = cursor.fetchone()
            
            if not row:
                return False, "Invalid session"
                
            code_hash, expires_at, attempts, verified = row
            
            # Check expiry
            if time.time() > expires_at:
                return False, "Code expired"
                
            # Check if already verified
            if verified:
                return False, "Already verified"
                
            # Check attempts
            if attempts >= 3:
                return False, "Max attempts exceeded"
                
            # Verify code
            input_hash = hashlib.sha256(code.encode()).hexdigest()
            
            if input_hash == code_hash:
                # Mark as verified
                cursor.execute('''
                    UPDATE verification_sessions 
                    SET verified = 1 
                    WHERE session_id = ?
                ''', (session_id,))
                conn.commit()
                conn.close()
                return True, "Verified"
            else:
                # Increment attempts
                cursor.execute('''
                    UPDATE verification_sessions 
                    SET attempts = attempts + 1 
                    WHERE session_id = ?
                ''', (session_id,))
                conn.commit()
                conn.close()
                return False, "Invalid code"
                
    def create_auth_token(self, user_id: str, device_id: str = None) -> str:
        """Create authentication token"""
        token = secrets.token_hex(32)
        
        with self.lock:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                INSERT INTO auth_tokens 
                (token, user_id, device_id, created_at, expires_at, last_used)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (token, user_id, device_id, time.time(), time.time() + 2592000, time.time()))  # 30 days
            
            conn.commit()
            conn.close()
            
        return token
        
    def validate_token(self, token: str) -> Optional[str]:
        """Validate auth token and return user_id"""
        with self.lock:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT user_id, expires_at FROM auth_tokens WHERE token = ?
            ''', (token,))
            
            row = cursor.fetchone()
            
            if row:
                user_id, expires_at = row
                
                if time.time() < expires_at:
                    # Update last used
                    cursor.execute('''
                        UPDATE auth_tokens SET last_used = ? WHERE token = ?
                    ''', (time.time(), token))
                    conn.commit()
                    conn.close()
                    return user_id
                    
            conn.close()
        return None
        
    def find_users_by_phones(self, phone_numbers: list) -> list:
        """Find users by list of phone numbers"""
        users = []
        
        if not phone_numbers:
            return users
            
        with self.lock:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Create placeholders for IN clause
            placeholders = ','.join(['?' for _ in phone_numbers])
            
            cursor.execute(f'''
                SELECT user_id, phone_number, display_name, status, avatar_url, last_seen
                FROM palmsync_users 
                WHERE phone_number IN ({placeholders})
            ''', phone_numbers)
            
            for row in cursor.fetchall():
                users.append({
                    'user_id': row[0],
                    'phone_number': row[1],
                    'display_name': row[2],
                    'status': row[3],
                    'avatar_url': row[4],
                    'last_seen': row[5]
                })
                
            conn.close()
            
        return users
        
    def sync_contacts(self, user_id: str, contacts: list):
        """Sync user's contacts"""
        with self.lock:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            current_time = time.time()
            
            for contact in contacts:
                phone = contact.get('phone_number')
                name = contact.get('name', '')
                
                if phone:
                    cursor.execute('''
                        INSERT OR REPLACE INTO user_contacts 
                        (user_id, contact_phone, contact_name, last_synced)
                        VALUES (?, ?, ?, ?)
                    ''', (user_id, phone, name, current_time))
                    
            conn.commit()
            conn.close()


class TwilioSMSService:
    """Twilio SMS service for sending OTP"""
    
    def __init__(self, account_sid: str = None, auth_token: str = None, 
                 from_number: str = None):
        self.account_sid = account_sid or os.environ.get('TWILIO_ACCOUNT_SID')
        self.auth_token = auth_token or os.environ.get('TWILIO_AUTH_TOKEN')
        self.from_number = from_number or os.environ.get('TWILIO_PHONE_NUMBER')
        
        self.client = None
        self.is_configured = False
        
        if TWILIO_AVAILABLE and self.account_sid and self.auth_token:
            try:
                self.client = Client(self.account_sid, self.auth_token)
                self.is_configured = True
                print("[Twilio] ✅ SMS service configured")
            except Exception as e:
                print(f"[Twilio] ⚠️ Configuration error: {e}")
        else:
            print("[Twilio] ⚠️ SMS service not configured - using MOCK mode")
            
    def send_verification_code(self, phone_number: str, code: str) -> bool:
        """Send verification code via SMS"""
        
        # Format phone number (E.164 format)
        if not phone_number.startswith('+'):
            phone_number = '+' + phone_number
            
        message_body = f"Your Palmsync verification code is: {code}\n\nThis code will expire in 5 minutes."
        
        if self.is_configured and self.client:
            try:
                message = self.client.messages.create(
                    body=message_body,
                    from_=self.from_number,
                    to=phone_number
                )
                print(f"[Twilio] ✅ SMS sent to {phone_number[-4:]} (SID: {message.sid})")
                return True
                
            except TwilioRestException as e:
                print(f"[Twilio] ❌ SMS failed: {e}")
                return False
        else:
            # MOCK mode - just print to console
            print("\n" + "="*50)
            print(f"📱 MOCK SMS to {phone_number}")
            print(f"   Message: {message_body}")
            print(f"   Your code: {code}")
            print("="*50 + "\n")
            return True


class PhoneAuthManager:
    """
    Main phone authentication manager
    Handles OTP generation, verification, and user sessions
    """
    
    def __init__(self, db_path: str = "palmsync.db"):
        self.db = PhoneAuthDatabase(db_path)
        self.sms_service = TwilioSMSService()
        
        # Active sessions (in memory cache)
        self.active_sessions: Dict[str, VerificationSession] = {}
        self.sessions_lock = threading.Lock()
        
        # Code expiry (5 minutes)
        self.code_expiry = 300
        
    def send_verification_code(self, phone_number: str) -> Dict[str, Any]:
        """Send verification code to phone number"""
        
        # Generate 6-digit code
        code = str(random.randint(100000, 999999))
        
        # Create session
        import uuid
        session_id = str(uuid.uuid4())
        
        session = VerificationSession(
            session_id=session_id,
            phone_number=phone_number,
            code=code,
            created_at=time.time(),
            expires_at=time.time() + self.code_expiry
        )
        
        # Store in memory
        with self.sessions_lock:
            # Clean old sessions for this phone
            to_remove = []
            for sid, sess in self.active_sessions.items():
                if sess.phone_number == phone_number:
                    to_remove.append(sid)
            for sid in to_remove:
                del self.active_sessions[sid]
                
            self.active_sessions[session_id] = session
            
        # Store in database
        self.db.save_verification_code(session_id, phone_number, code, session.expires_at)
        
        # Send SMS
        success = self.sms_service.send_verification_code(phone_number, code)
        
        return {
            'success': success,
            'session_id': session_id,
            'expires_in': self.code_expiry,
            'phone_number': phone_number[-4:].rjust(len(phone_number), '*')
        }
        
    def verify_code(self, session_id: str, phone_number: str, 
                    code: str, display_name: str = None) -> Dict[str, Any]:
        """Verify OTP code and create/get user"""
        
        # Check memory cache first
        with self.sessions_lock:
            session = self.active_sessions.get(session_id)
            
        if session:
            if session.is_expired():
                return {'success': False, 'error': 'Code expired'}
                
            if not session.verify(code):
                if session.attempts >= session.max_attempts:
                    return {'success': False, 'error': 'Max attempts exceeded'}
                return {'success': False, 'error': 'Invalid code'}
        else:
            # Verify from database
            verified, message = self.db.verify_code(session_id, phone_number, code)
            if not verified:
                return {'success': False, 'error': message}
                
        # Code verified - create/get user
        user = self.db.create_or_update_user(phone_number, display_name)
        
        # Create auth token
        token = self.db.create_auth_token(user.user_id)
        
        # Clean up session
        with self.sessions_lock:
            if session_id in self.active_sessions:
                del self.active_sessions[session_id]
                
        return {
            'success': True,
            'user': user.to_dict(include_private=True),
            'token': token,
            'is_new_user': time.time() - user.created_at < 5  # Created in last 5 seconds
        }
        
    def validate_token(self, token: str) -> Optional[PalmsyncUser]:
        """Validate auth token and return user"""
        user_id = self.db.validate_token(token)
        
        if user_id:
            return self.db.get_user_by_id(user_id)
        return None
        
    def sync_contacts(self, user_id: str, contacts: list) -> Dict[str, Any]:
        """Sync user contacts and find Palmsync users"""
        
        # Extract phone numbers
        phone_numbers = []
        for contact in contacts:
            phone = contact.get('phone_number', '').strip()
            if phone:
                # Normalize phone number
                phone = phone.replace(' ', '').replace('-', '').replace('(', '').replace(')', '')
                if not phone.startswith('+'):
                    phone = '+' + phone
                phone_numbers.append(phone)
                
        # Save contacts to database
        self.db.sync_contacts(user_id, contacts)
        
        # Find which contacts are on Palmsync
        palmsync_users = self.db.find_users_by_phones(phone_numbers)
        
        # Add online status
        for user in palmsync_users:
            user['is_online'] = (time.time() - user['last_seen']) < 60  # Online if seen in last minute
            
        return {
            'success': True,
            'total_contacts': len(contacts),
            'palmsync_contacts': palmsync_users,
            'palmsync_count': len(palmsync_users)
        }
        
    def get_user_profile(self, user_id: str) -> Optional[Dict]:
        """Get user profile"""
        user = self.db.get_user_by_id(user_id)
        
        if user:
            return user.to_dict()
        return None
        
    def update_user_status(self, user_id: str, status: str):
        """Update user online status"""
        with self.db.lock:
            conn = sqlite3.connect(self.db.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                UPDATE palmsync_users 
                SET status = ?, last_seen = ?
                WHERE user_id = ?
            ''', (status, time.time(), user_id))
            
            conn.commit()
            conn.close()


# ============================================
# FLASK ROUTES (Add to relay_server.py)
# ============================================

def register_auth_routes(app, auth_manager: PhoneAuthManager):
    """Register authentication routes with Flask app"""
    
    @app.route('/api/auth/send_code', methods=['POST'])
    def send_verification_code():
        """Send OTP to phone number"""
        data = request.json
        phone_number = data.get('phone_number')
        
        if not phone_number:
            return jsonify({'error': 'Phone number required'}), 400
            
        result = auth_manager.send_verification_code(phone_number)
        return jsonify(result)
        
    @app.route('/api/auth/verify', methods=['POST'])
    def verify_code():
        """Verify OTP and login/register"""
        data = request.json
        
        required = ['session_id', 'phone_number', 'code']
        for field in required:
            if field not in data:
                return jsonify({'error': f'{field} required'}), 400
                
        result = auth_manager.verify_code(
            data['session_id'],
            data['phone_number'],
            data['code'],
            data.get('display_name')
        )
        
        return jsonify(result)
        
    @app.route('/api/auth/validate', methods=['POST'])
    def validate_token():
        """Validate auth token"""
        auth_header = request.headers.get('Authorization', '')
        token = auth_header.replace('Bearer ', '')
        
        user = auth_manager.validate_token(token)
        
        if user:
            return jsonify({'valid': True, 'user': user.to_dict()})
        return jsonify({'valid': False}), 401
        
    @app.route('/api/contacts/sync', methods=['POST'])
    def sync_contacts():
        """Sync contacts and find Palmsync users"""
        auth_header = request.headers.get('Authorization', '')
        token = auth_header.replace('Bearer ', '')
        
        user = auth_manager.validate_token(token)
        if not user:
            return jsonify({'error': 'Unauthorized'}), 401
            
        data = request.json
        contacts = data.get('contacts', [])
        
        result = auth_manager.sync_contacts(user.user_id, contacts)
        return jsonify(result)
        
    @app.route('/api/user/profile', methods=['GET'])
    def get_profile():
        """Get user profile"""
        auth_header = request.headers.get('Authorization', '')
        token = auth_header.replace('Bearer ', '')
        
        user = auth_manager.validate_token(token)
        if not user:
            return jsonify({'error': 'Unauthorized'}), 401
            
        profile = auth_manager.get_user_profile(user.user_id)
        return jsonify(profile)
        
    @app.route('/api/user/status', methods=['POST'])
    def update_status():
        """Update user status"""
        auth_header = request.headers.get('Authorization', '')
        token = auth_header.replace('Bearer ', '')
        
        user = auth_manager.validate_token(token)
        if not user:
            return jsonify({'error': 'Unauthorized'}), 401
            
        data = request.json
        status = data.get('status', 'online')
        
        auth_manager.update_user_status(user.user_id, status)
        return jsonify({'success': True})


# ============================================
# TEST FUNCTION
# ============================================

def test_phone_auth():
    """Test phone authentication"""
    print("📱 Palmsync Phone Authentication Test")
    print("=" * 50)
    
    auth = PhoneAuthManager("test_palmsync.db")
    
    # Test phone number
    phone = input("\nEnter test phone number (or press Enter for mock): ").strip()
    if not phone:
        phone = "+919876543210"
        
    print(f"\n1️⃣ Sending verification code to {phone}...")
    result = auth.send_verification_code(phone)
    
    if result['success']:
        print(f"   ✅ Code sent!")
        print(f"   Session ID: {result['session_id'][:8]}...")
        print(f"   Expires in: {result['expires_in']}s")
        
        # Get code from user
        code = input("\n2️⃣ Enter the code you received: ").strip()
        
        # Verify
        print("\n3️⃣ Verifying code...")
        verify_result = auth.verify_code(
            result['session_id'],
            phone,
            code,
            "Test User"
        )
        
        if verify_result['success']:
            print(f"   ✅ Verified!")
            print(f"   User: {verify_result['user']['display_name']}")
            print(f"   Token: {verify_result['token'][:16]}...")
            
            # Test contact sync
            print("\n4️⃣ Syncing contacts...")
            contacts = [
                {'phone_number': '+919876543211', 'name': 'Alice'},
                {'phone_number': '+919876543212', 'name': 'Bob'},
                {'phone_number': phone, 'name': 'Self'}
            ]
            
            sync_result = auth.sync_contacts(verify_result['user']['user_id'], contacts)
            print(f"   Total contacts: {sync_result['total_contacts']}")
            print(f"   On Palmsync: {sync_result['palmsync_count']}")
            
            for user in sync_result['palmsync_contacts']:
                status = "🟢" if user['is_online'] else "⚫"
                print(f"   {status} {user['display_name']} ({user['phone_number']})")
                
        else:
            print(f"   ❌ Verification failed: {verify_result['error']}")
    else:
        print("   ❌ Failed to send code")
        
    # Cleanup
    import os
    if os.path.exists("test_palmsync.db"):
        os.remove("test_palmsync.db")
        
    print("\n✅ Test complete!")


if __name__ == "__main__":
    test_phone_auth()
"""
server/palmsync_server.py
PalmSync Relay Server - Fast, Simple, Reliable
Phone & Username lookup with real Twilio SMS OTP
"""

import os
import sys
import json
import time
import uuid
import hmac
import hashlib
import secrets
import sqlite3
import threading
import logging
from datetime import datetime, timedelta
from functools import wraps
from typing import Dict, List, Optional, Any, Tuple
from pathlib import Path

# Flask
try:
    from flask import Flask, request, jsonify, g, make_response
    from flask_cors import CORS
except ImportError:
    print("❌ Install: pip install Flask Flask-CORS")
    sys.exit(1)

# ============================================
# SMS SERVICE IMPORT
# ============================================
try:
    from sms_service import SMSService
    SMS_AVAILABLE = True
except ImportError:
    SMS_AVAILABLE = False
    print("⚠️ sms_service.py not found. SMS will use console fallback.")

# ============================================
# CONFIGURATION
# ============================================

class Config:
    HOST = '0.0.0.0'
    PORT = 5000
    DB_PATH = 'palmsync.db'
    STORAGE_PATH = './storage'
    SECRET_KEY = secrets.token_hex(32)
    TOKEN_EXPIRY = 86400 * 30  # 30 days
    
    # ============================================
    # TWILIO CREDENTIALS (REPLACE THESE!)
    # ============================================
    # Get these from: https://console.twilio.com
    TWILIO_SID = "ACxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"    # ← REPLACE
    TWILIO_TOKEN = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"     # ← REPLACE
    TWILIO_PHONE = "+15550100"                            # ← REPLACE

# ============================================
# INITIALIZE SMS SERVICE
# ============================================
if SMS_AVAILABLE:
    sms_service = SMSService(
        account_sid=Config.TWILIO_SID,
        auth_token=Config.TWILIO_TOKEN,
        from_number=Config.TWILIO_PHONE
    )
else:
    sms_service = None
    print("⚠️ SMS service not available - OTPs will print to console only")

# ============================================
# LOGGING
# ============================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger('PalmSync')

# ============================================
# DATABASE
# ============================================

class Database:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._init_db()
        
    def _init_db(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        # Users table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS users (
                id TEXT PRIMARY KEY,
                phone_hash TEXT UNIQUE NOT NULL,
                username TEXT UNIQUE NOT NULL,
                display_name TEXT NOT NULL,
                avatar_url TEXT,
                created_at REAL NOT NULL,
                last_seen REAL NOT NULL,
                is_online INTEGER DEFAULT 0,
                status TEXT DEFAULT 'offline'
            )
        ''')
        
        # Auth tokens
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS auth_tokens (
                token TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                device_id TEXT,
                created_at REAL NOT NULL,
                expires_at REAL NOT NULL,
                FOREIGN KEY (user_id) REFERENCES users(id)
            )
        ''')
        
        # Transfers
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS transfers (
                id TEXT PRIMARY KEY,
                sender_id TEXT NOT NULL,
                recipient_id TEXT,
                filename TEXT,
                file_size INTEGER,
                total_chunks INTEGER,
                status TEXT DEFAULT 'pending',
                created_at REAL NOT NULL,
                expires_at REAL NOT NULL,
                sharing_code TEXT UNIQUE,
                FOREIGN KEY (sender_id) REFERENCES users(id)
            )
        ''')
        
        # Transfer chunks
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS chunks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                transfer_id TEXT NOT NULL,
                chunk_index INTEGER NOT NULL,
                stored_path TEXT,
                uploaded_at REAL,
                FOREIGN KEY (transfer_id) REFERENCES transfers(id)
            )
        ''')
        
        # OTP codes
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS otp_codes (
                phone_hash TEXT PRIMARY KEY,
                code TEXT NOT NULL,
                created_at REAL NOT NULL,
                expires_at REAL NOT NULL,
                attempts INTEGER DEFAULT 0
            )
        ''')
        
        conn.commit()
        conn.close()
        logger.info("✅ Database initialized")
        
    def query(self, sql: str, params: tuple = ()):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute(sql, params)
        result = cursor.fetchall()
        conn.commit()
        conn.close()
        return result
        
    def execute(self, sql: str, params: tuple = ()):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(sql, params)
        conn.commit()
        last_id = cursor.lastrowid
        conn.close()
        return last_id
        
    def query_one(self, sql: str, params: tuple = ()):
        results = self.query(sql, params)
        return results[0] if results else None

# ============================================
# HELPERS
# ============================================

def hash_phone(phone: str) -> str:
    """Hash phone number for privacy"""
    return hashlib.sha256(f"palmsync:{phone.strip()}".encode()).hexdigest()

def generate_token() -> str:
    """Generate secure auth token"""
    return secrets.token_hex(32)

def generate_otp() -> str:
    """Generate 6-digit OTP"""
    return ''.join([str(secrets.randbelow(10)) for _ in range(6)])

def generate_sharing_code() -> str:
    """Generate human-readable sharing code"""
    chars = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789'
    return '-'.join([
        ''.join(secrets.choice(chars) for _ in range(4)),
        ''.join(secrets.choice(chars) for _ in range(4))
    ])

def format_phone(phone: str) -> str:
    """Clean phone number"""
    return ''.join(c for c in phone if c.isdigit())

# ============================================
# AUTH MIDDLEWARE
# ============================================

def require_auth(f):
    """Decorator to require valid auth token"""
    @wraps(f)
    def decorated(*args, **kwargs):
        token = request.headers.get('Authorization', '').replace('Bearer ', '')
        
        if not token:
            return jsonify({'error': 'No auth token provided'}), 401
            
        user = db.query_one(
            "SELECT u.* FROM users u JOIN auth_tokens t ON u.id = t.user_id "
            "WHERE t.token = ? AND t.expires_at > ?",
            (token, time.time())
        )
        
        if not user:
            return jsonify({'error': 'Invalid or expired token'}), 401
            
        g.user = user
        g.token = token
        return f(*args, **kwargs)
    return decorated

# ============================================
# FLASK APP
# ============================================

app = Flask(__name__)
app.config['SECRET_KEY'] = Config.SECRET_KEY
app.config['MAX_CONTENT_LENGTH'] = 5 * 1024 * 1024 * 1024  # 5GB

CORS(app, resources={r"/api/*": {"origins": "*"}})

db = Database(Config.DB_PATH)

# Ensure storage exists
os.makedirs(Config.STORAGE_PATH, exist_ok=True)

# ============================================
# AUTH ROUTES
# ============================================

@app.route('/api/health', methods=['GET'])
def health():
    """Health check"""
    return jsonify({
        'status': 'ok',
        'server': 'PalmSync Relay',
        'version': '1.0.0',
        'timestamp': time.time(),
        'sms_available': sms_service is not None and sms_service.is_available if sms_service else False
    })

@app.route('/api/auth/send_otp', methods=['POST'])
def send_otp():
    """
    Send OTP to phone number
    Uses Twilio SMS if configured, falls back to console
    """
    data = request.json
    phone = format_phone(data.get('phone', ''))
    
    if not phone or len(phone) < 10:
        return jsonify({'error': 'Valid phone number required'}), 400
        
    phone_hash = hash_phone(phone)
    
    # Generate OTP
    otp = generate_otp()
    
    # Store OTP in database
    db.execute('''
        INSERT OR REPLACE INTO otp_codes (phone_hash, code, created_at, expires_at, attempts)
        VALUES (?, ?, ?, ?, 0)
    ''', (phone_hash, otp, time.time(), time.time() + 300))
    
    # ============================================
    # 📱 SEND REAL SMS VIA TWILIO (or console fallback)
    # ============================================
    if sms_service:
        result = sms_service.send_otp(phone, otp)
        logger.info(f"📱 OTP for {phone}: sent via {result['method']}")
    else:
        # No SMS service - console only
        logger.info(f"🔧 DEV MODE - OTP for {phone}: {otp}")
        result = {
            'sent': True,
            'method': 'console',
            'dev_otp': otp
        }
    
    response = {
        'success': result['sent'],
        'method': result['method'],
        'expires_in': 300,
        'message': f"OTP sent via {result['method']}"
    }
    
    # Include OTP in response ONLY in dev/console mode
    if result['method'] == 'console':
        response['dev_otp'] = result.get('dev_otp')
        
    return jsonify(response)

@app.route('/api/auth/verify_otp', methods=['POST'])
def verify_otp():
    """Verify OTP and login/register"""
    data = request.json
    phone = format_phone(data.get('phone', ''))
    otp = data.get('otp', '')
    device_id = data.get('device_id', str(uuid.uuid4()))
    display_name = data.get('name', '')
    
    if not phone or not otp:
        return jsonify({'error': 'Phone and OTP required'}), 400
        
    phone_hash = hash_phone(phone)
    
    # Check OTP
    otp_record = db.query_one(
        "SELECT * FROM otp_codes WHERE phone_hash = ? AND expires_at > ?",
        (phone_hash, time.time())
    )
    
    if not otp_record:
        return jsonify({'error': 'OTP expired. Request new one.'}), 401
        
    if otp_record['attempts'] >= 5:
        db.execute("DELETE FROM otp_codes WHERE phone_hash = ?", (phone_hash,))
        return jsonify({'error': 'Too many attempts. Request new OTP.'}), 429
        
    if otp_record['code'] != otp:
        db.execute(
            "UPDATE otp_codes SET attempts = attempts + 1 WHERE phone_hash = ?",
            (phone_hash,)
        )
        return jsonify({'error': 'Invalid OTP'}), 401
        
    # OTP verified - delete it
    db.execute("DELETE FROM otp_codes WHERE phone_hash = ?", (phone_hash,))
    
    # Check if user exists
    user = db.query_one("SELECT * FROM users WHERE phone_hash = ?", (phone_hash,))
    
    is_new = False
    
    if not user:
        # Register new user
        user_id = str(uuid.uuid4())
        username = f"user_{secrets.token_hex(4)}"
        
        if not display_name:
            display_name = f"User{phone[-4:]}"
        
        db.execute('''
            INSERT INTO users (id, phone_hash, username, display_name, created_at, last_seen, is_online, status)
            VALUES (?, ?, ?, ?, ?, ?, 1, 'online')
        ''', (user_id, phone_hash, username, display_name, time.time(), time.time()))
        
        user = db.query_one("SELECT * FROM users WHERE id = ?", (user_id,))
        is_new = True
        
        # Send welcome SMS
        if sms_service:
            sms_service.send_welcome(phone, username)
            
        logger.info(f"🆕 New user registered: {display_name} (@{username})")
    else:
        # Update last seen
        db.execute(
            "UPDATE users SET last_seen = ?, is_online = 1, status = 'online' WHERE id = ?",
            (time.time(), user['id'])
        )
        user = db.query_one("SELECT * FROM users WHERE id = ?", (user['id'],))
        logger.info(f"🔑 User logged in: {user['display_name']} (@{user['username']})")
        
    # Generate auth token
    token = generate_token()
    
    db.execute('''
        INSERT INTO auth_tokens (token, user_id, device_id, created_at, expires_at)
        VALUES (?, ?, ?, ?, ?)
    ''', (token, user['id'], device_id, time.time(), time.time() + Config.TOKEN_EXPIRY))
    
    return jsonify({
        'success': True,
        'is_new_user': is_new,
        'token': token,
        'expires_at': time.time() + Config.TOKEN_EXPIRY,
        'user': {
            'id': user['id'],
            'username': user['username'],
            'display_name': user['display_name'],
            'is_online': True
        }
    })

# ============================================
# USER LOOKUP ROUTES
# ============================================

@app.route('/api/user/lookup', methods=['GET'])
@require_auth
def lookup_user():
    """
    FAST user lookup by phone OR username
    Single query, instant response
    """
    phone = request.args.get('phone', '').strip()
    username = request.args.get('username', '').strip().lower()
    
    if not phone and not username:
        return jsonify({'error': 'Provide phone or username'}), 400
        
    user = None
    
    if phone:
        phone = format_phone(phone)
        phone_hash = hash_phone(phone)
        user = db.query_one("SELECT * FROM users WHERE phone_hash = ?", (phone_hash,))
    elif username:
        username = username.lstrip('@')
        user = db.query_one("SELECT * FROM users WHERE username = ?", (username,))
        
    if not user:
        return jsonify({
            'found': False,
            'message': 'This person is not on PalmSync yet. Invite them!'
        })
        
    if user['id'] == g.user['id']:
        return jsonify({
            'found': False,
            'message': 'This is your own account!'
        })
        
    return jsonify({
        'found': True,
        'user': {
            'id': user['id'],
            'username': user['username'],
            'display_name': user['display_name'],
            'avatar_url': user['avatar_url'],
            'is_online': bool(user['is_online']),
            'last_seen': user['last_seen']
        }
    })

@app.route('/api/user/search', methods=['GET'])
@require_auth
def search_users():
    """Search users by partial name or username"""
    query = request.args.get('q', '').strip().lower()
    
    if len(query) < 2:
        return jsonify({'users': [], 'count': 0})
        
    results = db.query(
        "SELECT * FROM users WHERE (display_name LIKE ? OR username LIKE ?) AND id != ? LIMIT 10",
        (f'%{query}%', f'%{query}%', g.user['id'])
    )
    
    users = [{
        'id': u['id'],
        'username': u['username'],
        'display_name': u['display_name'],
        'avatar_url': u['avatar_url'],
        'is_online': bool(u['is_online'])
    } for u in results]
    
    return jsonify({'users': users, 'count': len(users)})

# ============================================
# ONLINE PRESENCE
# ============================================

@app.route('/api/user/online', methods=['GET'])
@require_auth
def get_online_users():
    """Get all online users"""
    results = db.query(
        "SELECT * FROM users WHERE is_online = 1 AND id != ? ORDER BY last_seen DESC",
        (g.user['id'],)
    )
    
    users = [{
        'id': u['id'],
        'username': u['username'],
        'display_name': u['display_name'],
        'avatar_url': u['avatar_url'],
        'last_seen': u['last_seen']
    } for u in results]
    
    return jsonify({'users': users, 'count': len(users)})

@app.route('/api/user/heartbeat', methods=['POST'])
@require_auth
def heartbeat():
    """Update user's online status"""
    db.execute(
        "UPDATE users SET last_seen = ?, is_online = 1, status = 'online' WHERE id = ?",
        (time.time(), g.user['id'])
    )
    return jsonify({'success': True})

@app.route('/api/user/status', methods=['PUT'])
@require_auth
def update_status():
    """Update user status"""
    data = request.json
    status = data.get('status', 'online')
    
    db.execute(
        "UPDATE users SET status = ?, is_online = ?, last_seen = ? WHERE id = ?",
        (status, 1 if status == 'online' else 0, time.time(), g.user['id'])
    )
    
    return jsonify({'success': True, 'status': status})

# ============================================
# PROFILE MANAGEMENT
# ============================================

@app.route('/api/user/profile', methods=['GET'])
@require_auth
def get_profile():
    """Get current user's profile"""
    user = g.user
    
    return jsonify({
        'id': user['id'],
        'username': user['username'],
        'display_name': user['display_name'],
        'avatar_url': user['avatar_url'],
        'is_online': bool(user['is_online']),
        'created_at': user['created_at']
    })

@app.route('/api/user/profile', methods=['PUT'])
@require_auth
def update_profile():
    """Update user profile"""
    data = request.json
    
    updates = []
    params = []
    
    if 'display_name' in data:
        updates.append("display_name = ?")
        params.append(data['display_name'])
        
    if 'username' in data:
        username = data['username'].lower().lstrip('@')
        
        existing = db.query_one(
            "SELECT id FROM users WHERE username = ? AND id != ?",
            (username, g.user['id'])
        )
        if existing:
            return jsonify({'error': 'Username already taken'}), 409
            
        updates.append("username = ?")
        params.append(username)
        
    if 'avatar_url' in data:
        updates.append("avatar_url = ?")
        params.append(data['avatar_url'])
        
    if updates:
        params.append(g.user['id'])
        db.execute(
            f"UPDATE users SET {', '.join(updates)} WHERE id = ?",
            tuple(params)
        )
        
    return jsonify({'success': True, 'message': 'Profile updated'})

# ============================================
# TRANSFER MANAGEMENT
# ============================================

@app.route('/api/transfer/create', methods=['POST'])
@require_auth
def create_transfer():
    """Create a new file transfer"""
    data = request.json
    
    transfer_id = str(uuid.uuid4())
    sharing_code = generate_sharing_code()
    
    db.execute('''
        INSERT INTO transfers (id, sender_id, recipient_id, filename, file_size,
                              total_chunks, created_at, expires_at, sharing_code)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        transfer_id,
        g.user['id'],
        data.get('recipient_id'),
        data.get('filename', 'unknown'),
        data.get('file_size', 0),
        data.get('total_chunks', 0),
        time.time(),
        time.time() + 86400,
        sharing_code
    ))
    
    logger.info(f"📤 Transfer created: {transfer_id[:8]}... by {g.user['display_name']}")
    
    return jsonify({
        'transfer_id': transfer_id,
        'sharing_code': sharing_code,
        'expires_at': time.time() + 86400
    })

@app.route('/api/transfer/<transfer_id>/chunk/<int:chunk_id>', methods=['POST'])
@require_auth
def upload_chunk(transfer_id, chunk_id):
    """Upload a chunk"""
    transfer = db.query_one("SELECT * FROM transfers WHERE id = ?", (transfer_id,))
    if not transfer:
        return jsonify({'error': 'Transfer not found'}), 404
        
    transfer_dir = os.path.join(Config.STORAGE_PATH, transfer_id)
    os.makedirs(transfer_dir, exist_ok=True)
    
    chunk_path = os.path.join(transfer_dir, f"chunk_{chunk_id:06d}.enc")
    chunk_data = request.get_data()
    
    with open(chunk_path, 'wb') as f:
        f.write(chunk_data)
        
    db.execute('''
        INSERT OR REPLACE INTO chunks (transfer_id, chunk_index, stored_path, uploaded_at)
        VALUES (?, ?, ?, ?)
    ''', (transfer_id, chunk_id, chunk_path, time.time()))
    
    return jsonify({'success': True, 'chunk_id': chunk_id})

@app.route('/api/transfer/<transfer_id>/chunk/<int:chunk_id>', methods=['GET'])
@require_auth
def download_chunk(transfer_id, chunk_id):
    """Download a chunk"""
    chunk = db.query_one(
        "SELECT * FROM chunks WHERE transfer_id = ? AND chunk_index = ?",
        (transfer_id, chunk_id)
    )
    
    if not chunk:
        return jsonify({'error': 'Chunk not found'}), 404
        
    chunk_path = chunk['stored_path']
    
    if not os.path.exists(chunk_path):
        return jsonify({'error': 'Chunk file missing'}), 404
        
    with open(chunk_path, 'rb') as f:
        data = f.read()
        
    return data, 200, {'Content-Type': 'application/octet-stream'}

@app.route('/api/transfer/<transfer_id>/status', methods=['GET'])
@require_auth
def transfer_status(transfer_id):
    """Get transfer status"""
    transfer = db.query_one("SELECT * FROM transfers WHERE id = ?", (transfer_id,))
    
    if not transfer:
        return jsonify({'error': 'Transfer not found'}), 404
        
    chunks = db.query(
        "SELECT COUNT(*) as count FROM chunks WHERE transfer_id = ?",
        (transfer_id,)
    )
    
    chunk_count = chunks[0]['count'] if chunks else 0
    
    return jsonify({
        'transfer_id': transfer_id,
        'filename': transfer['filename'],
        'file_size': transfer['file_size'],
        'total_chunks': transfer['total_chunks'],
        'chunks_uploaded': chunk_count,
        'status': transfer['status'],
        'sharing_code': transfer['sharing_code'],
        'expires_at': transfer['expires_at']
    })

# ============================================
# SHARING CODE
# ============================================

@app.route('/api/code/<code>', methods=['GET'])
@require_auth
def lookup_code(code):
    """Look up transfer by sharing code"""
    code = code.upper().replace('-', '')
    
    transfer = db.query_one(
        "SELECT t.*, u.display_name as sender_name FROM transfers t "
        "JOIN users u ON t.sender_id = u.id "
        "WHERE t.sharing_code = ? AND t.expires_at > ?",
        (code, time.time())
    )
    
    if not transfer:
        return jsonify({'error': 'Invalid or expired code'}), 404
        
    return jsonify({
        'transfer_id': transfer['id'],
        'sender_name': transfer['sender_name'],
        'filename': transfer['filename'],
        'file_size': transfer['file_size'],
        'total_chunks': transfer['total_chunks']
    })

# ============================================
# STATISTICS
# ============================================

@app.route('/api/stats', methods=['GET'])
def stats():
    """Get server statistics"""
    total_users = db.query_one("SELECT COUNT(*) as count FROM users")
    online_users = db.query_one("SELECT COUNT(*) as count FROM users WHERE is_online = 1")
    active_transfers = db.query_one(
        "SELECT COUNT(*) as count FROM transfers WHERE expires_at > ?",
        (time.time(),)
    )
    
    return jsonify({
        'total_users': total_users['count'] if total_users else 0,
        'online_users': online_users['count'] if online_users else 0,
        'active_transfers': active_transfers['count'] if active_transfers else 0,
        'uptime': time.time() - start_time,
        'sms_available': sms_service.is_available if sms_service else False
    })

# ============================================
# STARTUP
# ============================================

start_time = time.time()

if __name__ == '__main__':
    print("=" * 60)
    print("🔐 PALMSYNC RELAY SERVER")
    print("=" * 60)
    print(f"\n📡 Starting server on http://{Config.HOST}:{Config.PORT}")
    print(f"💾 Database: {Config.DB_PATH}")
    print(f"📦 Storage: {Config.STORAGE_PATH}")
    
    # SMS Status
    if sms_service and sms_service.is_available:
        print(f"📱 SMS: Twilio Connected ✅")
        balance = sms_service.get_balance()
        if balance:
            print(f"💰 Trial balance: ${balance:.2f}")
    else:
        print(f"📱 SMS: Console Mode (OTPs printed here)")
        print(f"   To enable real SMS:")
        print(f"   1. Sign up at https://www.twilio.com")
        print(f"   2. Update TWILIO_SID, TWILIO_TOKEN, TWILIO_PHONE in Config")
    
    print(f"\n📋 API Endpoints:")
    print(f"   POST /api/auth/send_otp     - Send OTP")
    print(f"   POST /api/auth/verify_otp   - Verify OTP & Login")
    print(f"   GET  /api/user/lookup       - Lookup by phone/username")
    print(f"   GET  /api/user/search       - Search users")
    print(f"   GET  /api/user/online       - Online users")
    print(f"   POST /api/transfer/create   - Create transfer")
    print(f"   POST /api/transfer/{{id}}/chunk/{{n}} - Upload chunk")
    print(f"   GET  /api/transfer/{{id}}/chunk/{{n}} - Download chunk")
    print(f"   GET  /api/code/{{code}}      - Lookup sharing code")
    print(f"\n✨ Server ready!\n")
    
    app.run(host=Config.HOST, port=Config.PORT, debug=True)
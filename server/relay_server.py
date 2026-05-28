"""
server/relay_server.py
PALMSYNC - Production Relay Server
Handles authentication, device registration, encrypted file relay, and WebSocket
"""

import os
import sys
import json
import time
import uuid
import hashlib
import secrets
import sqlite3
import threading
import shutil
import random
import string
import io
from typing import Dict, Optional
from contextlib import contextmanager
from functools import wraps

# Flask imports
try:
    from flask import Flask, request, jsonify, send_file, current_app
    from flask_cors import CORS
    from flask_sock import Sock
    FLASK_AVAILABLE = True
except ImportError:
    FLASK_AVAILABLE = False
    print("[RelayServer] ❌ Flask not installed. Run: pip install flask flask-cors flask-sock")

# ============================================
# DATABASE
# ============================================

class Database:
    def __init__(self, db_path: str = "relay_server.db"):
        self.db_path = db_path
        self.lock = threading.Lock()
        self._init_db()
    
    @contextmanager
    def get_connection(self):
        """Context manager for database connections"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
    
    def _init_db(self):
        """Initialize database tables"""
        with self.lock:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS users (
                    user_id TEXT PRIMARY KEY,
                    username TEXT UNIQUE NOT NULL,
                    display_name TEXT,
                    auth_token TEXT UNIQUE,
                    created_at REAL,
                    last_seen REAL,
                    is_online INTEGER DEFAULT 0
                )
            ''')
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS devices (
                    device_id TEXT PRIMARY KEY,
                    user_id TEXT,
                    device_name TEXT,
                    ip_address TEXT,
                    created_at REAL,
                    FOREIGN KEY (user_id) REFERENCES users(user_id)
                )
            ''')
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS transfers (
                    transfer_id TEXT PRIMARY KEY,
                    sender_id TEXT,
                    recipient_id TEXT,
                    filename TEXT,
                    file_size INTEGER,
                    total_chunks INTEGER,
                    status TEXT DEFAULT 'pending',
                    sharing_code TEXT,
                    created_at REAL,
                    expires_at REAL,
                    metadata_stored INTEGER DEFAULT 0,
                    chunks_stored INTEGER DEFAULT 0,
                    FOREIGN KEY (sender_id) REFERENCES users(user_id)
                )
            ''')
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS chunks (
                    transfer_id TEXT,
                    chunk_id INTEGER,
                    stored_path TEXT,
                    size INTEGER,
                    uploaded_at REAL,
                    PRIMARY KEY (transfer_id, chunk_id)
                )
            ''')
            
            conn.commit()
            conn.close()
    
    def create_user(self, username: str, display_name: str) -> Optional[Dict]:
        """Create a new user"""
        user_id = str(uuid.uuid4())
        auth_token = secrets.token_hex(32)
        
        try:
            with self.lock:
                with self.get_connection() as conn:
                    cursor = conn.cursor()
                    cursor.execute('''
                        INSERT INTO users (user_id, username, display_name, auth_token, created_at, last_seen, is_online)
                        VALUES (?, ?, ?, ?, ?, ?, 1)
                    ''', (user_id, username, display_name, auth_token, time.time(), time.time()))
                    
                    return {
                        'user_id': user_id,
                        'username': username,
                        'display_name': display_name,
                        'token': auth_token
                    }
        except sqlite3.IntegrityError:
            return None
    
    def login_user(self, username: str) -> Optional[Dict]:
        """Login existing user"""
        with self.lock:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('SELECT * FROM users WHERE username = ?', (username,))
            row = cursor.fetchone()
            
            if row:
                cursor.execute('''
                    UPDATE users SET last_seen = ?, is_online = 1 WHERE user_id = ?
                ''', (time.time(), row[0]))
                conn.commit()
                conn.close()
                
                return {
                    'user_id': row[0],
                    'username': row[1],
                    'display_name': row[2],
                    'token': row[3]
                }
            
            conn.close()
            return None
    
    def get_user_by_token(self, token: str) -> Optional[Dict]:
        """Get user by authentication token"""
        with self.lock:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('SELECT * FROM users WHERE auth_token = ?', (token,))
            row = cursor.fetchone()
            conn.close()
            
            if row:
                return {
                    'user_id': row[0],
                    'username': row[1],
                    'display_name': row[2],
                    'token': row[3]
                }
            return None
    
    def get_online_users(self, exclude_id: str = None) -> list:
        """Get all online users"""
        with self.lock:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            if exclude_id:
                cursor.execute('SELECT * FROM users WHERE is_online = 1 AND user_id != ?', (exclude_id,))
            else:
                cursor.execute('SELECT * FROM users WHERE is_online = 1')
            
            users = []
            for row in cursor.fetchall():
                users.append({
                    'user_id': row[0],
                    'username': row[1],
                    'display_name': row[2],
                    'last_seen': row[4]
                })
            
            conn.close()
            return users
    
    def create_transfer(self, sender_id: str, filename: str, file_size: int, 
                        total_chunks: int, recipient_id: str = None) -> str:
        """Create a new file transfer"""
        transfer_id = str(uuid.uuid4())
        
        with self.lock:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    INSERT INTO transfers (transfer_id, sender_id, recipient_id, filename, 
                        file_size, total_chunks, status, created_at, expires_at)
                    VALUES (?, ?, ?, ?, ?, ?, 'pending', ?, ?)
                ''', (transfer_id, sender_id, recipient_id, filename, file_size, 
                      total_chunks, time.time(), time.time() + 86400))
        
        return transfer_id
    
    def get_transfer(self, transfer_id: str) -> Optional[Dict]:
        """Get transfer information"""
        with self.lock:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('SELECT * FROM transfers WHERE transfer_id = ?', (transfer_id,))
            row = cursor.fetchone()
            conn.close()
            
            if row:
                return {
                    'transfer_id': row[0],
                    'sender_id': row[1],
                    'recipient_id': row[2],
                    'filename': row[3],
                    'file_size': row[4],
                    'total_chunks': row[5],
                    'status': row[6],
                    'created_at': row[8],
                    'expires_at': row[9]
                }
            return None
    
    def add_chunk(self, transfer_id: str, chunk_id: int, stored_path: str, size: int):
        """Record a chunk upload"""
        with self.lock:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    INSERT OR REPLACE INTO chunks (transfer_id, chunk_id, stored_path, size, uploaded_at)
                    VALUES (?, ?, ?, ?, ?)
                ''', (transfer_id, chunk_id, stored_path, size, time.time()))
                
                cursor.execute('''
                    UPDATE transfers SET chunks_stored = chunks_stored + 1 WHERE transfer_id = ?
                ''', (transfer_id,))
    
    def get_chunk_path(self, transfer_id: str, chunk_id: int) -> Optional[str]:
        """Get the path to a stored chunk"""
        with self.lock:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('SELECT stored_path FROM chunks WHERE transfer_id = ? AND chunk_id = ?',
                          (transfer_id, chunk_id))
            row = cursor.fetchone()
            conn.close()
            
            return row[0] if row else None
    
    def update_transfer_status(self, transfer_id: str, status: str):
        """Update transfer status"""
        with self.lock:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('UPDATE transfers SET status = ? WHERE transfer_id = ?', (status, transfer_id))
    
    def cleanup_expired_transfers(self):
        """Mark expired transfers"""
        with self.lock:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    UPDATE transfers SET status = 'expired' 
                    WHERE expires_at < ? AND status NOT IN ('completed', 'expired', 'cancelled')
                ''', (time.time(),))
                return cursor.rowcount

# ============================================
# STORAGE MANAGER
# ============================================

class StorageManager:
    def __init__(self, base_path: str = "storage"):
        self.base_path = base_path
        os.makedirs(base_path, exist_ok=True)
    
    def store_chunk(self, transfer_id: str, chunk_id: int, data: bytes) -> str:
        """Store an encrypted chunk"""
        transfer_path = os.path.join(self.base_path, transfer_id)
        os.makedirs(transfer_path, exist_ok=True)
        
        chunk_path = os.path.join(transfer_path, f"chunk_{chunk_id:06d}.enc")
        
        with open(chunk_path, 'wb') as f:
            f.write(data)
        
        return chunk_path
    
    def get_chunk(self, transfer_id: str, chunk_id: int) -> Optional[bytes]:
        """Retrieve an encrypted chunk"""
        chunk_path = os.path.join(self.base_path, transfer_id, f"chunk_{chunk_id:06d}.enc")
        
        if os.path.exists(chunk_path):
            with open(chunk_path, 'rb') as f:
                return f.read()
        return None
    
    def store_metadata(self, transfer_id: str, metadata: bytes) -> str:
        """Store encrypted metadata"""
        transfer_path = os.path.join(self.base_path, transfer_id)
        os.makedirs(transfer_path, exist_ok=True)
        
        meta_path = os.path.join(transfer_path, "metadata.enc")
        
        with open(meta_path, 'wb') as f:
            f.write(metadata)
        
        return meta_path
    
    def get_metadata(self, transfer_id: str) -> Optional[bytes]:
        """Retrieve encrypted metadata"""
        meta_path = os.path.join(self.base_path, transfer_id, "metadata.enc")
        
        if os.path.exists(meta_path):
            with open(meta_path, 'rb') as f:
                return f.read()
        return None
    
    def delete_transfer(self, transfer_id: str):
        """Delete all files for a transfer"""
        transfer_path = os.path.join(self.base_path, transfer_id)
        if os.path.exists(transfer_path):
            shutil.rmtree(transfer_path)
    
    def cleanup_expired(self, max_age_hours: int = 24):
        """Remove expired transfers"""
        current_time = time.time()
        
        if not os.path.exists(self.base_path):
            return
            
        for item in os.listdir(self.base_path):
            item_path = os.path.join(self.base_path, item)
            if os.path.isdir(item_path):
                mtime = os.path.getmtime(item_path)
                if current_time - mtime > max_age_hours * 3600:
                    shutil.rmtree(item_path)
                    print(f"[Storage] Cleaned up: {item}")

# ============================================
# FLASK SERVER
# ============================================

def require_auth(f):
    """Decorator to require authentication"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        auth_header = request.headers.get('Authorization', '')
        token = auth_header.replace('Bearer ', '')
        
        if not token:
            return jsonify({'error': 'No authentication token provided'}), 401
        
        server = current_app.config.get('SERVER_INSTANCE')
        if not server:
            return jsonify({'error': 'Server configuration error'}), 500
        
        user = server.db.get_user_by_token(token)
        if not user:
            return jsonify({'error': 'Invalid or expired token'}), 401
        
        request.current_user = user
        return f(*args, **kwargs)
    return decorated_function

class RelayServer:
    def __init__(self, host: str = "0.0.0.0", port: int = 5000):
        self.host = host
        self.port = port
        self.db = Database()
        self.storage = StorageManager()
        self.start_time = time.time()
        self.is_running = False
        
        # Connected WebSocket clients
        self.ws_clients: Dict[str, any] = {}
        self.ws_lock = threading.Lock()
        
        if FLASK_AVAILABLE:
            self.app = Flask(__name__)
            CORS(self.app)
            self.sock = Sock(self.app)
            self.app.config['SERVER_INSTANCE'] = self
            self._setup_routes()
            self._setup_websocket()
            
            # Start cleanup thread
            self.cleanup_thread = threading.Thread(target=self._cleanup_loop, daemon=True)
            self.cleanup_thread.start()
    
    def _cleanup_loop(self):
        """Background cleanup of expired transfers"""
        while self.is_running:
            time.sleep(3600)  # Every hour
            expired = self.db.cleanup_expired_transfers()
            self.storage.cleanup_expired(max_age_hours=24)
            if expired > 0:
                print(f"[Server] 🧹 Cleaned up {expired} expired transfers")
    
    def _setup_routes(self):
        """Setup all API routes"""
        
        # ============================================
        # HOME
        # ============================================
        @self.app.route('/', methods=['GET'])
        def home():
            return '''
        <html>
        <head>
            <title>Palmsync Relay Server</title>
            <style>
                body { font-family: Arial; text-align: center; padding: 50px; background: #1a1a2e; color: white; }
                h1 { color: #00b894; }
                .status { color: #00b894; font-size: 20px; }
                .container { max-width: 800px; margin: 0 auto; }
                .endpoints { text-align: left; margin: 20px auto; background: #16213e; padding: 20px; border-radius: 10px; }
                .method { display: inline-block; width: 60px; color: #fdcb6e; font-weight: bold; }
                code { color: #fdcb6e; background: #0f3460; padding: 2px 6px; border-radius: 3px; }
                .endpoint { margin: 10px 0; padding: 8px; background: #0f3460; border-radius: 5px; }
            </style>
        </head>
        <body>
            <div class="container">
                <h1>🔐 PALMSYNC</h1>
                <p>Relay Server is Running!</p>
                <p class="status">✅ Online</p>
                <div class="endpoints">
                    <h3>📡 API Endpoints:</h3>
                    <div class="endpoint"><span class="method">POST</span><code>/api/register</code> - Register</div>
                    <div class="endpoint"><span class="method">POST</span><code>/api/login</code> - Login</div>
                    <div class="endpoint"><span class="method">GET</span><code>/api/users/online</code> - Online users</div>
                    <div class="endpoint"><span class="method">POST</span><code>/api/transfer/create</code> - Create transfer</div>
                    <div class="endpoint"><span class="method">POST</span><code>/api/transfer/{id}/chunk/{n}</code> - Upload</div>
                    <div class="endpoint"><span class="method">GET</span><code>/api/transfer/{id}/chunk/{n}</code> - Download</div>
                    <div class="endpoint"><span class="method">GET</span><code>/api/health</code> - Health</div>
                    <div class="endpoint"><span class="method">WS</span><code>/ws</code> - WebSocket</div>
                </div>
                <p>📦 Storage: <code>storage/</code> | 🗄️ DB: <code>relay_server.db</code></p>
                <p>⏰ Files auto-delete after 24 hours</p>
            </div>
        </body>
        </html>
        '''
        
        # ============================================
        # HEALTH
        # ============================================
        @self.app.route('/api/health', methods=['GET'])
        def health():
            return jsonify({
                'status': 'ok',
                'server': 'Palmsync',
                'version': '1.0.0',
                'uptime': time.time() - self.start_time
            })
        
        # ============================================
        # AUTHENTICATION
        # ============================================
        @self.app.route('/api/register', methods=['POST'])
        def register():
            if not request.is_json:
                return jsonify({'error': 'Content-Type must be application/json'}), 400
            
            data = request.json or {}
            username = data.get('username', '').strip()
            display_name = data.get('display_name', username)
            
            if not username:
                return jsonify({'error': 'Username required'}), 400
            if len(username) < 3:
                return jsonify({'error': 'Username must be at least 3 characters'}), 400
            
            user = self.db.create_user(username, display_name)
            
            if user:
                print(f"[Server] ✅ Registered: {username}")
                return jsonify({'success': True, 'user': user})
            else:
                user = self.db.login_user(username)
                if user:
                    print(f"[Server] ✅ Logged in (existing): {username}")
                    return jsonify({'success': True, 'user': user, 'message': 'Already registered'})
            
            return jsonify({'error': 'Registration failed'}), 500
        
        @self.app.route('/api/login', methods=['POST'])
        def login():
            if not request.is_json:
                return jsonify({'error': 'Content-Type must be application/json'}), 400
            
            data = request.json or {}
            username = data.get('username', '').strip()
            
            if not username:
                return jsonify({'error': 'Username required'}), 400
            
            user = self.db.login_user(username)
            
            if user:
                print(f"[Server] ✅ Login: {username}")
                return jsonify({'success': True, 'user': user})
            
            return jsonify({'error': 'User not found'}), 404
        
        @self.app.route('/api/heartbeat', methods=['POST'])
        def heartbeat():
            auth_header = request.headers.get('Authorization', '')
            token = auth_header.replace('Bearer ', '')
            
            if not token:
                return jsonify({'error': 'No token'}), 401
            
            user = self.db.get_user_by_token(token)
            if user:
                return jsonify({'status': 'ok', 'timestamp': time.time()})
            return jsonify({'error': 'Invalid token'}), 401
        
        # ============================================
        # USERS
        # ============================================
        @self.app.route('/api/users/online', methods=['GET'])
        def online_users():
            auth_header = request.headers.get('Authorization', '')
            token = auth_header.replace('Bearer ', '')
            
            if not token:
                return jsonify({'error': 'No token'}), 401
            
            user = self.db.get_user_by_token(token)
            if not user:
                return jsonify({'error': 'Invalid token'}), 401
            
            exclude_id = user['user_id']
            users = self.db.get_online_users(exclude_id)
            
            return jsonify({'users': users, 'count': len(users)})
        
        # ============================================
        # TRANSFERS
        # ============================================
        @self.app.route('/api/transfer/create', methods=['POST'])
        def create_transfer():
            if not request.is_json:
                return jsonify({'error': 'Content-Type must be application/json'}), 400
            
            auth_header = request.headers.get('Authorization', '')
            token = auth_header.replace('Bearer ', '')
            
            if not token:
                return jsonify({'error': 'No token'}), 401
            
            user = self.db.get_user_by_token(token)
            if not user:
                return jsonify({'error': 'Invalid token'}), 401
            
            data = request.json or {}
            
            transfer_id = self.db.create_transfer(
                sender_id=user['user_id'],
                filename=data.get('filename', 'unknown'),
                file_size=data.get('file_size', 0),
                total_chunks=data.get('total_chunks', 0),
                recipient_id=data.get('recipient_id')
            )
            
            print(f"[Server] 📦 Transfer created: {transfer_id[:8]}... by {user['username']}")
            
            # Notify recipient via WebSocket if connected
            recipient_id = data.get('recipient_id')
            if recipient_id:
                with self.ws_lock:
                    ws = self.ws_clients.get(recipient_id)
                    if ws:
                        try:
                            ws.send(json.dumps({
                                'type': 'transfer_request',
                                'transfer_id': transfer_id,
                                'sender_name': user['display_name'],
                                'filename': data.get('filename', 'unknown'),
                                'file_size': data.get('file_size', 0),
                                'total_chunks': data.get('total_chunks', 0)
                            }))
                        except:
                            pass
            
            return jsonify({'transfer_id': transfer_id, 'status': 'created'})
        
        @self.app.route('/api/transfer/<transfer_id>', methods=['GET'])
        def get_transfer(transfer_id):
            transfer = self.db.get_transfer(transfer_id)
            if transfer:
                return jsonify(transfer)
            return jsonify({'error': 'Transfer not found'}), 404
        
        @self.app.route('/api/transfer/<transfer_id>/metadata', methods=['POST'])
        def upload_metadata(transfer_id):
            metadata = request.get_data()
            if not metadata:
                return jsonify({'error': 'No metadata'}), 400
            
            self.storage.store_metadata(transfer_id, metadata)
            self.db.update_transfer_status(transfer_id, 'uploading')
            return jsonify({'status': 'ok', 'size': len(metadata)})
        
        @self.app.route('/api/transfer/<transfer_id>/metadata', methods=['GET'])
        def download_metadata(transfer_id):
            metadata = self.storage.get_metadata(transfer_id)
            if metadata:
                return send_file(io.BytesIO(metadata), mimetype='application/octet-stream',
                               as_attachment=True, download_name='metadata.enc')
            return jsonify({'error': 'Metadata not found'}), 404
        
        @self.app.route('/api/transfer/<transfer_id>/chunk/<int:chunk_id>', methods=['POST'])
        def upload_chunk(transfer_id, chunk_id):
            if 'chunk' not in request.files:
                return jsonify({'error': 'No chunk file'}), 400
            
            chunk_file = request.files['chunk']
            chunk_data = chunk_file.read()
            
            if not chunk_data:
                return jsonify({'error': 'Empty chunk'}), 400
            
            chunk_path = self.storage.store_chunk(transfer_id, chunk_id, chunk_data)
            self.db.add_chunk(transfer_id, chunk_id, chunk_path, len(chunk_data))
            
            return jsonify({'status': 'ok', 'chunk_id': chunk_id, 'size': len(chunk_data)})
        
        @self.app.route('/api/transfer/<transfer_id>/chunk/<int:chunk_id>', methods=['GET'])
        def download_chunk(transfer_id, chunk_id):
            chunk_data = self.storage.get_chunk(transfer_id, chunk_id)
            if chunk_data:
                return send_file(io.BytesIO(chunk_data), mimetype='application/octet-stream',
                               as_attachment=True, download_name=f'chunk_{chunk_id:06d}.enc')
            return jsonify({'error': 'Chunk not found'}), 404
        
        @self.app.route('/api/transfer/<transfer_id>/complete', methods=['POST'])
        def complete_transfer(transfer_id):
            self.db.update_transfer_status(transfer_id, 'completed')
            print(f"[Server] ✅ Transfer complete: {transfer_id[:8]}...")
            return jsonify({'status': 'ok'})
        
        # ============================================
        # SHARING CODES
        # ============================================
        @self.app.route('/api/code/generate', methods=['POST'])
        def generate_code():
            code = ''.join(random.choices(string.ascii_uppercase + string.digits, k=6))
            return jsonify({'code': code, 'expires_in': 3600})
        
        @self.app.route('/api/code/<code>', methods=['GET'])
        def lookup_code(code):
            return jsonify({'code': code, 'valid': True})
    
    def _setup_websocket(self):
        """Setup WebSocket routes for real-time notifications"""
        print("[Server] Setting up WebSocket endpoint at /ws")
        
        @self.sock.route('/ws')
        def websocket(ws):
            """WebSocket connection handler"""
            device_id = None
            
            print("[Server] 🔌 WebSocket client connected")
            
            # Send welcome
            try:
                ws.send(json.dumps({
                    'type': 'connected',
                    'message': 'Welcome to Palmsync Relay!',
                    'timestamp': time.time()
                }))
            except:
                pass
            
            try:
                while True:
                    message = ws.receive()
                    
                    if message:
                        try:
                            data = json.loads(message)
                            msg_type = data.get('type', '')
                            
                            if msg_type == 'ping':
                                ws.send(json.dumps({
                                    'type': 'pong',
                                    'timestamp': time.time()
                                }))
                                
                            elif msg_type == 'subscribe':
                                device_id = data.get('device_id', '') or data.get('auth_token', '')[:12]
                                
                                # Register this connection
                                if data.get('auth_token'):
                                    user = self.db.get_user_by_token(data['auth_token'])
                                    if user:
                                        device_id = user['user_id']
                                        with self.ws_lock:
                                            self.ws_clients[user['user_id']] = ws
                                
                                print(f"[Server] 📱 Subscribed: {device_id}...")
                                ws.send(json.dumps({
                                    'type': 'subscribed',
                                    'device_id': device_id,
                                    'status': 'ok',
                                    'timestamp': time.time()
                                }))
                                
                            elif msg_type == 'heartbeat':
                                ws.send(json.dumps({
                                    'type': 'heartbeat_ack',
                                    'timestamp': time.time()
                                }))
                                
                        except json.JSONDecodeError:
                            pass
                            
            except Exception as e:
                if device_id:
                    print(f"[Server] WebSocket disconnected: {device_id}...")
                    with self.ws_lock:
                        self.ws_clients.pop(device_id, None)
                else:
                    print(f"[Server] WebSocket disconnected")
    
    def start(self):
        """Start the server"""
        self.is_running = True
        
        print("\n" + "=" * 60)
        print("🚀 PALMSYNC RELAY SERVER")
        print("=" * 60)
        print(f"   Host: {self.host}")
        print(f"   Port: {self.port}")
        print(f"   Database: {self.db.db_path}")
        print(f"   Storage: {self.storage.base_path}")
        print(f"   WebSocket: ws://{self.host}:{self.port}/ws")
        print(f"   Auto-cleanup: Every 1 hour (24h expiry)")
        print("=" * 60 + "\n")
        
        if FLASK_AVAILABLE:
            self.app.run(host=self.host, port=self.port, threaded=True, debug=False)
        else:
            print("❌ Flask not installed!")
            print("   Run: pip install flask flask-cors flask-sock")
            sys.exit(1)

# ============================================
# MAIN
# ============================================

if __name__ == "__main__":
    print("=" * 60)
    print("🔐 PALMSYNC RELAY SERVER")
    print("=" * 60)
    
    if not FLASK_AVAILABLE:
        print("\n❌ Missing dependencies!")
        print("   pip install flask flask-cors flask-sock")
        sys.exit(1)
    
    server = RelayServer(host="0.0.0.0", port=5000)
    
    try:
        server.start()
    except KeyboardInterrupt:
        print("\n\n👋 Server stopped gracefully")
    except Exception as e:
        print(f"\n❌ Server error: {e}")
        sys.exit(1)
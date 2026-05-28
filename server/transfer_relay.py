"""
server/transfer_relay.py
Relay server for WAN file transfers
Handles chunk upload, download, and transfer management
"""

import os
import time
import uuid
import json
import sqlite3
import threading
import shutil
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, field
from enum import Enum
from datetime import datetime

class TransferStatus(Enum):
    PENDING = "pending"          # Created, waiting for upload
    UPLOADING = "uploading"      # Currently uploading chunks
    UPLOADED = "uploaded"        # All chunks uploaded
    DOWNLOADING = "downloading"  # Currently downloading
    COMPLETED = "completed"      # Transfer complete
    EXPIRED = "expired"          # Timed out
    CANCELLED = "cancelled"      # User cancelled
    FAILED = "failed"            # Transfer failed

class TransferType(Enum):
    FILE = "file"
    LINK = "link"
    SCREENSHOT = "screenshot"

@dataclass
class TransferRecord:
    """Transfer record in database"""
    transfer_id: str
    sender_id: str
    recipient_id: Optional[str]
    transfer_type: TransferType
    filename: str
    file_size: int
    total_chunks: int
    status: TransferStatus
    created_at: float
    expires_at: float
    completed_at: Optional[float] = None
    metadata_stored: bool = False
    chunks_stored: int = 0
    chunks_acked: int = 0
    sharing_code: Optional[str] = None
    share_link: Optional[str] = None
    is_encrypted: bool = True
    
    def to_dict(self) -> dict:
        return {
            'transfer_id': self.transfer_id,
            'sender_id': self.sender_id,
            'recipient_id': self.recipient_id,
            'transfer_type': self.transfer_type.value,
            'filename': self.filename,
            'file_size': self.file_size,
            'total_chunks': self.total_chunks,
            'status': self.status.value,
            'created_at': self.created_at,
            'expires_at': self.expires_at,
            'progress': (self.chunks_stored / self.total_chunks * 100) if self.total_chunks > 0 else 0,
            'is_encrypted': self.is_encrypted
        }
    
    def is_expired(self) -> bool:
        return time.time() > self.expires_at
    
    def get_progress(self) -> float:
        if self.total_chunks == 0:
            return 0.0
        return (self.chunks_stored / self.total_chunks) * 100

@dataclass
class ChunkInfo:
    """Information about a stored chunk"""
    transfer_id: str
    chunk_id: int
    size: int
    stored_path: str
    uploaded_at: float
    downloaded_count: int = 0

class TransferDatabase:
    """Database operations for transfer relay"""
    
    def __init__(self, db_path: str = "palmsync.db"):
        self.db_path = db_path
        self.lock = threading.Lock()
        self._init_tables()
        
    def _init_tables(self):
        """Initialize transfer-related tables"""
        with self.lock:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Transfers table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS transfers (
                    transfer_id TEXT PRIMARY KEY,
                    sender_id TEXT NOT NULL,
                    recipient_id TEXT,
                    transfer_type TEXT DEFAULT 'file',
                    filename TEXT NOT NULL,
                    file_size INTEGER DEFAULT 0,
                    total_chunks INTEGER DEFAULT 0,
                    status TEXT DEFAULT 'pending',
                    created_at REAL,
                    expires_at REAL,
                    completed_at REAL,
                    metadata_stored INTEGER DEFAULT 0,
                    chunks_stored INTEGER DEFAULT 0,
                    chunks_acked INTEGER DEFAULT 0,
                    sharing_code TEXT UNIQUE,
                    share_link TEXT,
                    is_encrypted INTEGER DEFAULT 1
                )
            ''')
            
            # Chunks table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS transfer_chunks (
                    transfer_id TEXT NOT NULL,
                    chunk_id INTEGER NOT NULL,
                    size INTEGER DEFAULT 0,
                    stored_path TEXT,
                    uploaded_at REAL,
                    downloaded_count INTEGER DEFAULT 0,
                    PRIMARY KEY (transfer_id, chunk_id),
                    FOREIGN KEY (transfer_id) REFERENCES transfers(transfer_id)
                )
            ''')
            
            # Transfer notifications
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS transfer_notifications (
                    notification_id TEXT PRIMARY KEY,
                    transfer_id TEXT NOT NULL,
                    recipient_id TEXT NOT NULL,
                    sent_at REAL,
                    delivered_at REAL,
                    read_at REAL,
                    status TEXT DEFAULT 'pending'
                )
            ''')
            
            # Sharing codes
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS sharing_codes (
                    code TEXT PRIMARY KEY,
                    transfer_id TEXT,
                    sender_id TEXT,
                    created_at REAL,
                    expires_at REAL,
                    max_uses INTEGER DEFAULT 1,
                    used_count INTEGER DEFAULT 0,
                    FOREIGN KEY (transfer_id) REFERENCES transfers(transfer_id)
                )
            ''')
            
            conn.commit()
            conn.close()
            
    def create_transfer(self, transfer: TransferRecord) -> bool:
        """Create a new transfer record"""
        with self.lock:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                INSERT INTO transfers 
                (transfer_id, sender_id, recipient_id, transfer_type, filename, 
                 file_size, total_chunks, status, created_at, expires_at, 
                 is_encrypted, sharing_code, share_link)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                transfer.transfer_id, transfer.sender_id, transfer.recipient_id,
                transfer.transfer_type.value, transfer.filename, transfer.file_size,
                transfer.total_chunks, transfer.status.value, transfer.created_at,
                transfer.expires_at, int(transfer.is_encrypted),
                transfer.sharing_code, transfer.share_link
            ))
            
            conn.commit()
            conn.close()
            return True
            
    def get_transfer(self, transfer_id: str) -> Optional[TransferRecord]:
        """Get transfer by ID"""
        with self.lock:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('SELECT * FROM transfers WHERE transfer_id = ?', (transfer_id,))
            row = cursor.fetchone()
            conn.close()
            
            if row:
                return TransferRecord(
                    transfer_id=row[0],
                    sender_id=row[1],
                    recipient_id=row[2],
                    transfer_type=TransferType(row[3]),
                    filename=row[4],
                    file_size=row[5],
                    total_chunks=row[6],
                    status=TransferStatus(row[7]),
                    created_at=row[8],
                    expires_at=row[9],
                    completed_at=row[10],
                    metadata_stored=bool(row[11]),
                    chunks_stored=row[12],
                    chunks_acked=row[13],
                    sharing_code=row[14],
                    share_link=row[15],
                    is_encrypted=bool(row[16])
                )
        return None
        
    def update_transfer_status(self, transfer_id: str, status: TransferStatus):
        """Update transfer status"""
        with self.lock:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            if status == TransferStatus.COMPLETED:
                cursor.execute('''
                    UPDATE transfers 
                    SET status = ?, completed_at = ?
                    WHERE transfer_id = ?
                ''', (status.value, time.time(), transfer_id))
            else:
                cursor.execute('''
                    UPDATE transfers SET status = ? WHERE transfer_id = ?
                ''', (status.value, transfer_id))
                
            conn.commit()
            conn.close()
            
    def add_chunk(self, transfer_id: str, chunk_id: int, 
                  stored_path: str, size: int) -> bool:
        """Record a stored chunk"""
        with self.lock:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Insert chunk record
            cursor.execute('''
                INSERT OR REPLACE INTO transfer_chunks 
                (transfer_id, chunk_id, size, stored_path, uploaded_at)
                VALUES (?, ?, ?, ?, ?)
            ''', (transfer_id, chunk_id, size, stored_path, time.time()))
            
            # Update chunks count
            cursor.execute('''
                UPDATE transfers 
                SET chunks_stored = chunks_stored + 1,
                    status = CASE 
                        WHEN chunks_stored + 1 >= total_chunks THEN 'uploaded'
                        ELSE 'uploading'
                    END
                WHERE transfer_id = ?
            ''', (transfer_id,))
            
            conn.commit()
            conn.close()
            return True
            
    def get_chunk_path(self, transfer_id: str, chunk_id: int) -> Optional[str]:
        """Get stored chunk path"""
        with self.lock:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute(
                'SELECT stored_path FROM transfer_chunks WHERE transfer_id = ? AND chunk_id = ?',
                (transfer_id, chunk_id)
            )
            row = cursor.fetchone()
            
            # Update download count
            if row:
                cursor.execute('''
                    UPDATE transfer_chunks 
                    SET downloaded_count = downloaded_count + 1
                    WHERE transfer_id = ? AND chunk_id = ?
                ''', (transfer_id, chunk_id))
                conn.commit()
                
            conn.close()
            return row[0] if row else None
            
    def get_missing_chunks(self, transfer_id: str) -> List[int]:
        """Get list of missing chunk IDs"""
        with self.lock:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Get total chunks
            cursor.execute('SELECT total_chunks FROM transfers WHERE transfer_id = ?', (transfer_id,))
            row = cursor.fetchone()
            
            if not row:
                return []
                
            total_chunks = row[0]
            
            # Get stored chunks
            cursor.execute('SELECT chunk_id FROM transfer_chunks WHERE transfer_id = ?', (transfer_id,))
            stored = {row[0] for row in cursor.fetchall()}
            
            conn.close()
            
            return [i for i in range(total_chunks) if i not in stored]
            
    def get_pending_transfers(self, user_id: str) -> List[TransferRecord]:
        """Get pending transfers for a user"""
        with self.lock:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT * FROM transfers 
                WHERE recipient_id = ? 
                AND status IN ('pending', 'uploading', 'uploaded')
                AND expires_at > ?
                ORDER BY created_at DESC
            ''', (user_id, time.time()))
            
            transfers = []
            for row in cursor.fetchall():
                transfers.append(TransferRecord(
                    transfer_id=row[0],
                    sender_id=row[1],
                    recipient_id=row[2],
                    transfer_type=TransferType(row[3]),
                    filename=row[4],
                    file_size=row[5],
                    total_chunks=row[6],
                    status=TransferStatus(row[7]),
                    created_at=row[8],
                    expires_at=row[9],
                    completed_at=row[10],
                    metadata_stored=bool(row[11]),
                    chunks_stored=row[12],
                    chunks_acked=row[13],
                    sharing_code=row[14],
                    share_link=row[15],
                    is_encrypted=bool(row[16])
                ))
                
            conn.close()
            return transfers
            
    def create_sharing_code(self, code: str, transfer_id: str, 
                            sender_id: str, expires_at: float,
                            max_uses: int = 1) -> bool:
        """Create a sharing code"""
        with self.lock:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                INSERT INTO sharing_codes 
                (code, transfer_id, sender_id, created_at, expires_at, max_uses)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (code, transfer_id, sender_id, time.time(), expires_at, max_uses))
            
            conn.commit()
            conn.close()
            return True
            
    def validate_sharing_code(self, code: str) -> Optional[str]:
        """Validate sharing code and return transfer_id"""
        with self.lock:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT transfer_id, expires_at, max_uses, used_count 
                FROM sharing_codes 
                WHERE code = ?
            ''', (code,))
            
            row = cursor.fetchone()
            
            if row:
                transfer_id, expires_at, max_uses, used_count = row
                
                if time.time() < expires_at and used_count < max_uses:
                    # Increment used count
                    cursor.execute('''
                        UPDATE sharing_codes 
                        SET used_count = used_count + 1 
                        WHERE code = ?
                    ''', (code,))
                    conn.commit()
                    conn.close()
                    return transfer_id
                    
            conn.close()
        return None
        
    def cleanup_expired_transfers(self) -> int:
        """Clean up expired transfers"""
        cleaned = 0
        
        with self.lock:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT transfer_id FROM transfers 
                WHERE expires_at < ? AND status != 'completed'
            ''', (time.time(),))
            
            expired = [row[0] for row in cursor.fetchall()]
            
            for transfer_id in expired:
                cursor.execute('UPDATE transfers SET status = ? WHERE transfer_id = ?', 
                             (TransferStatus.EXPIRED.value, transfer_id))
                cleaned += 1
                
            conn.commit()
            conn.close()
            
        return cleaned


class StorageManager:
    """Manages encrypted file chunk storage"""
    
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
        """Delete all data for a transfer"""
        transfer_path = os.path.join(self.base_path, transfer_id)
        
        if os.path.exists(transfer_path):
            shutil.rmtree(transfer_path)
            
    def get_transfer_size(self, transfer_id: str) -> int:
        """Get total size of stored transfer"""
        transfer_path = os.path.join(self.base_path, transfer_id)
        
        if not os.path.exists(transfer_path):
            return 0
            
        total_size = 0
        for root, dirs, files in os.walk(transfer_path):
            for file in files:
                file_path = os.path.join(root, file)
                total_size += os.path.getsize(file_path)
                
        return total_size


class TransferRelay:
    """
    Main transfer relay service
    Handles WAN file transfers through central server
    """
    
    # Transfer expiry times
    EXPIRY_WAN = 86400      # 24 hours
    EXPIRY_LAN = 600        # 10 minutes
    EXPIRY_CODE = 3600      # 1 hour for sharing codes
    
    def __init__(self, db_path: str = "palmsync.db", storage_path: str = "storage"):
        self.db = TransferDatabase(db_path)
        self.storage = StorageManager(storage_path)
        
        # WebSocket connections for notifications
        self.ws_connections: Dict[str, Any] = {}
        self.ws_lock = threading.Lock()
        
        # Cleanup thread
        self.cleanup_thread = threading.Thread(target=self._cleanup_loop, daemon=True)
        self.cleanup_thread.start()
        
    def create_transfer(self, sender_id: str, filename: str, 
                        file_size: int, total_chunks: int,
                        recipient_id: str = None,
                        transfer_type: TransferType = TransferType.FILE,
                        is_encrypted: bool = True) -> TransferRecord:
        """Create a new transfer"""
        transfer_id = str(uuid.uuid4())
        
        transfer = TransferRecord(
            transfer_id=transfer_id,
            sender_id=sender_id,
            recipient_id=recipient_id,
            transfer_type=transfer_type,
            filename=filename,
            file_size=file_size,
            total_chunks=total_chunks,
            status=TransferStatus.PENDING,
            created_at=time.time(),
            expires_at=time.time() + self.EXPIRY_WAN,
            is_encrypted=is_encrypted
        )
        
        self.db.create_transfer(transfer)
        
        print(f"[TransferRelay] Created transfer {transfer_id[:8]}... - {filename}")
        
        return transfer
        
    def upload_chunk(self, transfer_id: str, chunk_id: int, data: bytes) -> bool:
        """Upload a chunk"""
        transfer = self.db.get_transfer(transfer_id)
        
        if not transfer:
            return False
            
        if transfer.is_expired():
            self.db.update_transfer_status(transfer_id, TransferStatus.EXPIRED)
            return False
            
        # Store chunk
        chunk_path = self.storage.store_chunk(transfer_id, chunk_id, data)
        
        # Update database
        self.db.add_chunk(transfer_id, chunk_id, chunk_path, len(data))
        
        # Update status
        if transfer.status == TransferStatus.PENDING:
            self.db.update_transfer_status(transfer_id, TransferStatus.UPLOADING)
            
        return True
        
    def download_chunk(self, transfer_id: str, chunk_id: int) -> Optional[bytes]:
        """Download a chunk"""
        transfer = self.db.get_transfer(transfer_id)
        
        if not transfer:
            return None
            
        if transfer.is_expired():
            self.db.update_transfer_status(transfer_id, TransferStatus.EXPIRED)
            return None
            
        chunk_path = self.db.get_chunk_path(transfer_id, chunk_id)
        
        if chunk_path:
            return self.storage.get_chunk(transfer_id, chunk_id)
            
        return None
        
    def upload_metadata(self, transfer_id: str, metadata: bytes) -> bool:
        """Upload encrypted metadata"""
        transfer = self.db.get_transfer(transfer_id)
        
        if not transfer:
            return False
            
        self.storage.store_metadata(transfer_id, metadata)
        
        with self.db.lock:
            conn = sqlite3.connect(self.db.db_path)
            cursor = conn.cursor()
            cursor.execute('UPDATE transfers SET metadata_stored = 1 WHERE transfer_id = ?', (transfer_id,))
            conn.commit()
            conn.close()
            
        return True
        
    def download_metadata(self, transfer_id: str) -> Optional[bytes]:
        """Download encrypted metadata"""
        return self.storage.get_metadata(transfer_id)
        
    def complete_transfer(self, transfer_id: str) -> bool:
        """Mark transfer as complete"""
        transfer = self.db.get_transfer(transfer_id)
        
        if not transfer:
            return False
            
        # Verify all chunks are stored
        missing = self.db.get_missing_chunks(transfer_id)
        
        if missing:
            print(f"[TransferRelay] Transfer {transfer_id[:8]} missing chunks: {missing}")
            return False
            
        self.db.update_transfer_status(transfer_id, TransferStatus.COMPLETED)
        
        print(f"[TransferRelay] Transfer {transfer_id[:8]} completed!")
        
        # Notify recipient if online
        if transfer.recipient_id:
            self._notify_recipient(transfer.recipient_id, {
                'type': 'transfer_complete',
                'transfer_id': transfer_id,
                'filename': transfer.filename
            })
            
        return True
        
    def get_transfer_info(self, transfer_id: str) -> Optional[dict]:
        """Get transfer information"""
        transfer = self.db.get_transfer(transfer_id)
        
        if transfer:
            return transfer.to_dict()
        return None
        
    def get_missing_chunks(self, transfer_id: str) -> List[int]:
        """Get missing chunks for a transfer"""
        return self.db.get_missing_chunks(transfer_id)
        
    def generate_sharing_code(self, transfer_id: str, sender_id: str,
                              duration: int = None) -> Optional[str]:
        """Generate a sharing code"""
        import random
        import string
        
        # Generate 6-character code
        code = ''.join(random.choices(string.ascii_uppercase + string.digits, k=6))
        
        expires_at = time.time() + (duration or self.EXPIRY_CODE)
        
        if self.db.create_sharing_code(code, transfer_id, sender_id, expires_at):
            return code
        return None
        
    def get_transfer_by_code(self, code: str) -> Optional[TransferRecord]:
        """Get transfer by sharing code"""
        transfer_id = self.db.validate_sharing_code(code)
        
        if transfer_id:
            return self.db.get_transfer(transfer_id)
        return None
        
    def get_pending_transfers(self, user_id: str) -> List[dict]:
        """Get pending transfers for a user"""
        transfers = self.db.get_pending_transfers(user_id)
        return [t.to_dict() for t in transfers]
        
    def cancel_transfer(self, transfer_id: str) -> bool:
        """Cancel a transfer"""
        self.db.update_transfer_status(transfer_id, TransferStatus.CANCELLED)
        return True
        
    def register_websocket(self, user_id: str, ws):
        """Register WebSocket connection for notifications"""
        with self.ws_lock:
            self.ws_connections[user_id] = ws
            
    def unregister_websocket(self, user_id: str):
        """Unregister WebSocket connection"""
        with self.ws_lock:
            self.ws_connections.pop(user_id, None)
            
    def _notify_recipient(self, recipient_id: str, notification: dict):
        """Send notification to recipient"""
        with self.ws_lock:
            ws = self.ws_connections.get(recipient_id)
            
        if ws:
            try:
                ws.send(json.dumps(notification))
            except:
                pass
                
    def _cleanup_loop(self):
        """Periodic cleanup of expired transfers"""
        while True:
            time.sleep(3600)  # Every hour
            
            cleaned = self.db.cleanup_expired_transfers()
            
            if cleaned > 0:
                print(f"[TransferRelay] Cleaned up {cleaned} expired transfers")
                
            # Also clean up old storage
            for transfer_id in os.listdir(self.storage.base_path):
                transfer = self.db.get_transfer(transfer_id)
                
                if not transfer or transfer.status in [TransferStatus.EXPIRED, TransferStatus.CANCELLED]:
                    # Delete if expired and older than 1 hour
                    transfer_path = os.path.join(self.storage.base_path, transfer_id)
                    mtime = os.path.getmtime(transfer_path) if os.path.exists(transfer_path) else 0
                    
                    if time.time() - mtime > 3600:
                        self.storage.delete_transfer(transfer_id)
                        print(f"[TransferRelay] Deleted storage for {transfer_id[:8]}...")


# ============================================
# FLASK ROUTES (Add to relay_server.py)
# ============================================

def register_transfer_routes(app, transfer_relay: TransferRelay, auth_manager):
    """Register transfer routes with Flask app"""
    
    from flask import request, jsonify, send_file
    import io
    
    @app.route('/api/transfer/create', methods=['POST'])
    def create_transfer():
        """Create a new transfer"""
        auth_header = request.headers.get('Authorization', '')
        token = auth_header.replace('Bearer ', '')
        
        user = auth_manager.validate_token(token)
        if not user:
            return jsonify({'error': 'Unauthorized'}), 401
            
        data = request.json
        
        transfer = transfer_relay.create_transfer(
            sender_id=user.user_id,
            filename=data.get('filename', 'unknown'),
            file_size=data.get('file_size', 0),
            total_chunks=data.get('total_chunks', 0),
            recipient_id=data.get('recipient_id'),
            transfer_type=TransferType(data.get('transfer_type', 'file'))
        )
        
        return jsonify(transfer.to_dict())
        
    @app.route('/api/transfer/<transfer_id>/metadata', methods=['POST'])
    def upload_metadata(transfer_id):
        """Upload encrypted metadata"""
        metadata = request.get_data()
        
        if transfer_relay.upload_metadata(transfer_id, metadata):
            return jsonify({'success': True})
        return jsonify({'error': 'Failed to upload metadata'}), 400
        
    @app.route('/api/transfer/<transfer_id>/metadata', methods=['GET'])
    def download_metadata(transfer_id):
        """Download encrypted metadata"""
        metadata = transfer_relay.download_metadata(transfer_id)
        
        if metadata:
            return send_file(
                io.BytesIO(metadata),
                mimetype='application/octet-stream',
                as_attachment=True,
                download_name='metadata.enc'
            )
        return jsonify({'error': 'Metadata not found'}), 404
        
    @app.route('/api/transfer/<transfer_id>/chunk/<int:chunk_id>', methods=['POST'])
    def upload_chunk(transfer_id, chunk_id):
        """Upload a chunk"""
        if 'chunk' not in request.files:
            return jsonify({'error': 'No chunk file'}), 400
            
        chunk_file = request.files['chunk']
        chunk_data = chunk_file.read()
        
        if transfer_relay.upload_chunk(transfer_id, chunk_id, chunk_data):
            return jsonify({'success': True, 'chunk_id': chunk_id})
        return jsonify({'error': 'Failed to upload chunk'}), 400
        
    @app.route('/api/transfer/<transfer_id>/chunk/<int:chunk_id>', methods=['GET'])
    def download_chunk(transfer_id, chunk_id):
        """Download a chunk"""
        chunk_data = transfer_relay.download_chunk(transfer_id, chunk_id)
        
        if chunk_data:
            return send_file(
                io.BytesIO(chunk_data),
                mimetype='application/octet-stream',
                as_attachment=True,
                download_name=f'chunk_{chunk_id}.enc'
            )
        return jsonify({'error': 'Chunk not found'}), 404
        
    @app.route('/api/transfer/<transfer_id>/complete', methods=['POST'])
    def complete_transfer(transfer_id):
        """Mark transfer as complete"""
        if transfer_relay.complete_transfer(transfer_id):
            return jsonify({'success': True})
        return jsonify({'error': 'Failed to complete transfer'}), 400
        
    @app.route('/api/transfer/<transfer_id>', methods=['GET'])
    def get_transfer(transfer_id):
        """Get transfer information"""
        info = transfer_relay.get_transfer_info(transfer_id)
        
        if info:
            return jsonify(info)
        return jsonify({'error': 'Transfer not found'}), 404
        
    @app.route('/api/transfer/<transfer_id>/missing', methods=['GET'])
    def get_missing_chunks(transfer_id):
        """Get missing chunks"""
        missing = transfer_relay.get_missing_chunks(transfer_id)
        return jsonify({'missing_chunks': missing, 'count': len(missing)})
        
    @app.route('/api/transfers/pending', methods=['GET'])
    def get_pending_transfers():
        """Get pending transfers for user"""
        auth_header = request.headers.get('Authorization', '')
        token = auth_header.replace('Bearer ', '')
        
        user = auth_manager.validate_token(token)
        if not user:
            return jsonify({'error': 'Unauthorized'}), 401
            
        transfers = transfer_relay.get_pending_transfers(user.user_id)
        return jsonify({'transfers': transfers, 'count': len(transfers)})
        
    @app.route('/api/transfer/code/generate', methods=['POST'])
    def generate_sharing_code():
        """Generate sharing code"""
        auth_header = request.headers.get('Authorization', '')
        token = auth_header.replace('Bearer ', '')
        
        user = auth_manager.validate_token(token)
        if not user:
            return jsonify({'error': 'Unauthorized'}), 401
            
        data = request.json
        transfer_id = data.get('transfer_id')
        duration = data.get('duration', 3600)
        
        code = transfer_relay.generate_sharing_code(transfer_id, user.user_id, duration)
        
        if code:
            return jsonify({'code': code, 'expires_in': duration})
        return jsonify({'error': 'Failed to generate code'}), 400
        
    @app.route('/api/transfer/code/<code>', methods=['GET'])
    def get_transfer_by_code(code):
        """Get transfer by sharing code"""
        transfer = transfer_relay.get_transfer_by_code(code)
        
        if transfer:
            return jsonify(transfer.to_dict())
        return jsonify({'error': 'Invalid or expired code'}), 404


# ============================================
# TEST FUNCTION
# ============================================

def test_transfer_relay():
    """Test transfer relay"""
    print("📦 Palmsync Transfer Relay Test")
    print("=" * 50)
    
    relay = TransferRelay("test_palmsync.db", "test_storage")
    
    # Create test transfer
    print("\n1️⃣ Creating transfer...")
    transfer = relay.create_transfer(
        sender_id="test-sender-123",
        filename="vacation.mp4",
        file_size=50 * 1024 * 1024,  # 50 MB
        total_chunks=50,
        recipient_id="test-recipient-456"
    )
    
    print(f"   Transfer ID: {transfer.transfer_id[:8]}...")
    print(f"   File: {transfer.filename}")
    print(f"   Chunks: {transfer.total_chunks}")
    print(f"   Expires: {transfer.expires_at - transfer.created_at:.0f}s")
    
    # Upload test chunks
    print("\n2️⃣ Uploading test chunks...")
    for i in range(5):
        test_data = b"TEST_CHUNK_DATA_" + str(i).encode() * 1000
        relay.upload_chunk(transfer.transfer_id, i, test_data)
        print(f"   Uploaded chunk {i}")
        
    # Check missing
    missing = relay.get_missing_chunks(transfer.transfer_id)
    print(f"\n3️⃣ Missing chunks: {len(missing)}")
    
    # Get transfer info
    info = relay.get_transfer_info(transfer.transfer_id)
    print(f"\n4️⃣ Transfer info:")
    print(f"   Status: {info['status']}")
    print(f"   Progress: {info['progress']:.1f}%")
    
    # Test sharing code
    print("\n5️⃣ Generating sharing code...")
    code = relay.generate_sharing_code(transfer.transfer_id, "test-sender-123", 300)
    print(f"   Code: {code}")
    
    if code:
        lookup = relay.get_transfer_by_code(code)
        print(f"   Lookup successful: {lookup.filename}")
        
    # Cleanup
    import shutil
    if os.path.exists("test_storage"):
        shutil.rmtree("test_storage")
    if os.path.exists("test_palmsync.db"):
        os.remove("test_palmsync.db")
        
    print("\n✅ Transfer Relay test complete!")


if __name__ == "__main__":
    test_transfer_relay()
"""
transfer/parallel_upload.py
Parallel chunk upload manager for sending files
Handles both LAN (TCP) and WAN (HTTP) uploads
"""

import socket
import threading
import queue
import time
import json
import os
from typing import Callable, Optional, Dict, List, Any
from dataclasses import dataclass
from enum import Enum

# Try to import requests for WAN
try:
    import requests
    REQUESTS_AVAILABLE = True
except ImportError:
    REQUESTS_AVAILABLE = False
    print("[ParallelUpload] ⚠️ requests not found. Install: pip install requests")

class UploadMode(Enum):
    LAN = "lan"
    WAN = "wan"

class UploadStatus(Enum):
    IDLE = "idle"
    CONNECTING = "connecting"
    UPLOADING = "uploading"
    WAITING_ACK = "waiting_ack"
    PAUSED = "paused"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"

@dataclass
class UploadProgress:
    """Upload progress information"""
    total_chunks: int
    uploaded_chunks: int
    acked_chunks: int
    pending_chunks: int
    failed_chunks: int
    retry_count: int
    upload_speed: float  # MB/s
    percentage: float
    status: UploadStatus
    
    def to_dict(self) -> dict:
        return {
            'total_chunks': self.total_chunks,
            'uploaded_chunks': self.uploaded_chunks,
            'acked_chunks': self.acked_chunks,
            'pending_chunks': self.pending_chunks,
            'failed_chunks': self.failed_chunks,
            'retry_count': self.retry_count,
            'upload_speed': self.upload_speed,
            'percentage': self.percentage,
            'status': self.status.value
        }

class LANUploader:
    """
    TCP-based chunk uploader for LAN transfers
    Sends chunks directly to receiver
    """
    
    def __init__(self, receiver_ip: str, receiver_port: int = 8888):
        self.receiver_ip = receiver_ip
        self.receiver_port = receiver_port
        self.socket = None
        self.is_connected = False
        self.send_lock = threading.Lock()
        
    def connect(self) -> bool:
        """Connect to receiver"""
        try:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.settimeout(30.0)
            self.socket.connect((self.receiver_ip, self.receiver_port))
            self.is_connected = True
            print(f"[LANUploader] ✅ Connected to {self.receiver_ip}:{self.receiver_port}")
            return True
        except Exception as e:
            print(f"[LANUploader] ❌ Connection failed: {e}")
            return False
            
    def disconnect(self):
        """Disconnect from receiver"""
        if self.socket:
            try:
                self.socket.close()
            except:
                pass
        self.is_connected = False
        
    def send_chunk(self, chunk_id: int, encrypted_chunk: bytes, 
                   transfer_id: str) -> bool:
        """Send a chunk to receiver"""
        if not self.is_connected:
            return False
            
        with self.send_lock:
            try:
                # Create chunk packet
                packet = {
                    'type': 'chunk',
                    'transfer_id': transfer_id,
                    'chunk_id': chunk_id,
                    'size': len(encrypted_chunk)
                }
                
                # Send header
                packet_json = json.dumps(packet).encode('utf-8')
                self.socket.send(len(packet_json).to_bytes(4, 'big'))
                self.socket.send(packet_json)
                
                # Send chunk data
                self.socket.send(len(encrypted_chunk).to_bytes(4, 'big'))
                self.socket.send(encrypted_chunk)
                
                return True
                
            except Exception as e:
                print(f"[LANUploader] Chunk {chunk_id} send failed: {e}")
                return False
                
    def wait_for_ack(self, timeout: float = 2.0) -> Optional[dict]:
        """Wait for ACK from receiver"""
        if not self.is_connected:
            return None
            
        try:
            self.socket.settimeout(timeout)
            
            # Read ACK size
            size_data = self.socket.recv(4)
            if len(size_data) < 4:
                return None
                
            ack_size = int.from_bytes(size_data, 'big')
            
            # Read ACK data
            ack_data = b''
            while len(ack_data) < ack_size:
                chunk = self.socket.recv(min(4096, ack_size - len(ack_data)))
                if not chunk:
                    return None
                ack_data += chunk
                
            return json.loads(ack_data.decode('utf-8'))
            
        except socket.timeout:
            return None
        except Exception as e:
            print(f"[LANUploader] ACK wait failed: {e}")
            return None
            
    def send_metadata(self, metadata: bytes, transfer_id: str) -> bool:
        """Send encrypted metadata to receiver"""
        if not self.is_connected:
            return False
            
        try:
            packet = {
                'type': 'metadata',
                'transfer_id': transfer_id,
                'size': len(metadata)
            }
            
            packet_json = json.dumps(packet).encode('utf-8')
            self.socket.send(len(packet_json).to_bytes(4, 'big'))
            self.socket.send(packet_json)
            self.socket.send(metadata)
            
            return True
            
        except Exception as e:
            print(f"[LANUploader] Metadata send failed: {e}")
            return False


class WANUploader:
    """
    HTTP-based chunk uploader for WAN transfers
    Uploads chunks to relay server
    """
    
    def __init__(self, server_url: str, auth_token: str = None):
        self.server_url = server_url.rstrip('/')
        self.auth_token = auth_token
        
    def upload_chunk(self, chunk_id: int, encrypted_chunk: bytes,
                     transfer_id: str) -> bool:
        """Upload chunk to relay server"""
        if not REQUESTS_AVAILABLE:
            print("[WANUploader] ❌ requests library not available")
            return False
            
        try:
            headers = {}
            if self.auth_token:
                headers['Authorization'] = f'Bearer {self.auth_token}'
                
            files = {
                'chunk': (f'chunk_{chunk_id}.enc', encrypted_chunk, 'application/octet-stream')
            }
            
            data = {
                'transfer_id': transfer_id,
                'chunk_id': chunk_id
            }
            
            response = requests.post(
                f"{self.server_url}/api/upload_chunk",
                headers=headers,
                data=data,
                files=files,
                timeout=60
            )
            
            return response.status_code == 200
            
        except Exception as e:
            print(f"[WANUploader] Chunk {chunk_id} upload failed: {e}")
            return False
            
    def upload_metadata(self, metadata: bytes, transfer_id: str) -> bool:
        """Upload encrypted metadata to relay server"""
        if not REQUESTS_AVAILABLE:
            return False
            
        try:
            headers = {}
            if self.auth_token:
                headers['Authorization'] = f'Bearer {self.auth_token}'
                
            response = requests.post(
                f"{self.server_url}/api/upload_metadata",
                headers=headers,
                json={
                    'transfer_id': transfer_id,
                    'metadata': metadata.hex()
                },
                timeout=30
            )
            
            return response.status_code == 200
            
        except Exception as e:
            print(f"[WANUploader] Metadata upload failed: {e}")
            return False
            
    def check_ack(self, transfer_id: str, chunk_id: int) -> bool:
        """Check if chunk was acknowledged"""
        if not REQUESTS_AVAILABLE:
            return False
            
        try:
            headers = {}
            if self.auth_token:
                headers['Authorization'] = f'Bearer {self.auth_token}'
                
            response = requests.get(
                f"{self.server_url}/api/ack_status/{transfer_id}/{chunk_id}",
                headers=headers,
                timeout=10
            )
            
            if response.status_code == 200:
                data = response.json()
                return data.get('acked', False)
                
            return False
            
        except Exception as e:
            return False


class ParallelUploader:
    """
    Main parallel upload manager
    Uploads multiple chunks simultaneously with ACK tracking
    """
    
    MAX_PARALLEL_UPLOADS = 5
    CHUNK_RETRY_LIMIT = 3
    ACK_TIMEOUT = 2.0  # seconds
    
    def __init__(self, transfer_id: str, mode: UploadMode,
                 chunks: List[bytes], metadata: bytes,
                 receiver_ip: str = None, server_url: str = None,
                 auth_token: str = None):
        """
        Initialize parallel uploader
        
        Args:
            transfer_id: Unique transfer ID
            mode: LAN or WAN
            chunks: List of encrypted chunks
            metadata: Encrypted metadata
            receiver_ip: Receiver IP for LAN
            server_url: Relay server URL for WAN
            auth_token: Auth token for WAN
        """
        self.transfer_id = transfer_id
        self.mode = mode
        self.chunks = chunks
        self.metadata = metadata
        self.total_chunks = len(chunks)
        
        # Uploader
        if mode == UploadMode.LAN:
            self.uploader = LANUploader(receiver_ip)
        else:
            self.uploader = WANUploader(server_url, auth_token)
            
        # State
        self.status = UploadStatus.IDLE
        
        # Tracking
        self.uploaded_chunks = set()
        self.acked_chunks = set()
        self.failed_chunks: Dict[int, int] = {}
        self.pending_acks: Dict[int, float] = {}
        self.lock = threading.Lock()
        
        # Queues
        self.upload_queue = queue.Queue()
        self.ack_queue = queue.Queue()
        
        # Threading
        self.is_running = False
        self.upload_threads = []
        self.ack_thread = None
        
        # Progress tracking
        self.start_time = 0
        self.bytes_uploaded = 0
        self.progress_callback = None
        
        # Statistics
        self.stats = {
            'total_uploaded': 0,
            'retry_count': 0,
            'peak_speed': 0
        }
        
    def start(self, progress_callback: Callable = None) -> bool:
        """
        Start parallel upload
        
        Args:
            progress_callback: Optional progress callback
            
        Returns:
            True if started successfully
        """
        self.progress_callback = progress_callback
        
        # Connect for LAN
        if self.mode == UploadMode.LAN:
            if not self.uploader.connect():
                self.status = UploadStatus.FAILED
                return False
                
            # Send metadata first
            if not self.uploader.send_metadata(self.metadata, self.transfer_id):
                self.status = UploadStatus.FAILED
                return False
        else:
            # Upload metadata for WAN
            if not self.uploader.upload_metadata(self.metadata, self.transfer_id):
                self.status = UploadStatus.FAILED
                return False
                
        # Initialize upload queue
        for chunk_id in range(self.total_chunks):
            self.upload_queue.put(chunk_id)
            
        # Start worker threads
        self.is_running = True
        self.status = UploadStatus.UPLOADING
        self.start_time = time.time()
        
        for i in range(self.MAX_PARALLEL_UPLOADS):
            thread = threading.Thread(
                target=self._upload_worker,
                name=f"Uploader-{i}",
                daemon=True
            )
            thread.start()
            self.upload_threads.append(thread)
            
        # Start ACK thread for LAN
        if self.mode == UploadMode.LAN:
            self.ack_thread = threading.Thread(
                target=self._ack_worker,
                daemon=True
            )
            self.ack_thread.start()
        else:
            # Start WAN ACK checker
            self.ack_thread = threading.Thread(
                target=self._wan_ack_worker,
                daemon=True
            )
            self.ack_thread.start()
            
        print(f"[ParallelUploader] ✅ Started uploading {self.total_chunks} chunks")
        print(f"                   Mode: {self.mode.value}")
        print(f"                   Parallel uploads: {self.MAX_PARALLEL_UPLOADS}")
        
        return True
        
    def _upload_worker(self):
        """Worker thread for uploading chunks"""
        while self.is_running and self.status == UploadStatus.UPLOADING:
            try:
                # Get chunk ID from queue
                chunk_id = self.upload_queue.get(timeout=1.0)
                
                # Skip if already acked
                with self.lock:
                    if chunk_id in self.acked_chunks:
                        self.upload_queue.task_done()
                        continue
                        
                # Upload chunk
                encrypted_chunk = self.chunks[chunk_id]
                
                if self.mode == UploadMode.LAN:
                    success = self.uploader.send_chunk(
                        chunk_id, encrypted_chunk, self.transfer_id
                    )
                else:
                    success = self.uploader.upload_chunk(
                        chunk_id, encrypted_chunk, self.transfer_id
                    )
                    
                if success:
                    with self.lock:
                        self.uploaded_chunks.add(chunk_id)
                        self.bytes_uploaded += len(encrypted_chunk)
                        
                        if self.mode == UploadMode.LAN:
                            # Track for ACK
                            self.pending_acks[chunk_id] = time.time()
                            
                    self._update_progress()
                else:
                    self._handle_failed_chunk(chunk_id)
                    
                self.upload_queue.task_done()
                
            except queue.Empty:
                # Check if all chunks processed
                if self._all_chunks_processed():
                    break
                continue
            except Exception as e:
                print(f"[ParallelUploader] Worker error: {e}")
                
    def _ack_worker(self):
        """Worker thread for processing ACKs (LAN only)"""
        while self.is_running and self.status == UploadStatus.UPLOADING:
            try:
                # Wait for ACK
                ack = self.uploader.wait_for_ack(timeout=1.0)
                
                if ack and ack.get('type') == 'ack':
                    chunk_id = ack.get('chunk_id')
                    
                    with self.lock:
                        if chunk_id in self.pending_acks:
                            del self.pending_acks[chunk_id]
                        self.acked_chunks.add(chunk_id)
                        
                    self._update_progress()
                    
                    # Check if complete
                    if self._all_chunks_acked():
                        self._finalize_upload()
                        break
                        
                # Check for ACK timeouts
                self._check_ack_timeouts()
                
            except Exception as e:
                print(f"[ParallelUploader] ACK worker error: {e}")
                
    def _wan_ack_worker(self):
        """Worker thread for checking ACKs (WAN only)"""
        while self.is_running and self.status == UploadStatus.UPLOADING:
            with self.lock:
                # Get uploaded but not acked chunks
                pending = self.uploaded_chunks - self.acked_chunks
                
            for chunk_id in list(pending):
                if self.uploader.check_ack(self.transfer_id, chunk_id):
                    with self.lock:
                        self.acked_chunks.add(chunk_id)
                        
                    self._update_progress()
                    
            # Check if complete
            if self._all_chunks_acked():
                self._finalize_upload()
                break
                
            time.sleep(1)
            
    def _check_ack_timeouts(self):
        """Check for chunks waiting too long for ACK"""
        current_time = time.time()
        timeout_chunks = []
        
        with self.lock:
            for chunk_id, send_time in list(self.pending_acks.items()):
                if current_time - send_time > self.ACK_TIMEOUT:
                    timeout_chunks.append(chunk_id)
                    del self.pending_acks[chunk_id]
                    
        # Re-queue timed out chunks
        for chunk_id in timeout_chunks:
            print(f"[ParallelUploader] ACK timeout for chunk {chunk_id}")
            self._handle_failed_chunk(chunk_id)
            
    def _handle_failed_chunk(self, chunk_id: int):
        """Handle failed chunk upload"""
        with self.lock:
            # Increment retry count
            retries = self.failed_chunks.get(chunk_id, 0) + 1
            self.failed_chunks[chunk_id] = retries
            self.stats['retry_count'] += 1
            
            if retries < self.CHUNK_RETRY_LIMIT:
                # Re-queue for retry
                self.upload_queue.put(chunk_id)
                print(f"[ParallelUploader] Chunk {chunk_id} failed, retry {retries}/{self.CHUNK_RETRY_LIMIT}")
            else:
                print(f"[ParallelUploader] ❌ Chunk {chunk_id} failed permanently after {retries} retries")
                
    def _all_chunks_processed(self) -> bool:
        """Check if all chunks have been uploaded"""
        with self.lock:
            return len(self.uploaded_chunks) == self.total_chunks
            
    def _all_chunks_acked(self) -> bool:
        """Check if all chunks have been ACKed"""
        with self.lock:
            return len(self.acked_chunks) == self.total_chunks
            
    def _update_progress(self):
        """Update upload progress"""
        with self.lock:
            uploaded = len(self.uploaded_chunks)
            acked = len(self.acked_chunks)
            percentage = (acked / self.total_chunks * 100) if self.total_chunks > 0 else 0
            
            # Calculate speed
            elapsed = time.time() - self.start_time if self.start_time > 0 else 1
            speed = (self.bytes_uploaded / 1024 / 1024) / elapsed if elapsed > 0 else 0
            
            if speed > self.stats['peak_speed']:
                self.stats['peak_speed'] = speed
                
            progress = UploadProgress(
                total_chunks=self.total_chunks,
                uploaded_chunks=uploaded,
                acked_chunks=acked,
                pending_chunks=self.total_chunks - acked,
                failed_chunks=len(self.failed_chunks),
                retry_count=self.stats['retry_count'],
                upload_speed=speed,
                percentage=percentage,
                status=self.status
            )
            
        if self.progress_callback:
            self.progress_callback(progress)
            
    def _finalize_upload(self):
        """Finalize upload"""
        elapsed = time.time() - self.start_time
        
        self.status = UploadStatus.COMPLETED
        self.is_running = False
        
        print(f"[ParallelUploader] ✅ Upload complete!")
        print(f"                   Chunks: {len(self.acked_chunks)}/{self.total_chunks}")
        print(f"                   Time: {elapsed:.2f}s")
        print(f"                   Peak speed: {self.stats['peak_speed']:.2f} MB/s")
        print(f"                   Retries: {self.stats['retry_count']}")
        
        # Disconnect for LAN
        if self.mode == UploadMode.LAN:
            self.uploader.disconnect()
            
    def pause(self):
        """Pause upload"""
        self.status = UploadStatus.PAUSED
        print("[ParallelUploader] Upload paused")
        
    def resume(self):
        """Resume upload"""
        self.status = UploadStatus.UPLOADING
        print("[ParallelUploader] Upload resumed")
        
    def cancel(self):
        """Cancel upload"""
        self.status = UploadStatus.CANCELLED
        self.is_running = False
        
        if self.mode == UploadMode.LAN:
            self.uploader.disconnect()
            
        print("[ParallelUploader] Upload cancelled")
        
    def get_progress(self) -> Optional[UploadProgress]:
        """Get current upload progress"""
        with self.lock:
            uploaded = len(self.uploaded_chunks)
            acked = len(self.acked_chunks)
            percentage = (acked / self.total_chunks * 100) if self.total_chunks > 0 else 0
            
            elapsed = time.time() - self.start_time if self.start_time > 0 else 1
            speed = (self.bytes_uploaded / 1024 / 1024) / elapsed if elapsed > 0 else 0
            
            return UploadProgress(
                total_chunks=self.total_chunks,
                uploaded_chunks=uploaded,
                acked_chunks=acked,
                pending_chunks=self.total_chunks - acked,
                failed_chunks=len(self.failed_chunks),
                retry_count=self.stats['retry_count'],
                upload_speed=speed,
                percentage=percentage,
                status=self.status
            )
            
    def wait_for_completion(self, timeout: float = None) -> bool:
        """Wait for upload to complete"""
        start = time.time()
        
        while self.status == UploadStatus.UPLOADING:
            if timeout and time.time() - start > timeout:
                return False
            time.sleep(0.1)
            
        return self.status == UploadStatus.COMPLETED


# ============================================
# TEST FUNCTION
# ============================================

def test_parallel_uploader():
    """Test parallel uploader"""
    print("📤 Parallel Uploader Test")
    print("=" * 50)
    
    # Create mock chunks
    print("\n1️⃣ Creating test chunks...")
    import secrets
    
    chunks = []
    for i in range(10):
        chunk_data = secrets.token_bytes(1024 * 1024)  # 1 MB chunks
        chunks.append(chunk_data)
        
    metadata = b"test_metadata"
    
    print(f"   Total chunks: {len(chunks)}")
    print(f"   Total size: {sum(len(c) for c in chunks) / 1024 / 1024:.2f} MB")
    
    # Create uploader (mock mode)
    print("\n2️⃣ Creating uploader...")
    
    uploader = ParallelUploader(
        transfer_id="test-123",
        mode=UploadMode.LAN,
        chunks=chunks,
        metadata=metadata,
        receiver_ip="192.168.1.10"
    )
    
    def progress_callback(progress: UploadProgress):
        print(f"\r   Progress: {progress.percentage:.1f}% "
              f"({progress.acked_chunks}/{progress.total_chunks} ACKed) "
              f"- {progress.upload_speed:.2f} MB/s", end="")
        
    print("\n3️⃣ Uploader ready!")
    print(f"   Mode: {uploader.mode.value}")
    print(f"   Max parallel: {uploader.MAX_PARALLEL_UPLOADS}")
    print(f"   ACK timeout: {uploader.ACK_TIMEOUT}s")
    print(f"   Retry limit: {uploader.CHUNK_RETRY_LIMIT}")
    
    print("\n✅ Parallel uploader test complete!")


if __name__ == "__main__":
    test_parallel_uploader()
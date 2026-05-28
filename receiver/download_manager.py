"""
receiver/download_manager.py
Manages parallel chunk downloading and file assembly
Handles both LAN (TCP) and WAN (Relay) downloads
"""

import socket
import threading
import queue
import time
import json
import os
from typing import Callable, Optional, Dict, List, Any
from dataclasses import dataclass, field
from enum import Enum

# Import our modules
from transfer.chunker import FileAssembler, FileMetadata
from security.hybrid import HybridEncryption
from reliability.ack_manager import ACKManager, ChunkACK
from reliability.retry_queue import RetryQueue

# Try to import requests for WAN
try:
    import requests
    REQUESTS_AVAILABLE = True
except ImportError:
    REQUESTS_AVAILABLE = False
    print("[DownloadManager] ⚠️ requests not found. Install: pip install requests")

class DownloadMode(Enum):
    LAN = "lan"
    WAN = "wan"

class DownloadStatus(Enum):
    IDLE = "idle"
    CONNECTING = "connecting"
    DOWNLOADING = "downloading"
    PAUSED = "paused"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"

@dataclass
class DownloadProgress:
    """Download progress information"""
    total_chunks: int
    received_chunks: int
    pending_chunks: int
    failed_chunks: int
    retry_count: int
    download_speed: float  # MB/s
    percentage: float
    status: DownloadStatus
    
    def to_dict(self) -> dict:
        return {
            'total_chunks': self.total_chunks,
            'received_chunks': self.received_chunks,
            'pending_chunks': self.pending_chunks,
            'failed_chunks': self.failed_chunks,
            'retry_count': self.retry_count,
            'download_speed': self.download_speed,
            'percentage': self.percentage,
            'status': self.status.value
        }

class LANDownloader:
    """
    TCP-based chunk downloader for LAN transfers
    Downloads chunks directly from sender
    """
    
    def __init__(self, sender_ip: str, sender_port: int = 8888):
        self.sender_ip = sender_ip
        self.sender_port = sender_port
        self.socket = None
        self.is_connected = False
        
    def connect(self) -> bool:
        """Connect to sender"""
        try:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.settimeout(30.0)
            self.socket.connect((self.sender_ip, self.sender_port))
            self.is_connected = True
            print(f"[LANDownloader] ✅ Connected to {self.sender_ip}:{self.sender_port}")
            return True
        except Exception as e:
            print(f"[LANDownloader] ❌ Connection failed: {e}")
            return False
            
    def disconnect(self):
        """Disconnect from sender"""
        if self.socket:
            try:
                self.socket.close()
            except:
                pass
        self.is_connected = False
        
    def request_chunk(self, chunk_id: int, transfer_id: str) -> Optional[bytes]:
        """Request a specific chunk from sender"""
        if not self.is_connected:
            return None
            
        try:
            # Create request
            request = {
                'type': 'chunk_request',
                'transfer_id': transfer_id,
                'chunk_id': chunk_id
            }
            
            request_json = json.dumps(request).encode('utf-8')
            self.socket.send(len(request_json).to_bytes(4, 'big'))
            self.socket.send(request_json)
            
            # Receive chunk
            return self._receive_chunk()
            
        except Exception as e:
            print(f"[LANDownloader] Chunk request failed: {e}")
            return None
            
    def request_missing_chunks(self, missing_ids: List[int], 
                               transfer_id: str) -> Dict[int, bytes]:
        """Request multiple missing chunks"""
        chunks = {}
        
        for chunk_id in missing_ids:
            chunk_data = self.request_chunk(chunk_id, transfer_id)
            if chunk_data:
                chunks[chunk_id] = chunk_data
                
        return chunks
        
    def _receive_chunk(self) -> Optional[bytes]:
        """Receive chunk data from socket"""
        try:
            # Read chunk size (4 bytes)
            size_data = self.socket.recv(4)
            if len(size_data) < 4:
                return None
                
            chunk_size = int.from_bytes(size_data, 'big')
            
            # Read chunk data
            chunk_data = b''
            while len(chunk_data) < chunk_size:
                remaining = chunk_size - len(chunk_data)
                data = self.socket.recv(min(4096, remaining))
                if not data:
                    return None
                chunk_data += data
                
            return chunk_data
            
        except Exception as e:
            print(f"[LANDownloader] Receive error: {e}")
            return None
            
    def send_ack(self, chunk_id: int, transfer_id: str):
        """Send acknowledgment for received chunk"""
        if not self.is_connected:
            return
            
        try:
            ack = {
                'type': 'ack',
                'transfer_id': transfer_id,
                'chunk_id': chunk_id
            }
            
            ack_json = json.dumps(ack).encode('utf-8')
            self.socket.send(len(ack_json).to_bytes(4, 'big'))
            self.socket.send(ack_json)
            
        except Exception as e:
            print(f"[LANDownloader] ACK failed: {e}")


class WANDownloader:
    """
    HTTP-based chunk downloader for WAN transfers
    Downloads chunks from relay server
    """
    
    def __init__(self, server_url: str, auth_token: str = None):
        self.server_url = server_url.rstrip('/')
        self.auth_token = auth_token
        
    def download_chunk(self, transfer_id: str, chunk_id: int) -> Optional[bytes]:
        """Download chunk from relay server"""
        if not REQUESTS_AVAILABLE:
            print("[WANDownloader] ❌ requests library not available")
            return None
            
        try:
            headers = {}
            if self.auth_token:
                headers['Authorization'] = f'Bearer {self.auth_token}'
                
            response = requests.get(
                f"{self.server_url}/api/chunk/{transfer_id}/{chunk_id}",
                headers=headers,
                timeout=30
            )
            
            if response.status_code == 200:
                return response.content
            else:
                print(f"[WANDownloader] Chunk {chunk_id} download failed: {response.status_code}")
                return None
                
        except Exception as e:
            print(f"[WANDownloader] Download error: {e}")
            return None
            
    def download_metadata(self, transfer_id: str) -> Optional[dict]:
        """Download transfer metadata"""
        if not REQUESTS_AVAILABLE:
            return None
            
        try:
            headers = {}
            if self.auth_token:
                headers['Authorization'] = f'Bearer {self.auth_token}'
                
            response = requests.get(
                f"{self.server_url}/api/metadata/{transfer_id}",
                headers=headers,
                timeout=30
            )
            
            if response.status_code == 200:
                return response.json()
            else:
                return None
                
        except Exception as e:
            print(f"[WANDownloader] Metadata download failed: {e}")
            return None


class DownloadManager:
    """
    Main download manager - orchestrates parallel chunk downloading
    """
    
    MAX_PARALLEL_DOWNLOADS = 5
    CHUNK_RETRY_LIMIT = 3
    
    def __init__(self, transfer_id: str, mode: DownloadMode,
                 sender_ip: str = None, server_url: str = None,
                 auth_token: str = None):
        """
        Initialize download manager
        
        Args:
            transfer_id: Unique transfer ID
            mode: LAN or WAN
            sender_ip: Sender IP for LAN
            server_url: Relay server URL for WAN
            auth_token: Auth token for WAN
        """
        self.transfer_id = transfer_id
        self.mode = mode
        
        # Downloaders
        if mode == DownloadMode.LAN:
            self.downloader = LANDownloader(sender_ip)
        else:
            self.downloader = WANDownloader(server_url, auth_token)
            
        # State
        self.status = DownloadStatus.IDLE
        self.metadata: Optional[dict] = None
        self.assembler: Optional[FileAssembler] = None
        self.hybrid: Optional[HybridEncryption] = None
        
        # Chunk management
        self.received_chunks: Dict[int, bytes] = {}
        self.downloaded_ids = set()
        self.failed_chunks: Dict[int, int] = {}  # chunk_id -> retry_count
        self.lock = threading.Lock()
        
        # Queues
        self.download_queue = queue.Queue()
        self.result_queue = queue.Queue()
        
        # Threading
        self.is_running = False
        self.download_threads = []
        self.assembler_thread = None
        
        # ACK and Retry
        self.ack_manager = ACKManager()
        self.retry_queue = RetryQueue(max_retries=self.CHUNK_RETRY_LIMIT)
        
        # Progress tracking
        self.start_time = 0
        self.bytes_downloaded = 0
        self.progress_callback = None
        
        # Statistics
        self.stats = {
            'total_downloaded': 0,
            'retry_count': 0,
            'peak_speed': 0
        }
        
    def start(self, metadata: dict, hybrid: HybridEncryption,
              output_dir: str = None,
              progress_callback: Callable = None) -> bool:
        """
        Start downloading
        
        Args:
            metadata: File metadata
            hybrid: Hybrid encryption instance
            output_dir: Output directory for file
            progress_callback: Optional progress callback
            
        Returns:
            True if started successfully
        """
        self.metadata = metadata
        self.hybrid = hybrid
        self.progress_callback = progress_callback
        
        # Connect for LAN
        if self.mode == DownloadMode.LAN:
            if not self.downloader.connect():
                self.status = DownloadStatus.FAILED
                return False
                
        # Create file metadata
        file_metadata = FileMetadata(
            filename=metadata.get('original_filename', 'received_file'),
            file_size=metadata.get('original_size', 0),
            total_chunks=metadata.get('total_chunks', 0),
            chunk_size=metadata.get('chunk_size', 1024 * 1024),
            file_hash=metadata.get('file_hash', '')
        )
        
        # Create assembler
        self.assembler = FileAssembler(file_metadata, output_dir)
        
        # Initialize download queue with all chunks
        for chunk_id in range(file_metadata.total_chunks):
            self.download_queue.put(chunk_id)
            
        # Start worker threads
        self.is_running = True
        self.status = DownloadStatus.DOWNLOADING
        self.start_time = time.time()
        
        for i in range(self.MAX_PARALLEL_DOWNLOADS):
            thread = threading.Thread(
                target=self._download_worker,
                name=f"Downloader-{i}",
                daemon=True
            )
            thread.start()
            self.download_threads.append(thread)
            
        # Start assembler thread
        self.assembler_thread = threading.Thread(
            target=self._assembler_worker,
            daemon=True
        )
        self.assembler_thread.start()
        
        print(f"[DownloadManager] ✅ Started downloading {file_metadata.total_chunks} chunks")
        print(f"                 Mode: {self.mode.value}")
        print(f"                 Parallel downloads: {self.MAX_PARALLEL_DOWNLOADS}")
        
        return True
        
    def _download_worker(self):
        """Worker thread for downloading chunks"""
        while self.is_running and self.status == DownloadStatus.DOWNLOADING:
            try:
                # Get chunk ID from queue
                chunk_id = self.download_queue.get(timeout=1.0)
                
                # Download chunk
                self._download_single_chunk(chunk_id)
                
                self.download_queue.task_done()
                
            except queue.Empty:
                # Check if all chunks downloaded
                if self._all_chunks_downloaded():
                    break
                continue
            except Exception as e:
                print(f"[DownloadManager] Worker error: {e}")
                
    def _download_single_chunk(self, chunk_id: int):
        """Download a single chunk"""
        # Skip if already downloaded
        with self.lock:
            if chunk_id in self.downloaded_ids:
                return
                
        # Download chunk
        if self.mode == DownloadMode.LAN:
            encrypted_chunk = self.downloader.request_chunk(chunk_id, self.transfer_id)
        else:
            encrypted_chunk = self.downloader.download_chunk(self.transfer_id, chunk_id)
            
        if encrypted_chunk is None:
            # Download failed
            self._handle_failed_chunk(chunk_id)
            return
            
        # Decrypt chunk
        try:
            decrypted_chunk, cid, total = self.hybrid.decrypt_chunk(encrypted_chunk)
            
            if decrypted_chunk is None:
                print(f"[DownloadManager] Failed to decrypt chunk {chunk_id}")
                self._handle_failed_chunk(chunk_id)
                return
                
            # Store chunk
            with self.lock:
                self.received_chunks[chunk_id] = decrypted_chunk
                self.downloaded_ids.add(chunk_id)
                self.bytes_downloaded += len(decrypted_chunk)
                
            # Send ACK for LAN
            if self.mode == DownloadMode.LAN:
                self.downloader.send_ack(chunk_id, self.transfer_id)
                
            # Update ACK manager
            self.ack_manager.mark_acked(chunk_id)
            
            # Update progress
            self._update_progress()
            
        except Exception as e:
            print(f"[DownloadManager] Chunk {chunk_id} processing failed: {e}")
            self._handle_failed_chunk(chunk_id)
            
    def _handle_failed_chunk(self, chunk_id: int):
        """Handle failed chunk download"""
        with self.lock:
            # Increment retry count
            retries = self.failed_chunks.get(chunk_id, 0) + 1
            self.failed_chunks[chunk_id] = retries
            self.stats['retry_count'] += 1
            
            if retries < self.CHUNK_RETRY_LIMIT:
                # Add to retry queue
                self.retry_queue.add_chunk(chunk_id, retries)
                print(f"[DownloadManager] Chunk {chunk_id} failed, retry {retries}/{self.CHUNK_RETRY_LIMIT}")
            else:
                print(f"[DownloadManager] ❌ Chunk {chunk_id} failed permanently after {retries} retries")
                
    def _assembler_worker(self):
        """Worker thread for assembling received chunks"""
        while self.is_running:
            # Process received chunks
            with self.lock:
                chunks_to_assemble = list(self.received_chunks.keys())
                
            for chunk_id in chunks_to_assemble:
                with self.lock:
                    chunk_data = self.received_chunks.pop(chunk_id, None)
                    
                if chunk_data:
                    self.assembler.add_chunk(chunk_id, chunk_data)
                    
            # Process retry queue
            retry_chunks = self.retry_queue.get_chunks_to_retry()
            for chunk_id in retry_chunks:
                self.download_queue.put(chunk_id)
                
            # Check if complete
            if self._all_chunks_downloaded() and len(self.received_chunks) == 0:
                self._finalize_download()
                break
                
            time.sleep(0.1)
            
    def _all_chunks_downloaded(self) -> bool:
        """Check if all chunks have been downloaded"""
        with self.lock:
            return len(self.downloaded_ids) == self.metadata.get('total_chunks', 0)
            
    def _update_progress(self):
        """Update download progress"""
        with self.lock:
            total_chunks = self.metadata.get('total_chunks', 1)
            received = len(self.downloaded_ids)
            percentage = (received / total_chunks * 100) if total_chunks > 0 else 0
            
            # Calculate speed
            elapsed = time.time() - self.start_time
            speed = (self.bytes_downloaded / 1024 / 1024) / elapsed if elapsed > 0 else 0
            
            if speed > self.stats['peak_speed']:
                self.stats['peak_speed'] = speed
                
            progress = DownloadProgress(
                total_chunks=total_chunks,
                received_chunks=received,
                pending_chunks=total_chunks - received,
                failed_chunks=len(self.failed_chunks),
                retry_count=self.stats['retry_count'],
                download_speed=speed,
                percentage=percentage,
                status=self.status
            )
            
        if self.progress_callback:
            self.progress_callback(progress)
            
    def _finalize_download(self):
        """Finalize download and assemble file"""
        print(f"[DownloadManager] All chunks received, assembling file...")
        
        # Assemble file
        output_path = self.assembler.assemble()
        
        if output_path:
            self.status = DownloadStatus.COMPLETED
            print(f"[DownloadManager] ✅ Download complete!")
            print(f"                 File: {output_path}")
            print(f"                 Size: {self.bytes_downloaded / 1024 / 1024:.2f} MB")
            print(f"                 Time: {time.time() - self.start_time:.2f}s")
            print(f"                 Peak speed: {self.stats['peak_speed']:.2f} MB/s")
        else:
            self.status = DownloadStatus.FAILED
            print(f"[DownloadManager] ❌ Assembly failed")
            
        self.is_running = False
        
        # Disconnect for LAN
        if self.mode == DownloadMode.LAN:
            self.downloader.disconnect()
            
    def pause(self):
        """Pause download"""
        self.status = DownloadStatus.PAUSED
        print("[DownloadManager] Download paused")
        
    def resume(self):
        """Resume download"""
        self.status = DownloadStatus.DOWNLOADING
        print("[DownloadManager] Download resumed")
        
    def cancel(self):
        """Cancel download"""
        self.status = DownloadStatus.CANCELLED
        self.is_running = False
        
        if self.mode == DownloadMode.LAN:
            self.downloader.disconnect()
            
        # Clear received chunks
        self.assembler.clear_chunks()
        
        print("[DownloadManager] Download cancelled")
        
    def get_progress(self) -> Optional[DownloadProgress]:
        """Get current download progress"""
        with self.lock:
            if not self.metadata:
                return None
                
            total_chunks = self.metadata.get('total_chunks', 1)
            received = len(self.downloaded_ids)
            percentage = (received / total_chunks * 100) if total_chunks > 0 else 0
            
            elapsed = time.time() - self.start_time if self.start_time > 0 else 1
            speed = (self.bytes_downloaded / 1024 / 1024) / elapsed if elapsed > 0 else 0
            
            return DownloadProgress(
                total_chunks=total_chunks,
                received_chunks=received,
                pending_chunks=total_chunks - received,
                failed_chunks=len(self.failed_chunks),
                retry_count=self.stats['retry_count'],
                download_speed=speed,
                percentage=percentage,
                status=self.status
            )
            
    def wait_for_completion(self, timeout: float = None) -> bool:
        """Wait for download to complete"""
        start = time.time()
        
        while self.status in [DownloadStatus.DOWNLOADING, DownloadStatus.CONNECTING]:
            if timeout and time.time() - start > timeout:
                return False
            time.sleep(0.1)
            
        return self.status == DownloadStatus.COMPLETED


# ============================================
# TEST FUNCTION
# ============================================

def test_download_manager():
    """Test download manager"""
    print("📥 Download Manager Test")
    print("=" * 50)
    
    # Test progress tracking
    print("\n1️⃣ Testing progress tracking...")
    
    # Create mock components
    from security.hybrid import HybridEncryption, HybridTransferHandler
    
    handler = HybridTransferHandler()
    handler.init_as_receiver(b"dummy_public_key")
    
    # Create download manager
    manager = DownloadManager(
        transfer_id="test-123",
        mode=DownloadMode.LAN,
        sender_ip="192.168.1.10"
    )
    
    def progress_callback(progress: DownloadProgress):
        print(f"\r   Progress: {progress.percentage:.1f}% "
              f"({progress.received_chunks}/{progress.total_chunks}) "
              f"- {progress.download_speed:.2f} MB/s", end="")
        
    # Mock metadata
    metadata = {
        'original_filename': 'test.txt',
        'original_size': 5 * 1024 * 1024,
        'total_chunks': 5,
        'chunk_size': 1024 * 1024,
        'file_hash': 'dummy_hash'
    }
    
    print("\n2️⃣ Download manager ready!")
    print(f"   Mode: {manager.mode.value}")
    print(f"   Max parallel: {manager.MAX_PARALLEL_DOWNLOADS}")
    print(f"   Retry limit: {manager.CHUNK_RETRY_LIMIT}")
    
    print("\n✅ Download manager test complete!")


if __name__ == "__main__":
    test_download_manager()
"""
connection/relay_client.py
Relay server client for WAN file transfers
Handles authentication, chunk upload/download, and server communication
"""

import os
import json
import time
import threading
import queue
from typing import Callable, Optional, Dict, Any, List, Tuple
from dataclasses import dataclass, field
from enum import Enum

# Try to import requests
try:
    import requests
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry
    REQUESTS_AVAILABLE = True
except ImportError:
    REQUESTS_AVAILABLE = False
    print("[RelayClient] ⚠️ requests not found. Install: pip install requests")

# Try to import websocket
try:
    import websocket
    WEBSOCKET_AVAILABLE = True
except ImportError:
    WEBSOCKET_AVAILABLE = False
    print("[RelayClient] ⚠️ websocket-client not found. Install: pip install websocket-client")

class RelayClientState(Enum):
    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    CONNECTED = "connected"
    AUTHENTICATED = "authenticated"
    ERROR = "error"

class TransferStatus(Enum):
    PENDING = "pending"
    UPLOADING = "uploading"
    DOWNLOADING = "downloading"
    COMPLETED = "completed"
    FAILED = "failed"
    EXPIRED = "expired"
    CANCELLED = "cancelled"

@dataclass
class RelayConfig:
    """Configuration for relay client"""
    server_url: str = "http://localhost:5000"
    auth_token: Optional[str] = None
    device_id: str = ""
    device_name: str = ""
    chunk_size: int = 1024 * 1024  # 1 MB
    max_retries: int = 3
    timeout: int = 30
    heartbeat_interval: int = 30
    reconnect_delay: int = 5

@dataclass
class TransferInfo:
    """Information about a relay transfer"""
    transfer_id: str
    direction: str  # 'upload' or 'download'
    filename: str
    file_size: int
    total_chunks: int
    status: TransferStatus
    created_at: float
    expires_at: float
    progress: float = 0.0
    chunks_uploaded: int = 0
    chunks_downloaded: int = 0
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def is_expired(self) -> bool:
        return time.time() > self.expires_at
        
    def get_remaining_time(self) -> float:
        return max(0, self.expires_at - time.time())

class RelayClient:
    """
    Client for communicating with relay server
    Handles authentication, file upload/download, and real-time notifications
    """
    
    def __init__(self, config: RelayConfig = None):
        """
        Initialize relay client
        
        Args:
            config: Relay configuration
        """
        self.config = config or RelayConfig()
        self.state = RelayClientState.DISCONNECTED
        
        # HTTP session
        self.session = None
        self._setup_session()
        
        # WebSocket connection
        self.ws = None
        self.ws_thread = None
        
        # Active transfers
        self.transfers: Dict[str, TransferInfo] = {}
        self.transfer_lock = threading.Lock()
        
        # Message queue for async operations
        self.message_queue = queue.Queue()
        
        # Background threads
        self.is_running = False
        self.heartbeat_thread = None
        self.process_thread = None
        
        # Callbacks
        self.on_connected = None
        self.on_disconnected = None
        self.on_transfer_update = None
        self.on_notification = None
        self.on_error = None
        
    def _setup_session(self):
        """Setup HTTP session with retry logic"""
        if not REQUESTS_AVAILABLE:
            return
            
        self.session = requests.Session()
        
        # Configure retry strategy
        retry_strategy = Retry(
            total=self.config.max_retries,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504]
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        
        # Set default headers
        self.session.headers.update({
            'Content-Type': 'application/json',
            'User-Agent': f'SecureTransfer/1.0 ({self.config.device_name})'
        })
        
        if self.config.auth_token:
            self.session.headers.update({
                'Authorization': f'Bearer {self.config.auth_token}'
            })
            
    def connect(self) -> bool:
        """
        Connect to relay server
        
        Returns:
            True if connected successfully
        """
        self.state = RelayClientState.CONNECTING
        
        # Test connection
        if not self._test_connection():
            self.state = RelayClientState.ERROR
            return False
            
        # Setup WebSocket for real-time notifications
        if WEBSOCKET_AVAILABLE:
            self._connect_websocket()
            
        self.state = RelayClientState.CONNECTED
        self.is_running = True
        
        # Start background threads
        self.heartbeat_thread = threading.Thread(target=self._heartbeat_loop, daemon=True)
        self.heartbeat_thread.start()
        
        self.process_thread = threading.Thread(target=self._process_loop, daemon=True)
        self.process_thread.start()
        
        print(f"[RelayClient] ✅ Connected to {self.config.server_url}")
        
        if self.on_connected:
            self.on_connected()
            
        return True
        
    def disconnect(self):
        """Disconnect from relay server"""
        self.is_running = False
        self.state = RelayClientState.DISCONNECTED
        
        if self.ws:
            self.ws.close()
            
        if self.session:
            self.session.close()
            
        print("[RelayClient] Disconnected")
        
        if self.on_disconnected:
            self.on_disconnected()
            
    def _test_connection(self) -> bool:
        """Test server connection"""
        if not REQUESTS_AVAILABLE:
            return False
            
        try:
            response = self.session.get(
                f"{self.config.server_url}/api/health",
                timeout=5
            )
            return response.status_code == 200
        except:
            return False
            
    def _connect_websocket(self):
        """Connect WebSocket for real-time notifications"""
        ws_url = self.config.server_url.replace('http://', 'ws://').replace('https://', 'wss://')
        ws_url = f"{ws_url}/ws"
        
        if self.config.auth_token:
            ws_url += f"?token={self.config.auth_token}"
            
        self.ws = websocket.WebSocketApp(
            ws_url,
            on_message=self._on_ws_message,
            on_error=self._on_ws_error,
            on_close=self._on_ws_close,
            on_open=self._on_ws_open
        )
        
        self.ws_thread = threading.Thread(target=self.ws.run_forever, daemon=True)
        self.ws_thread.start()
        
    def _on_ws_message(self, ws, message):
        """Handle WebSocket message"""
        try:
            data = json.loads(message)
            self.message_queue.put(data)
        except:
            pass
            
    def _on_ws_error(self, ws, error):
        """Handle WebSocket error"""
        print(f"[RelayClient] WebSocket error: {error}")
        
    def _on_ws_close(self, ws, close_status_code, close_msg):
        """Handle WebSocket close"""
        print(f"[RelayClient] WebSocket closed")
        
        # Reconnect if still running
        if self.is_running:
            time.sleep(self.config.reconnect_delay)
            self._connect_websocket()
            
    def _on_ws_open(self, ws):
        """Handle WebSocket open"""
        print("[RelayClient] WebSocket connected")
        
        # Subscribe to notifications
        subscribe_msg = {
            'type': 'subscribe',
            'device_id': self.config.device_id
        }
        ws.send(json.dumps(subscribe_msg))
        
    def _heartbeat_loop(self):
        """Send periodic heartbeat"""
        while self.is_running:
            time.sleep(self.config.heartbeat_interval)
            
            if self.state == RelayClientState.CONNECTED:
                self._send_heartbeat()
                
    def _send_heartbeat(self):
        """Send heartbeat to server"""
        if not REQUESTS_AVAILABLE:
            return
            
        try:
            self.session.post(
                f"{self.config.server_url}/api/heartbeat",
                json={'device_id': self.config.device_id},
                timeout=5
            )
        except:
            pass
            
    def _process_loop(self):
        """Process incoming messages"""
        while self.is_running:
            try:
                message = self.message_queue.get(timeout=1.0)
                self._process_message(message)
            except queue.Empty:
                continue
            except Exception as e:
                print(f"[RelayClient] Process error: {e}")
                
    def _process_message(self, message: Dict):
        """Process incoming message"""
        msg_type = message.get('type')
        
        if msg_type == 'transfer_update':
            self._handle_transfer_update(message)
        elif msg_type == 'notification':
            self._handle_notification(message)
        elif msg_type == 'transfer_request':
            self._handle_transfer_request(message)
            
    def _handle_transfer_update(self, message: Dict):
        """Handle transfer update"""
        data = message.get('data', {})
        transfer_id = data.get('transfer_id')
        
        with self.transfer_lock:
            if transfer_id in self.transfers:
                transfer = self.transfers[transfer_id]
                transfer.progress = data.get('progress', 0)
                transfer.chunks_uploaded = data.get('chunks_uploaded', 0)
                transfer.chunks_downloaded = data.get('chunks_downloaded', 0)
                
                if self.on_transfer_update:
                    self.on_transfer_update(transfer)
                    
    def _handle_notification(self, message: Dict):
        """Handle notification"""
        if self.on_notification:
            self.on_notification(message.get('data', {}))
            
    def _handle_transfer_request(self, message: Dict):
        """Handle incoming transfer request"""
        data = message.get('data', {})
        
        transfer = TransferInfo(
            transfer_id=data.get('transfer_id'),
            direction='download',
            filename=data.get('filename'),
            file_size=data.get('file_size'),
            total_chunks=data.get('total_chunks'),
            status=TransferStatus.PENDING,
            created_at=time.time(),
            expires_at=time.time() + 86400,  # 24 hours
            metadata=data.get('metadata', {})
        )
        
        with self.transfer_lock:
            self.transfers[transfer.transfer_id] = transfer
            
        if self.on_notification:
            self.on_notification({
                'type': 'transfer_request',
                'transfer': transfer
            })
            
    def authenticate(self, username: str, password: str = None) -> bool:
        """
        Authenticate with relay server
        
        Args:
            username: Username
            password: Optional password
            
        Returns:
            True if authenticated
        """
        if not REQUESTS_AVAILABLE:
            return False
            
        try:
            response = self.session.post(
                f"{self.config.server_url}/api/auth/login",
                json={
                    'username': username,
                    'password': password,
                    'device_id': self.config.device_id,
                    'device_name': self.config.device_name
                },
                timeout=self.config.timeout
            )
            
            if response.status_code == 200:
                data = response.json()
                self.config.auth_token = data.get('token')
                self.session.headers.update({
                    'Authorization': f'Bearer {self.config.auth_token}'
                })
                self.state = RelayClientState.AUTHENTICATED
                return True
                
        except Exception as e:
            print(f"[RelayClient] Auth error: {e}")
            
        return False
        
    def register_device(self, device_name: str) -> bool:
        """
        Register device with relay server
        
        Args:
            device_name: Display name for device
            
        Returns:
            True if registered
        """
        if not REQUESTS_AVAILABLE:
            return False
            
        try:
            response = self.session.post(
                f"{self.config.server_url}/api/device/register",
                json={
                    'device_name': device_name,
                    'device_id': self.config.device_id
                },
                timeout=self.config.timeout
            )
            
            if response.status_code == 200:
                data = response.json()
                self.config.device_id = data.get('device_id')
                return True
                
        except Exception as e:
            print(f"[RelayClient] Register error: {e}")
            
        return False
        
    def create_transfer(self, filename: str, file_size: int, 
                        total_chunks: int, metadata: Dict = None) -> Optional[str]:
        """
        Create a new transfer on relay server
        
        Args:
            filename: Name of file
            file_size: Total file size
            total_chunks: Number of chunks
            metadata: Additional metadata
            
        Returns:
            Transfer ID if created
        """
        if not REQUESTS_AVAILABLE:
            return None
            
        try:
            response = self.session.post(
                f"{self.config.server_url}/api/transfer/create",
                json={
                    'filename': filename,
                    'file_size': file_size,
                    'total_chunks': total_chunks,
                    'metadata': metadata or {}
                },
                timeout=self.config.timeout
            )
            
            if response.status_code == 200:
                data = response.json()
                transfer_id = data.get('transfer_id')
                
                # Store locally
                transfer = TransferInfo(
                    transfer_id=transfer_id,
                    direction='upload',
                    filename=filename,
                    file_size=file_size,
                    total_chunks=total_chunks,
                    status=TransferStatus.PENDING,
                    created_at=time.time(),
                    expires_at=time.time() + 86400,  # 24 hours
                    metadata=metadata or {}
                )
                
                with self.transfer_lock:
                    self.transfers[transfer_id] = transfer
                    
                return transfer_id
                
        except Exception as e:
            print(f"[RelayClient] Create transfer error: {e}")
            
        return None
        
    def upload_chunk(self, transfer_id: str, chunk_id: int, 
                     data: bytes) -> bool:
        """
        Upload a chunk to relay server
        
        Args:
            transfer_id: Transfer ID
            chunk_id: Chunk ID
            data: Encrypted chunk data
            
        Returns:
            True if uploaded successfully
        """
        if not REQUESTS_AVAILABLE:
            return False
            
        try:
            files = {
                'chunk': (f'chunk_{chunk_id}.enc', data, 'application/octet-stream')
            }
            
            response = self.session.post(
                f"{self.config.server_url}/api/transfer/{transfer_id}/chunk/{chunk_id}",
                files=files,
                timeout=self.config.timeout * 2  # Longer timeout for uploads
            )
            
            if response.status_code == 200:
                with self.transfer_lock:
                    if transfer_id in self.transfers:
                        transfer = self.transfers[transfer_id]
                        transfer.chunks_uploaded += 1
                        transfer.progress = (transfer.chunks_uploaded / transfer.total_chunks) * 100
                        
                return True
                
        except Exception as e:
            print(f"[RelayClient] Upload chunk {chunk_id} error: {e}")
            
        return False
        
    def download_chunk(self, transfer_id: str, chunk_id: int) -> Optional[bytes]:
        """
        Download a chunk from relay server
        
        Args:
            transfer_id: Transfer ID
            chunk_id: Chunk ID
            
        Returns:
            Chunk data or None
        """
        if not REQUESTS_AVAILABLE:
            return None
            
        try:
            response = self.session.get(
                f"{self.config.server_url}/api/transfer/{transfer_id}/chunk/{chunk_id}",
                timeout=self.config.timeout
            )
            
            if response.status_code == 200:
                with self.transfer_lock:
                    if transfer_id in self.transfers:
                        transfer = self.transfers[transfer_id]
                        transfer.chunks_downloaded += 1
                        transfer.progress = (transfer.chunks_downloaded / transfer.total_chunks) * 100
                        
                return response.content
                
        except Exception as e:
            print(f"[RelayClient] Download chunk {chunk_id} error: {e}")
            
        return None
        
    def upload_metadata(self, transfer_id: str, metadata: bytes) -> bool:
        """
        Upload encrypted metadata
        
        Args:
            transfer_id: Transfer ID
            metadata: Encrypted metadata
            
        Returns:
            True if uploaded
        """
        if not REQUESTS_AVAILABLE:
            return False
            
        try:
            response = self.session.post(
                f"{self.config.server_url}/api/transfer/{transfer_id}/metadata",
                data=metadata,
                headers={'Content-Type': 'application/octet-stream'},
                timeout=self.config.timeout
            )
            
            return response.status_code == 200
            
        except Exception as e:
            print(f"[RelayClient] Upload metadata error: {e}")
            return False
            
    def download_metadata(self, transfer_id: str) -> Optional[bytes]:
        """
        Download encrypted metadata
        
        Args:
            transfer_id: Transfer ID
            
        Returns:
            Encrypted metadata or None
        """
        if not REQUESTS_AVAILABLE:
            return None
            
        try:
            response = self.session.get(
                f"{self.config.server_url}/api/transfer/{transfer_id}/metadata",
                timeout=self.config.timeout
            )
            
            if response.status_code == 200:
                return response.content
                
        except Exception as e:
            print(f"[RelayClient] Download metadata error: {e}")
            
        return None
        
    def complete_transfer(self, transfer_id: str) -> bool:
        """
        Mark transfer as complete
        
        Args:
            transfer_id: Transfer ID
            
        Returns:
            True if marked complete
        """
        if not REQUESTS_AVAILABLE:
            return False
            
        try:
            response = self.session.post(
                f"{self.config.server_url}/api/transfer/{transfer_id}/complete",
                timeout=self.config.timeout
            )
            
            if response.status_code == 200:
                with self.transfer_lock:
                    if transfer_id in self.transfers:
                        self.transfers[transfer_id].status = TransferStatus.COMPLETED
                return True
                
        except Exception as e:
            print(f"[RelayClient] Complete transfer error: {e}")
            
        return False
        
    def cancel_transfer(self, transfer_id: str) -> bool:
        """
        Cancel a transfer
        
        Args:
            transfer_id: Transfer ID
            
        Returns:
            True if cancelled
        """
        if not REQUESTS_AVAILABLE:
            return False
            
        try:
            response = self.session.post(
                f"{self.config.server_url}/api/transfer/{transfer_id}/cancel",
                timeout=self.config.timeout
            )
            
            if response.status_code == 200:
                with self.transfer_lock:
                    if transfer_id in self.transfers:
                        self.transfers[transfer_id].status = TransferStatus.CANCELLED
                return True
                
        except Exception as e:
            print(f"[RelayClient] Cancel transfer error: {e}")
            
        return False
        
    def get_online_users(self) -> List[Dict]:
        """Get list of online users"""
        if not REQUESTS_AVAILABLE:
            return []
            
        try:
            response = self.session.get(
                f"{self.config.server_url}/api/users/online",
                timeout=self.config.timeout
            )
            
            if response.status_code == 200:
                return response.json().get('users', [])
                
        except:
            pass
            
        return []
        
    def generate_sharing_code(self, duration: int = 3600) -> Optional[str]:
        """
        Generate a sharing code
        
        Args:
            duration: Code validity in seconds
            
        Returns:
            Sharing code or None
        """
        if not REQUESTS_AVAILABLE:
            return None
            
        try:
            response = self.session.post(
                f"{self.config.server_url}/api/code/generate",
                json={'duration': duration},
                timeout=self.config.timeout
            )
            
            if response.status_code == 200:
                return response.json().get('code')
                
        except:
            pass
            
        return None
        
    def lookup_code(self, code: str) -> Optional[Dict]:
        """
        Look up a sharing code
        
        Args:
            code: Sharing code
            
        Returns:
            User info or None
        """
        if not REQUESTS_AVAILABLE:
            return None
            
        try:
            response = self.session.get(
                f"{self.config.server_url}/api/code/{code}",
                timeout=self.config.timeout
            )
            
            if response.status_code == 200:
                return response.json()
                
        except:
            pass
            
        return None
        
    def get_transfer(self, transfer_id: str) -> Optional[TransferInfo]:
        """Get transfer info by ID"""
        with self.transfer_lock:
            return self.transfers.get(transfer_id)
            
    def get_all_transfers(self) -> List[TransferInfo]:
        """Get all transfers"""
        with self.transfer_lock:
            return list(self.transfers.values())


# ============================================
# TEST FUNCTION
# ============================================

def test_relay_client():
    """Test relay client"""
    print("🌐 Relay Client Test")
    print("=" * 50)
    
    # Create config
    config = RelayConfig(
        server_url="http://localhost:5000",
        device_name="Test Device"
    )
    
    # Create client
    client = RelayClient(config)
    
    def on_connected():
        print("   ✅ Connected to relay server")
        
    def on_notification(data):
        print(f"   📨 Notification: {data.get('type')}")
        
    client.on_connected = on_connected
    client.on_notification = on_notification
    
    print("\n1️⃣ Testing connection...")
    
    # Mock connection test
    print("   Relay client ready!")
    print(f"   Server: {config.server_url}")
    print(f"   Device: {config.device_name}")
    
    print("\n2️⃣ Testing transfer creation...")
    
    # Mock transfer
    transfer = TransferInfo(
        transfer_id="test-123",
        direction="upload",
        filename="test.mp4",
        file_size=50 * 1024 * 1024,
        total_chunks=50,
        status=TransferStatus.PENDING,
        created_at=time.time(),
        expires_at=time.time() + 86400
    )
    
    print(f"   Transfer ID: {transfer.transfer_id}")
    print(f"   File: {transfer.filename}")
    print(f"   Chunks: {transfer.total_chunks}")
    print(f"   Expires: {transfer.get_remaining_time() / 3600:.1f} hours")
    
    print("\n3️⃣ Testing chunk operations...")
    print(f"   Upload chunk: POST /api/transfer/{{id}}/chunk/{{id}}")
    print(f"   Download chunk: GET /api/transfer/{{id}}/chunk/{{id}}")
    
    print("\n4️⃣ Testing sharing codes...")
    print(f"   Generate: POST /api/code/generate")
    print(f"   Lookup: GET /api/code/{{code}}")
    
    print("\n✅ Relay Client test complete!")


if __name__ == "__main__":
    test_relay_client()
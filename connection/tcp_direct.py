"""
connection/tcp_direct.py
Direct TCP connection handler for LAN P2P file transfer
Manages connection establishment, data exchange, and cleanup
"""

import socket
import threading
import json
import time
import struct
from typing import Callable, Optional, Dict, Any, Tuple
from dataclasses import dataclass
from enum import Enum

class ConnectionState(Enum):
    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    CONNECTED = "connected"
    HANDSHAKING = "handshaking"
    TRANSFERRING = "transferring"
    CLOSING = "closing"
    ERROR = "error"

class MessageType(Enum):
    HANDSHAKE = "handshake"
    HANDSHAKE_ACK = "handshake_ack"
    METADATA = "metadata"
    CHUNK = "chunk"
    ACK = "ack"
    BATCH_ACK = "batch_ack"
    REQUEST_CHUNK = "request_chunk"
    TRANSFER_COMPLETE = "transfer_complete"
    ERROR = "error"
    PING = "ping"
    PONG = "pong"
    DISCONNECT = "disconnect"

@dataclass
class ConnectionInfo:
    """Information about a TCP connection"""
    remote_ip: str
    remote_port: int
    local_port: int
    state: ConnectionState
    connected_at: float = 0.0
    bytes_sent: int = 0
    bytes_received: int = 0
    chunks_sent: int = 0
    chunks_received: int = 0
    
    def get_speed(self) -> Tuple[float, float]:
        """Get current upload/download speed in MB/s"""
        elapsed = time.time() - self.connected_at
        if elapsed == 0:
            return 0.0, 0.0
        return (self.bytes_sent / 1024 / 1024) / elapsed, (self.bytes_received / 1024 / 1024) / elapsed

class TCPConnection:
    """
    Handles a single TCP connection for P2P transfer
    """
    
    # Protocol constants
    HEADER_SIZE = 4  # 4 bytes for message length
    MAX_MESSAGE_SIZE = 100 * 1024 * 1024  # 100 MB max message
    SOCKET_TIMEOUT = 30.0  # 30 seconds
    KEEPALIVE_INTERVAL = 10  # Send ping every 10 seconds
    
    def __init__(self, socket_obj: socket.socket = None):
        """
        Initialize TCP connection
        
        Args:
            socket_obj: Existing socket (for server mode) or None (for client mode)
        """
        self.socket = socket_obj
        self.state = ConnectionState.DISCONNECTED
        
        # Connection info
        self.info: Optional[ConnectionInfo] = None
        self.remote_address = None
        
        # Threading
        self.is_running = False
        self.receive_thread = None
        self.keepalive_thread = None
        self.send_lock = threading.Lock()
        self.receive_lock = threading.Lock()
        
        # Message handlers
        self.message_handlers = {}
        self._setup_default_handlers()
        
        # Callbacks
        self.on_connected = None
        self.on_disconnected = None
        self.on_message = None
        self.on_error = None
        self.on_chunk_received = None
        self.on_ack_received = None
        
        # Statistics
        self.stats = {
            'messages_sent': 0,
            'messages_received': 0,
            'errors': 0,
            'retransmits': 0
        }
        
    def connect(self, host: str, port: int, timeout: float = 10.0) -> bool:
        """
        Connect to remote host (client mode)
        
        Args:
            host: Remote host IP
            port: Remote port
            timeout: Connection timeout
            
        Returns:
            True if connected successfully
        """
        try:
            self.state = ConnectionState.CONNECTING
            self.remote_address = (host, port)
            
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.settimeout(timeout)
            self.socket.connect((host, port))
            self.socket.settimeout(self.SOCKET_TIMEOUT)
            
            # Enable TCP_NODELAY for lower latency
            self.socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            
            # Enable keepalive
            self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            
            self.state = ConnectionState.CONNECTED
            self.info = ConnectionInfo(
                remote_ip=host,
                remote_port=port,
                local_port=self.socket.getsockname()[1],
                state=self.state,
                connected_at=time.time()
            )
            
            self.is_running = True
            
            # Start receive thread
            self.receive_thread = threading.Thread(target=self._receive_loop, daemon=True)
            self.receive_thread.start()
            
            # Start keepalive thread
            self.keepalive_thread = threading.Thread(target=self._keepalive_loop, daemon=True)
            self.keepalive_thread.start()
            
            print(f"[TCPConnection] ✅ Connected to {host}:{port}")
            
            if self.on_connected:
                self.on_connected(self.info)
                
            return True
            
        except Exception as e:
            self.state = ConnectionState.ERROR
            print(f"[TCPConnection] ❌ Connection failed: {e}")
            
            if self.on_error:
                self.on_error(str(e))
                
            return False
            
    def accept(self, timeout: float = None):
        """
        Accept connection (server mode - called by listener)
        
        Args:
            timeout: Optional accept timeout
        """
        if self.socket:
            self.socket.settimeout(timeout or self.SOCKET_TIMEOUT)
            
        self.state = ConnectionState.CONNECTED
        self.remote_address = self.socket.getpeername()
        
        self.info = ConnectionInfo(
            remote_ip=self.remote_address[0],
            remote_port=self.remote_address[1],
            local_port=self.socket.getsockname()[1],
            state=self.state,
            connected_at=time.time()
        )
        
        # Enable TCP_NODELAY
        self.socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        
        self.is_running = True
        
        # Start receive thread
        self.receive_thread = threading.Thread(target=self._receive_loop, daemon=True)
        self.receive_thread.start()
        
        # Start keepalive thread
        self.keepalive_thread = threading.Thread(target=self._keepalive_loop, daemon=True)
        self.keepalive_thread.start()
        
        print(f"[TCPConnection] ✅ Accepted connection from {self.remote_address[0]}:{self.remote_address[1]}")
        
        if self.on_connected:
            self.on_connected(self.info)
            
    def disconnect(self):
        """Close connection gracefully"""
        if self.state == ConnectionState.DISCONNECTED:
            return
            
        self.state = ConnectionState.CLOSING
        
        # Send disconnect message
        try:
            self._send_message(MessageType.DISCONNECT, {'reason': 'user_disconnect'})
        except:
            pass
            
        self.is_running = False
        
        if self.socket:
            try:
                self.socket.shutdown(socket.SHUT_RDWR)
                self.socket.close()
            except:
                pass
                
        self.state = ConnectionState.DISCONNECTED
        
        print(f"[TCPConnection] Disconnected from {self.remote_address}")
        
        if self.on_disconnected:
            self.on_disconnected()
            
    def send_message(self, msg_type: MessageType, data: Any) -> bool:
        """
        Send a message
        
        Args:
            msg_type: Type of message
            data: Message data (will be JSON serialized)
            
        Returns:
            True if sent successfully
        """
        if not self.is_connected():
            return False
            
        message = {
            'type': msg_type.value,
            'timestamp': time.time(),
            'data': data
        }
        
        return self._send_message(msg_type, data)
        
    def _send_message(self, msg_type: MessageType, data: Any) -> bool:
        """Internal send method"""
        with self.send_lock:
            try:
                message = {
                    'type': msg_type.value,
                    'timestamp': time.time(),
                    'data': data
                }
                
                message_json = json.dumps(message).encode('utf-8')
                
                # Send header (length)
                self.socket.send(len(message_json).to_bytes(self.HEADER_SIZE, 'big'))
                
                # Send body
                self.socket.send(message_json)
                
                self.stats['messages_sent'] += 1
                
                if self.info:
                    self.info.bytes_sent += len(message_json) + self.HEADER_SIZE
                    
                return True
                
            except Exception as e:
                print(f"[TCPConnection] Send error: {e}")
                self.stats['errors'] += 1
                return False
                
    def send_chunk(self, chunk_id: int, encrypted_data: bytes, 
                   metadata: Dict = None) -> bool:
        """
        Send a file chunk
        
        Args:
            chunk_id: Chunk ID
            encrypted_data: Encrypted chunk data
            metadata: Additional metadata
            
        Returns:
            True if sent successfully
        """
        if not self.is_connected():
            return False
            
        with self.send_lock:
            try:
                # Create chunk header
                header = {
                    'type': MessageType.CHUNK.value,
                    'chunk_id': chunk_id,
                    'size': len(encrypted_data),
                    'timestamp': time.time()
                }
                
                if metadata:
                    header['metadata'] = metadata
                    
                header_json = json.dumps(header).encode('utf-8')
                
                # Send header
                self.socket.send(len(header_json).to_bytes(self.HEADER_SIZE, 'big'))
                self.socket.send(header_json)
                
                # Send chunk data
                self.socket.send(len(encrypted_data).to_bytes(self.HEADER_SIZE, 'big'))
                self.socket.send(encrypted_data)
                
                if self.info:
                    self.info.bytes_sent += len(header_json) + len(encrypted_data) + (self.HEADER_SIZE * 2)
                    self.info.chunks_sent += 1
                    
                return True
                
            except Exception as e:
                print(f"[TCPConnection] Chunk send error: {e}")
                self.stats['errors'] += 1
                return False
                
    def send_ack(self, chunk_id: int, transfer_id: str = None) -> bool:
        """
        Send acknowledgment for received chunk
        
        Args:
            chunk_id: Acknowledged chunk ID
            transfer_id: Optional transfer ID
            
        Returns:
            True if sent successfully
        """
        data = {
            'chunk_id': chunk_id,
            'transfer_id': transfer_id
        }
        
        return self.send_message(MessageType.ACK, data)
        
    def send_batch_ack(self, chunk_ids: list, transfer_id: str = None) -> bool:
        """
        Send batch acknowledgment
        
        Args:
            chunk_ids: List of acknowledged chunk IDs
            transfer_id: Optional transfer ID
            
        Returns:
            True if sent successfully
        """
        data = {
            'chunk_ids': chunk_ids,
            'count': len(chunk_ids),
            'transfer_id': transfer_id
        }
        
        return self.send_message(MessageType.BATCH_ACK, data)
        
    def request_chunk(self, chunk_id: int, transfer_id: str = None) -> bool:
        """
        Request a specific chunk (for missing chunks)
        
        Args:
            chunk_id: Requested chunk ID
            transfer_id: Optional transfer ID
            
        Returns:
            True if request sent
        """
        data = {
            'chunk_id': chunk_id,
            'transfer_id': transfer_id
        }
        
        return self.send_message(MessageType.REQUEST_CHUNK, data)
        
    def _receive_loop(self):
        """Main receive loop"""
        while self.is_running and self.is_connected():
            try:
                message = self._receive_message()
                
                if message:
                    self._handle_message(message)
                else:
                    # Connection closed
                    break
                    
            except socket.timeout:
                continue
            except Exception as e:
                print(f"[TCPConnection] Receive error: {e}")
                self.stats['errors'] += 1
                break
                
        # Connection lost
        if self.is_running:
            self.disconnect()
            
    def _receive_message(self) -> Optional[Dict]:
        """Receive a single message"""
        with self.receive_lock:
            try:
                # Read header
                header_data = self._recv_exact(self.HEADER_SIZE)
                if not header_data:
                    return None
                    
                msg_length = int.from_bytes(header_data, 'big')
                
                if msg_length > self.MAX_MESSAGE_SIZE:
                    print(f"[TCPConnection] Message too large: {msg_length}")
                    return None
                    
                # Read body
                body_data = self._recv_exact(msg_length)
                if not body_data:
                    return None
                    
                self.stats['messages_received'] += 1
                
                if self.info:
                    self.info.bytes_received += len(body_data) + self.HEADER_SIZE
                    
                return json.loads(body_data.decode('utf-8'))
                
            except Exception as e:
                print(f"[TCPConnection] Message receive error: {e}")
                return None
                
    def _recv_exact(self, size: int) -> Optional[bytes]:
        """Receive exactly size bytes"""
        data = b''
        
        while len(data) < size:
            try:
                chunk = self.socket.recv(min(4096, size - len(data)))
                if not chunk:
                    return None
                data += chunk
            except socket.timeout:
                continue
                
        return data
        
    def _receive_chunk_data(self, expected_size: int) -> Optional[bytes]:
        """Receive chunk data after header"""
        size_data = self._recv_exact(self.HEADER_SIZE)
        if not size_data:
            return None
            
        chunk_size = int.from_bytes(size_data, 'big')
        
        if chunk_size != expected_size:
            print(f"[TCPConnection] Chunk size mismatch: expected {expected_size}, got {chunk_size}")
            return None
            
        chunk_data = self._recv_exact(chunk_size)
        
        if chunk_data and self.info:
            self.info.bytes_received += chunk_size
            self.info.chunks_received += 1
            
        return chunk_data
        
    def _handle_message(self, message: Dict):
        """Handle incoming message"""
        msg_type = message.get('type')
        
        if self.on_message:
            self.on_message(message)
            
        # Route to specific handler
        handler = self.message_handlers.get(msg_type)
        if handler:
            handler(message)
            
    def _setup_default_handlers(self):
        """Setup default message handlers"""
        self.message_handlers = {
            MessageType.PING.value: self._handle_ping,
            MessageType.PONG.value: self._handle_pong,
            MessageType.DISCONNECT.value: self._handle_disconnect,
            MessageType.CHUNK.value: self._handle_chunk,
            MessageType.ACK.value: self._handle_ack,
            MessageType.BATCH_ACK.value: self._handle_batch_ack,
        }
        
    def _handle_ping(self, message: Dict):
        """Handle ping - respond with pong"""
        self.send_message(MessageType.PONG, {'ping_time': message.get('timestamp')})
        
    def _handle_pong(self, message: Dict):
        """Handle pong response"""
        pass  # Just for keepalive
        
    def _handle_disconnect(self, message: Dict):
        """Handle disconnect message"""
        print(f"[TCPConnection] Peer requested disconnect: {message.get('data', {}).get('reason')}")
        self.disconnect()
        
    def _handle_chunk(self, message: Dict):
        """Handle incoming chunk"""
        data = message.get('data', {})
        chunk_id = data.get('chunk_id')
        chunk_size = data.get('size')
        
        # Receive actual chunk data
        chunk_data = self._receive_chunk_data(chunk_size)
        
        if chunk_data and self.on_chunk_received:
            self.on_chunk_received(chunk_id, chunk_data, data)
            
    def _handle_ack(self, message: Dict):
        """Handle ACK message"""
        data = message.get('data', {})
        chunk_id = data.get('chunk_id')
        
        if self.on_ack_received:
            self.on_ack_received(chunk_id)
            
    def _handle_batch_ack(self, message: Dict):
        """Handle batch ACK message"""
        data = message.get('data', {})
        chunk_ids = data.get('chunk_ids', [])
        
        if self.on_ack_received:
            for chunk_id in chunk_ids:
                self.on_ack_received(chunk_id)
                
    def _keepalive_loop(self):
        """Send periodic ping to keep connection alive"""
        while self.is_running and self.is_connected():
            time.sleep(self.KEEPALIVE_INTERVAL)
            
            if self.is_connected():
                self.send_message(MessageType.PING, {})
                
    def is_connected(self) -> bool:
        """Check if connection is active"""
        return self.state == ConnectionState.CONNECTED
        
    def get_stats(self) -> Dict:
        """Get connection statistics"""
        stats = self.stats.copy()
        
        if self.info:
            upload_speed, download_speed = self.info.get_speed()
            stats.update({
                'bytes_sent': self.info.bytes_sent,
                'bytes_received': self.info.bytes_received,
                'chunks_sent': self.info.chunks_sent,
                'chunks_received': self.info.chunks_received,
                'upload_speed_mbps': upload_speed,
                'download_speed_mbps': download_speed,
                'connection_time': time.time() - self.info.connected_at if self.info.connected_at else 0
            })
            
        return stats


class TCPConnectionManager:
    """
    Manages multiple TCP connections
    """
    
    def __init__(self):
        self.connections: Dict[str, TCPConnection] = {}
        self.lock = threading.Lock()
        
    def create_connection(self, connection_id: str = None) -> TCPConnection:
        """
        Create a new connection
        
        Args:
            connection_id: Optional connection ID
            
        Returns:
            New TCPConnection instance
        """
        conn = TCPConnection()
        
        if connection_id:
            with self.lock:
                self.connections[connection_id] = conn
                
        return conn
        
    def add_connection(self, connection_id: str, conn: TCPConnection):
        """Add existing connection to manager"""
        with self.lock:
            self.connections[connection_id] = conn
            
    def get_connection(self, connection_id: str) -> Optional[TCPConnection]:
        """Get connection by ID"""
        with self.lock:
            return self.connections.get(connection_id)
            
    def remove_connection(self, connection_id: str):
        """Remove and disconnect a connection"""
        with self.lock:
            conn = self.connections.pop(connection_id, None)
            
        if conn:
            conn.disconnect()
            
    def disconnect_all(self):
        """Disconnect all connections"""
        with self.lock:
            for conn in self.connections.values():
                conn.disconnect()
            self.connections.clear()
            
    def get_all_stats(self) -> Dict:
        """Get statistics for all connections"""
        stats = {}
        
        with self.lock:
            for conn_id, conn in self.connections.items():
                stats[conn_id] = conn.get_stats()
                
        return stats


# ============================================
# TEST FUNCTION
# ============================================

def test_tcp_connection():
    """Test TCP connection"""
    print("🔌 TCP Connection Test")
    print("=" * 50)
    
    import threading
    import time
    
    # Create server socket for testing
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind(('localhost', 9999))
    server_socket.listen(1)
    
    print("\n1️⃣ Server listening on localhost:9999")
    
    # Server accept thread
    def server_thread():
        client_sock, addr = server_socket.accept()
        print(f"\n   Server accepted connection from {addr}")
        
        server_conn = TCPConnection(client_sock)
        server_conn.accept()
        
        # Wait for messages
        time.sleep(5)
        server_conn.disconnect()
        
    thread = threading.Thread(target=server_thread, daemon=True)
    thread.start()
    
    # Client connect
    print("\n2️⃣ Client connecting...")
    time.sleep(0.5)
    
    client_conn = TCPConnection()
    
    def on_connected(info):
        print(f"   ✅ Client connected to {info.remote_ip}:{info.remote_port}")
        
    def on_message(message):
        print(f"   📨 Received: {message.get('type')}")
        
    client_conn.on_connected = on_connected
    client_conn.on_message = on_message
    
    if client_conn.connect('localhost', 9999):
        print("\n3️⃣ Sending test messages...")
        
        # Send test messages
        client_conn.send_message(MessageType.HANDSHAKE, {'version': '1.0', 'device': 'test'})
        time.sleep(1)
        
        # Send test chunk
        test_data = b"Test chunk data" * 100
        client_conn.send_chunk(1, test_data, {'test': 'metadata'})
        time.sleep(1)
        
        # Send ACK
        client_conn.send_ack(1, 'test-transfer')
        time.sleep(1)
        
        # Get stats
        stats = client_conn.get_stats()
        print(f"\n4️⃣ Connection stats:")
        print(f"   Messages sent: {stats['messages_sent']}")
        print(f"   Bytes sent: {stats.get('bytes_sent', 0)}")
        print(f"   Chunks sent: {stats.get('chunks_sent', 0)}")
        
        client_conn.disconnect()
        
    server_socket.close()
    
    print("\n✅ TCP Connection test complete!")


if __name__ == "__main__":
    test_tcp_connection()
"""
receiver/listener.py
Background listener for incoming transfer requests
Handles both LAN (TCP) and WAN (WebSocket) connections
"""

import socket
import threading
import json
import time
import queue
from typing import Callable, Optional, Dict, Any
from dataclasses import dataclass, asdict
from enum import Enum

# Try to import WebSocket for WAN
try:
    import websocket
    WEBSOCKET_AVAILABLE = True
except ImportError:
    WEBSOCKET_AVAILABLE = False
    print("[Listener] ⚠️ websocket-client not found. Install: pip install websocket-client")

class TransferType(Enum):
    LAN = "lan"
    WAN = "wan"
    UNKNOWN = "unknown"

class RequestStatus(Enum):
    PENDING = "pending"
    ACCEPTED = "accepted"
    REJECTED = "rejected"
    TIMEOUT = "timeout"
    CANCELLED = "cancelled"

@dataclass
class IncomingTransfer:
    """Represents an incoming transfer request"""
    transfer_id: str
    sender_id: str
    sender_name: str
    sender_ip: str
    transfer_type: TransferType
    filename: str
    file_size: int
    file_type: str
    total_chunks: int = 0
    status: RequestStatus = RequestStatus.PENDING
    received_at: float = 0
    expires_at: float = 0
    metadata: Dict[str, Any] = None
    
    def __post_init__(self):
        self.received_at = time.time()
        if self.transfer_type == TransferType.LAN:
            self.expires_at = self.received_at + 600  # 10 minutes
        else:
            self.expires_at = self.received_at + 86400  # 24 hours
            
    def is_expired(self) -> bool:
        return time.time() > self.expires_at
        
    def get_time_remaining(self) -> int:
        return max(0, int(self.expires_at - time.time()))
        
    def format_size(self) -> str:
        size = self.file_size
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if size < 1024:
                return f"{size:.1f} {unit}"
            size /= 1024
        return f"{size:.1f} PB"
        
    def to_dict(self) -> dict:
        return {
            'transfer_id': self.transfer_id,
            'sender_id': self.sender_id,
            'sender_name': self.sender_name,
            'sender_ip': self.sender_ip,
            'transfer_type': self.transfer_type.value,
            'filename': self.filename,
            'file_size': self.file_size,
            'file_type': self.file_type,
            'total_chunks': self.total_chunks,
            'status': self.status.value,
            'received_at': self.received_at,
            'expires_at': self.expires_at
        }

class LANListener:
    """TCP listener for LAN transfer requests"""
    
    def __init__(self, port: int = 8888):
        self.port = port
        self.server_socket = None
        self.is_running = False
        self.listen_thread = None
        self.pending_connections = queue.Queue()
        self.active_connections: Dict[str, socket.socket] = {}
        
    def start(self):
        if self.is_running:
            return
            
        try:
            self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.server_socket.bind(('0.0.0.0', self.port))
            self.server_socket.listen(5)
            self.server_socket.settimeout(1.0)
            
            self.is_running = True
            self.listen_thread = threading.Thread(target=self._listen_loop, daemon=True)
            self.listen_thread.start()
            
            print(f"[LANListener] ✅ Listening on port {self.port}")
        except Exception as e:
            print(f"[LANListener] ❌ Failed to start: {e}")
            
    def stop(self):
        self.is_running = False
        if self.server_socket:
            self.server_socket.close()
        for conn in self.active_connections.values():
            try:
                conn.close()
            except:
                pass
        self.active_connections.clear()
        print("[LANListener] Stopped")
        
    def _listen_loop(self):
        while self.is_running:
            try:
                client_socket, client_address = self.server_socket.accept()
                thread = threading.Thread(
                    target=self._handle_connection,
                    args=(client_socket, client_address),
                    daemon=True
                )
                thread.start()
            except socket.timeout:
                continue
            except Exception as e:
                if self.is_running:
                    print(f"[LANListener] Accept error: {e}")
                    
    def _handle_connection(self, client_socket: socket.socket, client_address: tuple):
        client_ip = client_address[0]
        try:
            client_socket.settimeout(30.0)
            data = self._receive_json(client_socket)
            
            if data and data.get('type') == 'transfer_request':
                transfer = self._parse_transfer_request(data, client_ip)
                if transfer:
                    self.active_connections[transfer.transfer_id] = client_socket
                    self.pending_connections.put((transfer, client_socket))
                    print(f"[LANListener] 📨 Incoming transfer from {transfer.sender_name}")
        except socket.timeout:
            pass
        except Exception as e:
            print(f"[LANListener] Connection error: {e}")
            
    def _receive_json(self, sock: socket.socket) -> Optional[dict]:
        try:
            length_data = sock.recv(4)
            if len(length_data) < 4:
                return None
            msg_length = int.from_bytes(length_data, 'big')
            data = b''
            while len(data) < msg_length:
                chunk = sock.recv(min(4096, msg_length - len(data)))
                if not chunk:
                    return None
                data += chunk
            return json.loads(data.decode('utf-8'))
        except:
            return None
            
    def _parse_transfer_request(self, data: dict, client_ip: str) -> Optional[IncomingTransfer]:
        try:
            return IncomingTransfer(
                transfer_id=data.get('transfer_id', ''),
                sender_id=data.get('sender_id', ''),
                sender_name=data.get('sender_name', 'Unknown'),
                sender_ip=client_ip,
                transfer_type=TransferType.LAN,
                filename=data.get('filename', 'unknown'),
                file_size=data.get('file_size', 0),
                file_type=data.get('file_type', ''),
                total_chunks=data.get('total_chunks', 0),
                metadata=data.get('metadata', {})
            )
        except:
            return None
            
    def send_response(self, transfer_id: str, accepted: bool) -> bool:
        conn = self.active_connections.get(transfer_id)
        if not conn:
            return False
        try:
            response = {
                'type': 'transfer_response',
                'transfer_id': transfer_id,
                'accepted': accepted,
                'timestamp': time.time()
            }
            response_json = json.dumps(response).encode('utf-8')
            conn.send(len(response_json).to_bytes(4, 'big'))
            conn.send(response_json)
            if not accepted:
                conn.close()
                del self.active_connections[transfer_id]
            return True
        except:
            return False
            
    def get_pending_transfer(self, timeout: float = 1.0) -> Optional[tuple]:
        try:
            return self.pending_connections.get(timeout=timeout)
        except queue.Empty:
            return None


class WANListener:
    """WebSocket listener for WAN transfer notifications"""
    
    def __init__(self, server_url: str, auth_token: str = None):
        self.server_url = server_url
        self.auth_token = auth_token
        self.ws = None
        self.is_running = False
        self.ws_thread = None
        self.pending_transfers = queue.Queue()
        self.on_transfer_received = None
        
        # Error rate limiting
        self._last_error_time = 0
        self._error_count = 0
        
    def start(self):
        if not WEBSOCKET_AVAILABLE:
            print("[WANListener] ❌ WebSocket not available")
            return
            
        self.is_running = True
        self.ws_thread = threading.Thread(target=self._ws_loop, daemon=True)
        self.ws_thread.start()
        
    def stop(self):
        self.is_running = False
        if self.ws:
            self.ws.close()
        print("[WANListener] Stopped")
        
    def _ws_loop(self):
        while self.is_running:
            try:
                ws_url = self.server_url.replace('http://', 'ws://').replace('https://', 'wss://')
                ws_url = f"{ws_url}/ws"
                
                if self.auth_token:
                    ws_url += f"?token={self.auth_token}"
                    
                self.ws = websocket.WebSocketApp(
                    ws_url,
                    on_message=self._on_message,
                    on_error=self._on_error,
                    on_close=self._on_close,
                    on_open=self._on_open
                )
                
                self.ws.run_forever()
                
            except Exception as e:
                self._log_error(f"WebSocket error: {e}")
                
            # Reconnect after delay (longer to reduce spam)
            if self.is_running:
                time.sleep(10)
                
    def _log_error(self, msg: str):
        """Rate-limited error logging"""
        now = time.time()
        self._error_count += 1
        
        # Only print first error, then every 30 seconds
        if self._error_count == 1 or now - self._last_error_time > 30:
            print(f"[WANListener] {msg}")
            self._last_error_time = now
            
    def _on_message(self, ws, message):
        try:
            data = json.loads(message)
            msg_type = data.get('type', '')
            
            if msg_type == 'connected':
                print(f"[WANListener] ✅ Connected to relay server")
            elif msg_type == 'subscribed':
                print(f"[WANListener] ✅ Subscribed for notifications")
            elif msg_type == 'transfer_request':
                transfer = self._parse_wan_request(data)
                if transfer:
                    self.pending_transfers.put(transfer)
                    if self.on_transfer_received:
                        self.on_transfer_received(transfer)
                    print(f"[WANListener] 📨 Incoming WAN transfer: {transfer.filename}")
            elif msg_type in ['pong', 'heartbeat_ack']:
                pass  # Silent
                    
        except Exception as e:
            pass
            
    def _on_error(self, ws, error):
        self._log_error(f"WebSocket error: {str(error)[:100]}")
        
    def _on_close(self, ws, close_status_code, close_msg):
        self._log_error("WebSocket closed")
        
    def _on_open(self, ws):
        self._error_count = 0  # Reset error count on successful connection
        print(f"[WANListener] ✅ Connected to relay server")
        
        # Subscribe to notifications
        subscribe_msg = {
            'type': 'subscribe',
            'auth_token': self.auth_token,
            'device_id': self.auth_token[:12] if self.auth_token else 'unknown'
        }
        ws.send(json.dumps(subscribe_msg))
        
    def _parse_wan_request(self, data: dict) -> Optional[IncomingTransfer]:
        try:
            return IncomingTransfer(
                transfer_id=data.get('transfer_id', ''),
                sender_id=data.get('sender_id', ''),
                sender_name=data.get('sender_name', 'Unknown'),
                sender_ip=data.get('sender_ip', ''),
                transfer_type=TransferType.WAN,
                filename=data.get('filename', 'unknown'),
                file_size=data.get('file_size', 0),
                file_type=data.get('file_type', ''),
                total_chunks=data.get('total_chunks', 0),
                metadata=data.get('metadata', {})
            )
        except:
            return None
            
    def get_pending_transfer(self, timeout: float = 1.0) -> Optional[IncomingTransfer]:
        try:
            return self.pending_transfers.get(timeout=timeout)
        except queue.Empty:
            return None


class ReceiverListener:
    """Unified listener for both LAN and WAN transfers"""
    
    def __init__(self, server_url: str = None, auth_token: str = None):
        self.lan_listener = LANListener()
        self.wan_listener = None
        if server_url and server_url.startswith('http'):
            self.wan_listener = WANListener(server_url, auth_token)
            
        self.on_transfer_request = None
        self.is_running = False
        self.process_thread = None
        self.active_transfers: Dict[str, IncomingTransfer] = {}
        self.lock = threading.Lock()
        
    def start(self, on_transfer_request: Callable = None):
        self.on_transfer_request = on_transfer_request
        self.is_running = True
        
        self.lan_listener.start()
        
        if self.wan_listener:
            self.wan_listener.on_transfer_received = self._handle_wan_transfer
            self.wan_listener.start()
            
        self.process_thread = threading.Thread(target=self._process_loop, daemon=True)
        self.process_thread.start()
        
        print("[ReceiverListener] ✅ Started")
        print(f"                  LAN: Active on port 8888")
        if self.wan_listener:
            print(f"                  WAN: WebSocket active")
            
    def stop(self):
        self.is_running = False
        self.lan_listener.stop()
        if self.wan_listener:
            self.wan_listener.stop()
        print("[ReceiverListener] Stopped")
        
    def _process_loop(self):
        while self.is_running:
            try:
                lan_result = self.lan_listener.get_pending_transfer(timeout=0.5)
                if lan_result:
                    transfer, conn = lan_result
                    self._handle_transfer(transfer)
                    
                if self.wan_listener:
                    wan_transfer = self.wan_listener.get_pending_transfer(timeout=0.5)
                    if wan_transfer:
                        self._handle_transfer(wan_transfer)
            except:
                pass
                
    def _handle_transfer(self, transfer: IncomingTransfer):
        with self.lock:
            if transfer.is_expired():
                return
            self.active_transfers[transfer.transfer_id] = transfer
            
        if self.on_transfer_request:
            self.on_transfer_request(transfer)
            
    def _handle_wan_transfer(self, transfer: IncomingTransfer):
        self._handle_transfer(transfer)
        
    def accept_transfer(self, transfer_id: str) -> bool:
        with self.lock:
            transfer = self.active_transfers.get(transfer_id)
            if not transfer:
                return False
            transfer.status = RequestStatus.ACCEPTED
            if transfer.transfer_type == TransferType.LAN:
                return self.lan_listener.send_response(transfer_id, True)
            return True
                
    def reject_transfer(self, transfer_id: str) -> bool:
        with self.lock:
            transfer = self.active_transfers.get(transfer_id)
            if not transfer:
                return False
            transfer.status = RequestStatus.REJECTED
            if transfer.transfer_type == TransferType.LAN:
                return self.lan_listener.send_response(transfer_id, False)
            del self.active_transfers[transfer_id]
            return True
            
    def get_active_transfers(self) -> list:
        with self.lock:
            return list(self.active_transfers.values())
            
    def get_transfer(self, transfer_id: str) -> Optional[IncomingTransfer]:
        with self.lock:
            return self.active_transfers.get(transfer_id)
            
    def cleanup_expired(self):
        with self.lock:
            expired = [tid for tid, t in self.active_transfers.items() if t.is_expired()]
            for tid in expired:
                del self.active_transfers[tid]
            return len(expired)


# ============================================
# TEST FUNCTION
# ============================================

def test_listener():
    print("📡 Receiver Listener Test")
    print("=" * 50)
    
    def on_transfer(transfer: IncomingTransfer):
        print(f"\n📨 INCOMING: {transfer.filename} from {transfer.sender_name}")
        
    listener = ReceiverListener()
    listener.start(on_transfer_request=on_transfer)
    
    print("\n✅ Listener running!")
    print("   Ctrl+C to exit\n")
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nStopping...")
        listener.stop()


if __name__ == "__main__":
    test_listener()
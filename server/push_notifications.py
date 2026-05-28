"""
server/push_notifications.py
Real-time push notifications for Palmsync
WebSocket-based notifications for incoming transfers and status updates
"""

import json
import time
import threading
import queue
from typing import Dict, List, Optional, Any, Set
from dataclasses import dataclass, field
from enum import Enum

class NotificationType(Enum):
    """Types of notifications"""
    TRANSFER_REQUEST = "transfer_request"      # Someone wants to send a file
    TRANSFER_ACCEPTED = "transfer_accepted"    # Recipient accepted
    TRANSFER_REJECTED = "transfer_rejected"    # Recipient rejected
    TRANSFER_COMPLETE = "transfer_complete"    # Transfer finished
    TRANSFER_EXPIRED = "transfer_expired"      # Transfer timed out
    CHUNK_ACK = "chunk_ack"                    # Chunk acknowledged
    USER_ONLINE = "user_online"                # Contact came online
    USER_OFFLINE = "user_offline"              # Contact went offline
    CONTACT_JOINED = "contact_joined"          # New contact joined Palmsync
    FILE_RECEIVED = "file_received"            # File download complete

class NotificationPriority(Enum):
    LOW = "low"
    NORMAL = "normal"
    HIGH = "high"
    URGENT = "urgent"

@dataclass
class Notification:
    """Notification message"""
    notification_id: str
    type: NotificationType
    priority: NotificationPriority
    recipient_id: str
    sender_id: Optional[str]
    title: str
    body: str
    data: Dict[str, Any] = field(default_factory=dict)
    created_at: float = field(default_factory=time.time)
    expires_at: Optional[float] = None
    is_read: bool = False
    delivered: bool = False
    
    def to_dict(self) -> dict:
        return {
            'notification_id': self.notification_id,
            'type': self.type.value,
            'priority': self.priority.value,
            'title': self.title,
            'body': self.body,
            'data': self.data,
            'created_at': self.created_at,
            'is_read': self.is_read
        }
    
    def to_json(self) -> str:
        return json.dumps(self.to_dict())

@dataclass
class TransferRequestNotification:
    """Specialized notification for transfer requests"""
    transfer_id: str
    sender_id: str
    sender_name: str
    filename: str
    file_size: int
    file_type: str
    total_chunks: int
    transfer_type: str  # 'lan' or 'wan'
    expires_at: float
    
    def to_notification(self, recipient_id: str) -> Notification:
        import uuid
        
        size_str = self._format_size(self.file_size)
        
        return Notification(
            notification_id=str(uuid.uuid4()),
            type=NotificationType.TRANSFER_REQUEST,
            priority=NotificationPriority.HIGH,
            recipient_id=recipient_id,
            sender_id=self.sender_id,
            title=f"📨 {self.sender_name} wants to send a file",
            body=f"{self.filename} ({size_str})",
            data={
                'transfer_id': self.transfer_id,
                'sender_id': self.sender_id,
                'sender_name': self.sender_name,
                'filename': self.filename,
                'file_size': self.file_size,
                'file_type': self.file_type,
                'total_chunks': self.total_chunks,
                'transfer_type': self.transfer_type,
                'expires_at': self.expires_at
            },
            expires_at=self.expires_at
        )
    
    def _format_size(self, size: int) -> str:
        for unit in ['B', 'KB', 'MB', 'GB']:
            if size < 1024:
                return f"{size:.1f} {unit}"
            size /= 1024
        return f"{size:.1f} TB"

class NotificationManager:
    """
    Manages push notifications for Palmsync users
    Handles WebSocket connections and notification delivery
    """
    
    def __init__(self):
        # WebSocket connections: user_id -> WebSocket
        self.connections: Dict[str, Any] = {}
        self.connection_lock = threading.Lock()
        
        # Pending notifications (for offline users)
        self.pending_notifications: Dict[str, List[Notification]] = {}
        self.pending_lock = threading.Lock()
        
        # Notification queue for async processing
        self.notification_queue = queue.Queue()
        
        # User devices (multiple devices per user)
        self.user_devices: Dict[str, Set[str]] = {}
        self.device_lock = threading.Lock()
        
        # Background worker
        self.is_running = True
        self.worker_thread = threading.Thread(target=self._process_queue, daemon=True)
        self.worker_thread.start()
        
        # Callbacks
        self.on_notification_sent = None
        self.on_notification_failed = None
        
        # Stats
        self.stats = {
            'notifications_sent': 0,
            'notifications_failed': 0,
            'notifications_pending': 0,
            'active_connections': 0
        }
        
    def register_connection(self, user_id: str, device_id: str, websocket) -> bool:
        """
        Register a WebSocket connection for a user
        
        Args:
            user_id: User ID
            device_id: Device identifier
            websocket: WebSocket connection object
            
        Returns:
            True if registered successfully
        """
        with self.connection_lock:
            self.connections[user_id] = websocket
            
        with self.device_lock:
            if user_id not in self.user_devices:
                self.user_devices[user_id] = set()
            self.user_devices[user_id].add(device_id)
            
        self.stats['active_connections'] = len(self.connections)
        
        print(f"[Notifications] ✅ User {user_id[:8]}... connected (device: {device_id})")
        
        # Send any pending notifications
        self._deliver_pending_notifications(user_id)
        
        return True
        
    def unregister_connection(self, user_id: str, device_id: str = None):
        """
        Unregister a WebSocket connection
        
        Args:
            user_id: User ID
            device_id: Optional device ID to remove
        """
        with self.connection_lock:
            if user_id in self.connections:
                del self.connections[user_id]
                
        with self.device_lock:
            if user_id in self.user_devices:
                if device_id:
                    self.user_devices[user_id].discard(device_id)
                if not self.user_devices[user_id]:
                    del self.user_devices[user_id]
                    
        self.stats['active_connections'] = len(self.connections)
        
        print(f"[Notifications] User {user_id[:8]}... disconnected")
        
    def is_user_online(self, user_id: str) -> bool:
        """Check if user has active WebSocket connection"""
        with self.connection_lock:
            return user_id in self.connections
        
    def get_online_users(self) -> List[str]:
        """Get list of online user IDs"""
        with self.connection_lock:
            return list(self.connections.keys())
        
    def send_notification(self, notification: Notification) -> bool:
        """
        Send a notification to a user
        
        Args:
            notification: Notification object
            
        Returns:
            True if queued for delivery
        """
        self.notification_queue.put(notification)
        return True
        
    def send_transfer_request(self, recipient_id: str, sender_id: str,
                              sender_name: str, transfer_id: str,
                              filename: str, file_size: int,
                              file_type: str, total_chunks: int,
                              transfer_type: str = 'wan') -> bool:
        """
        Send a transfer request notification
        
        Args:
            recipient_id: Who receives the notification
            sender_id: Who sent the file
            sender_name: Display name of sender
            transfer_id: Transfer ID
            filename: Name of file
            file_size: Size in bytes
            file_type: MIME type
            total_chunks: Number of chunks
            transfer_type: 'lan' or 'wan'
            
        Returns:
            True if notification queued
        """
        # Create transfer request notification
        req = TransferRequestNotification(
            transfer_id=transfer_id,
            sender_id=sender_id,
            sender_name=sender_name,
            filename=filename,
            file_size=file_size,
            file_type=file_type,
            total_chunks=total_chunks,
            transfer_type=transfer_type,
            expires_at=time.time() + (86400 if transfer_type == 'wan' else 600)
        )
        
        notification = req.to_notification(recipient_id)
        
        return self.send_notification(notification)
        
    def send_status_notification(self, recipient_id: str, notification_type: NotificationType,
                                 title: str, body: str, data: Dict = None) -> bool:
        """
        Send a status notification
        
        Args:
            recipient_id: Who receives the notification
            notification_type: Type of notification
            title: Notification title
            body: Notification body
            data: Additional data
            
        Returns:
            True if notification queued
        """
        import uuid
        
        notification = Notification(
            notification_id=str(uuid.uuid4()),
            type=notification_type,
            priority=NotificationPriority.NORMAL,
            recipient_id=recipient_id,
            sender_id=None,
            title=title,
            body=body,
            data=data or {}
        )
        
        return self.send_notification(notification)
        
    def send_user_online_notification(self, user_id: str, display_name: str):
        """Notify contacts that a user came online"""
        # This would query contacts and notify them
        pass
        
    def send_contact_joined_notification(self, user_id: str, contact_phone: str,
                                         contact_name: str):
        """Notify user that a contact joined Palmsync"""
        import uuid
        
        notification = Notification(
            notification_id=str(uuid.uuid4()),
            type=NotificationType.CONTACT_JOINED,
            priority=NotificationPriority.NORMAL,
            recipient_id=user_id,
            sender_id=None,
            title="🎉 Contact joined Palmsync!",
            body=f"{contact_name} is now on Palmsync",
            data={
                'contact_phone': contact_phone,
                'contact_name': contact_name
            }
        )
        
        self.send_notification(notification)
        
    def _process_queue(self):
        """Background worker to process notification queue"""
        while self.is_running:
            try:
                notification = self.notification_queue.get(timeout=1.0)
                self._deliver_notification(notification)
            except queue.Empty:
                continue
            except Exception as e:
                print(f"[Notifications] Queue error: {e}")
                
    def _deliver_notification(self, notification: Notification):
        """
        Deliver a notification to the recipient
        
        Args:
            notification: Notification to deliver
        """
        recipient_id = notification.recipient_id
        
        with self.connection_lock:
            websocket = self.connections.get(recipient_id)
            
        if websocket:
            # User is online - deliver immediately
            try:
                websocket.send(notification.to_json())
                notification.delivered = True
                self.stats['notifications_sent'] += 1
                
                print(f"[Notifications] 📨 Sent to {recipient_id[:8]}: {notification.title}")
                
                if self.on_notification_sent:
                    self.on_notification_sent(notification)
                    
            except Exception as e:
                # Delivery failed - store for later
                print(f"[Notifications] Failed to send: {e}")
                self._store_pending_notification(notification)
                self.stats['notifications_failed'] += 1
                
                if self.on_notification_failed:
                    self.on_notification_failed(notification, str(e))
        else:
            # User offline - store for later
            self._store_pending_notification(notification)
            
    def _store_pending_notification(self, notification: Notification):
        """Store notification for offline user"""
        with self.pending_lock:
            if notification.recipient_id not in self.pending_notifications:
                self.pending_notifications[notification.recipient_id] = []
                
            # Check expiry
            if notification.expires_at and time.time() > notification.expires_at:
                print(f"[Notifications] Notification expired: {notification.notification_id}")
                return
                
            self.pending_notifications[notification.recipient_id].append(notification)
            self.stats['notifications_pending'] = len(self.pending_notifications)
            
            print(f"[Notifications] 📦 Stored for offline user {notification.recipient_id[:8]}")
            
    def _deliver_pending_notifications(self, user_id: str):
        """Deliver stored notifications when user comes online"""
        with self.pending_lock:
            pending = self.pending_notifications.pop(user_id, [])
            
        if not pending:
            return
            
        # Filter expired
        valid_notifications = []
        for n in pending:
            if n.expires_at and time.time() > n.expires_at:
                continue
            valid_notifications.append(n)
            
        if not valid_notifications:
            return
            
        print(f"[Notifications] Delivering {len(valid_notifications)} pending to {user_id[:8]}")
        
        # Deliver each notification
        for notification in valid_notifications:
            self.notification_queue.put(notification)
            
        self.stats['notifications_pending'] = sum(len(v) for v in self.pending_notifications.values())
        
    def get_pending_count(self, user_id: str) -> int:
        """Get count of pending notifications for user"""
        with self.pending_lock:
            pending = self.pending_notifications.get(user_id, [])
            return len(pending)
            
    def get_pending_notifications(self, user_id: str) -> List[Notification]:
        """Get pending notifications for user"""
        with self.pending_lock:
            return self.pending_notifications.get(user_id, [])
            
    def mark_as_read(self, user_id: str, notification_ids: List[str]):
        """Mark notifications as read"""
        with self.pending_lock:
            pending = self.pending_notifications.get(user_id, [])
            for n in pending:
                if n.notification_id in notification_ids:
                    n.is_read = True
                    
    def clear_all_notifications(self, user_id: str):
        """Clear all notifications for a user"""
        with self.pending_lock:
            self.pending_notifications.pop(user_id, None)
            
    def get_stats(self) -> dict:
        """Get notification statistics"""
        self.stats['active_connections'] = len(self.connections)
        self.stats['online_users'] = len(self.connections)
        self.stats['pending_count'] = sum(len(v) for v in self.pending_notifications.values())
        return self.stats.copy()
        
    def broadcast(self, notification_type: NotificationType, title: str, body: str,
                  data: Dict = None, exclude_users: List[str] = None):
        """
        Broadcast a notification to all online users
        
        Args:
            notification_type: Type of notification
            title: Notification title
            body: Notification body
            data: Additional data
            exclude_users: Users to exclude
        """
        import uuid
        
        exclude_set = set(exclude_users or [])
        
        with self.connection_lock:
            online_users = list(self.connections.keys())
            
        for user_id in online_users:
            if user_id in exclude_set:
                continue
                
            notification = Notification(
                notification_id=str(uuid.uuid4()),
                type=notification_type,
                priority=NotificationPriority.LOW,
                recipient_id=user_id,
                sender_id=None,
                title=title,
                body=body,
                data=data or {}
            )
            
            self.send_notification(notification)
            
    def shutdown(self):
        """Shutdown notification manager"""
        self.is_running = False
        print("[Notifications] Shutting down...")


# ============================================
# FLASK WEBSOCKET INTEGRATION
# ============================================

class WebSocketHandler:
    """
    WebSocket handler for Flask-Sock integration
    Manages real-time connections
    """
    
    def __init__(self, notification_manager: NotificationManager, auth_manager=None):
        self.notification_manager = notification_manager
        self.auth_manager = auth_manager
        
    def handle_connection(self, ws):
        """
        Handle WebSocket connection
        
        Args:
            ws: WebSocket connection object
        """
        user_id = None
        device_id = None
        
        try:
            # Wait for authentication message
            message = ws.receive()
            
            if message:
                data = json.loads(message)
                
                if data.get('type') == 'auth':
                    token = data.get('token')
                    device_id = data.get('device_id', 'unknown')
                    
                    # Validate token
                    if self.auth_manager:
                        user = self.auth_manager.validate_token(token)
                        if user:
                            user_id = user.user_id
                            
                            # Register connection
                            self.notification_manager.register_connection(
                                user_id, device_id, ws
                            )
                            
                            # Send confirmation
                            ws.send(json.dumps({
                                'type': 'connected',
                                'user_id': user_id,
                                'pending_count': self.notification_manager.get_pending_count(user_id)
                            }))
                            
                            print(f"[WebSocket] User {user_id[:8]} authenticated")
                            
                    if not user_id:
                        ws.send(json.dumps({'type': 'error', 'message': 'Authentication failed'}))
                        return
                        
            # Main message loop
            while True:
                message = ws.receive()
                
                if message:
                    data = json.loads(message)
                    self._handle_message(user_id, data)
                    
        except Exception as e:
            print(f"[WebSocket] Connection error: {e}")
            
        finally:
            # Cleanup
            if user_id:
                self.notification_manager.unregister_connection(user_id, device_id)
                
    def _handle_message(self, user_id: str, data: dict):
        """Handle incoming WebSocket message"""
        msg_type = data.get('type')
        
        if msg_type == 'ping':
            # Respond to ping
            pass
            
        elif msg_type == 'ack_notification':
            # Mark notification as read
            notification_ids = data.get('notification_ids', [])
            self.notification_manager.mark_as_read(user_id, notification_ids)
            
        elif msg_type == 'get_pending':
            # Send pending notifications
            pending = self.notification_manager.get_pending_notifications(user_id)
            # Response handled separately
            
        elif msg_type == 'status_update':
            # User status update
            status = data.get('status', 'online')
            print(f"[WebSocket] User {user_id[:8]} status: {status}")


# ============================================
# TEST FUNCTION
# ============================================

def test_notifications():
    """Test notification system"""
    print("🔔 Palmsync Push Notifications Test")
    print("=" * 50)
    
    # Create notification manager
    manager = NotificationManager()
    
    print("\n1️⃣ Testing notification creation...")
    
    # Create transfer request
    success = manager.send_transfer_request(
        recipient_id="test-user-456",
        sender_id="test-user-123",
        sender_name="Alice",
        transfer_id="test-transfer-789",
        filename="vacation.mp4",
        file_size=50 * 1024 * 1024,
        file_type="video/mp4",
        total_chunks=50,
        transfer_type="wan"
    )
    
    print(f"   Transfer request sent: {success}")
    
    # Create status notification
    success = manager.send_status_notification(
        recipient_id="test-user-456",
        notification_type=NotificationType.TRANSFER_COMPLETE,
        title="✅ Transfer Complete",
        body="vacation.mp4 has been downloaded",
        data={"transfer_id": "test-transfer-789"}
    )
    
    print(f"   Status notification sent: {success}")
    
    # Test contact joined
    manager.send_contact_joined_notification(
        user_id="test-user-456",
        contact_phone="+919876543210",
        contact_name="Bob"
    )
    
    print(f"   Contact joined notification sent")
    
    print("\n2️⃣ Testing pending notifications...")
    
    pending_count = manager.get_pending_count("test-user-456")
    print(f"   Pending for test-user-456: {pending_count}")
    
    print("\n3️⃣ Testing stats...")
    
    stats = manager.get_stats()
    print(f"   Notifications sent: {stats['notifications_sent']}")
    print(f"   Notifications failed: {stats['notifications_failed']}")
    print(f"   Pending total: {stats['pending_count']}")
    print(f"   Active connections: {stats['active_connections']}")
    
    print("\n4️⃣ Testing broadcast...")
    
    manager.broadcast(
        notification_type=NotificationType.USER_ONLINE,
        title="System",
        body="Palmsync server is running",
        data={"version": "1.0.0"}
    )
    
    print(f"   Broadcast sent to all online users")
    
    print("\n✅ Push Notifications test complete!")
    
    manager.shutdown()


if __name__ == "__main__":
    test_notifications()
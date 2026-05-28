"""
reliability/timeout_manager.py
Transfer timeout management with automatic cleanup
Handles LAN (10 min) and WAN (24 hour) transfer expiration
"""

import time
import threading
import os
import shutil
import json
from typing import Callable, Optional, Dict, Any, List
from dataclasses import dataclass, field
from enum import Enum
from datetime import datetime, timedelta

class TimeoutType(Enum):
    LAN = "lan"
    WAN = "wan"
    CUSTOM = "custom"

class TransferState(Enum):
    PENDING = "pending"        # Waiting for acceptance
    ACTIVE = "active"          # Transfer in progress
    PAUSED = "paused"          # Transfer paused
    COMPLETED = "completed"    # Successfully completed
    EXPIRED = "expired"        # Timed out
    CANCELLED = "cancelled"    # User cancelled
    FAILED = "failed"          # Transfer failed

@dataclass
class TimeoutConfig:
    """Timeout configuration for different transfer types"""
    lan_timeout: int = 600        # 10 minutes
    wan_timeout: int = 86400      # 24 hours
    acceptance_timeout: int = 30  # 30 seconds to accept
    chunk_timeout: int = 5        # 5 seconds per chunk
    idle_timeout: int = 60        # 60 seconds idle timeout
    
    def get_timeout(self, timeout_type: TimeoutType) -> int:
        if timeout_type == TimeoutType.LAN:
            return self.lan_timeout
        elif timeout_type == TimeoutType.WAN:
            return self.wan_timeout
        return self.lan_timeout

@dataclass
class TimeoutInfo:
    """Information about a transfer timeout"""
    transfer_id: str
    timeout_type: TimeoutType
    state: TransferState
    created_at: float
    expires_at: float
    last_activity: float
    total_timeout: int  # seconds
    storage_path: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def get_remaining_time(self) -> float:
        """Get remaining time in seconds"""
        return max(0, self.expires_at - time.time())
        
    def is_expired(self) -> bool:
        """Check if timeout has expired"""
        return time.time() >= self.expires_at
        
    def get_progress_percentage(self) -> float:
        """Get percentage of timeout elapsed"""
        total = self.total_timeout
        if total == 0:
            return 0
        elapsed = time.time() - self.created_at
        return min(100, (elapsed / total) * 100)
        
    def format_remaining_time(self) -> str:
        """Format remaining time as human readable string"""
        remaining = self.get_remaining_time()
        
        if remaining < 60:
            return f"{int(remaining)} seconds"
        elif remaining < 3600:
            return f"{int(remaining / 60)} minutes"
        elif remaining < 86400:
            return f"{int(remaining / 3600)} hours"
        else:
            return f"{int(remaining / 86400)} days"

class TimeoutManager:
    """
    Manages transfer timeouts and automatic cleanup
    """
    
    def __init__(self, config: TimeoutConfig = None):
        """
        Initialize timeout manager
        
        Args:
            config: Timeout configuration
        """
        self.config = config or TimeoutConfig()
        
        # Active timeouts
        self.timeouts: Dict[str, TimeoutInfo] = {}
        self.lock = threading.RLock()
        
        # Background monitoring
        self.is_running = False
        self.monitor_thread = None
        self.check_interval = 5  # Check every 5 seconds
        
        # Storage paths
        self.temp_storage_path = os.path.join(os.path.expanduser("~"), ".secure_transfer", "temp")
        os.makedirs(self.temp_storage_path, exist_ok=True)
        
        # Callbacks
        self.on_timeout_expired = None
        self.on_timeout_warning = None
        self.on_cleanup_complete = None
        
        # Warning thresholds (seconds before expiry)
        self.warning_thresholds = [300, 60, 30]  # 5 min, 1 min, 30 sec
        
    def start(self):
        """Start timeout monitoring"""
        if self.is_running:
            return
            
        self.is_running = True
        self.monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self.monitor_thread.start()
        
        print(f"[TimeoutManager] ✅ Started monitoring")
        print(f"                 LAN timeout: {self.config.lan_timeout}s ({self.config.lan_timeout/60:.0f} min)")
        print(f"                 WAN timeout: {self.config.wan_timeout}s ({self.config.wan_timeout/3600:.0f} hours)")
        
    def stop(self):
        """Stop timeout monitoring"""
        self.is_running = False
        if self.monitor_thread:
            self.monitor_thread.join(timeout=2.0)
        print("[TimeoutManager] Stopped")
        
    def register_transfer(self, transfer_id: str, timeout_type: TimeoutType,
                          storage_path: str = None, metadata: Dict = None) -> TimeoutInfo:
        """
        Register a new transfer for timeout tracking
        
        Args:
            transfer_id: Unique transfer ID
            timeout_type: LAN or WAN
            storage_path: Path to temporary storage
            metadata: Additional metadata
            
        Returns:
            TimeoutInfo for the transfer
        """
        with self.lock:
            timeout_seconds = self.config.get_timeout(timeout_type)
            
            info = TimeoutInfo(
                transfer_id=transfer_id,
                timeout_type=timeout_type,
                state=TransferState.PENDING,
                created_at=time.time(),
                expires_at=time.time() + timeout_seconds,
                last_activity=time.time(),
                total_timeout=timeout_seconds,
                storage_path=storage_path or self._get_storage_path(transfer_id),
                metadata=metadata or {}
            )
            
            self.timeouts[transfer_id] = info
            
            print(f"[TimeoutManager] Registered transfer {transfer_id[:8]}...")
            print(f"                 Type: {timeout_type.value}")
            print(f"                 Expires: {info.format_remaining_time()}")
            
            return info
            
    def _get_storage_path(self, transfer_id: str) -> str:
        """Get temporary storage path for transfer"""
        return os.path.join(self.temp_storage_path, transfer_id)
        
    def update_activity(self, transfer_id: str):
        """
        Update last activity timestamp
        
        Args:
            transfer_id: Transfer ID
        """
        with self.lock:
            if transfer_id in self.timeouts:
                self.timeouts[transfer_id].last_activity = time.time()
                
    def update_state(self, transfer_id: str, state: TransferState):
        """
        Update transfer state
        
        Args:
            transfer_id: Transfer ID
            state: New state
        """
        with self.lock:
            if transfer_id in self.timeouts:
                self.timeouts[transfer_id].state = state
                
                if state == TransferState.COMPLETED:
                    # Clean up immediately on completion
                    self._cleanup_transfer(transfer_id)
                    
    def extend_timeout(self, transfer_id: str, additional_seconds: int) -> bool:
        """
        Extend timeout for a transfer
        
        Args:
            transfer_id: Transfer ID
            additional_seconds: Seconds to add
            
        Returns:
            True if extended successfully
        """
        with self.lock:
            if transfer_id in self.timeouts:
                info = self.timeouts[transfer_id]
                info.expires_at += additional_seconds
                info.total_timeout += additional_seconds
                
                print(f"[TimeoutManager] Extended {transfer_id[:8]}... by {additional_seconds}s")
                return True
        return False
        
    def cancel_transfer(self, transfer_id: str):
        """
        Cancel a transfer and clean up
        
        Args:
            transfer_id: Transfer ID
        """
        with self.lock:
            if transfer_id in self.timeouts:
                info = self.timeouts[transfer_id]
                info.state = TransferState.CANCELLED
                self._cleanup_transfer(transfer_id)
                
    def get_timeout_info(self, transfer_id: str) -> Optional[TimeoutInfo]:
        """Get timeout info for a transfer"""
        with self.lock:
            return self.timeouts.get(transfer_id)
            
    def get_all_active_transfers(self) -> List[TimeoutInfo]:
        """Get all active transfers"""
        with self.lock:
            return [info for info in self.timeouts.values() 
                   if info.state in [TransferState.PENDING, TransferState.ACTIVE, TransferState.PAUSED]]
                   
    def check_idle_timeout(self, transfer_id: str) -> bool:
        """
        Check if transfer has been idle too long
        
        Args:
            transfer_id: Transfer ID
            
        Returns:
            True if idle timeout exceeded
        """
        with self.lock:
            if transfer_id in self.timeouts:
                info = self.timeouts[transfer_id]
                idle_time = time.time() - info.last_activity
                return idle_time > self.config.idle_timeout
        return False
        
    def _monitor_loop(self):
        """Main monitoring loop"""
        while self.is_running:
            try:
                self._check_timeouts()
                self._check_warnings()
                self._check_idle_transfers()
            except Exception as e:
                print(f"[TimeoutManager] Monitor error: {e}")
                
            time.sleep(self.check_interval)
            
    def _check_timeouts(self):
        """Check for expired timeouts"""
        expired_transfers = []
        
        with self.lock:
            current_time = time.time()
            
            for transfer_id, info in self.timeouts.items():
                if info.state in [TransferState.COMPLETED, TransferState.CANCELLED, TransferState.FAILED]:
                    continue
                    
                if current_time >= info.expires_at:
                    expired_transfers.append((transfer_id, info))
                    
        # Process expired transfers
        for transfer_id, info in expired_transfers:
            print(f"[TimeoutManager] ⏰ Transfer {transfer_id[:8]}... EXPIRED")
            print(f"                 Type: {info.timeout_type.value}")
            print(f"                 Duration: {info.total_timeout}s")
            
            with self.lock:
                info.state = TransferState.EXPIRED
                
            # Callback
            if self.on_timeout_expired:
                self.on_timeout_expired(transfer_id, info)
                
            # Cleanup
            self._cleanup_transfer(transfer_id)
            
    def _check_warnings(self):
        """Check for approaching timeouts and send warnings"""
        with self.lock:
            for transfer_id, info in self.timeouts.items():
                if info.state not in [TransferState.PENDING, TransferState.ACTIVE]:
                    continue
                    
                remaining = info.get_remaining_time()
                
                for threshold in self.warning_thresholds:
                    # Check if we just crossed this threshold
                    if abs(remaining - threshold) < self.check_interval:
                        if self.on_timeout_warning:
                            self.on_timeout_warning(transfer_id, info, int(remaining))
                            
                        print(f"[TimeoutManager] ⚠️ Transfer {transfer_id[:8]}... expires in {int(remaining)}s")
                        
    def _check_idle_transfers(self):
        """Check for idle transfers"""
        with self.lock:
            for transfer_id, info in self.timeouts.items():
                if info.state != TransferState.ACTIVE:
                    continue
                    
                idle_time = time.time() - info.last_activity
                
                if idle_time > self.config.idle_timeout:
                    print(f"[TimeoutManager] 💤 Transfer {transfer_id[:8]}... idle for {idle_time:.0f}s")
                    
                    # Mark as paused
                    info.state = TransferState.PAUSED
                    
    def _cleanup_transfer(self, transfer_id: str):
        """
        Clean up transfer resources
        
        Args:
            transfer_id: Transfer ID
        """
        with self.lock:
            info = self.timeouts.get(transfer_id)
            if not info:
                return
                
            # Delete temporary files
            if info.storage_path and os.path.exists(info.storage_path):
                try:
                    shutil.rmtree(info.storage_path)
                    print(f"[TimeoutManager] 🧹 Cleaned up {info.storage_path}")
                except Exception as e:
                    print(f"[TimeoutManager] Cleanup error: {e}")
                    
            # Remove from tracking
            del self.timeouts[transfer_id]
            
        # Callback
        if self.on_cleanup_complete:
            self.on_cleanup_complete(transfer_id, info)
            
    def create_storage_directory(self, transfer_id: str) -> str:
        """
        Create temporary storage directory for transfer
        
        Args:
            transfer_id: Transfer ID
            
        Returns:
            Path to storage directory
        """
        path = self._get_storage_path(transfer_id)
        os.makedirs(path, exist_ok=True)
        return path
        
    def save_chunk(self, transfer_id: str, chunk_id: int, data: bytes) -> str:
        """
        Save a chunk to temporary storage
        
        Args:
            transfer_id: Transfer ID
            chunk_id: Chunk ID
            data: Chunk data
            
        Returns:
            Path to saved chunk
        """
        path = self._get_storage_path(transfer_id)
        os.makedirs(path, exist_ok=True)
        
        chunk_path = os.path.join(path, f"chunk_{chunk_id:06d}.enc")
        
        with open(chunk_path, 'wb') as f:
            f.write(data)
            
        self.update_activity(transfer_id)
        
        return chunk_path
        
    def load_chunk(self, transfer_id: str, chunk_id: int) -> Optional[bytes]:
        """
        Load a chunk from temporary storage
        
        Args:
            transfer_id: Transfer ID
            chunk_id: Chunk ID
            
        Returns:
            Chunk data or None
        """
        path = self._get_storage_path(transfer_id)
        chunk_path = os.path.join(path, f"chunk_{chunk_id:06d}.enc")
        
        if os.path.exists(chunk_path):
            with open(chunk_path, 'rb') as f:
                return f.read()
        return None
        
    def save_metadata(self, transfer_id: str, metadata: Dict) -> str:
        """
        Save metadata to temporary storage
        
        Args:
            transfer_id: Transfer ID
            metadata: Metadata dictionary
            
        Returns:
            Path to metadata file
        """
        path = self._get_storage_path(transfer_id)
        os.makedirs(path, exist_ok=True)
        
        meta_path = os.path.join(path, "metadata.json")
        
        with open(meta_path, 'w') as f:
            json.dump(metadata, f, indent=2)
            
        return meta_path
        
    def load_metadata(self, transfer_id: str) -> Optional[Dict]:
        """
        Load metadata from temporary storage
        
        Args:
            transfer_id: Transfer ID
            
        Returns:
            Metadata dictionary or None
        """
        path = self._get_storage_path(transfer_id)
        meta_path = os.path.join(path, "metadata.json")
        
        if os.path.exists(meta_path):
            with open(meta_path, 'r') as f:
                return json.load(f)
        return None
        
    def get_storage_stats(self, transfer_id: str) -> Dict[str, Any]:
        """
        Get storage statistics for a transfer
        
        Args:
            transfer_id: Transfer ID
            
        Returns:
            Dictionary with storage stats
        """
        path = self._get_storage_path(transfer_id)
        
        if not os.path.exists(path):
            return {'exists': False}
            
        # Count chunks and total size
        chunk_count = 0
        total_size = 0
        
        for filename in os.listdir(path):
            if filename.startswith("chunk_"):
                chunk_count += 1
                total_size += os.path.getsize(os.path.join(path, filename))
                
        return {
            'exists': True,
            'path': path,
            'chunk_count': chunk_count,
            'total_size': total_size,
            'total_size_mb': total_size / (1024 * 1024)
        }
        
    def cleanup_all_expired(self) -> int:
        """
        Clean up all expired transfers
        
        Returns:
            Number of transfers cleaned up
        """
        cleaned = 0
        
        with self.lock:
            expired = [tid for tid, info in self.timeouts.items() if info.is_expired()]
            
        for transfer_id in expired:
            self._cleanup_transfer(transfer_id)
            cleaned += 1
            
        return cleaned
        
    def cleanup_old_temp_files(self, max_age_hours: int = 48) -> int:
        """
        Clean up old temporary files
        
        Args:
            max_age_hours: Maximum age in hours
            
        Returns:
            Number of directories cleaned
        """
        cleaned = 0
        current_time = time.time()
        max_age_seconds = max_age_hours * 3600
        
        if not os.path.exists(self.temp_storage_path):
            return 0
            
        for item in os.listdir(self.temp_storage_path):
            item_path = os.path.join(self.temp_storage_path, item)
            
            # Check if directory is old
            if os.path.isdir(item_path):
                mtime = os.path.getmtime(item_path)
                
                if current_time - mtime > max_age_seconds:
                    try:
                        shutil.rmtree(item_path)
                        cleaned += 1
                    except:
                        pass
                        
        if cleaned > 0:
            print(f"[TimeoutManager] Cleaned up {cleaned} old temp directories")
            
        return cleaned


# ============================================
# TEST FUNCTION
# ============================================

def test_timeout_manager():
    """Test timeout manager"""
    print("⏰ Timeout Manager Test")
    print("=" * 50)
    
    # Create timeout manager with short timeouts for testing
    config = TimeoutConfig(
        lan_timeout=10,  # 10 seconds for testing
        wan_timeout=30,  # 30 seconds for testing
        acceptance_timeout=5
    )
    
    timeout_manager = TimeoutManager(config)
    
    def on_expired(transfer_id, info):
        print(f"\n   ⏰ EXPIRED: {transfer_id[:8]}... ({info.timeout_type.value})")
        
    def on_warning(transfer_id, info, remaining):
        print(f"\n   ⚠️ WARNING: {transfer_id[:8]}... expires in {remaining}s")
        
    timeout_manager.on_timeout_expired = on_expired
    timeout_manager.on_timeout_warning = on_warning
    
    timeout_manager.start()
    
    # Register transfers
    print("\n1️⃣ Registering transfers...")
    lan_info = timeout_manager.register_transfer(
        "lan-test-123",
        TimeoutType.LAN,
        metadata={"filename": "test.mp4"}
    )
    print(f"   LAN transfer expires in: {lan_info.format_remaining_time()}")
    
    wan_info = timeout_manager.register_transfer(
        "wan-test-456",
        TimeoutType.WAN,
        metadata={"filename": "large_file.zip"}
    )
    print(f"   WAN transfer expires in: {wan_info.format_remaining_time()}")
    
    # Test activity update
    print("\n2️⃣ Testing activity update...")
    time.sleep(2)
    timeout_manager.update_activity("lan-test-123")
    print("   Activity updated for LAN transfer")
    
    # Test state update
    print("\n3️⃣ Testing state update...")
    timeout_manager.update_state("lan-test-123", TransferState.ACTIVE)
    print("   State updated to ACTIVE")
    
    # Test storage
    print("\n4️⃣ Testing temporary storage...")
    timeout_manager.create_storage_directory("lan-test-123")
    timeout_manager.save_chunk("lan-test-123", 0, b"test chunk data")
    timeout_manager.save_metadata("lan-test-123", {"test": "metadata"})
    
    stats = timeout_manager.get_storage_stats("lan-test-123")
    print(f"   Storage exists: {stats['exists']}")
    print(f"   Chunks: {stats['chunk_count']}")
    print(f"   Total size: {stats['total_size_mb']:.2f} MB")
    
    # Test timeout extension
    print("\n5️⃣ Testing timeout extension...")
    timeout_manager.extend_timeout("lan-test-123", 5)
    info = timeout_manager.get_timeout_info("lan-test-123")
    print(f"   New expiry: {info.format_remaining_time()}")
    
    # Wait for some warnings
    print("\n6️⃣ Waiting for timeout warnings...")
    time.sleep(8)
    
    # Check active transfers
    print("\n7️⃣ Active transfers:")
    active = timeout_manager.get_all_active_transfers()
    for info in active:
        print(f"   • {info.transfer_id[:8]}... - {info.state.value} - {info.format_remaining_time()}")
        
    # Clean up
    print("\n8️⃣ Cleaning up...")
    timeout_manager.cancel_transfer("lan-test-123")
    timeout_manager.cancel_transfer("wan-test-456")
    
    timeout_manager.stop()
    
    print("\n✅ Timeout Manager test complete!")


if __name__ == "__main__":
    test_timeout_manager()
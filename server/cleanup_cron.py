"""
server/cleanup_cron.py
Automated cleanup service for relay server
Handles expired transfers, orphaned files, and old sessions
"""

import os
import time
import sqlite3
import threading
import shutil
import json
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum

class CleanupTask(Enum):
    EXPIRED_TRANSFERS = "expired_transfers"
    ORPHANED_CHUNKS = "orphaned_chunks"
    OLD_SESSIONS = "old_sessions"
    TEMP_FILES = "temp_files"
    LOGS = "logs"
    INACTIVE_USERS = "inactive_users"

@dataclass
class CleanupStats:
    """Statistics from cleanup operation"""
    task: CleanupTask
    items_cleaned: int
    bytes_freed: int
    duration: float
    timestamp: float
    details: dict = None
    
    def format_bytes(self) -> str:
        """Format bytes to human readable"""
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if self.bytes_freed < 1024:
                return f"{self.bytes_freed:.1f} {unit}"
            self.bytes_freed /= 1024
        return f"{self.bytes_freed:.1f} PB"

class CleanupScheduler:
    """
    Automated cleanup service for relay server
    Runs periodic cleanup tasks
    """
    
    def __init__(self, db_path: str = "relay_server.db",
                 storage_path: str = "storage",
                 logs_path: str = "logs"):
        """
        Initialize cleanup scheduler
        
        Args:
            db_path: Path to SQLite database
            storage_path: Path to chunk storage
            logs_path: Path to log files
        """
        self.db_path = db_path
        self.storage_path = storage_path
        self.logs_path = logs_path
        
        # Cleanup intervals (seconds)
        self.intervals = {
            CleanupTask.EXPIRED_TRANSFERS: 3600,      # 1 hour
            CleanupTask.ORPHANED_CHUNKS: 7200,        # 2 hours
            CleanupTask.OLD_SESSIONS: 1800,           # 30 minutes
            CleanupTask.TEMP_FILES: 86400,            # 24 hours
            CleanupTask.LOGS: 604800,                 # 7 days
            CleanupTask.INACTIVE_USERS: 2592000       # 30 days
        }
        
        # Retention periods (seconds)
        self.retention = {
            'transfer_expiry': 86400,          # 24 hours for WAN
            'lan_transfer_expiry': 600,        # 10 minutes for LAN
            'session_expiry': 86400,           # 24 hours
            'temp_file_max_age': 172800,       # 48 hours
            'log_max_age': 2592000,            # 30 days
            'inactive_user_days': 90           # 90 days
        }
        
        # Statistics
        self.stats: Dict[CleanupTask, List[CleanupStats]] = {}
        self.stats_lock = threading.Lock()
        
        # Threading
        self.is_running = False
        self.scheduler_thread = None
        self.last_run: Dict[CleanupTask, float] = {}
        
        # Callbacks
        self.on_cleanup_complete = None
        self.on_error = None
        
    def start(self):
        """Start cleanup scheduler"""
        if self.is_running:
            return
            
        self.is_running = True
        self.scheduler_thread = threading.Thread(target=self._scheduler_loop, daemon=True)
        self.scheduler_thread.start()
        
        print("[CleanupScheduler] ✅ Started")
        print(f"                 Transfer expiry: {self.retention['transfer_expiry'] // 3600}h")
        print(f"                 Session expiry: {self.retention['session_expiry'] // 3600}h")
        
    def stop(self):
        """Stop cleanup scheduler"""
        self.is_running = False
        if self.scheduler_thread:
            self.scheduler_thread.join(timeout=5.0)
        print("[CleanupScheduler] Stopped")
        
    def _scheduler_loop(self):
        """Main scheduler loop"""
        while self.is_running:
            current_time = time.time()
            
            for task, interval in self.intervals.items():
                last_run = self.last_run.get(task, 0)
                
                if current_time - last_run >= interval:
                    self._run_cleanup_task(task)
                    self.last_run[task] = current_time
                    
            time.sleep(60)  # Check every minute
            
    def _run_cleanup_task(self, task: CleanupTask):
        """Execute a cleanup task"""
        start_time = time.time()
        stats = None
        
        try:
            if task == CleanupTask.EXPIRED_TRANSFERS:
                stats = self._cleanup_expired_transfers()
            elif task == CleanupTask.ORPHANED_CHUNKS:
                stats = self._cleanup_orphaned_chunks()
            elif task == CleanupTask.OLD_SESSIONS:
                stats = self._cleanup_old_sessions()
            elif task == CleanupTask.TEMP_FILES:
                stats = self._cleanup_temp_files()
            elif task == CleanupTask.LOGS:
                stats = self._cleanup_logs()
            elif task == CleanupTask.INACTIVE_USERS:
                stats = self._cleanup_inactive_users()
                
        except Exception as e:
            print(f"[CleanupScheduler] Error in {task.value}: {e}")
            if self.on_error:
                self.on_error(task, str(e))
                
        if stats:
            stats.duration = time.time() - start_time
            stats.timestamp = time.time()
            
            with self.stats_lock:
                if task not in self.stats:
                    self.stats[task] = []
                self.stats[task].append(stats)
                
            print(f"[CleanupScheduler] {task.value}: cleaned {stats.items_cleaned} items, "
                  f"freed {stats.format_bytes()} in {stats.duration:.2f}s")
                  
            if self.on_cleanup_complete:
                self.on_cleanup_complete(stats)
                
    def _cleanup_expired_transfers(self) -> CleanupStats:
        """Clean up expired transfers"""
        items_cleaned = 0
        bytes_freed = 0
        expired_transfers = []
        
        # Find expired transfers in database
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            current_time = time.time()
            
            cursor.execute('''
                SELECT transfer_id, expires_at FROM transfers 
                WHERE status != 'completed' AND status != 'cancelled'
            ''')
            
            for row in cursor.fetchall():
                transfer_id, expires_at = row
                
                if expires_at < current_time:
                    expired_transfers.append(transfer_id)
                    
            # Update status to expired
            for transfer_id in expired_transfers:
                cursor.execute('''
                    UPDATE transfers SET status = 'expired' WHERE transfer_id = ?
                ''', (transfer_id,))
                
            conn.commit()
            conn.close()
            
        except Exception as e:
            print(f"[Cleanup] Database error: {e}")
            
        # Delete transfer files
        for transfer_id in expired_transfers:
            transfer_path = os.path.join(self.storage_path, transfer_id)
            
            if os.path.exists(transfer_path):
                # Calculate size before deletion
                transfer_size = self._get_directory_size(transfer_path)
                bytes_freed += transfer_size
                
                try:
                    shutil.rmtree(transfer_path)
                    items_cleaned += 1
                except Exception as e:
                    print(f"[Cleanup] Failed to delete {transfer_id}: {e}")
                    
        return CleanupStats(
            task=CleanupTask.EXPIRED_TRANSFERS,
            items_cleaned=items_cleaned,
            bytes_freed=bytes_freed,
            duration=0,
            timestamp=0,
            details={'transfer_ids': expired_transfers}
        )
        
    def _cleanup_orphaned_chunks(self) -> CleanupStats:
        """Clean up chunks without database records"""
        items_cleaned = 0
        bytes_freed = 0
        
        if not os.path.exists(self.storage_path):
            return CleanupStats(
                task=CleanupTask.ORPHANED_CHUNKS,
                items_cleaned=0,
                bytes_freed=0,
                duration=0,
                timestamp=0
            )
            
        # Get all valid transfer IDs from database
        valid_transfers = set()
        
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('SELECT transfer_id FROM transfers')
            for row in cursor.fetchall():
                valid_transfers.add(row[0])
                
            conn.close()
        except:
            pass
            
        # Check storage directory
        for item in os.listdir(self.storage_path):
            if item not in valid_transfers:
                item_path = os.path.join(self.storage_path, item)
                
                if os.path.isdir(item_path):
                    # Check if old (more than 48 hours)
                    mtime = os.path.getmtime(item_path)
                    
                    if time.time() - mtime > self.retention['temp_file_max_age']:
                        size = self._get_directory_size(item_path)
                        bytes_freed += size
                        
                        try:
                            shutil.rmtree(item_path)
                            items_cleaned += 1
                        except:
                            pass
                            
        return CleanupStats(
            task=CleanupTask.ORPHANED_CHUNKS,
            items_cleaned=items_cleaned,
            bytes_freed=bytes_freed,
            duration=0,
            timestamp=0
        )
        
    def _cleanup_old_sessions(self) -> CleanupStats:
        """Clean up expired sessions"""
        items_cleaned = 0
        
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            current_time = time.time()
            
            cursor.execute('''
                DELETE FROM sessions WHERE expires_at < ?
            ''', (current_time,))
            
            items_cleaned = cursor.rowcount
            
            conn.commit()
            conn.close()
            
        except Exception as e:
            print(f"[Cleanup] Session cleanup error: {e}")
            
        # Update offline status for users with no active sessions
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                UPDATE users SET status = 'offline'
                WHERE user_id NOT IN (
                    SELECT DISTINCT user_id FROM sessions
                )
            ''')
            
            conn.commit()
            conn.close()
            
        except:
            pass
            
        return CleanupStats(
            task=CleanupTask.OLD_SESSIONS,
            items_cleaned=items_cleaned,
            bytes_freed=0,
            duration=0,
            timestamp=0
        )
        
    def _cleanup_temp_files(self) -> CleanupStats:
        """Clean up temporary files"""
        items_cleaned = 0
        bytes_freed = 0
        
        temp_paths = [
            os.path.join(self.storage_path, "temp"),
            "/tmp/secure_transfer",
            os.path.expanduser("~/.secure_transfer/temp")
        ]
        
        max_age = self.retention['temp_file_max_age']
        current_time = time.time()
        
        for temp_path in temp_paths:
            if os.path.exists(temp_path):
                for root, dirs, files in os.walk(temp_path):
                    for file in files:
                        file_path = os.path.join(root, file)
                        
                        try:
                            mtime = os.path.getmtime(file_path)
                            
                            if current_time - mtime > max_age:
                                size = os.path.getsize(file_path)
                                os.remove(file_path)
                                bytes_freed += size
                                items_cleaned += 1
                        except:
                            pass
                            
                    # Remove empty directories
                    for dir_name in dirs:
                        dir_path = os.path.join(root, dir_name)
                        try:
                            if not os.listdir(dir_path):
                                os.rmdir(dir_path)
                        except:
                            pass
                            
        return CleanupStats(
            task=CleanupTask.TEMP_FILES,
            items_cleaned=items_cleaned,
            bytes_freed=bytes_freed,
            duration=0,
            timestamp=0
        )
        
    def _cleanup_logs(self) -> CleanupStats:
        """Clean up old log files"""
        items_cleaned = 0
        bytes_freed = 0
        
        if not os.path.exists(self.logs_path):
            return CleanupStats(
                task=CleanupTask.LOGS,
                items_cleaned=0,
                bytes_freed=0,
                duration=0,
                timestamp=0
            )
            
        max_age = self.retention['log_max_age']
        current_time = time.time()
        
        for filename in os.listdir(self.logs_path):
            if filename.endswith('.log') or filename.endswith('.log.gz'):
                file_path = os.path.join(self.logs_path, filename)
                
                try:
                    mtime = os.path.getmtime(file_path)
                    
                    if current_time - mtime > max_age:
                        size = os.path.getsize(file_path)
                        os.remove(file_path)
                        bytes_freed += size
                        items_cleaned += 1
                except:
                    pass
                    
        return CleanupStats(
            task=CleanupTask.LOGS,
            items_cleaned=items_cleaned,
            bytes_freed=bytes_freed,
            duration=0,
            timestamp=0
        )
        
    def _cleanup_inactive_users(self) -> CleanupStats:
        """Clean up inactive user accounts"""
        items_cleaned = 0
        
        inactive_days = self.retention['inactive_user_days']
        cutoff_time = time.time() - (inactive_days * 86400)
        
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Find inactive users
            cursor.execute('''
                SELECT user_id FROM users 
                WHERE last_seen < ? 
                AND role = 'user'
                AND user_id NOT IN (SELECT DISTINCT sender_id FROM transfers)
            ''', (cutoff_time,))
            
            inactive_users = [row[0] for row in cursor.fetchall()]
            
            # Delete inactive users
            for user_id in inactive_users:
                # Delete contacts
                cursor.execute('DELETE FROM contacts WHERE user_id = ? OR contact_id = ?', 
                             (user_id, user_id))
                
                # Delete user devices
                cursor.execute('DELETE FROM user_devices WHERE user_id = ?', (user_id,))
                
                # Delete user
                cursor.execute('DELETE FROM users WHERE user_id = ?', (user_id,))
                
                items_cleaned += 1
                
            conn.commit()
            conn.close()
            
        except Exception as e:
            print(f"[Cleanup] Inactive user cleanup error: {e}")
            
        return CleanupStats(
            task=CleanupTask.INACTIVE_USERS,
            items_cleaned=items_cleaned,
            bytes_freed=0,
            duration=0,
            timestamp=0
        )
        
    def _get_directory_size(self, path: str) -> int:
        """Calculate total size of directory"""
        total_size = 0
        
        try:
            for root, dirs, files in os.walk(path):
                for file in files:
                    file_path = os.path.join(root, file)
                    try:
                        total_size += os.path.getsize(file_path)
                    except:
                        pass
        except:
            pass
            
        return total_size
        
    def run_manual_cleanup(self, task: CleanupTask) -> CleanupStats:
        """Manually trigger a cleanup task"""
        return self._run_cleanup_task(task)
        
    def get_stats(self, task: CleanupTask = None) -> dict:
        """Get cleanup statistics"""
        with self.stats_lock:
            if task:
                stats_list = self.stats.get(task, [])
            else:
                stats_list = []
                for task_stats in self.stats.values():
                    stats_list.extend(task_stats)
                    
        if not stats_list:
            return {}
            
        total_items = sum(s.items_cleaned for s in stats_list)
        total_bytes = sum(s.bytes_freed for s in stats_list)
        
        return {
            'total_cleanups': len(stats_list),
            'total_items_cleaned': total_items,
            'total_bytes_freed': total_bytes,
            'total_bytes_freed_formatted': self._format_bytes(total_bytes),
            'last_cleanup': stats_list[-1].timestamp if stats_list else 0,
            'tasks': {
                task.value: {
                    'count': len([s for s in self.stats.get(task, [])]),
                    'items': sum(s.items_cleaned for s in self.stats.get(task, [])),
                    'bytes': sum(s.bytes_freed for s in self.stats.get(task, []))
                }
                for task in CleanupTask if task in self.stats
            }
        }
        
    def _format_bytes(self, size: int) -> str:
        """Format bytes to human readable"""
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if size < 1024:
                return f"{size:.1f} {unit}"
            size /= 1024
        return f"{size:.1f} PB"
        
    def set_retention(self, key: str, value: int):
        """Update retention period"""
        if key in self.retention:
            self.retention[key] = value
            
    def set_interval(self, task: CleanupTask, interval: int):
        """Update cleanup interval"""
        self.intervals[task] = interval


# ============================================
# TEST FUNCTION
# ============================================

def test_cleanup_scheduler():
    """Test cleanup scheduler"""
    print("🧹 Cleanup Scheduler Test")
    print("=" * 50)
    
    import tempfile
    
    # Create test directories
    test_storage = tempfile.mkdtemp()
    test_logs = tempfile.mkdtemp()
    
    # Create test database
    test_db = os.path.join(tempfile.gettempdir(), "test_cleanup.db")
    
    # Create scheduler with short intervals for testing
    scheduler = CleanupScheduler(
        db_path=test_db,
        storage_path=test_storage,
        logs_path=test_logs
    )
    
    # Set short intervals for testing
    scheduler.set_retention('transfer_expiry', 5)  # 5 seconds
    scheduler.set_retention('temp_file_max_age', 10)  # 10 seconds
    
    def on_cleanup(stats: CleanupStats):
        print(f"\n   ✅ Cleanup: {stats.task.value}")
        print(f"      Items: {stats.items_cleaned}")
        print(f"      Freed: {stats.format_bytes()}")
        print(f"      Time: {stats.duration:.3f}s")
        
    scheduler.on_cleanup_complete = on_cleanup
    
    print("\n1️⃣ Testing manual cleanup...")
    
    # Create some test files
    test_transfer_path = os.path.join(test_storage, "test-transfer-123")
    os.makedirs(test_transfer_path)
    
    with open(os.path.join(test_transfer_path, "chunk_0.enc"), 'wb') as f:
        f.write(b"test data" * 1000)
        
    # Run cleanup
    stats = scheduler.run_manual_cleanup(CleanupTask.ORPHANED_CHUNKS)
    print(f"\n   Orphaned cleanup: {stats.items_cleaned} items, {stats.format_bytes()}")
    
    print("\n2️⃣ Testing scheduler start...")
    scheduler.start()
    
    print("\n3️⃣ Waiting for automatic cleanup...")
    time.sleep(3)
    
    print("\n4️⃣ Getting statistics...")
    stats = scheduler.get_stats()
    
    print(f"\n   Total cleanups: {stats.get('total_cleanups', 0)}")
    print(f"   Total items: {stats.get('total_items_cleaned', 0)}")
    print(f"   Total freed: {stats.get('total_bytes_freed_formatted', '0 B')}")
    
    scheduler.stop()
    
    # Cleanup test directories
    shutil.rmtree(test_storage, ignore_errors=True)
    shutil.rmtree(test_logs, ignore_errors=True)
    
    if os.path.exists(test_db):
        os.remove(test_db)
        
    print("\n✅ Cleanup Scheduler test complete!")


if __name__ == "__main__":
    import time
    test_cleanup_scheduler()
"""
reliability/ack_manager.py
ACK tracking and management system for reliable chunk transfer
Tracks sent chunks and their acknowledgment status
"""

import time
import threading
from typing import Dict, Set, Optional, List, Tuple
from dataclasses import dataclass, field
from enum import Enum
from collections import defaultdict

class ACKStatus(Enum):
    PENDING = "pending"      # Sent, waiting for ACK
    ACKED = "acked"          # Acknowledgment received
    TIMEOUT = "timeout"      # ACK timeout, needs retry
    FAILED = "failed"        # Failed after max retries
    SKIPPED = "skipped"      # Skipped (already acked)

@dataclass
class ChunkACK:
    """Tracks ACK status for a single chunk"""
    chunk_id: int
    status: ACKStatus = ACKStatus.PENDING
    sent_time: float = 0.0
    ack_time: float = 0.0
    retry_count: int = 0
    size_bytes: int = 0
    
    def is_pending(self) -> bool:
        return self.status == ACKStatus.PENDING
        
    def is_acked(self) -> bool:
        return self.status == ACKStatus.ACKED
        
    def can_retry(self, max_retries: int) -> bool:
        return self.retry_count < max_retries and self.status in [ACKStatus.PENDING, ACKStatus.TIMEOUT]
        
    def get_latency(self) -> float:
        """Get ACK latency in seconds"""
        if self.ack_time > 0 and self.sent_time > 0:
            return self.ack_time - self.sent_time
        return 0.0

@dataclass
class TransferStats:
    """Transfer statistics"""
    total_chunks: int = 0
    acked_chunks: int = 0
    pending_chunks: int = 0
    failed_chunks: int = 0
    retry_count: int = 0
    avg_latency: float = 0.0
    min_latency: float = float('inf')
    max_latency: float = 0.0
    start_time: float = 0.0
    end_time: float = 0.0
    
    def get_completion_percentage(self) -> float:
        if self.total_chunks == 0:
            return 0.0
        return (self.acked_chunks / self.total_chunks) * 100
        
    def get_duration(self) -> float:
        if self.end_time > 0:
            return self.end_time - self.start_time
        return time.time() - self.start_time if self.start_time > 0 else 0.0

class ACKManager:
    """
    Manages acknowledgment tracking for chunk transfers
    Provides timeout detection and retry coordination
    """
    
    def __init__(self, ack_timeout: float = 2.0, max_retries: int = 3):
        """
        Initialize ACK manager
        
        Args:
            ack_timeout: Timeout in seconds for ACK
            max_retries: Maximum retry attempts per chunk
        """
        self.ack_timeout = ack_timeout
        self.max_retries = max_retries
        
        # Chunk tracking
        self.chunks: Dict[int, ChunkACK] = {}
        self.acked_ids: Set[int] = set()
        self.failed_ids: Set[int] = set()
        
        # Statistics
        self.stats = TransferStats()
        
        # Thread safety
        self.lock = threading.RLock()
        
        # Callbacks
        self.on_chunk_acked = None
        self.on_chunk_timeout = None
        self.on_chunk_failed = None
        self.on_all_acked = None
        
    def initialize(self, total_chunks: int, chunk_sizes: List[int] = None):
        """
        Initialize ACK tracking for a transfer
        
        Args:
            total_chunks: Total number of chunks
            chunk_sizes: Optional list of chunk sizes
        """
        with self.lock:
            self.chunks.clear()
            self.acked_ids.clear()
            self.failed_ids.clear()
            
            for chunk_id in range(total_chunks):
                chunk_ack = ChunkACK(chunk_id=chunk_id)
                if chunk_sizes and chunk_id < len(chunk_sizes):
                    chunk_ack.size_bytes = chunk_sizes[chunk_id]
                self.chunks[chunk_id] = chunk_ack
                
            self.stats = TransferStats(
                total_chunks=total_chunks,
                start_time=time.time()
            )
            
        print(f"[ACKManager] Initialized for {total_chunks} chunks")
        
    def mark_sent(self, chunk_id: int, size_bytes: int = 0):
        """
        Mark a chunk as sent (waiting for ACK)
        
        Args:
            chunk_id: Chunk ID
            size_bytes: Size of chunk in bytes
        """
        with self.lock:
            if chunk_id not in self.chunks:
                print(f"[ACKManager] Warning: Unknown chunk {chunk_id}")
                return
                
            chunk = self.chunks[chunk_id]
            chunk.status = ACKStatus.PENDING
            chunk.sent_time = time.time()
            
            if size_bytes > 0:
                chunk.size_bytes = size_bytes
                
            self.stats.pending_chunks = self._count_pending()
            
    def mark_acked(self, chunk_id: int) -> bool:
        """
        Mark a chunk as acknowledged
        
        Args:
            chunk_id: Chunk ID
            
        Returns:
            True if successfully marked
        """
        with self.lock:
            if chunk_id not in self.chunks:
                return False
                
            chunk = self.chunks[chunk_id]
            
            # Already acked
            if chunk.status == ACKStatus.ACKED:
                return True
                
            chunk.status = ACKStatus.ACKED
            chunk.ack_time = time.time()
            
            self.acked_ids.add(chunk_id)
            self.stats.acked_chunks = len(self.acked_ids)
            self.stats.pending_chunks = self._count_pending()
            
            # Update latency stats
            latency = chunk.get_latency()
            if latency > 0:
                if latency < self.stats.min_latency:
                    self.stats.min_latency = latency
                if latency > self.stats.max_latency:
                    self.stats.max_latency = latency
                    
                # Update average
                total_latency = self.stats.avg_latency * (self.stats.acked_chunks - 1)
                self.stats.avg_latency = (total_latency + latency) / self.stats.acked_chunks
                
            # Check if all acked
            if self.stats.acked_chunks == self.stats.total_chunks:
                self.stats.end_time = time.time()
                if self.on_all_acked:
                    self.on_all_acked()
                    
            # Callback
            if self.on_chunk_acked:
                self.on_chunk_acked(chunk_id, chunk)
                
            return True
            
    def mark_timeout(self, chunk_id: int) -> bool:
        """
        Mark a chunk as timed out
        
        Args:
            chunk_id: Chunk ID
            
        Returns:
            True if chunk can be retried
        """
        with self.lock:
            if chunk_id not in self.chunks:
                return False
                
            chunk = self.chunks[chunk_id]
            
            # Already acked
            if chunk.status == ACKStatus.ACKED:
                return False
                
            chunk.status = ACKStatus.TIMEOUT
            
            # Callback
            if self.on_chunk_timeout:
                self.on_chunk_timeout(chunk_id, chunk)
                
            # Check if can retry
            return chunk.can_retry(self.max_retries)
            
    def mark_failed(self, chunk_id: int):
        """
        Mark a chunk as permanently failed
        
        Args:
            chunk_id: Chunk ID
        """
        with self.lock:
            if chunk_id not in self.chunks:
                return
                
            chunk = self.chunks[chunk_id]
            chunk.status = ACKStatus.FAILED
            
            self.failed_ids.add(chunk_id)
            self.stats.failed_chunks = len(self.failed_ids)
            self.stats.pending_chunks = self._count_pending()
            
            # Callback
            if self.on_chunk_failed:
                self.on_chunk_failed(chunk_id, chunk)
                
            print(f"[ACKManager] ❌ Chunk {chunk_id} permanently failed")
            
    def increment_retry(self, chunk_id: int) -> int:
        """
        Increment retry count for a chunk
        
        Args:
            chunk_id: Chunk ID
            
        Returns:
            New retry count
        """
        with self.lock:
            if chunk_id not in self.chunks:
                return 0
                
            chunk = self.chunks[chunk_id]
            chunk.retry_count += 1
            chunk.status = ACKStatus.PENDING
            chunk.sent_time = time.time()
            
            self.stats.retry_count += 1
            
            # Check if exceeded max retries
            if chunk.retry_count >= self.max_retries:
                self.mark_failed(chunk_id)
                
            return chunk.retry_count
            
    def check_timeouts(self) -> List[int]:
        """
        Check for chunks that have timed out
        
        Returns:
            List of chunk IDs that timed out
        """
        timeout_chunks = []
        current_time = time.time()
        
        with self.lock:
            for chunk_id, chunk in self.chunks.items():
                if chunk.status == ACKStatus.PENDING:
                    if current_time - chunk.sent_time > self.ack_timeout:
                        if self.mark_timeout(chunk_id):
                            timeout_chunks.append(chunk_id)
                            
        return timeout_chunks
        
    def get_missing_acks(self) -> List[int]:
        """
        Get list of chunks that haven't been ACKed
        
        Returns:
            List of unacked chunk IDs
        """
        with self.lock:
            all_ids = set(self.chunks.keys())
            return sorted(list(all_ids - self.acked_ids))
            
    def get_pending_chunks(self) -> List[int]:
        """
        Get list of chunks currently pending ACK
        
        Returns:
            List of pending chunk IDs
        """
        with self.lock:
            return [cid for cid, chunk in self.chunks.items() 
                   if chunk.status == ACKStatus.PENDING]
                   
    def get_chunks_for_retry(self) -> List[int]:
        """
        Get list of chunks that need retry
        
        Returns:
            List of chunk IDs to retry
        """
        with self.lock:
            retry_chunks = []
            for chunk_id, chunk in self.chunks.items():
                if chunk.status == ACKStatus.TIMEOUT and chunk.can_retry(self.max_retries):
                    retry_chunks.append(chunk_id)
            return retry_chunks
            
    def is_complete(self) -> bool:
        """Check if all chunks have been ACKed"""
        with self.lock:
            return self.stats.acked_chunks == self.stats.total_chunks
            
    def has_failures(self) -> bool:
        """Check if any chunks have failed"""
        with self.lock:
            return self.stats.failed_chunks > 0
            
    def get_completion_percentage(self) -> float:
        """Get completion percentage"""
        return self.stats.get_completion_percentage()
        
    def get_chunk_status(self, chunk_id: int) -> Optional[ACKStatus]:
        """Get status of a specific chunk"""
        with self.lock:
            chunk = self.chunks.get(chunk_id)
            return chunk.status if chunk else None
            
    def get_stats(self) -> TransferStats:
        """Get transfer statistics"""
        with self.lock:
            # Update pending count
            self.stats.pending_chunks = self._count_pending()
            return self.stats
            
    def _count_pending(self) -> int:
        """Count pending chunks"""
        return sum(1 for chunk in self.chunks.values() 
                  if chunk.status == ACKStatus.PENDING)
                  
    def reset(self):
        """Reset ACK manager"""
        with self.lock:
            self.chunks.clear()
            self.acked_ids.clear()
            self.failed_ids.clear()
            self.stats = TransferStats()
            
    def set_callbacks(self, on_acked=None, on_timeout=None, 
                      on_failed=None, on_all_acked=None):
        """Set callback functions"""
        self.on_chunk_acked = on_acked
        self.on_chunk_timeout = on_timeout
        self.on_chunk_failed = on_failed
        self.on_all_acked = on_all_acked


class ACKReceiver:
    """
    Receives and processes ACK messages from the network
    Works with ACKManager to track acknowledgment status
    """
    
    def __init__(self, ack_manager: ACKManager):
        """
        Initialize ACK receiver
        
        Args:
            ack_manager: ACKManager instance
        """
        self.ack_manager = ack_manager
        
    def process_ack(self, message: dict) -> bool:
        """
        Process an ACK message
        
        Args:
            message: ACK message dictionary
            
        Returns:
            True if ACK processed successfully
        """
        chunk_id = message.get('chunk_id')
        
        if chunk_id is None:
            return False
            
        return self.ack_manager.mark_acked(chunk_id)
        
    def process_batch_ack(self, message: dict) -> int:
        """
        Process a batch ACK message
        
        Args:
            message: Batch ACK message with 'chunk_ids' list
            
        Returns:
            Number of chunks ACKed
        """
        chunk_ids = message.get('chunk_ids', [])
        count = 0
        
        for chunk_id in chunk_ids:
            if self.ack_manager.mark_acked(chunk_id):
                count += 1
                
        return count


class ACKSender:
    """
    Sends ACK messages for received chunks
    """
    
    def __init__(self, transfer_id: str):
        """
        Initialize ACK sender
        
        Args:
            transfer_id: Transfer ID
        """
        self.transfer_id = transfer_id
        self.received_chunks: Set[int] = set()
        self.pending_acks: List[int] = []
        self.lock = threading.Lock()
        
        # Configuration
        self.batch_size = 10  # Send ACKs in batches
        self.batch_timeout = 0.5  # Seconds to wait for batching
        
    def add_received_chunk(self, chunk_id: int):
        """
        Add a received chunk to ACK queue
        
        Args:
            chunk_id: Received chunk ID
        """
        with self.lock:
            if chunk_id not in self.received_chunks:
                self.received_chunks.add(chunk_id)
                self.pending_acks.append(chunk_id)
                
    def get_ack_message(self, chunk_id: int) -> dict:
        """
        Create ACK message for a single chunk
        
        Args:
            chunk_id: Chunk ID
            
        Returns:
            ACK message dictionary
        """
        return {
            'type': 'ack',
            'transfer_id': self.transfer_id,
            'chunk_id': chunk_id,
            'timestamp': time.time()
        }
        
    def get_batch_ack_message(self) -> Optional[dict]:
        """
        Get batch ACK message for pending chunks
        
        Returns:
            Batch ACK message or None if no pending ACKs
        """
        with self.lock:
            if not self.pending_acks:
                return None
                
            chunk_ids = self.pending_acks.copy()
            self.pending_acks.clear()
            
        return {
            'type': 'batch_ack',
            'transfer_id': self.transfer_id,
            'chunk_ids': chunk_ids,
            'count': len(chunk_ids),
            'timestamp': time.time()
        }
        
    def has_pending_acks(self) -> bool:
        """Check if there are pending ACKs"""
        with self.lock:
            return len(self.pending_acks) > 0
            
    def get_pending_count(self) -> int:
        """Get number of pending ACKs"""
        with self.lock:
            return len(self.pending_acks)
            
    def clear(self):
        """Clear all pending ACKs"""
        with self.lock:
            self.pending_acks.clear()
            self.received_chunks.clear()


# ============================================
# TEST FUNCTION
# ============================================

def test_ack_manager():
    """Test ACK manager"""
    print("📋 ACK Manager Test")
    print("=" * 50)
    
    # Create ACK manager
    print("\n1️⃣ Creating ACK manager...")
    ack_manager = ACKManager(ack_timeout=1.0, max_retries=3)
    
    def on_chunk_acked(chunk_id, chunk):
        print(f"   ✅ Chunk {chunk_id} ACKED (latency: {chunk.get_latency():.3f}s)")
        
    def on_chunk_timeout(chunk_id, chunk):
        print(f"   ⏰ Chunk {chunk_id} TIMEOUT (retry {chunk.retry_count}/{ack_manager.max_retries})")
        
    def on_all_acked():
        print(f"   🎉 ALL CHUNKS ACKED!")
        
    ack_manager.set_callbacks(
        on_acked=on_chunk_acked,
        on_timeout=on_chunk_timeout,
        on_all_acked=on_all_acked
    )
    
    # Initialize for 10 chunks
    print("\n2️⃣ Initializing for 10 chunks...")
    ack_manager.initialize(10)
    
    # Simulate sending chunks
    print("\n3️⃣ Simulating chunk sends...")
    for i in range(10):
        ack_manager.mark_sent(i)
        print(f"   Sent chunk {i}")
        
    # Simulate receiving ACKs
    print("\n4️⃣ Simulating ACKs...")
    time.sleep(0.5)
    ack_manager.mark_acked(0)
    ack_manager.mark_acked(1)
    ack_manager.mark_acked(2)
    
    # Check timeouts
    print("\n5️⃣ Checking timeouts...")
    time.sleep(1.5)  # Wait for timeout
    timeout_chunks = ack_manager.check_timeouts()
    print(f"   Timed out chunks: {timeout_chunks}")
    
    # Retry timed out chunks
    for chunk_id in timeout_chunks:
        ack_manager.increment_retry(chunk_id)
        ack_manager.mark_sent(chunk_id)
        print(f"   Retrying chunk {chunk_id}")
        
    # ACK remaining chunks
    print("\n6️⃣ ACKing remaining chunks...")
    for i in range(3, 10):
        ack_manager.mark_acked(i)
        
    # Get statistics
    print("\n7️⃣ Transfer Statistics:")
    stats = ack_manager.get_stats()
    print(f"   Total chunks: {stats.total_chunks}")
    print(f"   ACKed chunks: {stats.acked_chunks}")
    print(f"   Failed chunks: {stats.failed_chunks}")
    print(f"   Retry count: {stats.retry_count}")
    print(f"   Completion: {stats.get_completion_percentage():.1f}%")
    print(f"   Duration: {stats.get_duration():.2f}s")
    print(f"   Avg latency: {stats.avg_latency:.3f}s")
    
    # Test ACK sender/receiver
    print("\n8️⃣ Testing ACK sender/receiver...")
    
    ack_sender = ACKSender("test-transfer-123")
    ack_receiver = ACKReceiver(ack_manager)
    
    # Reset for test
    ack_manager.initialize(5)
    
    # Simulate receiving chunks
    for i in range(5):
        ack_sender.add_received_chunk(i)
        
    # Get batch ACK
    batch_ack = ack_sender.get_batch_ack_message()
    print(f"   Batch ACK: {batch_ack['count']} chunks")
    
    # Process batch ACK
    processed = ack_receiver.process_batch_ack(batch_ack)
    print(f"   Processed: {processed} ACKs")
    
    print("\n✅ ACK Manager test complete!")


if __name__ == "__main__":
    test_ack_manager()
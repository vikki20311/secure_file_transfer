"""
reliability/retry_queue.py
Intelligent retry queue with exponential backoff
Manages failed chunk retransmission with adaptive timing
"""

import time
import threading
import queue
from typing import Dict, List, Set, Optional, Callable, Tuple
from dataclasses import dataclass, field
from enum import Enum
from collections import deque
import heapq

class RetryPriority(Enum):
    HIGH = 1      # Critical chunks (beginning/end of file)
    NORMAL = 2    # Regular chunks
    LOW = 3       # Less important chunks

class RetryStrategy(Enum):
    FIXED = "fixed"              # Fixed delay between retries
    EXPONENTIAL = "exponential"  # Exponential backoff
    ADAPTIVE = "adaptive"        # Adapt based on network conditions

@dataclass
class RetryItem:
    """Item in retry queue"""
    chunk_id: int
    priority: RetryPriority = RetryPriority.NORMAL
    retry_count: int = 0
    last_retry_time: float = 0.0
    next_retry_time: float = 0.0
    size_bytes: int = 0
    failure_reason: str = ""
    
    def __lt__(self, other):
        """For priority queue ordering"""
        if self.priority != other.priority:
            return self.priority.value < other.priority.value
        return self.next_retry_time < other.next_retry_time

@dataclass
class RetryStats:
    """Retry statistics"""
    total_retries: int = 0
    successful_retries: int = 0
    failed_retries: int = 0
    max_retry_count: int = 0
    avg_retry_delay: float = 0.0
    current_queue_size: int = 0
    
    def get_success_rate(self) -> float:
        if self.total_retries == 0:
            return 100.0
        return (self.successful_retries / self.total_retries) * 100

class RetryQueue:
    """
    Intelligent retry queue with exponential backoff
    Manages failed chunks and schedules retransmission
    """
    
    def __init__(self, max_retries: int = 3, 
                 strategy: RetryStrategy = RetryStrategy.EXPONENTIAL,
                 base_delay: float = 1.0,
                 max_delay: float = 30.0):
        """
        Initialize retry queue
        
        Args:
            max_retries: Maximum retry attempts per chunk
            strategy: Retry strategy (fixed/exponential/adaptive)
            base_delay: Base delay in seconds
            max_delay: Maximum delay in seconds
        """
        self.max_retries = max_retries
        self.strategy = strategy
        self.base_delay = base_delay
        self.max_delay = max_delay
        
        # Priority queue for retry items
        self.retry_heap: List[RetryItem] = []
        self.retry_map: Dict[int, RetryItem] = {}
        self.heap_lock = threading.Lock()
        
        # Tracking
        self.completed_chunks: Set[int] = set()
        self.failed_chunks: Set[int] = set()
        self.active_retries: Set[int] = set()
        
        # Statistics
        self.stats = RetryStats()
        
        # Callbacks
        self.on_retry_scheduled = None
        self.on_retry_success = None
        self.on_retry_failed = None
        self.on_max_retries_exceeded = None
        
        # Adaptive learning
        self.network_latency_history = deque(maxlen=10)
        self.successful_retry_delays = deque(maxlen=20)
        
    def add_chunk(self, chunk_id: int, priority: RetryPriority = RetryPriority.NORMAL,
                  size_bytes: int = 0, failure_reason: str = ""):
        """
        Add a chunk to retry queue
        
        Args:
            chunk_id: Chunk ID to retry
            priority: Retry priority
            size_bytes: Size of chunk in bytes
            failure_reason: Reason for failure
        """
        with self.heap_lock:
            # Check if already in queue
            if chunk_id in self.retry_map:
                item = self.retry_map[chunk_id]
                
                # Check if exceeded max retries
                if item.retry_count >= self.max_retries:
                    self._mark_failed(chunk_id, item)
                    return
                    
                # Update existing item
                item.retry_count += 1
                item.failure_reason = failure_reason
                item.last_retry_time = time.time()
                item.next_retry_time = self._calculate_next_retry_time(item)
                
                # Re-heapify
                heapq.heapify(self.retry_heap)
            else:
                # Create new retry item
                item = RetryItem(
                    chunk_id=chunk_id,
                    priority=priority,
                    retry_count=1,
                    last_retry_time=time.time(),
                    size_bytes=size_bytes,
                    failure_reason=failure_reason
                )
                item.next_retry_time = self._calculate_next_retry_time(item)
                
                self.retry_map[chunk_id] = item
                heapq.heappush(self.retry_heap, item)
                
            self.stats.total_retries += 1
            self.stats.max_retry_count = max(self.stats.max_retry_count, item.retry_count)
            self.stats.current_queue_size = len(self.retry_heap)
            
            # Callback
            if self.on_retry_scheduled:
                self.on_retry_scheduled(chunk_id, item)
                
            print(f"[RetryQueue] Chunk {chunk_id} scheduled for retry ({item.retry_count}/{self.max_retries})")
            print(f"             Delay: {item.next_retry_time - item.last_retry_time:.2f}s")
            
    def add_batch(self, chunk_ids: List[int], 
                  priority: RetryPriority = RetryPriority.NORMAL,
                  failure_reason: str = ""):
        """
        Add multiple chunks to retry queue
        
        Args:
            chunk_ids: List of chunk IDs
            priority: Retry priority
            failure_reason: Reason for failure
        """
        for chunk_id in chunk_ids:
            self.add_chunk(chunk_id, priority, failure_reason=failure_reason)
            
    def _calculate_next_retry_time(self, item: RetryItem) -> float:
        """
        Calculate next retry time based on strategy
        
        Args:
            item: Retry item
            
        Returns:
            Next retry timestamp
        """
        current_time = time.time()
        
        if self.strategy == RetryStrategy.FIXED:
            delay = self.base_delay
            
        elif self.strategy == RetryStrategy.EXPONENTIAL:
            # Exponential backoff: base_delay * 2^retry_count
            delay = self.base_delay * (2 ** (item.retry_count - 1))
            delay = min(delay, self.max_delay)
            
        else:  # ADAPTIVE
            delay = self._calculate_adaptive_delay(item)
            
        # Add jitter (±10%) to prevent thundering herd
        import random
        jitter = delay * 0.1 * (random.random() * 2 - 1)
        delay = max(0.1, delay + jitter)
        
        return current_time + delay
        
    def _calculate_adaptive_delay(self, item: RetryItem) -> float:
        """
        Calculate adaptive delay based on network conditions
        
        Args:
            item: Retry item
            
        Returns:
            Adaptive delay in seconds
        """
        base = self.base_delay * (1.5 ** (item.retry_count - 1))
        
        # Adjust based on network latency
        if self.network_latency_history:
            avg_latency = sum(self.network_latency_history) / len(self.network_latency_history)
            # Longer latency = longer wait
            latency_factor = min(2.0, avg_latency)
            base *= latency_factor
            
        # Adjust based on recent success rates
        if self.successful_retry_delays:
            avg_success_delay = sum(self.successful_retry_delays) / len(self.successful_retry_delays)
            base = min(base, avg_success_delay * 2)
            
        return min(base, self.max_delay)
        
    def get_ready_chunks(self, max_count: int = None) -> List[int]:
        """
        Get chunks that are ready for retry
        
        Args:
            max_count: Maximum number of chunks to return
            
        Returns:
            List of ready chunk IDs
        """
        ready_chunks = []
        current_time = time.time()
        
        with self.heap_lock:
            # Peek at heap without removing
            for item in self.retry_heap:
                if item.next_retry_time <= current_time:
                    if item.chunk_id not in self.active_retries:
                        ready_chunks.append(item.chunk_id)
                        self.active_retries.add(item.chunk_id)
                        
                if max_count and len(ready_chunks) >= max_count:
                    break
                    
        return ready_chunks
        
    def mark_success(self, chunk_id: int, latency: float = 0.0):
        """
        Mark a retry as successful
        
        Args:
            chunk_id: Chunk ID
            latency: Network latency for this retry
        """
        with self.heap_lock:
            if chunk_id in self.retry_map:
                item = self.retry_map[chunk_id]
                
                # Remove from heap
                self.retry_heap = [i for i in self.retry_heap if i.chunk_id != chunk_id]
                heapq.heapify(self.retry_heap)
                
                # Update tracking
                del self.retry_map[chunk_id]
                self.completed_chunks.add(chunk_id)
                self.active_retries.discard(chunk_id)
                
                # Update stats
                self.stats.successful_retries += 1
                self.stats.current_queue_size = len(self.retry_heap)
                
                # Update adaptive learning
                if latency > 0:
                    self.network_latency_history.append(latency)
                    
                retry_delay = item.next_retry_time - item.last_retry_time
                if retry_delay > 0:
                    self.successful_retry_delays.append(retry_delay)
                    
                # Callback
                if self.on_retry_success:
                    self.on_retry_success(chunk_id, item)
                    
                print(f"[RetryQueue] ✅ Chunk {chunk_id} retry successful (attempt {item.retry_count})")
                
    def mark_failed(self, chunk_id: int):
        """
        Mark a retry as failed (will be retried again if within limit)
        
        Args:
            chunk_id: Chunk ID
        """
        with self.heap_lock:
            if chunk_id in self.retry_map:
                item = self.retry_map[chunk_id]
                self.active_retries.discard(chunk_id)
                
                # Will be re-added by add_chunk with incremented retry count
                
    def _mark_failed(self, chunk_id: int, item: RetryItem):
        """
        Mark chunk as permanently failed
        
        Args:
            chunk_id: Chunk ID
            item: Retry item
        """
        # Remove from tracking
        self.retry_heap = [i for i in self.retry_heap if i.chunk_id != chunk_id]
        heapq.heapify(self.retry_heap)
        
        if chunk_id in self.retry_map:
            del self.retry_map[chunk_id]
            
        self.failed_chunks.add(chunk_id)
        self.active_retries.discard(chunk_id)
        
        self.stats.failed_retries += 1
        self.stats.current_queue_size = len(self.retry_heap)
        
        # Callback
        if self.on_max_retries_exceeded:
            self.on_max_retries_exceeded(chunk_id, item)
            
        if self.on_retry_failed:
            self.on_retry_failed(chunk_id, item)
            
        print(f"[RetryQueue] ❌ Chunk {chunk_id} failed permanently after {item.retry_count} retries")
        
    def update_network_latency(self, latency: float):
        """
        Update network latency for adaptive retry
        
        Args:
            latency: Measured network latency in seconds
        """
        self.network_latency_history.append(latency)
        
    def get_stats(self) -> RetryStats:
        """Get retry statistics"""
        with self.heap_lock:
            self.stats.current_queue_size = len(self.retry_heap)
            
            # Calculate average retry delay
            if self.successful_retry_delays:
                self.stats.avg_retry_delay = sum(self.successful_retry_delays) / len(self.successful_retry_delays)
                
            return self.stats
            
    def get_queue_size(self) -> int:
        """Get current queue size"""
        with self.heap_lock:
            return len(self.retry_heap)
            
    def get_pending_chunks(self) -> List[int]:
        """Get all pending chunk IDs in queue"""
        with self.heap_lock:
            return [item.chunk_id for item in self.retry_heap]
            
    def is_empty(self) -> bool:
        """Check if queue is empty"""
        with self.heap_lock:
            return len(self.retry_heap) == 0
            
    def has_failures(self) -> bool:
        """Check if any chunks have permanently failed"""
        with self.heap_lock:
            return len(self.failed_chunks) > 0
            
    def clear(self):
        """Clear the retry queue"""
        with self.heap_lock:
            self.retry_heap.clear()
            self.retry_map.clear()
            self.completed_chunks.clear()
            self.failed_chunks.clear()
            self.active_retries.clear()
            self.stats = RetryStats()
            
    def set_priority(self, chunk_id: int, priority: RetryPriority):
        """
        Change priority of a chunk in queue
        
        Args:
            chunk_id: Chunk ID
            priority: New priority
        """
        with self.heap_lock:
            if chunk_id in self.retry_map:
                self.retry_map[chunk_id].priority = priority
                heapq.heapify(self.retry_heap)


class RetryScheduler:
    """
    Schedules and executes retries from RetryQueue
    Runs in background thread
    """
    
    def __init__(self, retry_queue: RetryQueue, 
                 retry_callback: Callable[[int], bool],
                 check_interval: float = 0.5):
        """
        Initialize retry scheduler
        
        Args:
            retry_queue: RetryQueue instance
            retry_callback: Function to execute retry, returns True if successful
            check_interval: How often to check for ready chunks
        """
        self.retry_queue = retry_queue
        self.retry_callback = retry_callback
        self.check_interval = check_interval
        
        self.is_running = False
        self.scheduler_thread = None
        self.max_concurrent = 3  # Max concurrent retries
        
    def start(self):
        """Start retry scheduler"""
        if self.is_running:
            return
            
        self.is_running = True
        self.scheduler_thread = threading.Thread(target=self._scheduler_loop, daemon=True)
        self.scheduler_thread.start()
        
        print("[RetryScheduler] ✅ Started")
        
    def stop(self):
        """Stop retry scheduler"""
        self.is_running = False
        if self.scheduler_thread:
            self.scheduler_thread.join(timeout=2.0)
        print("[RetryScheduler] Stopped")
        
    def _scheduler_loop(self):
        """Main scheduler loop"""
        while self.is_running:
            try:
                # Check if we can execute more retries
                current_active = len(self.retry_queue.active_retries)
                
                if current_active < self.max_concurrent:
                    # Get ready chunks
                    ready_count = self.max_concurrent - current_active
                    ready_chunks = self.retry_queue.get_ready_chunks(ready_count)
                    
                    # Execute retries in separate threads
                    for chunk_id in ready_chunks:
                        thread = threading.Thread(
                            target=self._execute_retry,
                            args=(chunk_id,),
                            daemon=True
                        )
                        thread.start()
                        
            except Exception as e:
                print(f"[RetryScheduler] Error: {e}")
                
            time.sleep(self.check_interval)
            
    def _execute_retry(self, chunk_id: int):
        """
        Execute a single retry
        
        Args:
            chunk_id: Chunk ID to retry
        """
        start_time = time.time()
        
        try:
            success = self.retry_callback(chunk_id)
            latency = time.time() - start_time
            
            if success:
                self.retry_queue.mark_success(chunk_id, latency)
            else:
                self.retry_queue.mark_failed(chunk_id)
                # Re-add for another retry
                self.retry_queue.add_chunk(chunk_id, failure_reason="Retry callback returned False")
                
        except Exception as e:
            self.retry_queue.mark_failed(chunk_id)
            self.retry_queue.add_chunk(chunk_id, failure_reason=str(e))


# ============================================
# TEST FUNCTION
# ============================================

def test_retry_queue():
    """Test retry queue"""
    print("🔄 Retry Queue Test")
    print("=" * 50)
    
    # Create retry queue
    print("\n1️⃣ Creating retry queue...")
    retry_queue = RetryQueue(
        max_retries=3,
        strategy=RetryStrategy.EXPONENTIAL,
        base_delay=0.5,
        max_delay=5.0
    )
    
    def on_retry_scheduled(chunk_id, item):
        print(f"   📅 Chunk {chunk_id} scheduled (retry {item.retry_count})")
        
    def on_retry_success(chunk_id, item):
        print(f"   ✅ Chunk {chunk_id} succeeded after {item.retry_count} retries")
        
    retry_queue.on_retry_scheduled = on_retry_scheduled
    retry_queue.on_retry_success = on_retry_success
    
    # Add chunks to retry
    print("\n2️⃣ Adding failed chunks...")
    for i in range(5):
        retry_queue.add_chunk(i, failure_reason="Network timeout")
        
    print(f"\n3️⃣ Queue size: {retry_queue.get_queue_size()}")
    
    # Simulate retry callback
    retry_attempts = {}
    
    def retry_callback(chunk_id):
        retry_attempts[chunk_id] = retry_attempts.get(chunk_id, 0) + 1
        # Succeed on 2nd attempt
        return retry_attempts[chunk_id] >= 2
        
    # Create and start scheduler
    print("\n4️⃣ Starting retry scheduler...")
    scheduler = RetryScheduler(retry_queue, retry_callback, check_interval=0.3)
    scheduler.start()
    
    # Wait for retries
    print("\n5️⃣ Processing retries...")
    time.sleep(3)
    
    # Check results
    print("\n6️⃣ Results:")
    stats = retry_queue.get_stats()
    print(f"   Total retries: {stats.total_retries}")
    print(f"   Successful: {stats.successful_retries}")
    print(f"   Failed: {stats.failed_retries}")
    print(f"   Success rate: {stats.get_success_rate():.1f}%")
    print(f"   Avg delay: {stats.avg_retry_delay:.2f}s")
    
    scheduler.stop()
    
    # Test adaptive strategy
    print("\n7️⃣ Testing adaptive strategy...")
    adaptive_queue = RetryQueue(
        max_retries=3,
        strategy=RetryStrategy.ADAPTIVE,
        base_delay=0.5
    )
    
    # Simulate network conditions
    adaptive_queue.update_network_latency(0.1)
    adaptive_queue.update_network_latency(0.5)
    adaptive_queue.update_network_latency(0.3)
    
    adaptive_queue.add_chunk(100, failure_reason="Test")
    item = adaptive_queue.retry_map[100]
    print(f"   Adaptive delay: {item.next_retry_time - item.last_retry_time:.2f}s")
    
    print("\n✅ Retry Queue test complete!")


if __name__ == "__main__":
    test_retry_queue()
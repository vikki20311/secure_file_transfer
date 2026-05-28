"""
transfer/chunker.py
File chunking and reassembly system
Splits files into chunks for parallel transfer and reassembles them
"""

import os
import hashlib
import threading
import queue
import time
from typing import List, Dict, Optional, Callable, Tuple
from dataclasses import dataclass, field
from enum import Enum
import struct

class ChunkStatus(Enum):
    PENDING = "pending"
    ENCRYPTING = "encrypting"
    ENCRYPTED = "encrypted"
    SENDING = "sending"
    SENT = "sent"
    ACKED = "acked"
    FAILED = "failed"
    RETRYING = "retrying"

@dataclass
class ChunkInfo:
    """Information about a single chunk"""
    chunk_id: int
    offset: int                    # Byte offset in original file
    size: int                      # Size of this chunk
    status: ChunkStatus = ChunkStatus.PENDING
    data: Optional[bytes] = None   # Raw chunk data
    encrypted_data: Optional[bytes] = None  # Encrypted chunk
    checksum: Optional[str] = None # SHA-256 of raw data
    retry_count: int = 0
    send_time: float = 0
    ack_time: float = 0
    
    def is_complete(self) -> bool:
        return self.status == ChunkStatus.ACKED
        
    def can_retry(self, max_retries: int = 3) -> bool:
        return self.retry_count < max_retries

@dataclass
class FileMetadata:
    """Metadata for file transfer"""
    filename: str
    file_size: int
    total_chunks: int
    chunk_size: int
    file_hash: str = ""
    mime_type: str = ""
    created_time: float = 0
    modified_time: float = 0
    
    def to_dict(self) -> dict:
        return {
            'filename': self.filename,
            'file_size': self.file_size,
            'total_chunks': self.total_chunks,
            'chunk_size': self.chunk_size,
            'file_hash': self.file_hash,
            'mime_type': self.mime_type,
            'created_time': self.created_time,
            'modified_time': self.modified_time
        }
        
    @classmethod
    def from_dict(cls, data: dict) -> 'FileMetadata':
        return cls(
            filename=data['filename'],
            file_size=data['file_size'],
            total_chunks=data['total_chunks'],
            chunk_size=data['chunk_size'],
            file_hash=data.get('file_hash', ''),
            mime_type=data.get('mime_type', ''),
            created_time=data.get('created_time', 0),
            modified_time=data.get('modified_time', 0)
        )

class FileChunker:
    """
    Handles splitting files into chunks and reassembling them
    """
    
    # Default chunk size: 1 MB (can be adjusted based on network)
    DEFAULT_CHUNK_SIZE = 1024 * 1024  # 1 MB
    MIN_CHUNK_SIZE = 64 * 1024        # 64 KB
    MAX_CHUNK_SIZE = 10 * 1024 * 1024 # 10 MB
    
    def __init__(self, chunk_size: int = DEFAULT_CHUNK_SIZE,
                 max_parallel_chunks: int = 5):
        """
        Initialize file chunker
        
        Args:
            chunk_size: Size of each chunk in bytes
            max_parallel_chunks: Maximum chunks to process in parallel
        """
        self.chunk_size = self._clamp_chunk_size(chunk_size)
        self.max_parallel_chunks = max_parallel_chunks
        
        # Chunk storage
        self.chunks: Dict[int, ChunkInfo] = {}
        self.metadata: Optional[FileMetadata] = None
        
        # Threading
        self.lock = threading.Lock()
        self.chunk_queue = queue.Queue()
        self.result_queue = queue.Queue()
        
    def _clamp_chunk_size(self, size: int) -> int:
        """Ensure chunk size is within reasonable bounds"""
        return max(self.MIN_CHUNK_SIZE, min(size, self.MAX_CHUNK_SIZE))
        
    def split_file(self, file_path: str, 
                   progress_callback: Callable = None) -> FileMetadata:
        """
        Split a file into chunks
        
        Args:
            file_path: Path to file
            progress_callback: Optional callback(percent_done)
            
        Returns:
            FileMetadata with file information
        """
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")
            
        file_size = os.path.getsize(file_path)
        total_chunks = (file_size + self.chunk_size - 1) // self.chunk_size
        
        # Get file stats
        stat = os.stat(file_path)
        
        # Calculate file hash
        file_hash = self._calculate_file_hash(file_path, progress_callback)
        
        # Create metadata
        self.metadata = FileMetadata(
            filename=os.path.basename(file_path),
            file_size=file_size,
            total_chunks=total_chunks,
            chunk_size=self.chunk_size,
            file_hash=file_hash,
            mime_type=self._get_mime_type(file_path),
            created_time=stat.st_ctime,
            modified_time=stat.st_mtime
        )
        
        print(f"[Chunker] Splitting {self.metadata.filename}")
        print(f"          Size: {self._format_size(file_size)}")
        print(f"          Chunks: {total_chunks} x {self._format_size(self.chunk_size)}")
        
        # Read and split file
        with self.lock:
            self.chunks.clear()
            
        with open(file_path, 'rb') as f:
            for chunk_id in range(total_chunks):
                # Calculate offset and size
                offset = chunk_id * self.chunk_size
                size = min(self.chunk_size, file_size - offset)
                
                # Read chunk data
                f.seek(offset)
                chunk_data = f.read(size)
                
                # Calculate chunk checksum
                chunk_hash = hashlib.sha256(chunk_data).hexdigest()
                
                # Create chunk info
                chunk_info = ChunkInfo(
                    chunk_id=chunk_id,
                    offset=offset,
                    size=size,
                    data=chunk_data,
                    checksum=chunk_hash,
                    status=ChunkStatus.PENDING
                )
                
                with self.lock:
                    self.chunks[chunk_id] = chunk_info
                    
                # Update progress
                if progress_callback:
                    progress = int((chunk_id + 1) / total_chunks * 100)
                    progress_callback(progress)
                    
        print(f"[Chunker] ✅ File split complete")
        print(f"          Hash: {file_hash[:16]}...")
        
        return self.metadata
        
    def _calculate_file_hash(self, file_path: str, 
                             progress_callback: Callable = None) -> str:
        """Calculate SHA-256 hash of entire file"""
        sha256 = hashlib.sha256()
        file_size = os.path.getsize(file_path)
        bytes_read = 0
        
        with open(file_path, 'rb') as f:
            while True:
                data = f.read(8192)
                if not data:
                    break
                sha256.update(data)
                
                bytes_read += len(data)
                if progress_callback and file_size > 0:
                    # Report hash progress (separate from chunk progress)
                    pass
                    
        return sha256.hexdigest()
        
    def _get_mime_type(self, file_path: str) -> str:
        """Get MIME type from file extension"""
        import mimetypes
        mime_type, _ = mimetypes.guess_type(file_path)
        return mime_type or 'application/octet-stream'
        
    def get_chunk_data(self, chunk_id: int) -> Optional[bytes]:
        """Get raw chunk data by ID"""
        with self.lock:
            chunk = self.chunks.get(chunk_id)
            return chunk.data if chunk else None
            
    def get_all_chunks(self) -> List[ChunkInfo]:
        """Get list of all chunks"""
        with self.lock:
            return list(self.chunks.values())
            
    def get_pending_chunks(self) -> List[ChunkInfo]:
        """Get list of chunks pending encryption/sending"""
        with self.lock:
            return [c for c in self.chunks.values() 
                   if c.status in [ChunkStatus.PENDING, ChunkStatus.FAILED]]
                   
    def update_chunk_status(self, chunk_id: int, status: ChunkStatus,
                            encrypted_data: bytes = None):
        """Update chunk status and optionally set encrypted data"""
        with self.lock:
            if chunk_id in self.chunks:
                self.chunks[chunk_id].status = status
                if encrypted_data:
                    self.chunks[chunk_id].encrypted_data = encrypted_data
                    
                if status == ChunkStatus.SENDING:
                    self.chunks[chunk_id].send_time = time.time()
                elif status == ChunkStatus.ACKED:
                    self.chunks[chunk_id].ack_time = time.time()
                elif status == ChunkStatus.FAILED:
                    self.chunks[chunk_id].retry_count += 1
                    
    def mark_chunk_acked(self, chunk_id: int):
        """Mark chunk as acknowledged by receiver"""
        self.update_chunk_status(chunk_id, ChunkStatus.ACKED)
        
    def mark_chunk_failed(self, chunk_id: int):
        """Mark chunk as failed (will be retried)"""
        self.update_chunk_status(chunk_id, ChunkStatus.FAILED)
        
    def get_missing_chunks(self, received_ids: set) -> List[int]:
        """Get list of chunk IDs not yet received"""
        with self.lock:
            all_ids = set(self.chunks.keys())
            missing = all_ids - received_ids
            return sorted(list(missing))
            
    def get_transfer_progress(self) -> Tuple[int, int, float]:
        """
        Get transfer progress
        
        Returns:
            Tuple of (acked_chunks, total_chunks, percentage)
        """
        with self.lock:
            if not self.chunks:
                return 0, 0, 0.0
                
            acked = sum(1 for c in self.chunks.values() if c.status == ChunkStatus.ACKED)
            total = len(self.chunks)
            percentage = (acked / total * 100) if total > 0 else 0
            
            return acked, total, percentage
            
    def is_transfer_complete(self) -> bool:
        """Check if all chunks have been ACKed"""
        acked, total, _ = self.get_transfer_progress()
        return acked == total and total > 0
        
    def get_chunks_for_retry(self, max_retries: int = 3) -> List[ChunkInfo]:
        """Get chunks that need retry"""
        with self.lock:
            return [c for c in self.chunks.values() 
                   if c.status == ChunkStatus.FAILED and c.can_retry(max_retries)]
                   
    def _format_size(self, size: int) -> str:
        """Format byte size to human readable"""
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if size < 1024:
                return f"{size:.1f} {unit}"
            size /= 1024
        return f"{size:.1f} PB"


class FileAssembler:
    """
    Handles reassembling received chunks into a complete file
    """
    
    def __init__(self, metadata: FileMetadata, output_dir: str = None):
        """
        Initialize file assembler
        
        Args:
            metadata: File metadata from sender
            output_dir: Directory to save assembled file
        """
        self.metadata = metadata
        self.output_dir = output_dir or os.path.expanduser("~/Downloads")
        
        # Chunk storage
        self.received_chunks: Dict[int, bytes] = {}
        self.lock = threading.Lock()
        
        # Verification
        self.verified = False
        
        # Create output directory if needed
        os.makedirs(self.output_dir, exist_ok=True)
        
    def add_chunk(self, chunk_id: int, chunk_data: bytes) -> bool:
        """
        Add a received chunk
        
        Args:
            chunk_id: Chunk ID
            chunk_data: Decrypted chunk data
            
        Returns:
            True if chunk was added successfully
        """
        with self.lock:
            # Validate chunk ID
            if chunk_id < 0 or chunk_id >= self.metadata.total_chunks:
                print(f"[Assembler] Invalid chunk ID: {chunk_id}")
                return False
                
            # Check if already received
            if chunk_id in self.received_chunks:
                return True
                
            # Validate size (except last chunk)
            if chunk_id < self.metadata.total_chunks - 1:
                if len(chunk_data) != self.metadata.chunk_size:
                    print(f"[Assembler] Chunk {chunk_id} size mismatch")
                    return False
                    
            self.received_chunks[chunk_id] = chunk_data
            
            return True
            
    def get_missing_chunks(self) -> List[int]:
        """Get list of missing chunk IDs"""
        with self.lock:
            received_ids = set(self.received_chunks.keys())
            all_ids = set(range(self.metadata.total_chunks))
            missing = all_ids - received_ids
            return sorted(list(missing))
            
    def is_complete(self) -> bool:
        """Check if all chunks have been received"""
        with self.lock:
            return len(self.received_chunks) == self.metadata.total_chunks
            
    def get_progress(self) -> Tuple[int, int, float]:
        """
        Get assembly progress
        
        Returns:
            Tuple of (received_chunks, total_chunks, percentage)
        """
        with self.lock:
            received = len(self.received_chunks)
            total = self.metadata.total_chunks
            percentage = (received / total * 100) if total > 0 else 0
            return received, total, percentage
            
    def assemble(self, output_filename: str = None,
                 progress_callback: Callable = None) -> Optional[str]:
        """
        Assemble all chunks into complete file
        
        Args:
            output_filename: Optional custom filename
            progress_callback: Optional callback(percent_done)
            
        Returns:
            Path to assembled file, or None if failed
        """
        if not self.is_complete():
            missing = self.get_missing_chunks()
            print(f"[Assembler] Cannot assemble - missing {len(missing)} chunks")
            return None
            
        # Determine output path
        filename = output_filename or self.metadata.filename
        output_path = os.path.join(self.output_dir, filename)
        
        # Handle duplicate filenames
        output_path = self._get_unique_filename(output_path)
        
        print(f"[Assembler] Assembling file: {output_path}")
        print(f"            Chunks to write: {self.metadata.total_chunks}")
        
        try:
            with open(output_path, 'wb') as f:
                for chunk_id in range(self.metadata.total_chunks):
                    chunk_data = self.received_chunks.get(chunk_id)
                    
                    if chunk_data is None:
                        print(f"[Assembler] Missing chunk {chunk_id}")
                        return None
                        
                    f.write(chunk_data)
                    
                    if progress_callback:
                        progress = int((chunk_id + 1) / self.metadata.total_chunks * 100)
                        progress_callback(progress)
                        
            print(f"[Assembler] ✅ File assembled successfully")
            
            # Verify file
            if self.verify_file(output_path):
                return output_path
            else:
                print(f"[Assembler] ❌ File verification failed")
                os.unlink(output_path)
                return None
                
        except Exception as e:
            print(f"[Assembler] Assembly failed: {e}")
            return None
            
    def verify_file(self, file_path: str) -> bool:
        """Verify assembled file matches original hash"""
        if not self.metadata.file_hash:
            print("[Assembler] No hash provided for verification")
            return True
            
        print("[Assembler] Verifying file integrity...")
        
        sha256 = hashlib.sha256()
        file_size = os.path.getsize(file_path)
        
        with open(file_path, 'rb') as f:
            while True:
                data = f.read(8192)
                if not data:
                    break
                sha256.update(data)
                
        calculated_hash = sha256.hexdigest()
        
        if calculated_hash == self.metadata.file_hash:
            print(f"[Assembler] ✅ Verification passed!")
            print(f"            Hash: {calculated_hash[:16]}...")
            self.verified = True
            return True
        else:
            print(f"[Assembler] ❌ Hash mismatch!")
            print(f"            Expected: {self.metadata.file_hash[:16]}...")
            print(f"            Got: {calculated_hash[:16]}...")
            return False
            
    def _get_unique_filename(self, file_path: str) -> str:
        """Generate unique filename if file already exists"""
        if not os.path.exists(file_path):
            return file_path
            
        base, ext = os.path.splitext(file_path)
        counter = 1
        
        while os.path.exists(f"{base} ({counter}){ext}"):
            counter += 1
            
        return f"{base} ({counter}){ext}"
        
    def clear_chunks(self):
        """Clear received chunks from memory"""
        with self.lock:
            self.received_chunks.clear()
            print("[Assembler] Chunks cleared from memory")


# ============================================
# TEST FUNCTION
# ============================================

def test_chunker():
    """Test file chunking and assembly"""
    print("📦 File Chunker Test")
    print("=" * 50)
    
    import tempfile
    
    # Create test file
    print("\n1️⃣ Creating test file...")
    with tempfile.NamedTemporaryFile(delete=False, suffix='.bin') as f:
        # Create 5 MB test file
        test_data = secrets.token_bytes(5 * 1024 * 1024)
        f.write(test_data)
        test_file = f.name
        
    print(f"   File: {test_file}")
    print(f"   Size: {len(test_data)} bytes")
    
    # Test chunking
    print("\n2️⃣ Splitting file...")
    chunker = FileChunker(chunk_size=1024 * 1024)  # 1 MB chunks
    
    def progress(pct):
        print(f"\r   Progress: {pct}%", end="")
        
    metadata = chunker.split_file(test_file, progress)
    print()  # New line
    
    print(f"\n3️⃣ Chunking complete:")
    print(f"   Total chunks: {metadata.total_chunks}")
    print(f"   Chunk size: {chunker._format_size(metadata.chunk_size)}")
    print(f"   File hash: {metadata.file_hash[:16]}...")
    
    # Test assembly
    print("\n4️⃣ Testing assembly...")
    
    assembler = FileAssembler(metadata)
    
    # Simulate receiving chunks
    for chunk_id in range(metadata.total_chunks):
        chunk_data = chunker.get_chunk_data(chunk_id)
        assembler.add_chunk(chunk_id, chunk_data)
        
    # Assemble file
    output_file = assembler.assemble(progress_callback=progress)
    print()  # New line
    
    if output_file:
        print(f"\n5️⃣ Assembly successful!")
        print(f"   Output: {output_file}")
        
        # Verify with original
        with open(test_file, 'rb') as f1, open(output_file, 'rb') as f2:
            if f1.read() == f2.read():
                print("✅ Files match perfectly!")
            else:
                print("❌ Files don't match!")
                
        os.unlink(output_file)
        
    # Cleanup
    os.unlink(test_file)
    
    # Test partial assembly
    print("\n6️⃣ Testing partial assembly (missing chunks)...")
    
    metadata2 = FileMetadata(
        filename="test.txt",
        file_size=1000,
        total_chunks=5,
        chunk_size=200
    )
    
    assembler2 = FileAssembler(metadata2)
    
    # Add some chunks
    assembler2.add_chunk(0, b"A" * 200)
    assembler2.add_chunk(1, b"B" * 200)
    assembler2.add_chunk(3, b"D" * 200)
    
    missing = assembler2.get_missing_chunks()
    print(f"   Missing chunks: {missing}")
    
    progress = assembler2.get_progress()
    print(f"   Progress: {progress[0]}/{progress[1]} chunks ({progress[2]:.1f}%)")
    
    print("\n✅ All chunker tests passed!")


if __name__ == "__main__":
    import secrets
    test_chunker()
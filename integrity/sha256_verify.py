"""
integrity/sha256_verify.py
SHA-256 file integrity verification system
Ensures files are transferred without corruption
"""

import hashlib
import os
import threading
import time
from typing import Callable, Optional, Dict, List, Tuple
from dataclasses import dataclass
from enum import Enum

class VerificationStatus(Enum):
    PENDING = "pending"
    VERIFYING = "verifying"
    VERIFIED = "verified"
    FAILED = "failed"
    MISMATCH = "mismatch"

@dataclass
class VerificationResult:
    """Result of file verification"""
    file_path: str
    expected_hash: str
    actual_hash: str
    status: VerificationStatus
    file_size: int
    verification_time: float
    matches: bool = False
    
    def to_dict(self) -> dict:
        return {
            'file_path': self.file_path,
            'expected_hash': self.expected_hash,
            'actual_hash': self.actual_hash,
            'status': self.status.value,
            'file_size': self.file_size,
            'verification_time': self.verification_time,
            'matches': self.matches
        }

@dataclass
class ChunkVerification:
    """Verification info for a single chunk"""
    chunk_id: int
    expected_hash: str
    actual_hash: str
    matches: bool
    verification_time: float

class HashCalculator:
    """
    Calculates SHA-256 hashes for files and data chunks
    """
    
    def __init__(self):
        self.hash_algorithms = {
            'sha256': hashlib.sha256,
            'sha512': hashlib.sha512,
            'md5': hashlib.md5  # For quick checks only, not secure
        }
        
    def calculate_file_hash(self, file_path: str, 
                            algorithm: str = 'sha256',
                            progress_callback: Callable = None) -> Optional[str]:
        """
        Calculate hash of entire file
        
        Args:
            file_path: Path to file
            algorithm: Hash algorithm (sha256, sha512, md5)
            progress_callback: Optional progress callback(percent)
            
        Returns:
            Hex digest of hash or None
        """
        if not os.path.exists(file_path):
            print(f"[HashCalculator] File not found: {file_path}")
            return None
            
        hash_func = self.hash_algorithms.get(algorithm, hashlib.sha256)()
        file_size = os.path.getsize(file_path)
        bytes_read = 0
        
        try:
            with open(file_path, 'rb') as f:
                while True:
                    chunk = f.read(8192)  # 8 KB chunks
                    if not chunk:
                        break
                        
                    hash_func.update(chunk)
                    bytes_read += len(chunk)
                    
                    if progress_callback and file_size > 0:
                        progress = int((bytes_read / file_size) * 100)
                        progress_callback(progress)
                        
            return hash_func.hexdigest()
            
        except Exception as e:
            print(f"[HashCalculator] Error calculating hash: {e}")
            return None
            
    def calculate_chunk_hash(self, data: bytes, algorithm: str = 'sha256') -> str:
        """
        Calculate hash of a data chunk
        
        Args:
            data: Chunk data
            algorithm: Hash algorithm
            
        Returns:
            Hex digest of hash
        """
        hash_func = self.hash_algorithms.get(algorithm, hashlib.sha256)()
        hash_func.update(data)
        return hash_func.hexdigest()
        
    def calculate_stream_hash(self, data_stream, algorithm: str = 'sha256') -> str:
        """
        Calculate hash from a data stream
        
        Args:
            data_stream: Iterator/generator yielding bytes
            algorithm: Hash algorithm
            
        Returns:
            Hex digest of hash
        """
        hash_func = self.hash_algorithms.get(algorithm, hashlib.sha256)()
        
        for chunk in data_stream:
            hash_func.update(chunk)
            
        return hash_func.hexdigest()
        
    def verify_file(self, file_path: str, expected_hash: str,
                    algorithm: str = 'sha256',
                    progress_callback: Callable = None) -> VerificationResult:
        """
        Verify file against expected hash
        
        Args:
            file_path: Path to file
            expected_hash: Expected hash value
            algorithm: Hash algorithm
            progress_callback: Optional progress callback
            
        Returns:
            VerificationResult
        """
        start_time = time.time()
        file_size = os.path.getsize(file_path) if os.path.exists(file_path) else 0
        
        actual_hash = self.calculate_file_hash(file_path, algorithm, progress_callback)
        
        verification_time = time.time() - start_time
        
        if actual_hash is None:
            return VerificationResult(
                file_path=file_path,
                expected_hash=expected_hash,
                actual_hash="",
                status=VerificationStatus.FAILED,
                file_size=file_size,
                verification_time=verification_time,
                matches=False
            )
            
        matches = actual_hash.lower() == expected_hash.lower()
        
        return VerificationResult(
            file_path=file_path,
            expected_hash=expected_hash,
            actual_hash=actual_hash,
            status=VerificationStatus.VERIFIED if matches else VerificationStatus.MISMATCH,
            file_size=file_size,
            verification_time=verification_time,
            matches=matches
        )

class IntegrityVerifier:
    """
    Complete integrity verification system
    Verifies files chunk by chunk and as a whole
    """
    
    def __init__(self):
        self.hash_calculator = HashCalculator()
        
        # Verification tracking
        self.chunk_verifications: Dict[int, ChunkVerification] = {}
        self.file_verification: Optional[VerificationResult] = None
        
        self.lock = threading.Lock()
        
        # Callbacks
        self.on_chunk_verified = None
        self.on_file_verified = None
        self.on_verification_failed = None
        
    def verify_chunk(self, chunk_id: int, data: bytes, 
                     expected_hash: str) -> bool:
        """
        Verify a single chunk
        
        Args:
            chunk_id: Chunk ID
            data: Chunk data
            expected_hash: Expected SHA-256 hash
            
        Returns:
            True if chunk is valid
        """
        start_time = time.time()
        actual_hash = self.hash_calculator.calculate_chunk_hash(data)
        matches = actual_hash == expected_hash
        verification_time = time.time() - start_time
        
        verification = ChunkVerification(
            chunk_id=chunk_id,
            expected_hash=expected_hash,
            actual_hash=actual_hash,
            matches=matches,
            verification_time=verification_time
        )
        
        with self.lock:
            self.chunk_verifications[chunk_id] = verification
            
        if matches:
            if self.on_chunk_verified:
                self.on_chunk_verified(chunk_id, verification)
        else:
            print(f"[IntegrityVerifier] ❌ Chunk {chunk_id} hash mismatch!")
            print(f"                    Expected: {expected_hash[:16]}...")
            print(f"                    Actual:   {actual_hash[:16]}...")
            
            if self.on_verification_failed:
                self.on_verification_failed(chunk_id, verification)
                
        return matches
        
    def verify_file(self, file_path: str, expected_hash: str,
                    progress_callback: Callable = None) -> VerificationResult:
        """
        Verify complete file
        
        Args:
            file_path: Path to file
            expected_hash: Expected SHA-256 hash
            progress_callback: Optional progress callback
            
        Returns:
            VerificationResult
        """
        print(f"[IntegrityVerifier] Verifying file: {os.path.basename(file_path)}")
        
        result = self.hash_calculator.verify_file(
            file_path, expected_hash, progress_callback=progress_callback
        )
        
        with self.lock:
            self.file_verification = result
            
        if result.matches:
            print(f"[IntegrityVerifier] ✅ File verified successfully!")
            print(f"                    Hash: {result.actual_hash[:16]}...")
            print(f"                    Time: {result.verification_time:.2f}s")
            print(f"                    Speed: {result.file_size / result.verification_time / 1024 / 1024:.2f} MB/s")
            
            if self.on_file_verified:
                self.on_file_verified(result)
        else:
            print(f"[IntegrityVerifier] ❌ File verification failed!")
            print(f"                    Expected: {result.expected_hash[:16]}...")
            print(f"                    Actual:   {result.actual_hash[:16]}...")
            
            if self.on_verification_failed:
                self.on_verification_failed(file_path, result)
                
        return result
        
    def verify_chunks_batch(self, chunks: Dict[int, bytes],
                            expected_hashes: Dict[int, str]) -> Dict[int, bool]:
        """
        Verify multiple chunks in batch
        
        Args:
            chunks: Dictionary of chunk_id -> data
            expected_hashes: Dictionary of chunk_id -> expected_hash
            
        Returns:
            Dictionary of chunk_id -> verification_result
        """
        results = {}
        
        for chunk_id, data in chunks.items():
            expected_hash = expected_hashes.get(chunk_id)
            
            if expected_hash:
                results[chunk_id] = self.verify_chunk(chunk_id, data, expected_hash)
            else:
                results[chunk_id] = False
                
        return results
        
    def get_chunk_verification(self, chunk_id: int) -> Optional[ChunkVerification]:
        """Get verification result for a chunk"""
        with self.lock:
            return self.chunk_verifications.get(chunk_id)
            
    def get_all_chunk_verifications(self) -> Dict[int, ChunkVerification]:
        """Get all chunk verifications"""
        with self.lock:
            return self.chunk_verifications.copy()
            
    def get_file_verification(self) -> Optional[VerificationResult]:
        """Get file verification result"""
        with self.lock:
            return self.file_verification
            
    def get_verification_stats(self) -> dict:
        """Get verification statistics"""
        with self.lock:
            total_chunks = len(self.chunk_verifications)
            verified_chunks = sum(1 for v in self.chunk_verifications.values() if v.matches)
            failed_chunks = total_chunks - verified_chunks
            
            total_time = sum(v.verification_time for v in self.chunk_verifications.values())
            avg_time = total_time / total_chunks if total_chunks > 0 else 0
            
            return {
                'total_chunks': total_chunks,
                'verified_chunks': verified_chunks,
                'failed_chunks': failed_chunks,
                'success_rate': (verified_chunks / total_chunks * 100) if total_chunks > 0 else 0,
                'total_verification_time': total_time,
                'avg_verification_time': avg_time,
                'file_verified': self.file_verification is not None and self.file_verification.matches
            }
            
    def clear(self):
        """Clear all verification data"""
        with self.lock:
            self.chunk_verifications.clear()
            self.file_verification = None


class MerkleTreeVerifier:
    """
    Merkle tree-based verification for efficient integrity checking
    Allows verification of any chunk without full file hash
    """
    
    def __init__(self):
        self.hash_calculator = HashCalculator()
        self.merkle_root = None
        self.merkle_tree: List[List[str]] = []
        self.leaf_hashes: List[str] = []
        
    def build_tree(self, chunks: List[bytes]) -> str:
        """
        Build Merkle tree from chunks
        
        Args:
            chunks: List of chunk data
            
        Returns:
            Merkle root hash
        """
        # Calculate leaf hashes
        self.leaf_hashes = []
        for chunk in chunks:
            chunk_hash = self.hash_calculator.calculate_chunk_hash(chunk)
            self.leaf_hashes.append(chunk_hash)
            
        # Build tree
        self.merkle_tree = [self.leaf_hashes]
        
        current_level = self.leaf_hashes
        
        while len(current_level) > 1:
            next_level = []
            
            for i in range(0, len(current_level), 2):
                if i + 1 < len(current_level):
                    combined = current_level[i] + current_level[i + 1]
                else:
                    combined = current_level[i] + current_level[i]  # Duplicate last
                    
                node_hash = hashlib.sha256(combined.encode()).hexdigest()
                next_level.append(node_hash)
                
            self.merkle_tree.append(next_level)
            current_level = next_level
            
        self.merkle_root = current_level[0] if current_level else ""
        
        return self.merkle_root
        
    def get_proof(self, chunk_index: int) -> List[Tuple[str, bool]]:
        """
        Get Merkle proof for a specific chunk
        
        Args:
            chunk_index: Index of chunk to prove
            
        Returns:
            List of (hash, is_right) tuples for proof
        """
        if chunk_index >= len(self.leaf_hashes):
            return []
            
        proof = []
        current_index = chunk_index
        
        for level in range(len(self.merkle_tree) - 1):
            level_hashes = self.merkle_tree[level]
            
            if current_index % 2 == 0:
                # Left node, need right sibling
                if current_index + 1 < len(level_hashes):
                    proof.append((level_hashes[current_index + 1], True))
                else:
                    proof.append((level_hashes[current_index], True))
            else:
                # Right node, need left sibling
                proof.append((level_hashes[current_index - 1], False))
                
            current_index //= 2
            
        return proof
        
    def verify_proof(self, chunk_hash: str, chunk_index: int,
                     proof: List[Tuple[str, bool]]) -> bool:
        """
        Verify a chunk using Merkle proof
        
        Args:
            chunk_hash: Hash of the chunk
            chunk_index: Index of the chunk
            proof: Merkle proof from get_proof()
            
        Returns:
            True if proof is valid
        """
        current_hash = chunk_hash
        current_index = chunk_index
        
        for sibling_hash, is_right in proof:
            if is_right:
                combined = current_hash + sibling_hash
            else:
                combined = sibling_hash + current_hash
                
            current_hash = hashlib.sha256(combined.encode()).hexdigest()
            current_index //= 2
            
        return current_hash == self.merkle_root
        
    def get_merkle_root(self) -> Optional[str]:
        """Get Merkle root hash"""
        return self.merkle_root


# ============================================
# TEST FUNCTION
# ============================================

def test_integrity_verifier():
    """Test integrity verification"""
    print("🔒 Integrity Verifier Test")
    print("=" * 50)
    
    import tempfile
    import secrets
    
    # Create test file
    print("\n1️⃣ Creating test file...")
    with tempfile.NamedTemporaryFile(delete=False, suffix='.bin') as f:
        test_data = secrets.token_bytes(5 * 1024 * 1024)  # 5 MB
        f.write(test_data)
        test_file = f.name
        
    print(f"   File: {test_file}")
    print(f"   Size: {len(test_data)} bytes")
    
    # Calculate hash
    print("\n2️⃣ Calculating file hash...")
    calculator = HashCalculator()
    
    def progress(pct):
        print(f"\r   Progress: {pct}%", end="")
        
    file_hash = calculator.calculate_file_hash(test_file, progress_callback=progress)
    print()  # New line
    print(f"   SHA-256: {file_hash[:32]}...")
    
    # Verify file
    print("\n3️⃣ Verifying file...")
    verifier = IntegrityVerifier()
    
    def on_file_verified(result):
        print(f"\n   ✅ File verified in {result.verification_time:.2f}s")
        
    verifier.on_file_verified = on_file_verified
    
    result = verifier.verify_file(test_file, file_hash, progress_callback=progress)
    print()  # New line
    
    if result.matches:
        print("   ✅ Verification successful!")
    else:
        print("   ❌ Verification failed!")
        
    # Test chunk verification
    print("\n4️⃣ Testing chunk verification...")
    
    # Split file into chunks
    chunk_size = 1024 * 1024  # 1 MB
    chunks = []
    chunk_hashes = []
    
    with open(test_file, 'rb') as f:
        chunk_id = 0
        while True:
            chunk_data = f.read(chunk_size)
            if not chunk_data:
                break
            chunks.append(chunk_data)
            chunk_hash = calculator.calculate_chunk_hash(chunk_data)
            chunk_hashes.append(chunk_hash)
            chunk_id += 1
            
    print(f"   Split into {len(chunks)} chunks")
    
    # Verify each chunk
    for i, (chunk, expected_hash) in enumerate(zip(chunks, chunk_hashes)):
        valid = verifier.verify_chunk(i, chunk, expected_hash)
        
    stats = verifier.get_verification_stats()
    print(f"   Verified: {stats['verified_chunks']}/{stats['total_chunks']} chunks")
    print(f"   Success rate: {stats['success_rate']:.1f}%")
    print(f"   Total time: {stats['total_verification_time']:.3f}s")
    
    # Test Merkle tree
    print("\n5️⃣ Testing Merkle tree...")
    merkle = MerkleTreeVerifier()
    root = merkle.build_tree(chunks)
    print(f"   Merkle root: {root[:32]}...")
    
    # Get and verify proof for chunk 2
    if len(chunks) > 2:
        proof = merkle.get_proof(2)
        print(f"   Proof for chunk 2: {len(proof)} steps")
        
        valid = merkle.verify_proof(chunk_hashes[2], 2, proof)
        print(f"   Proof verification: {'✅ Valid' if valid else '❌ Invalid'}")
        
    # Test mismatch detection
    print("\n6️⃣ Testing mismatch detection...")
    
    # Corrupt the file
    with open(test_file, 'r+b') as f:
        f.seek(1000)
        f.write(b'corrupted')
        
    result = verifier.verify_file(test_file, file_hash)
    
    if not result.matches:
        print("   ✅ Corruption detected successfully!")
    else:
        print("   ❌ Failed to detect corruption!")
        
    # Cleanup
    os.unlink(test_file)
    
    print("\n✅ Integrity Verifier test complete!")


if __name__ == "__main__":
    test_integrity_verifier()
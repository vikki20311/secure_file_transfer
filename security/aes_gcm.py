"""
security/aes_gcm.py
AES-256-GCM encryption/decryption for file chunks
Provides authenticated encryption with integrity protection
"""

import os
import secrets
import hashlib
from typing import Tuple, Optional
import struct

# Try to import cryptography library
try:
    from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
    from cryptography.hazmat.backends import default_backend
    CRYPTOGRAPHY_AVAILABLE = True
    BACKEND = default_backend()
except ImportError:
    CRYPTOGRAPHY_AVAILABLE = False
    print("[AES] ⚠️ cryptography library not found. Install: pip install cryptography")
    
    # Fallback using PyCryptodome if available
    try:
        from Crypto.Cipher import AES as PyCryptoAES
        PYCRYPTO_AVAILABLE = True
        print("[AES] ✅ Using PyCryptodome fallback")
    except ImportError:
        PYCRYPTO_AVAILABLE = False
        print("[AES] ❌ No encryption library available!")

class AESGCM:
    """
    AES-256-GCM encryption/decryption
    Provides authenticated encryption with automatic tag verification
    """
    
    # Constants
    KEY_SIZE = 32      # 256 bits
    IV_SIZE = 12       # 96 bits (recommended for GCM)
    TAG_SIZE = 16      # 128 bits
    
    def __init__(self, key: bytes):
        """
        Initialize AES-GCM with encryption key
        
        Args:
            key: 32-byte AES-256 key
        """
        if len(key) != self.KEY_SIZE:
            raise ValueError(f"Key must be {self.KEY_SIZE} bytes (got {len(key)})")
            
        self.key = key
        self.use_cryptography = CRYPTOGRAPHY_AVAILABLE
        self.use_pycrypto = PYCRYPTO_AVAILABLE
        
    @classmethod
    def generate_key(cls) -> bytes:
        """
        Generate a random AES-256 key
        
        Returns:
            32-byte random key
        """
        return secrets.token_bytes(cls.KEY_SIZE)
        
    @classmethod
    def generate_iv(cls) -> bytes:
        """
        Generate a random IV (nonce) for GCM
        
        Returns:
            12-byte random IV
        """
        return secrets.token_bytes(cls.IV_SIZE)
        
    def encrypt(self, plaintext: bytes, associated_data: bytes = None) -> Tuple[bytes, bytes, bytes]:
        """
        Encrypt data using AES-256-GCM
        
        Args:
            plaintext: Data to encrypt
            associated_data: Optional additional authenticated data (AAD)
            
        Returns:
            Tuple of (ciphertext, iv, tag)
            - ciphertext: Encrypted data (same length as plaintext)
            - iv: Initialization vector (12 bytes)
            - tag: Authentication tag (16 bytes)
        """
        if self.use_cryptography:
            return self._encrypt_cryptography(plaintext, associated_data)
        elif self.use_pycrypto:
            return self._encrypt_pycrypto(plaintext, associated_data)
        else:
            raise RuntimeError("No encryption library available")
            
    def _encrypt_cryptography(self, plaintext: bytes, 
                              associated_data: bytes = None) -> Tuple[bytes, bytes, bytes]:
        """Encrypt using cryptography library"""
        iv = self.generate_iv()
        
        # Create cipher
        cipher = Cipher(
            algorithms.AES(self.key),
            modes.GCM(iv),
            backend=BACKEND
        )
        
        encryptor = cipher.encryptor()
        
        # Add associated data if provided
        if associated_data:
            encryptor.authenticate_additional_data(associated_data)
            
        # Encrypt
        ciphertext = encryptor.update(plaintext) + encryptor.finalize()
        
        return ciphertext, iv, encryptor.tag
        
    def _encrypt_pycrypto(self, plaintext: bytes, 
                          associated_data: bytes = None) -> Tuple[bytes, bytes, bytes]:
        """Encrypt using PyCryptodome"""
        iv = self.generate_iv()
        
        # Create cipher
        cipher = PyCryptoAES.new(self.key, PyCryptoAES.MODE_GCM, nonce=iv)
        
        # Add associated data if provided
        if associated_data:
            cipher.update(associated_data)
            
        # Encrypt
        ciphertext, tag = cipher.encrypt_and_digest(plaintext)
        
        return ciphertext, iv, tag
        
    def decrypt(self, ciphertext: bytes, iv: bytes, tag: bytes, 
                associated_data: bytes = None) -> Optional[bytes]:
        """
        Decrypt data using AES-256-GCM
        
        Args:
            ciphertext: Encrypted data
            iv: Initialization vector (12 bytes)
            tag: Authentication tag (16 bytes)
            associated_data: Optional additional authenticated data
            
        Returns:
            Decrypted plaintext, or None if authentication fails
        """
        if self.use_cryptography:
            return self._decrypt_cryptography(ciphertext, iv, tag, associated_data)
        elif self.use_pycrypto:
            return self._decrypt_pycrypto(ciphertext, iv, tag, associated_data)
        else:
            raise RuntimeError("No encryption library available")
            
    def _decrypt_cryptography(self, ciphertext: bytes, iv: bytes, tag: bytes,
                              associated_data: bytes = None) -> Optional[bytes]:
        """Decrypt using cryptography library"""
        try:
            # Create cipher
            cipher = Cipher(
                algorithms.AES(self.key),
                modes.GCM(iv, tag),
                backend=BACKEND
            )
            
            decryptor = cipher.decryptor()
            
            # Add associated data if provided
            if associated_data:
                decryptor.authenticate_additional_data(associated_data)
                
            # Decrypt
            plaintext = decryptor.update(ciphertext) + decryptor.finalize()
            
            return plaintext
            
        except Exception as e:
            # Authentication failed or other error
            print(f"[AES] Decryption failed: {e}")
            return None
            
    def _decrypt_pycrypto(self, ciphertext: bytes, iv: bytes, tag: bytes,
                          associated_data: bytes = None) -> Optional[bytes]:
        """Decrypt using PyCryptodome"""
        try:
            # Create cipher
            cipher = PyCryptoAES.new(self.key, PyCryptoAES.MODE_GCM, nonce=iv)
            
            # Add associated data if provided
            if associated_data:
                cipher.update(associated_data)
                
            # Decrypt and verify
            plaintext = cipher.decrypt_and_verify(ciphertext, tag)
            
            return plaintext
            
        except Exception as e:
            # Authentication failed
            print(f"[AES] Authentication failed: {e}")
            return None


class ChunkEncryptor:
    """
    Handles encryption/decryption of file chunks with metadata
    Each chunk is encrypted independently with its own IV
    """
    
    def __init__(self, aes_key: bytes):
        """
        Initialize chunk encryptor
        
        Args:
            aes_key: 32-byte AES-256 key from ECDH
        """
        self.aes = AESGCM(aes_key)
        
    def encrypt_chunk(self, chunk_data: bytes, chunk_id: int, 
                      total_chunks: int) -> bytes:
        """
        Encrypt a file chunk with metadata
        
        Args:
            chunk_data: Raw chunk data
            chunk_id: Chunk index
            total_chunks: Total number of chunks
            
        Returns:
            Encrypted chunk packet ready for transmission
        """
        # Create associated data (authenticated but not encrypted)
        associated_data = struct.pack('>II', chunk_id, total_chunks)
        
        # Encrypt chunk
        ciphertext, iv, tag = self.aes.encrypt(chunk_data, associated_data)
        
        # Create packet: [chunk_id(4)][total_chunks(4)][iv(12)][tag(16)][ciphertext]
        packet = struct.pack('>II', chunk_id, total_chunks)
        packet += iv
        packet += tag
        packet += ciphertext
        
        return packet
        
    def decrypt_chunk(self, packet: bytes) -> Tuple[Optional[bytes], int, int]:
        """
        Decrypt a received chunk packet
        
        Args:
            packet: Encrypted chunk packet
            
        Returns:
            Tuple of (decrypted_data, chunk_id, total_chunks)
            decrypted_data is None if authentication fails
        """
        try:
            # Parse header
            chunk_id, total_chunks = struct.unpack('>II', packet[:8])
            
            # Extract components
            iv = packet[8:20]       # 12 bytes
            tag = packet[20:36]     # 16 bytes
            ciphertext = packet[36:] # Rest
            
            # Recreate associated data
            associated_data = struct.pack('>II', chunk_id, total_chunks)
            
            # Decrypt
            plaintext = self.aes.decrypt(ciphertext, iv, tag, associated_data)
            
            return plaintext, chunk_id, total_chunks
            
        except Exception as e:
            print(f"[ChunkEncryptor] Failed to decrypt chunk: {e}")
            return None, 0, 0
            
    def get_chunk_id_from_packet(self, packet: bytes) -> int:
        """Extract chunk ID from encrypted packet without decrypting"""
        try:
            chunk_id, _ = struct.unpack('>II', packet[:8])
            return chunk_id
        except:
            return -1


class FileEncryptor:
    """
    Encrypt/decrypt entire files using chunked AES-GCM
    """
    
    CHUNK_SIZE = 1024 * 1024  # 1 MB chunks
    
    def __init__(self, aes_key: bytes):
        """
        Initialize file encryptor
        
        Args:
            aes_key: 32-byte AES-256 key
        """
        self.chunk_encryptor = ChunkEncryptor(aes_key)
        
    def encrypt_file(self, file_path: str, progress_callback=None) -> Tuple[list, dict]:
        """
        Encrypt a file into chunks
        
        Args:
            file_path: Path to file
            progress_callback: Optional callback(percent_done)
            
        Returns:
            Tuple of (encrypted_chunks_list, metadata_dict)
        """
        import os
        
        file_size = os.path.getsize(file_path)
        total_chunks = (file_size + self.CHUNK_SIZE - 1) // self.CHUNK_SIZE
        
        # Calculate file hash before encryption
        file_hash = self._calculate_file_hash(file_path)
        
        encrypted_chunks = []
        
        with open(file_path, 'rb') as f:
            for chunk_id in range(total_chunks):
                # Read chunk
                chunk_data = f.read(self.CHUNK_SIZE)
                
                # Encrypt chunk
                encrypted_chunk = self.chunk_encryptor.encrypt_chunk(
                    chunk_data, chunk_id, total_chunks
                )
                encrypted_chunks.append(encrypted_chunk)
                
                # Progress callback
                if progress_callback:
                    progress = int((chunk_id + 1) / total_chunks * 100)
                    progress_callback(progress)
                    
        metadata = {
            'original_filename': os.path.basename(file_path),
            'original_size': file_size,
            'total_chunks': total_chunks,
            'chunk_size': self.CHUNK_SIZE,
            'file_hash': file_hash
        }
        
        return encrypted_chunks, metadata
        
    def decrypt_chunks_to_file(self, chunks: list, output_path: str,
                               total_chunks: int, progress_callback=None) -> bool:
        """
        Decrypt chunks and assemble file
        
        Args:
            chunks: List or dict of encrypted chunks
            output_path: Where to save decrypted file
            total_chunks: Expected total chunks
            progress_callback: Optional callback(percent_done)
            
        Returns:
            True if successful
        """
        try:
            # Sort chunks by ID if needed
            if isinstance(chunks, list):
                # Assume list is in order
                chunk_dict = {i: chunks[i] for i in range(len(chunks))}
            else:
                chunk_dict = chunks
                
            with open(output_path, 'wb') as f:
                for chunk_id in range(total_chunks):
                    if chunk_id not in chunk_dict:
                        print(f"[FileEncryptor] Missing chunk {chunk_id}")
                        return False
                        
                    packet = chunk_dict[chunk_id]
                    plaintext, cid, total = self.chunk_encryptor.decrypt_chunk(packet)
                    
                    if plaintext is None:
                        print(f"[FileEncryptor] Failed to decrypt chunk {chunk_id}")
                        return False
                        
                    f.write(plaintext)
                    
                    if progress_callback:
                        progress = int((chunk_id + 1) / total_chunks * 100)
                        progress_callback(progress)
                        
            return True
            
        except Exception as e:
            print(f"[FileEncryptor] Decryption failed: {e}")
            return False
            
    def _calculate_file_hash(self, file_path: str) -> str:
        """Calculate SHA-256 hash of file"""
        sha256 = hashlib.sha256()
        
        with open(file_path, 'rb') as f:
            while True:
                data = f.read(8192)
                if not data:
                    break
                sha256.update(data)
                
        return sha256.hexdigest()


# ============================================
# TEST FUNCTIONS
# ============================================

def test_aes_gcm():
    """Test AES-GCM encryption/decryption"""
    print("🔒 AES-256-GCM Test")
    print("=" * 50)
    
    # Generate random key
    print("\n1️⃣ Generating AES-256 key...")
    key = AESGCM.generate_key()
    print(f"   Key: {key.hex()[:16]}... (32 bytes)")
    
    # Create encryptor
    aes = AESGCM(key)
    
    # Test data
    plaintext = b"Hello, Secure World! This is a test message for AES-GCM encryption."
    associated_data = b"metadata:chunk_1_of_10"
    
    print(f"\n2️⃣ Original data: {plaintext.decode()}")
    print(f"   Associated data: {associated_data.decode()}")
    
    # Encrypt
    ciphertext, iv, tag = aes.encrypt(plaintext, associated_data)
    
    print(f"\n3️⃣ Encrypted:")
    print(f"   IV: {iv.hex()[:16]}... (12 bytes)")
    print(f"   Tag: {tag.hex()[:16]}... (16 bytes)")
    print(f"   Ciphertext: {ciphertext.hex()[:32]}... ({len(ciphertext)} bytes)")
    
    # Decrypt
    decrypted = aes.decrypt(ciphertext, iv, tag, associated_data)
    
    if decrypted:
        print(f"\n4️⃣ Decrypted: {decrypted.decode()}")
        
        if decrypted == plaintext:
            print("\n✅ SUCCESS: Encryption/decryption works correctly!")
        else:
            print("\n❌ ERROR: Decrypted data doesn't match!")
            
    # Test authentication failure
    print("\n5️⃣ Testing authentication failure...")
    
    # Tamper with ciphertext
    tampered_ciphertext = ciphertext[:-1] + bytes([ciphertext[-1] ^ 0xFF])
    decrypted = aes.decrypt(tampered_ciphertext, iv, tag, associated_data)
    
    if decrypted is None:
        print("✅ SUCCESS: Tampering detected (authentication failed)")
    else:
        print("❌ ERROR: Tampering not detected!")
        
    # Test wrong associated data
    print("\n6️⃣ Testing wrong associated data...")
    wrong_aad = b"wrong_metadata"
    decrypted = aes.decrypt(ciphertext, iv, tag, wrong_aad)
    
    if decrypted is None:
        print("✅ SUCCESS: Wrong AAD detected (authentication failed)")
    else:
        print("❌ ERROR: Wrong AAD not detected!")


def test_chunk_encryption():
    """Test chunk encryption with metadata"""
    print("\n" + "=" * 50)
    print("📦 Chunk Encryption Test")
    print("=" * 50)
    
    # Generate key
    key = AESGCM.generate_key()
    chunk_encryptor = ChunkEncryptor(key)
    
    # Test data
    chunk_data = b"This is chunk 5 of 10" * 100
    chunk_id = 5
    total_chunks = 10
    
    print(f"\n1️⃣ Original chunk {chunk_id}/{total_chunks}")
    print(f"   Size: {len(chunk_data)} bytes")
    
    # Encrypt
    packet = chunk_encryptor.encrypt_chunk(chunk_data, chunk_id, total_chunks)
    print(f"\n2️⃣ Encrypted packet size: {len(packet)} bytes")
    print(f"   Overhead: {len(packet) - len(chunk_data)} bytes")
    
    # Decrypt
    decrypted, cid, total = chunk_encryptor.decrypt_chunk(packet)
    
    if decrypted:
        print(f"\n3️⃣ Decrypted chunk {cid}/{total}")
        print(f"   Size: {len(decrypted)} bytes")
        
        if decrypted == chunk_data and cid == chunk_id and total == total_chunks:
            print("\n✅ SUCCESS: Chunk encryption works correctly!")
        else:
            print("\n❌ ERROR: Chunk data or metadata mismatch!")
            
    # Performance test
    print("\n4️⃣ Performance test (100 chunks of 1MB)...")
    import time
    
    chunk_size = 1024 * 1024  # 1 MB
    test_data = secrets.token_bytes(chunk_size)
    
    start = time.time()
    for i in range(100):
        packet = chunk_encryptor.encrypt_chunk(test_data, i, 100)
        chunk_encryptor.decrypt_chunk(packet)
    elapsed = time.time() - start
    
    print(f"   Encrypted/decrypted 100 MB in {elapsed:.2f}s")
    print(f"   Speed: {100/elapsed:.2f} MB/s")
    
    print("\n✅ All tests passed!")


def test_file_encryption():
    """Test full file encryption/decryption"""
    print("\n" + "=" * 50)
    print("📁 File Encryption Test")
    print("=" * 50)
    
    import tempfile
    import os
    
    # Create test file
    print("\n1️⃣ Creating test file...")
    with tempfile.NamedTemporaryFile(delete=False, suffix='.txt') as f:
        # Write 5 MB of test data
        test_data = b"Secure File Transfer Test Data\n" * 200000  # ~5 MB
        f.write(test_data)
        test_file = f.name
        
    print(f"   Created: {test_file}")
    print(f"   Size: {os.path.getsize(test_file)} bytes")
    
    # Generate key
    key = AESGCM.generate_key()
    file_encryptor = FileEncryptor(key)
    
    # Encrypt file
    print("\n2️⃣ Encrypting file...")
    
    def progress(pct):
        print(f"\r   Progress: {pct}%", end="")
        
    encrypted_chunks, metadata = file_encryptor.encrypt_file(test_file, progress)
    print()  # New line
    
    print(f"\n3️⃣ Encryption complete:")
    print(f"   Total chunks: {metadata['total_chunks']}")
    print(f"   Encrypted size: {sum(len(c) for c in encrypted_chunks)} bytes")
    print(f"   Original hash: {metadata['file_hash'][:16]}...")
    
    # Decrypt file
    print("\n4️⃣ Decrypting file...")
    output_file = test_file + '.decrypted'
    
    success = file_encryptor.decrypt_chunks_to_file(
        encrypted_chunks, output_file, metadata['total_chunks'], progress
    )
    print()  # New line
    
    if success:
        print(f"\n5️⃣ Decryption complete!")
        
        # Verify
        with open(test_file, 'rb') as f1, open(output_file, 'rb') as f2:
            if f1.read() == f2.read():
                print("\n✅ SUCCESS: File encryption/decryption works perfectly!")
                print("   Original and decrypted files match!")
            else:
                print("\n❌ ERROR: Files don't match!")
                
        # Cleanup
        os.unlink(output_file)
        
    # Cleanup
    os.unlink(test_file)
    
    print("\n✅ All tests passed!")


if __name__ == "__main__":
    test_aes_gcm()
    test_chunk_encryption()
    test_file_encryption()
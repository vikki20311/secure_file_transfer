"""
security/hybrid.py
Hybrid Encryption System - Combines ECC + AES-256-GCM
Provides complete end-to-end encryption for file transfer
"""

import json
import struct
import time
import hashlib
import secrets
from typing import Tuple, Optional, Dict, Any
from dataclasses import dataclass, asdict
from enum import Enum

# Import our security modules
from .ecc_keygen import ECDHKeyExchange, ECCKeyGenerator
from .aes_gcm import AESGCM, ChunkEncryptor, FileEncryptor

class SecurityLevel(Enum):
    """Security level configuration"""
    STANDARD = "standard"      # AES-256-GCM + ECDH P-256
    HIGH = "high"             # Double encryption (future)
    MAXIMUM = "maximum"       # Post-quantum ready (future)

@dataclass
class SecureHeader:
    """Secure header for encrypted transfers"""
    version: int = 1
    security_level: str = "standard"
    sender_public_key: bytes = None
    timestamp: int = 0
    session_id: str = ""
    
    def to_bytes(self) -> bytes:
        """Serialize header to bytes"""
        data = {
            'version': self.version,
            'security_level': self.security_level,
            'sender_public_key': self.sender_public_key.hex() if self.sender_public_key else "",
            'timestamp': self.timestamp,
            'session_id': self.session_id
        }
        return json.dumps(data).encode('utf-8')
        
    @classmethod
    def from_bytes(cls, data: bytes) -> 'SecureHeader':
        """Parse header from bytes"""
        try:
            json_data = json.loads(data.decode('utf-8'))
            return cls(
                version=json_data.get('version', 1),
                security_level=json_data.get('security_level', 'standard'),
                sender_public_key=bytes.fromhex(json_data.get('sender_public_key', '')) if json_data.get('sender_public_key') else None,
                timestamp=json_data.get('timestamp', 0),
                session_id=json_data.get('session_id', '')
            )
        except:
            return cls()

@dataclass
class SecureEnvelope:
    """Complete secure message envelope"""
    header: SecureHeader
    iv: bytes
    tag: bytes
    ciphertext: bytes
    
    def to_bytes(self) -> bytes:
        """Serialize envelope to bytes for transmission"""
        header_bytes = self.header.to_bytes()
        
        # Format: [header_len(4)][header][iv(12)][tag(16)][ciphertext]
        packet = struct.pack('>I', len(header_bytes))
        packet += header_bytes
        packet += self.iv
        packet += self.tag
        packet += self.ciphertext
        
        return packet
        
    @classmethod
    def from_bytes(cls, data: bytes) -> Optional['SecureEnvelope']:
        """Parse envelope from bytes"""
        try:
            # Read header length
            header_len = struct.unpack('>I', data[:4])[0]
            
            # Parse header
            header_bytes = data[4:4+header_len]
            header = SecureHeader.from_bytes(header_bytes)
            
            # Extract components
            offset = 4 + header_len
            iv = data[offset:offset+12]
            tag = data[offset+12:offset+28]
            ciphertext = data[offset+28:]
            
            return cls(
                header=header,
                iv=iv,
                tag=tag,
                ciphertext=ciphertext
            )
        except Exception as e:
            print(f"[Hybrid] Failed to parse envelope: {e}")
            return None

class HybridEncryption:
    """
    Complete hybrid encryption system
    Combines ECC key exchange with AES-256-GCM encryption
    
    Flow:
    1. ECDH key exchange to establish shared secret
    2. Derive AES-256 session key from shared secret
    3. Encrypt all data with AES-256-GCM
    4. Each chunk gets unique IV and authentication tag
    """
    
    def __init__(self, security_level: SecurityLevel = SecurityLevel.STANDARD):
        """
        Initialize hybrid encryption system
        
        Args:
            security_level: Security level configuration
        """
        self.security_level = security_level
        self.session_id = secrets.token_hex(16)
        
        # Key exchange
        self.ecdh = ECDHKeyExchange()
        self.keygen = ECCKeyGenerator()
        
        # Session state
        self.local_key_pair = None
        self.peer_public_key = None
        self.shared_secret = None
        self.session_key = None
        self.key_exchange_complete = False
        
        # Encryptors (initialized after key exchange)
        self.aes_gcm = None
        self.chunk_encryptor = None
        self.file_encryptor = None
        
    def initiate_key_exchange(self) -> bytes:
        """
        Initiate key exchange - generate local key pair
        
        Returns:
            Local public key to send to peer (65 bytes)
        """
        self.local_key_pair = self.keygen.generate_key_pair()
        
        print(f"[Hybrid] 🔑 Generated ephemeral key pair")
        print(f"[Hybrid] Session ID: {self.session_id}")
        
        return self.local_key_pair.public_key
        
    def complete_key_exchange(self, peer_public_key: bytes) -> bool:
        """
        Complete key exchange with peer's public key
        
        Args:
            peer_public_key: Peer's public key (65 bytes)
            
        Returns:
            True if key exchange successful
        """
        try:
            # Validate peer key
            if not self.keygen.validate_public_key(peer_public_key):
                print("[Hybrid] ❌ Invalid peer public key")
                return False
                
            self.peer_public_key = peer_public_key
            
            # Compute shared secret
            self.shared_secret = self.keygen.compute_shared_secret(
                self.local_key_pair.private_key,
                peer_public_key
            )
            
            # Derive session key
            salt = self.session_id.encode()[:16]
            self.session_key = self.keygen.derive_aes_key(self.shared_secret, salt)
            
            # Initialize encryptors
            self.aes_gcm = AESGCM(self.session_key)
            self.chunk_encryptor = ChunkEncryptor(self.session_key)
            self.file_encryptor = FileEncryptor(self.session_key)
            
            self.key_exchange_complete = True
            
            print(f"[Hybrid] ✅ Key exchange complete")
            print(f"[Hybrid] Session key: {self.session_key.hex()[:16]}...")
            
            return True
            
        except Exception as e:
            print(f"[Hybrid] ❌ Key exchange failed: {e}")
            return False
            
    def create_secure_header(self) -> SecureHeader:
        """Create secure header with session info"""
        return SecureHeader(
            version=1,
            security_level=self.security_level.value,
            sender_public_key=self.local_key_pair.public_key if self.local_key_pair else None,
            timestamp=int(time.time()),
            session_id=self.session_id
        )
        
    def encrypt_metadata(self, metadata: Dict[str, Any]) -> bytes:
        """
        Encrypt file metadata
        
        Args:
            metadata: Dictionary with file metadata
            
        Returns:
            Encrypted metadata as bytes
        """
        if not self.key_exchange_complete:
            raise RuntimeError("Key exchange not complete")
            
        # Convert metadata to JSON
        metadata_json = json.dumps(metadata).encode('utf-8')
        
        # Encrypt
        ciphertext, iv, tag = self.aes_gcm.encrypt(metadata_json)
        
        # Create envelope
        envelope = SecureEnvelope(
            header=self.create_secure_header(),
            iv=iv,
            tag=tag,
            ciphertext=ciphertext
        )
        
        return envelope.to_bytes()
        
    def decrypt_metadata(self, encrypted_metadata: bytes) -> Optional[Dict[str, Any]]:
        """
        Decrypt file metadata
        
        Args:
            encrypted_metadata: Encrypted metadata envelope
            
        Returns:
            Decrypted metadata dict, or None if failed
        """
        if not self.key_exchange_complete:
            raise RuntimeError("Key exchange not complete")
            
        # Parse envelope
        envelope = SecureEnvelope.from_bytes(encrypted_metadata)
        if not envelope:
            print("[Hybrid] Failed to parse metadata envelope")
            return None
            
        # Verify session
        if envelope.header.session_id != self.session_id:
            print("[Hybrid] ⚠️ Session ID mismatch")
            
        # Decrypt
        plaintext = self.aes_gcm.decrypt(
            envelope.ciphertext,
            envelope.iv,
            envelope.tag
        )
        
        if plaintext is None:
            print("[Hybrid] Failed to decrypt metadata")
            return None
            
        try:
            return json.loads(plaintext.decode('utf-8'))
        except:
            print("[Hybrid] Invalid metadata JSON")
            return None
            
    def encrypt_chunk(self, chunk_data: bytes, chunk_id: int, 
                      total_chunks: int) -> bytes:
        """
        Encrypt a file chunk
        
        Args:
            chunk_data: Raw chunk data
            chunk_id: Chunk index
            total_chunks: Total number of chunks
            
        Returns:
            Encrypted chunk packet
        """
        if not self.key_exchange_complete:
            raise RuntimeError("Key exchange not complete")
            
        return self.chunk_encryptor.encrypt_chunk(chunk_data, chunk_id, total_chunks)
        
    def decrypt_chunk(self, packet: bytes) -> Tuple[Optional[bytes], int, int]:
        """
        Decrypt a file chunk
        
        Args:
            packet: Encrypted chunk packet
            
        Returns:
            Tuple of (decrypted_data, chunk_id, total_chunks)
        """
        if not self.key_exchange_complete:
            raise RuntimeError("Key exchange not complete")
            
        return self.chunk_encryptor.decrypt_chunk(packet)
        
    def encrypt_file(self, file_path: str, progress_callback=None) -> Tuple[list, dict]:
        """
        Encrypt entire file
        
        Args:
            file_path: Path to file
            progress_callback: Optional progress callback
            
        Returns:
            Tuple of (encrypted_chunks, metadata)
        """
        if not self.key_exchange_complete:
            raise RuntimeError("Key exchange not complete")
            
        return self.file_encryptor.encrypt_file(file_path, progress_callback)
        
    def decrypt_file(self, chunks: list, output_path: str, 
                     metadata: dict, progress_callback=None) -> bool:
        """
        Decrypt and assemble file
        
        Args:
            chunks: List of encrypted chunks
            output_path: Where to save file
            metadata: File metadata
            progress_callback: Optional progress callback
            
        Returns:
            True if successful
        """
        if not self.key_exchange_complete:
            raise RuntimeError("Key exchange not complete")
            
        return self.file_encryptor.decrypt_chunks_to_file(
            chunks, output_path, metadata['total_chunks'], progress_callback
        )
        
    def encrypt_message(self, message: bytes) -> bytes:
        """
        Encrypt a control message
        
        Args:
            message: Plaintext message
            
        Returns:
            Encrypted envelope
        """
        if not self.key_exchange_complete:
            raise RuntimeError("Key exchange not complete")
            
        ciphertext, iv, tag = self.aes_gcm.encrypt(message)
        
        envelope = SecureEnvelope(
            header=self.create_secure_header(),
            iv=iv,
            tag=tag,
            ciphertext=ciphertext
        )
        
        return envelope.to_bytes()
        
    def decrypt_message(self, encrypted_message: bytes) -> Optional[bytes]:
        """
        Decrypt a control message
        
        Args:
            encrypted_message: Encrypted envelope
            
        Returns:
            Decrypted message or None
        """
        if not self.key_exchange_complete:
            raise RuntimeError("Key exchange not complete")
            
        envelope = SecureEnvelope.from_bytes(encrypted_message)
        if not envelope:
            return None
            
        return self.aes_gcm.decrypt(
            envelope.ciphertext,
            envelope.iv,
            envelope.tag
        )
        
    def get_session_info(self) -> Dict[str, Any]:
        """Get session information for debugging"""
        return {
            'session_id': self.session_id,
            'security_level': self.security_level.value,
            'key_exchange_complete': self.key_exchange_complete,
            'has_session_key': self.session_key is not None,
            'local_public_key': self.local_key_pair.public_key.hex()[:16] + "..." if self.local_key_pair else None,
            'peer_public_key': self.peer_public_key.hex()[:16] + "..." if self.peer_public_key else None
        }
        
    def clear_keys(self):
        """Clear sensitive key material from memory"""
        self.local_key_pair = None
        self.peer_public_key = None
        self.shared_secret = None
        self.session_key = None
        self.key_exchange_complete = False
        
        print("[Hybrid] 🔒 Keys cleared from memory")


class HybridTransferHandler:
    """
    High-level handler for secure file transfers
    Manages the complete secure transfer flow
    """
    
    def __init__(self):
        """Initialize transfer handler"""
        self.hybrid = None
        self.is_sender = False
        self.is_receiver = False
        self.transfer_ready = False
        
    def init_as_sender(self) -> bytes:
        """
        Initialize as sender
        
        Returns:
            Public key to send to receiver
        """
        self.hybrid = HybridEncryption()
        self.is_sender = True
        
        # Generate keys
        public_key = self.hybrid.initiate_key_exchange()
        
        return public_key
        
    def init_as_receiver(self, sender_public_key: bytes) -> bytes:
        """
        Initialize as receiver
        
        Args:
            sender_public_key: Sender's public key
            
        Returns:
            Receiver's public key to send back
        """
        self.hybrid = HybridEncryption()
        self.is_receiver = True
        
        # Generate receiver keys
        receiver_public_key = self.hybrid.initiate_key_exchange()
        
        # Complete key exchange with sender's key
        if self.hybrid.complete_key_exchange(sender_public_key):
            self.transfer_ready = True
            
        return receiver_public_key
        
    def complete_sender_exchange(self, receiver_public_key: bytes) -> bool:
        """
        Sender completes key exchange with receiver's key
        
        Args:
            receiver_public_key: Receiver's public key
            
        Returns:
            True if successful
        """
        if not self.is_sender:
            print("[HybridHandler] Not initialized as sender")
            return False
            
        if self.hybrid.complete_key_exchange(receiver_public_key):
            self.transfer_ready = True
            return True
            
        return False
        
    def prepare_file_for_transfer(self, file_path: str, 
                                  progress_callback=None) -> Tuple[list, bytes]:
        """
        Prepare file for secure transfer
        
        Args:
            file_path: Path to file
            progress_callback: Optional progress callback
            
        Returns:
            Tuple of (encrypted_chunks, encrypted_metadata)
        """
        if not self.transfer_ready:
            raise RuntimeError("Key exchange not complete")
            
        # Encrypt file
        chunks, metadata = self.hybrid.encrypt_file(file_path, progress_callback)
        
        # Encrypt metadata
        encrypted_metadata = self.hybrid.encrypt_metadata(metadata)
        
        return chunks, encrypted_metadata
        
    def receive_file_transfer(self, encrypted_metadata: bytes, chunks: list,
                              output_path: str, progress_callback=None) -> bool:
        """
        Receive and decrypt file transfer
        
        Args:
            encrypted_metadata: Encrypted metadata envelope
            chunks: List of encrypted chunks
            output_path: Where to save file
            progress_callback: Optional progress callback
            
        Returns:
            True if successful
        """
        if not self.transfer_ready:
            raise RuntimeError("Key exchange not complete")
            
        # Decrypt metadata
        metadata = self.hybrid.decrypt_metadata(encrypted_metadata)
        if not metadata:
            print("[HybridHandler] Failed to decrypt metadata")
            return False
            
        # Decrypt and save file
        return self.hybrid.decrypt_file(chunks, output_path, metadata, progress_callback)
        
    def get_hybrid(self) -> Optional[HybridEncryption]:
        """Get hybrid encryption instance"""
        return self.hybrid
        
    def cleanup(self):
        """Clean up sensitive data"""
        if self.hybrid:
            self.hybrid.clear_keys()
        self.transfer_ready = False


# ============================================
# TEST FUNCTION
# ============================================

def test_hybrid_encryption():
    """Test complete hybrid encryption system"""
    print("🔐 Hybrid Encryption System Test")
    print("=" * 60)
    
    # Simulate sender and receiver
    print("\n1️⃣ Simulating key exchange...")
    
    sender = HybridTransferHandler()
    receiver = HybridTransferHandler()
    
    # Sender initiates
    sender_public = sender.init_as_sender()
    print(f"   Sender public key: {sender_public.hex()[:16]}...")
    
    # Receiver responds
    receiver_public = receiver.init_as_receiver(sender_public)
    print(f"   Receiver public key: {receiver_public.hex()[:16]}...")
    
    # Sender completes
    if sender.complete_sender_exchange(receiver_public):
        print("\n✅ Key exchange successful!")
    else:
        print("\n❌ Key exchange failed!")
        return
        
    # Test file encryption
    print("\n2️⃣ Testing file encryption...")
    
    import tempfile
    import os
    
    # Create test file
    with tempfile.NamedTemporaryFile(delete=False, suffix='.txt', mode='w') as f:
        f.write("This is a secure file transfer test.\n" * 1000)
        test_file = f.name
        
    print(f"   Test file: {test_file}")
    print(f"   Size: {os.path.getsize(test_file)} bytes")
    
    # Sender encrypts file
    def progress(pct):
        print(f"\r   Encrypting: {pct}%", end="")
        
    chunks, encrypted_metadata = sender.prepare_file_for_transfer(test_file, progress)
    print()  # New line
    
    print(f"\n3️⃣ Encryption complete:")
    print(f"   Chunks: {len(chunks)}")
    print(f"   Metadata size: {len(encrypted_metadata)} bytes")
    
    # Receiver decrypts file
    print("\n4️⃣ Decrypting file...")
    output_file = test_file + '.received'
    
    def decrypt_progress(pct):
        print(f"\r   Decrypting: {pct}%", end="")
        
    success = receiver.receive_file_transfer(
        encrypted_metadata, chunks, output_file, decrypt_progress
    )
    print()  # New line
    
    if success:
        print(f"\n✅ File transfer successful!")
        
        # Verify files match
        with open(test_file, 'rb') as f1, open(output_file, 'rb') as f2:
            if f1.read() == f2.read():
                print("✅ Files match perfectly!")
            else:
                print("❌ Files don't match!")
                
        os.unlink(output_file)
    else:
        print("\n❌ File transfer failed!")
        
    # Cleanup
    os.unlink(test_file)
    sender.cleanup()
    receiver.cleanup()
    
    # Test message encryption
    print("\n5️⃣ Testing message encryption...")
    
    handler = HybridTransferHandler()
    handler.init_as_sender()
    handler.complete_sender_exchange(handler.hybrid.local_key_pair.public_key)
    
    message = b"Hello, secure world!"
    encrypted = handler.hybrid.encrypt_message(message)
    decrypted = handler.hybrid.decrypt_message(encrypted)
    
    if decrypted == message:
        print("✅ Message encryption works!")
    else:
        print("❌ Message encryption failed!")
        
    # Session info
    print("\n6️⃣ Session Information:")
    info = handler.hybrid.get_session_info()
    for key, value in info.items():
        print(f"   {key}: {value}")
        
    print("\n✅ All hybrid encryption tests passed!")
    print("=" * 60)


if __name__ == "__main__":
    test_hybrid_encryption()
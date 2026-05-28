"""
server/contact_discovery.py
Contact sync and discovery - Find who's on Palmsync
"""

import os
import time
import sqlite3
import threading
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, field
from enum import Enum
import hashlib
import json

class ContactStatus(Enum):
    ONLINE = "online"
    AWAY = "away"
    OFFLINE = "offline"
    NOT_REGISTERED = "not_registered"

@dataclass
class PalmsyncContact:
    """Contact with Palmsync status"""
    phone_number: str
    display_name: str
    status: ContactStatus
    user_id: Optional[str] = None
    avatar_url: Optional[str] = None
    last_seen: float = 0.0
    is_favorite: bool = False
    local_name: Optional[str] = None  # Name in user's phone
    
    def to_dict(self) -> dict:
        return {
            'phone_number': self.phone_number,
            'display_name': self.display_name,
            'status': self.status.value,
            'user_id': self.user_id,
            'avatar_url': self.avatar_url,
            'last_seen': self.last_seen,
            'is_favorite': self.is_favorite,
            'local_name': self.local_name,
            'is_online': self.status == ContactStatus.ONLINE
        }

@dataclass
class ContactSyncResult:
    """Result of contact sync operation"""
    total_contacts: int
    palmsync_contacts: List[PalmsyncContact]
    online_contacts: List[PalmsyncContact]
    offline_contacts: List[PalmsyncContact]
    not_registered: List[PalmsyncContact]
    
    def to_dict(self) -> dict:
        return {
            'total_contacts': self.total_contacts,
            'palmsync_count': len(self.palmsync_contacts),
            'online_count': len(self.online_contacts),
            'offline_count': len(self.offline_contacts),
            'palmsync_contacts': [c.to_dict() for c in self.palmsync_contacts],
            'online_contacts': [c.to_dict() for c in self.online_contacts],
            'offline_contacts': [c.to_dict() for c in self.offline_contacts],
            'not_registered_count': len(self.not_registered)
        }

class ContactDatabase:
    """Database operations for contact management"""
    
    def __init__(self, db_path: str = "palmsync.db"):
        self.db_path = db_path
        self.lock = threading.Lock()
        self._init_tables()
        
    def _init_tables(self):
        """Initialize contact-related tables"""
        with self.lock:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Contacts table (synced from user's phone)
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS synced_contacts (
                    user_id TEXT NOT NULL,
                    contact_phone TEXT NOT NULL,
                    contact_name TEXT,
                    phone_hash TEXT,
                    times_synced INTEGER DEFAULT 1,
                    first_synced REAL,
                    last_synced REAL,
                    is_favorite INTEGER DEFAULT 0,
                    PRIMARY KEY (user_id, contact_phone)
                )
            ''')
            
            # Contact invitations table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS contact_invitations (
                    invitation_id TEXT PRIMARY KEY,
                    from_user_id TEXT NOT NULL,
                    to_phone TEXT NOT NULL,
                    status TEXT DEFAULT 'pending',
                    created_at REAL,
                    expires_at REAL,
                    accepted_at REAL
                )
            ''')
            
            # User status history
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS user_status_history (
                    user_id TEXT NOT NULL,
                    status TEXT NOT NULL,
                    changed_at REAL,
                    device_info TEXT
                )
            ''')
            
            # Blocked contacts
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS blocked_contacts (
                    user_id TEXT NOT NULL,
                    blocked_phone TEXT NOT NULL,
                    blocked_at REAL,
                    reason TEXT,
                    PRIMARY KEY (user_id, blocked_phone)
                )
            ''')
            
            conn.commit()
            conn.close()
            
    def normalize_phone(self, phone: str) -> str:
        """Normalize phone number to E.164 format"""
        # Remove all non-digits
        digits = ''.join(filter(str.isdigit, phone))
        
        # Add country code if missing (default +91 for India)
        if not digits.startswith('91') and len(digits) == 10:
            digits = '91' + digits
            
        return '+' + digits
        
    def hash_phone(self, phone: str) -> str:
        """Create hash of phone number for privacy"""
        normalized = self.normalize_phone(phone)
        return hashlib.sha256(normalized.encode()).hexdigest()
        
    def sync_contacts(self, user_id: str, contacts: List[Dict]) -> int:
        """Sync user's contacts to database"""
        synced_count = 0
        current_time = time.time()
        
        with self.lock:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            for contact in contacts:
                phone = contact.get('phone_number', '').strip()
                name = contact.get('name', '').strip()
                is_favorite = contact.get('is_favorite', False)
                
                if not phone:
                    continue
                    
                normalized = self.normalize_phone(phone)
                phone_hash = self.hash_phone(phone)
                
                # Insert or update
                cursor.execute('''
                    INSERT INTO synced_contacts 
                    (user_id, contact_phone, contact_name, phone_hash, first_synced, last_synced, is_favorite)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(user_id, contact_phone) DO UPDATE SET
                        contact_name = excluded.contact_name,
                        times_synced = times_synced + 1,
                        last_synced = excluded.last_synced,
                        is_favorite = excluded.is_favorite
                ''', (user_id, normalized, name, phone_hash, current_time, current_time, int(is_favorite)))
                
                synced_count += 1
                
            conn.commit()
            conn.close()
            
        return synced_count
        
    def find_palmsync_users(self, phone_numbers: List[str]) -> List[Dict]:
        """Find which phone numbers are registered on Palmsync"""
        users = []
        
        if not phone_numbers:
            return users
            
        with self.lock:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            placeholders = ','.join(['?' for _ in phone_numbers])
            
            cursor.execute(f'''
                SELECT u.user_id, u.phone_number, u.display_name, u.status, 
                       u.avatar_url, u.last_seen, u.public_key
                FROM palmsync_users u
                WHERE u.phone_number IN ({placeholders})
            ''', phone_numbers)
            
            for row in cursor.fetchall():
                users.append({
                    'user_id': row[0],
                    'phone_number': row[1],
                    'display_name': row[2],
                    'status': row[3],
                    'avatar_url': row[4],
                    'last_seen': row[5],
                    'public_key': row[6],
                    'is_online': (time.time() - row[5]) < 60 if row[5] else False
                })
                
            conn.close()
            
        return users
        
    def get_user_contacts(self, user_id: str) -> List[Dict]:
        """Get all contacts synced by a user"""
        contacts = []
        
        with self.lock:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT contact_phone, contact_name, is_favorite, last_synced
                FROM synced_contacts
                WHERE user_id = ?
                ORDER BY is_favorite DESC, contact_name ASC
            ''', (user_id,))
            
            for row in cursor.fetchall():
                contacts.append({
                    'phone_number': row[0],
                    'name': row[1],
                    'is_favorite': bool(row[2]),
                    'last_synced': row[3]
                })
                
            conn.close()
            
        return contacts
        
    def is_blocked(self, user_id: str, phone_number: str) -> bool:
        """Check if a contact is blocked"""
        normalized = self.normalize_phone(phone_number)
        
        with self.lock:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT 1 FROM blocked_contacts 
                WHERE user_id = ? AND blocked_phone = ?
            ''', (user_id, normalized))
            
            result = cursor.fetchone() is not None
            conn.close()
            
        return result
        
    def block_contact(self, user_id: str, phone_number: str, reason: str = None):
        """Block a contact"""
        normalized = self.normalize_phone(phone_number)
        
        with self.lock:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                INSERT OR REPLACE INTO blocked_contacts 
                (user_id, blocked_phone, blocked_at, reason)
                VALUES (?, ?, ?, ?)
            ''', (user_id, normalized, time.time(), reason))
            
            conn.commit()
            conn.close()
            
    def unblock_contact(self, user_id: str, phone_number: str):
        """Unblock a contact"""
        normalized = self.normalize_phone(phone_number)
        
        with self.lock:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                DELETE FROM blocked_contacts 
                WHERE user_id = ? AND blocked_phone = ?
            ''', (user_id, normalized))
            
            conn.commit()
            conn.close()
            
    def update_user_status(self, user_id: str, status: str, device_info: str = None):
        """Update user status and record history"""
        with self.lock:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Update current status
            cursor.execute('''
                UPDATE palmsync_users 
                SET status = ?, last_seen = ?
                WHERE user_id = ?
            ''', (status, time.time(), user_id))
            
            # Record history
            cursor.execute('''
                INSERT INTO user_status_history 
                (user_id, status, changed_at, device_info)
                VALUES (?, ?, ?, ?)
            ''', (user_id, status, time.time(), device_info))
            
            conn.commit()
            conn.close()


class ContactDiscovery:
    """
    Main contact discovery service
    Finds Palmsync users among user's contacts
    """
    
    def __init__(self, db_path: str = "palmsync.db"):
        self.db = ContactDatabase(db_path)
        self.cache = {}
        self.cache_ttl = 30  # 30 seconds
        self.lock = threading.Lock()
        
    def sync_and_discover(self, user_id: str, contacts: List[Dict]) -> ContactSyncResult:
        """
        Sync contacts and discover Palmsync users
        
        Args:
            user_id: User ID
            contacts: List of contacts from phone [{'phone_number': '...', 'name': '...'}]
            
        Returns:
            ContactSyncResult with categorized contacts
        """
        # Sync contacts to database
        synced_count = self.db.sync_contacts(user_id, contacts)
        
        # Extract phone numbers
        phone_numbers = []
        contact_map = {}  # phone -> contact info
        
        for contact in contacts:
            phone = contact.get('phone_number', '').strip()
            if phone:
                normalized = self.db.normalize_phone(phone)
                phone_numbers.append(normalized)
                contact_map[normalized] = {
                    'name': contact.get('name', ''),
                    'is_favorite': contact.get('is_favorite', False)
                }
                
        # Find which are on Palmsync
        palmsync_users = self.db.find_palmsync_users(phone_numbers)
        palmsync_phones = {u['phone_number'] for u in palmsync_users}
        
        # Build result
        palmsync_contacts = []
        online_contacts = []
        offline_contacts = []
        not_registered = []
        
        # Process Palmsync users
        for user in palmsync_users:
            contact_info = contact_map.get(user['phone_number'], {})
            
            status = ContactStatus.ONLINE if user['is_online'] else ContactStatus.OFFLINE
            
            contact = PalmsyncContact(
                phone_number=user['phone_number'],
                display_name=user['display_name'],
                status=status,
                user_id=user['user_id'],
                avatar_url=user.get('avatar_url'),
                last_seen=user['last_seen'],
                local_name=contact_info.get('name'),
                is_favorite=contact_info.get('is_favorite', False)
            )
            
            palmsync_contacts.append(contact)
            
            if user['is_online']:
                online_contacts.append(contact)
            else:
                offline_contacts.append(contact)
                
        # Process non-Palmsync contacts
        for phone in phone_numbers:
            if phone not in palmsync_phones:
                contact_info = contact_map.get(phone, {})
                
                contact = PalmsyncContact(
                    phone_number=phone,
                    display_name=contact_info.get('name', phone[-4:]),
                    status=ContactStatus.NOT_REGISTERED,
                    local_name=contact_info.get('name'),
                    is_favorite=contact_info.get('is_favorite', False)
                )
                
                not_registered.append(contact)
                
        return ContactSyncResult(
            total_contacts=len(contacts),
            palmsync_contacts=palmsync_contacts,
            online_contacts=online_contacts,
            offline_contacts=offline_contacts,
            not_registered=not_registered
        )
        
    def get_online_contacts(self, user_id: str) -> List[PalmsyncContact]:
        """Get online contacts for a user"""
        contacts = self.db.get_user_contacts(user_id)
        
        if not contacts:
            return []
            
        phones = [c['phone_number'] for c in contacts]
        palmsync_users = self.db.find_palmsync_users(phones)
        
        online = []
        for user in palmsync_users:
            if user['is_online']:
                contact_info = next(
                    (c for c in contacts if c['phone_number'] == user['phone_number']),
                    {}
                )
                
                online.append(PalmsyncContact(
                    phone_number=user['phone_number'],
                    display_name=user['display_name'],
                    status=ContactStatus.ONLINE,
                    user_id=user['user_id'],
                    avatar_url=user.get('avatar_url'),
                    last_seen=user['last_seen'],
                    local_name=contact_info.get('name'),
                    is_favorite=contact_info.get('is_favorite', False)
                ))
                
        return online
        
    def search_contacts(self, user_id: str, query: str) -> List[PalmsyncContact]:
        """Search user's contacts"""
        contacts = self.db.get_user_contacts(user_id)
        
        # Filter by query
        filtered = []
        query_lower = query.lower()
        
        for contact in contacts:
            name = contact.get('name', '').lower()
            phone = contact.get('phone_number', '')
            
            if query_lower in name or query_lower in phone:
                filtered.append(contact)
                
        if not filtered:
            return []
            
        # Get Palmsync status
        phones = [c['phone_number'] for c in filtered]
        palmsync_users = self.db.find_palmsync_users(phones)
        palmsync_map = {u['phone_number']: u for u in palmsync_users}
        
        results = []
        for contact in filtered:
            phone = contact['phone_number']
            user = palmsync_map.get(phone)
            
            if user:
                status = ContactStatus.ONLINE if user['is_online'] else ContactStatus.OFFLINE
                
                results.append(PalmsyncContact(
                    phone_number=phone,
                    display_name=user['display_name'],
                    status=status,
                    user_id=user['user_id'],
                    avatar_url=user.get('avatar_url'),
                    last_seen=user['last_seen'],
                    local_name=contact.get('name'),
                    is_favorite=contact.get('is_favorite', False)
                ))
            else:
                results.append(PalmsyncContact(
                    phone_number=phone,
                    display_name=contact.get('name', phone[-4:]),
                    status=ContactStatus.NOT_REGISTERED,
                    local_name=contact.get('name'),
                    is_favorite=contact.get('is_favorite', False)
                ))
                
        return results
        
    def update_status(self, user_id: str, status: str, device_info: str = None):
        """Update user's online status"""
        self.db.update_user_status(user_id, status, device_info)
        
        # Clear cache
        with self.lock:
            self.cache.pop(user_id, None)
            
    def block_contact(self, user_id: str, phone_number: str, reason: str = None):
        """Block a contact"""
        self.db.block_contact(user_id, phone_number, reason)
        
    def unblock_contact(self, user_id: str, phone_number: str):
        """Unblock a contact"""
        self.db.unblock_contact(user_id, phone_number)
        
    def get_invitable_contacts(self, user_id: str) -> List[PalmsyncContact]:
        """Get contacts that can be invited to Palmsync"""
        contacts = self.db.get_user_contacts(user_id)
        
        if not contacts:
            return []
            
        phones = [c['phone_number'] for c in contacts]
        palmsync_users = self.db.find_palmsync_users(phones)
        palmsync_phones = {u['phone_number'] for u in palmsync_users}
        
        invitable = []
        for contact in contacts:
            if contact['phone_number'] not in palmsync_phones:
                # Check if not blocked
                if not self.db.is_blocked(user_id, contact['phone_number']):
                    invitable.append(PalmsyncContact(
                        phone_number=contact['phone_number'],
                        display_name=contact.get('name', contact['phone_number'][-4:]),
                        status=ContactStatus.NOT_REGISTERED,
                        local_name=contact.get('name'),
                        is_favorite=contact.get('is_favorite', False)
                    ))
                    
        return invitable


# ============================================
# FLASK ROUTES (Add to relay_server.py)
# ============================================

def register_contact_routes(app, contact_discovery: ContactDiscovery, auth_manager):
    """Register contact discovery routes with Flask app"""
    
    @app.route('/api/contacts/sync', methods=['POST'])
    def sync_contacts():
        """Sync contacts and discover Palmsync users"""
        auth_header = request.headers.get('Authorization', '')
        token = auth_header.replace('Bearer ', '')
        
        user = auth_manager.validate_token(token)
        if not user:
            return jsonify({'error': 'Unauthorized'}), 401
            
        data = request.json
        contacts = data.get('contacts', [])
        
        result = contact_discovery.sync_and_discover(user.user_id, contacts)
        return jsonify(result.to_dict())
        
    @app.route('/api/contacts/online', methods=['GET'])
    def get_online_contacts():
        """Get online Palmsync contacts"""
        auth_header = request.headers.get('Authorization', '')
        token = auth_header.replace('Bearer ', '')
        
        user = auth_manager.validate_token(token)
        if not user:
            return jsonify({'error': 'Unauthorized'}), 401
            
        online = contact_discovery.get_online_contacts(user.user_id)
        return jsonify({
            'online_contacts': [c.to_dict() for c in online],
            'count': len(online)
        })
        
    @app.route('/api/contacts/search', methods=['POST'])
    def search_contacts():
        """Search user's contacts"""
        auth_header = request.headers.get('Authorization', '')
        token = auth_header.replace('Bearer ', '')
        
        user = auth_manager.validate_token(token)
        if not user:
            return jsonify({'error': 'Unauthorized'}), 401
            
        data = request.json
        query = data.get('query', '')
        
        results = contact_discovery.search_contacts(user.user_id, query)
        return jsonify({
            'results': [c.to_dict() for c in results],
            'count': len(results)
        })
        
    @app.route('/api/contacts/invitable', methods=['GET'])
    def get_invitable_contacts():
        """Get contacts that can be invited to Palmsync"""
        auth_header = request.headers.get('Authorization', '')
        token = auth_header.replace('Bearer ', '')
        
        user = auth_manager.validate_token(token)
        if not user:
            return jsonify({'error': 'Unauthorized'}), 401
            
        invitable = contact_discovery.get_invitable_contacts(user.user_id)
        return jsonify({
            'invitable_contacts': [c.to_dict() for c in invitable],
            'count': len(invitable)
        })
        
    @app.route('/api/contacts/block', methods=['POST'])
    def block_contact():
        """Block a contact"""
        auth_header = request.headers.get('Authorization', '')
        token = auth_header.replace('Bearer ', '')
        
        user = auth_manager.validate_token(token)
        if not user:
            return jsonify({'error': 'Unauthorized'}), 401
            
        data = request.json
        phone = data.get('phone_number')
        reason = data.get('reason')
        
        contact_discovery.block_contact(user.user_id, phone, reason)
        return jsonify({'success': True})
        
    @app.route('/api/contacts/unblock', methods=['POST'])
    def unblock_contact():
        """Unblock a contact"""
        auth_header = request.headers.get('Authorization', '')
        token = auth_header.replace('Bearer ', '')
        
        user = auth_manager.validate_token(token)
        if not user:
            return jsonify({'error': 'Unauthorized'}), 401
            
        data = request.json
        phone = data.get('phone_number')
        
        contact_discovery.unblock_contact(user.user_id, phone)
        return jsonify({'success': True})


# ============================================
# TEST FUNCTION
# ============================================

def test_contact_discovery():
    """Test contact discovery"""
    print("📞 Palmsync Contact Discovery Test")
    print("=" * 50)
    
    discovery = ContactDiscovery("test_palmsync.db")
    
    # Test user
    user_id = "test-user-123"
    
    # Mock contacts
    contacts = [
        {'phone_number': '+919876543210', 'name': 'Mom', 'is_favorite': True},
        {'phone_number': '+919876543211', 'name': 'Rohan', 'is_favorite': False},
        {'phone_number': '+919876543212', 'name': 'Priya', 'is_favorite': False},
        {'phone_number': '+919876543213', 'name': 'Dad', 'is_favorite': True},
        {'phone_number': '9876543214', 'name': 'Office', 'is_favorite': False},
    ]
    
    print(f"\n1️⃣ Syncing {len(contacts)} contacts...")
    result = discovery.sync_and_discover(user_id, contacts)
    
    print(f"\n2️⃣ Results:")
    print(f"   Total contacts: {result.total_contacts}")
    print(f"   On Palmsync: {len(result.palmsync_contacts)}")
    print(f"   Online: {len(result.online_contacts)}")
    print(f"   Offline: {len(result.offline_contacts)}")
    print(f"   Not registered: {len(result.not_registered)}")
    
    if result.online_contacts:
        print(f"\n3️⃣ Online Contacts:")
        for c in result.online_contacts:
            print(f"   🟢 {c.display_name} ({c.phone_number})")
            
    if result.offline_contacts:
        print(f"\n4️⃣ Offline Contacts:")
        for c in result.offline_contacts:
            print(f"   ⚫ {c.display_name} ({c.phone_number})")
            
    print(f"\n5️⃣ Invitable contacts: {len(discovery.get_invitable_contacts(user_id))}")
    
    # Test search
    print(f"\n6️⃣ Search 'Mom':")
    results = discovery.search_contacts(user_id, "Mom")
    for c in results:
        print(f"   • {c.display_name} - {c.status.value}")
        
    print("\n✅ Contact Discovery test complete!")


if __name__ == "__main__":
    test_contact_discovery()
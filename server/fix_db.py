# fix_db.py
import sqlite3

conn = sqlite3.connect('palmsync.db')
cursor = conn.cursor()

# Create palmsync_users table
cursor.execute('''
    CREATE TABLE IF NOT EXISTS palmsync_users (
        user_id TEXT PRIMARY KEY,
        phone_number TEXT UNIQUE,
        display_name TEXT,
        status TEXT,
        avatar_url TEXT,
        public_key TEXT,
        created_at REAL,
        last_seen REAL,
        device_tokens TEXT
    )
''')

# Add test user
cursor.execute('''
    INSERT OR REPLACE INTO palmsync_users 
    (user_id, phone_number, display_name, created_at, last_seen)
    VALUES (?, ?, ?, ?, ?)
''', ('test-1', '+919876543210', 'Mom', 1234567890.0, 1234567890.0))

conn.commit()
conn.close()

print('✅ Database ready!')
print('   - palmsync_users table created')
print('   - Test user "Mom" added')
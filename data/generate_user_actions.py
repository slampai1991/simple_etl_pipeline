import os
import sqlite3
import random
from datetime import datetime, timedelta

# Ensure the 'data' directory exists
os.makedirs('data', exist_ok=True)

# Connect to SQLite database
conn = sqlite3.connect('data/user_actions.db')
cursor = conn.cursor()

# Create the 'user_actions' table
cursor.execute('''
CREATE TABLE IF NOT EXISTS user_actions (
    id INTEGER PRIMARY KEY,
    user_id INTEGER,
    action TEXT,
    timestamp TEXT
)
''')

# Generate 10,000 rows of user actions data
actions = ['login', 'logout', 'purchase', 'view', 'click']
start_date = datetime(2024, 1, 1)

for i in range(10000):
    user_id = random.randint(1, 1000)
    action = random.choice(actions)
    timestamp = start_date + timedelta(seconds=random.randint(0, 31536000))
    cursor.execute(
        'INSERT INTO user_actions (user_id, action, timestamp) VALUES (?, ?, ?)',
        (user_id, action, timestamp.strftime('%Y-%m-%d %H:%M:%S'))
    )

# Commit changes and close the connection
conn.commit()
conn.close()
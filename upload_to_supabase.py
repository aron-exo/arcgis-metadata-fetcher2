import os
import psycopg2
import json

# Connect to Supabase
conn = psycopg2.connect(
    host=os.getenv('SUPABASE_DB_HOST'),
    database=os.getenv('SUPABASE_DB_NAME'),
    user=os.getenv('SUPABASE_DB_USER'),
    password=os.getenv('SUPABASE_DB_PASSWORD'),
    port=os.getenv('SUPABASE_DB_PORT')
)

cur = conn.cursor()

# Create table if not exists
cur.execute("""
CREATE TABLE IF NOT EXISTS layers (
    id SERIAL PRIMARY KEY,
    title TEXT,
    url TEXT
)
""")

# Read the added layers
with open('added_layers.json', 'r') as f:
    layers = json.load(f)

# Insert layers into Supabase
for layer in layers:
    cur.execute("""
    INSERT INTO layers (title, url) VALUES (%s, %s)
    """, (layer['title'], layer['url']))

# Commit and close connection
conn.commit()
cur.close()
conn.close()

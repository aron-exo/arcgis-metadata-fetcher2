import psycopg2

def connect_to_database():
    conn = psycopg2.connect(
        host=os.getenv('COCKROACH_DB_HOST'),
        database=os.getenv('COCKROACH_DB_DATABASE'),
        user=os.getenv('COCKROACH_DB_USER'),
        password=os.getenv('COCKROACH_DB_PASSWORD'),
        port=os.getenv('COCKROACH_DB_PORT')
    )
    return conn

def drop_all_tables(conn):
    cur = conn.cursor()
    
    # Get all table names
    cur.execute("SELECT tablename FROM pg_tables WHERE schemaname = 'public'")
    tables = cur.fetchall()
    
    # Drop each table
    for table in tables:
        table_name = table[0]
        drop_query = f"DROP TABLE IF EXISTS {table_name} CASCADE"
        cur.execute(drop_query)
        print(f"Dropped table {table_name}")
    
    conn.commit()
    cur.close()

if __name__ == "__main__":
    conn = connect_to_database()
    drop_all_tables(conn)
    conn.close()

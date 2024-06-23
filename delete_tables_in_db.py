import psycopg2

def connect_to_database():
    conn = psycopg2.connect(
        host='your_cockroachdb_host',
        database='your_database',
        user='your_username',
        password='your_password',
        port='your_port'
    )
    return conn

def generate_and_execute_drop_statements(conn):
    cur = conn.cursor()
    
    # Generate the DROP TABLE statements
    cur.execute("SELECT 'DROP TABLE IF EXISTS \"' || tablename || '\" CASCADE;' FROM pg_tables WHERE schemaname = 'public';")
    drop_statements = cur.fetchall()
    
    # Execute each DROP TABLE statement
    for statement in drop_statements:
        cur.execute(statement[0])
        print(f"Executed: {statement[0]}")
    
    conn.commit()
    cur.close()

if __name__ == "__main__":
    conn = connect_to_database()
    generate_and_execute_drop_statements(conn)
    conn.close()

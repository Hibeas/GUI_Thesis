import psycopg2

# Connect to the PostgreSQL database
conn = psycopg2.connect(dbname="experiment_db", user="admin", password="password123", host="localhost", port="5432")
cur = conn.cursor()

# Select and print the contents of each table
tables = ["participant", "participation", "session"]
for table in tables:
    print(f"\n--- Zawartość tabeli: {table} ---")
    query = f'SELECT * FROM "{table}";'
    cur.execute(query)
    rows = cur.fetchall()
    for row in rows:
        print(row)



cur.close()
conn.close()
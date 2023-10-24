import psycopg2
import src.config.config as config
import database as db

conn, error = db.get_connection()

if error:
    print(f"Could not establish connection: {error}")
    exit(1)

cur = conn.cursor()

cur.execute("SELECT version()")
db_version = cur.fetchone()
print(db_version)

conn.close()

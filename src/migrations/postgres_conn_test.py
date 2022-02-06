import psycopg2

conn = psycopg2.connect(host="localhost", database="formula_1_db", user="postgres", password="admin123")
cur = conn.cursor()

cur.execute("SELECT version()")
db_version = cur.fetchone()
print(db_version)

conn.close()

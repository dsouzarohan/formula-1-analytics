import psycopg2
import src.config.config as config

db_config = config.get_db_config()
print("Printing DB config in Conn Test", db_config)

conn = psycopg2.connect(host=db_config['HOST']
                        , database=db_config['DB']
                        , user=db_config['USERNAME']
                        , password=db_config['PASSWORD'])
cur = conn.cursor()

cur.execute("SELECT version()")
db_version = cur.fetchone()
print(db_version)

conn.close()

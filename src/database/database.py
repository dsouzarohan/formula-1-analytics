import psycopg2
import src.config.config as config
from sys import exit

config, error = config.load_config()

if error:
    print(f"Could not load config: {error}")
    exit(1)

db_config = config['DATABASE']

def get_connection():
    try:
        conn = psycopg2.connect(host=db_config['HOST']
                                , database=db_config['DB']
                                , user=db_config['USERNAME']
                                , password=db_config['PASSWORD'])
        return conn, None
    except (Exception, psycopg2.DatabaseError) as err:
        return None, err

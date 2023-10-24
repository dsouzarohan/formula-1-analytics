import psycopg2
import src.config.config as config

config, error = config.load_config()
db_config = config['DATABASE']

if error:
    print(f"Could not load config: {error}")
    exit(1)


def get_connection():
    try:
        conn = psycopg2.connect(host=db_config['HOST']
                                , database=db_config['DB']
                                , user=db_config['USERNAME']
                                , password=db_config['PASSWORD'])
        return conn, None
    except (Exception, psycopg2.DatabaseError) as err:
        return None, err

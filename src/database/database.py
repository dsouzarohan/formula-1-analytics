import psycopg2
import src.config.config as config


def get_connection():
    try:
        db_config = config.get_db_config()
        conn = psycopg2.connect(host=db_config['HOST']
                                , database=db_config['DB']
                                , user=db_config['USERNAME']
                                , password=db_config['PASSWORD'])
        return conn, None
    except (Exception, psycopg2.DatabaseError) as err:
        return None, err

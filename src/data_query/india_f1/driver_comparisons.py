from psycopg2.extras import RealDictCursor
from os.path import join
from src.database import database as db
from src.config.config import SQL_DIR


def get_driver_comparisons():
    conn, err = db.get_connection()
    if err:
        print("Error in connecting with the database", err)
        return None

    with open(join(SQL_DIR, 'india_f1/driver_comparisons.sql')) as file:
        query = file.read()

    curr = conn.cursor(cursor_factory=RealDictCursor)
    curr.execute(query)
    data = curr.fetchall()

    return data

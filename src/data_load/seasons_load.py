import datetime
from os.path import join
from src.database import database as db
import csv
from src.utilities.logger import log_data_load
from src.config.config import DATA_PATH


def load():
    conn, err = db.get_connection()
    if err:
        print("Error in connecting with database", err)
    curr = conn.cursor()

    curr.execute("""
        TRUNCATE TABLE SEASONS CASCADE 
    """)

    conn.commit()

    with open(join(DATA_PATH, "seasons.csv"), "r", encoding="utf8") as csvfile:
        reader = csv.DictReader(csvfile, delimiter=',')
        query = """
        INSERT INTO SEASONS (
            year,
            url
        ) VALUES (
            %(year)s,
            %(url)s       
        )
        """

        start = datetime.datetime.now()
        log_data_load("SEASONS", "START", None, None)
        count = 0

        for row in reader:

            data = {'year': row['year']
                , 'url': row['url']
                    }

            curr.execute(query, data)
            count += 1

        log_data_load("SEASONS", "END", start, count)

        conn.commit()
        curr.close()
        conn.close()

import datetime
from os.path import join
from src.database import database as db
import csv
from src.config.config import DATA_PATH
from src.utilities.logger import log_data_load


def load():
    conn, err = db.get_connection()
    if err:
        print("Error in connecting with database", err)
    curr = conn.cursor()

    curr.execute("""
        TRUNCATE TABLE STATUS CASCADE 
    """)

    conn.commit()

    with open(join(DATA_PATH, "status.csv"), "r", encoding="utf8") as csvfile:
        reader = csv.DictReader(csvfile, delimiter=',')
        query = """
        INSERT INTO STATUS (
            statusId,
            status
        ) VALUES (
            %(statusId)s,
            %(status)s       
        )
        """

        start = datetime.datetime.now()
        log_data_load("STATUS", "START", None, None)
        count = 0

        for row in reader:

            data = {'statusId': row['statusId']
                , 'status': row['status']
                    }

            curr.execute(query, data)
            count += 1

        log_data_load("STATUS", "END", start, count)

        conn.commit()
        curr.close()
        conn.close()

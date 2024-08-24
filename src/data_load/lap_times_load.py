import datetime
from os.path import join

from psycopg2.extras import execute_batch
from sqlalchemy.dialects.mysql import insert

from src.database import database as db
import csv
from src.utilities import load_transforms as lt
from src.utilities.logger import log_data_load
from src.config.config import DATA_PATH


def load():
    conn, err = db.get_connection()
    if err:
        print("Error in connecting with database", err)
    curr = conn.cursor()

    curr.execute("""
        TRUNCATE TABLE LAP_TIMES CASCADE 
    """)

    conn.commit()

    with open(join(DATA_PATH, "lap_times.csv"), "r", encoding="utf8") as csvfile:
        reader = csv.DictReader(csvfile, delimiter=',')
        query = """
        INSERT INTO LAP_TIMES (
            raceId,
            driverId,
            lap,
            position,
            time,
            timeInMillis 
        ) VALUES (
            %s,
            %s,
            %s,
            %s,
            %s,
            %s 
        )
        """

        start = datetime.datetime.now()
        log_data_load("LAP_TIMES", "START", None, None)
        insert_data = []

        for row in reader:
            insert_data.append(
                (
                    row['raceId'],
                    row['driverId'],
                    row['lap'],
                    row['position'],
                    lt.time_transform(row['time']).time(),
                    row['milliseconds']
                )
            )

        execute_batch(curr, query, insert_data)
        log_data_load("LAP_TIMES", "END", start, len(insert_data))

        conn.commit()
        curr.close()
        conn.close()

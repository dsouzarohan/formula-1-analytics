import datetime
from os.path import join

from psycopg2.extras import execute_batch

from src.database import database as db
import csv
from src.utilities import load_transforms as lt
from src.config.config import DATA_PATH
from src.utilities.logger import log_data_load


def load():
    conn, err = db.get_connection()
    if err:
        print("Error in connecting with database", err)
    curr = conn.cursor()

    curr.execute("""
        TRUNCATE TABLE PIT_STOPS CASCADE 
    """)

    conn.commit()

    with open(join(DATA_PATH, "pit_stops.csv"), "r", encoding="utf8") as csvfile:
        reader = csv.DictReader(csvfile, delimiter=',')
        query = """
        INSERT INTO PIT_STOPS (
            raceId,
            driverId,
            stop,
            lap,
            time,
            duration,
            timeInMillis 
        ) VALUES (
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s 
        )
        """

        start = datetime.datetime.now()
        log_data_load("PIT_STOPS", "START", None, None)
        insert_data = []

        for row in reader:
            insert_data.append(
                (
                    row["raceId"],
                    row["driverId"],
                    row["lap"],
                    row["lap"],
                    lt.time_of_day_transform(row['time']),
                    lt.time_transform(row['duration']).time(),
                    row['milliseconds']
                )
            )

        execute_batch(curr, query, insert_data)
        log_data_load("PIT_STOPS", "END", start, len(insert_data))

        conn.commit()
        curr.close()
        conn.close()

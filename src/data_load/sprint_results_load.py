import datetime
from os.path import join

from psycopg2.extras import execute_batch

from src.database import database as db
from src.utilities import load_transforms as lt
import csv
from src.config.config import DATA_PATH
from src.utilities.logger import log_data_load


def load():
    conn, err = db.get_connection()
    if err:
        print("Error in connecting with database", err)
    curr = conn.cursor()

    curr.execute("""
        TRUNCATE TABLE SPRINT_RESULTS CASCADE 
    """)

    conn.commit()

    with open(join(DATA_PATH, "sprint_results.csv"), "r", encoding="utf8") as csvfile:
        reader = csv.DictReader(csvfile, delimiter=',')
        query = """
        INSERT INTO SPRINT_RESULTS (
            sprintResultId,
            raceId,
            driverId,
            constructorId,
            carNumber,
            gridPosition,
            position,
            positionText,
            positionOrder,
            points,
            laps,
            time,
            timeInMillis,
            fastestLap,
            fastestLapTime,
            statusId
        ) VALUES (
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
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
        log_data_load("SPRINT_RESULTS", "START", None, None)
        insert_data = []

        last_time = None

        for row in reader:
            delta_string = row['time']
            curr_time = None

            if delta_string.find('+') > -1:
                curr_time = lt.delta_to_time(last_time, delta_string)
            else:
                last_time = lt.time_transform(delta_string)
                curr_time = last_time

            insert_data.append(
                (
                    row['resultId'],
                    row['raceId'],
                    row['driverId'],
                    row['constructorId'],
                    row['number'],
                    row['grid'],
                    lt.null_transform(row['position']),
                    row['positionText'],
                    row['positionOrder'],
                    row['points'],
                    row['laps'],
                    curr_time.time() if curr_time is not None else None,
                    lt.null_transform(row['milliseconds']),
                    lt.null_transform(row['fastestLap']),
                    lt.null_transform(row['fastestLapTime']),
                    row['statusId']
                )
            )
            last_time = curr_time

        execute_batch(curr, query, insert_data)
        log_data_load("SPRINT_RESULTS", "END", start, len(insert_data))

        conn.commit()
        curr.close()
        conn.close()

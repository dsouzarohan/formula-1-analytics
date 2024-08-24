import datetime
from os.path import join

from psycopg2.extras import execute_batch

from src.database import database as db
from src.utilities import load_transforms as lt
import csv
from src.utilities.logger import log_data_load
from src.config.config import DATA_PATH


def load():
    conn, err = db.get_connection()
    if err:
        print("Error in connecting with database", err)
    curr = conn.cursor()

    curr.execute("""
        TRUNCATE TABLE RESULTS CASCADE 
    """)

    conn.commit()

    with open(join(DATA_PATH, "results.csv"), "r", encoding="utf8") as csvfile:
        reader = csv.DictReader(csvfile, delimiter=',')
        query = """
        INSERT INTO RESULTS (
            resultId,
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
            rankFastestLap,
            fastestLapTime,
            fastestLapSpeed,
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
            %s,
            %s,
            %s
        )
        """

        last_time = None
        start = datetime.datetime.now()
        log_data_load("RESULTS", "START", None, None)
        insert_data = []

        for row in reader:
            delta_string = row['time']
            curr_time = None

            if delta_string.find('+') > -1:
                curr_time = lt.delta_to_time(last_time, delta_string)
            elif delta_string.find('\\N') > -1:
                pass
            else:
                last_time = lt.time_transform(delta_string)
                curr_time = last_time

            insert_data.append(
                (
                    row['resultId'],
                    row['raceId'],
                    row['driverId'],
                    row['constructorId'],
                    lt.null_transform(row['number']),
                    lt.null_transform(row['grid']),
                    lt.null_transform(row['position']),
                    lt.null_transform(row['positionText']),
                    lt.null_transform(row['positionOrder']),
                    row['points'],
                    row['laps'],
                    curr_time.time() if curr_time is not None else None,
                    lt.null_transform(row['milliseconds']),
                    lt.null_transform(row['fastestLap']),
                    lt.null_transform(row['rank']),
                    lt.null_transform(row['fastestLapTime']),
                    lt.null_transform(row['fastestLapSpeed']),
                    row['statusId']
                )
            )

            if curr_time is not None:
                last_time = curr_time

        execute_batch(curr, query, insert_data)

        log_data_load("RESULTS", "END", start, len(insert_data))

        conn.commit()
        curr.close()
        conn.close()

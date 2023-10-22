import datetime
from os.path import join
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
            %(resultId)s,
            %(raceId)s,
            %(driverId)s,
            %(constructorId)s,
            %(carNumber)s,
            %(gridPosition)s,
            %(position)s,
            %(positionText)s,
            %(positionOrder)s,
            %(points)s,
            %(laps)s,
            %(time)s,
            %(timeInMillis)s,
            %(fastestLap)s,
            %(rankFastestLap)s,
            %(fastestLapTime)s,
            %(fastestLapSpeed)s,
            %(statusId)s
        )
        """

        last_time = None
        start = datetime.datetime.now()
        log_data_load("RESULTS", "START", None, None)
        count = 0

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

            data = {'resultId': row['resultId']
                , 'raceId': row['raceId']
                , 'driverId': row['driverId']
                , 'constructorId': row['constructorId']
                , 'carNumber': lt.null_transform(row['number'])
                , 'gridPosition': lt.null_transform(row['grid'])
                , 'position': lt.null_transform(row['position'])
                , 'positionText': lt.null_transform(row['positionText'])
                , 'positionOrder': lt.null_transform(row['positionOrder'])
                , 'points': row['points']
                , 'laps': row['laps']
                , 'time': curr_time.time() if curr_time is not None else None
                , 'timeInMillis': lt.null_transform(row['milliseconds'])
                , 'fastestLap': lt.null_transform(row['fastestLap'])
                , 'rankFastestLap': lt.null_transform(row['rank'])
                , 'fastestLapTime': lt.null_transform(row['fastestLapTime'])
                , 'fastestLapSpeed': lt.null_transform(row['fastestLapSpeed'])
                , 'statusId': row['statusId']
                    }

            curr.execute(query, data)
            count += 1

            if curr_time is not None:
                last_time = curr_time

        log_data_load("RESULTS", "END", start, count)

        conn.commit()
        curr.close()
        conn.close()

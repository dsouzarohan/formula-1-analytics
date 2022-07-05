import datetime
from os.path import join
from src.migrations.database import database as db
from src.utilities import load_transforms as lt
import csv

# Dataset path, TODO: Move this to a config file so it can be changed
from src.utilities.logger import log_data_load

path = "D:\\Documents\\Python Projects\\formula-1-analytics\\data\\external"


def load():
    conn, err = db.get_connection()
    if err:
        print("Error in connecting with database", err)
    curr = conn.cursor()

    curr.execute("""
        TRUNCATE TABLE SPRINT_RESULTS CASCADE 
    """)

    conn.commit()

    with open(join(path, "sprint_results.csv"), "r", encoding="utf8") as csvfile:
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
            %(sprintResultId)s,
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
            %(fastestLapTime)s,
            %(statusId)s
        )
        """

        start = datetime.datetime.now()
        log_data_load("SPRINT_RESULTS", "START", None, None)
        count = 0

        last_time = None

        for row in reader:

            delta_string = row['time']
            curr_time = None

            if delta_string.find('+') > -1:
                curr_time = lt.delta_to_time(last_time, delta_string)
            else:
                last_time = lt.time_transform(delta_string)
                curr_time = last_time

            data = {'sprintResultId': row['resultId']
                , 'raceId': row['raceId']
                , 'driverId': row['driverId']
                , 'constructorId': row['constructorId']
                , 'carNumber': row['number']
                , 'gridPosition': row['grid']
                , 'position': lt.null_transform(row['position'])
                , 'positionText': row['positionText']
                , 'positionOrder': row['positionOrder']
                , 'points': row['points']
                , 'laps': row['laps']
                , 'time': curr_time.time() if curr_time is not None else None
                , 'timeInMillis': lt.null_transform(row['milliseconds'])
                , 'fastestLap': lt.null_transform(row['fastestLap'])
                , 'fastestLapTime': lt.null_transform(row['fastestLapTime'])
                , 'statusId': row['statusId']
                    }

            curr.execute(query, data)
            last_time = curr_time
            count += 1

        log_data_load("SPRINT_RESULTS", "END", start, count)

        conn.commit()
        curr.close()
        conn.close()

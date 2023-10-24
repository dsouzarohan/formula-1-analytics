import datetime
from os.path import join
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
            %(raceId)s,
            %(driverId)s,
            %(stop)s,
            %(lap)s,
            %(time)s,
            %(duration)s,
            %(timeInMillis)s 
        )
        """

        start = datetime.datetime.now()
        log_data_load("PIT_STOPS", "START", None, None)
        count = 0

        for row in reader:

            data = {'raceId': row['raceId']
                , 'driverId': row['driverId']
                , 'stop': row['lap']
                , 'lap': row['lap']
                , 'time': lt.time_of_day_transform(row['time'])
                , 'duration': lt.time_transform(row['duration']).time()
                , 'timeInMillis': row['milliseconds']
                    }

            curr.execute(query, data)
            count += 1

        log_data_load("PIT_STOPS", "END", start, count)

        conn.commit()
        curr.close()
        conn.close()

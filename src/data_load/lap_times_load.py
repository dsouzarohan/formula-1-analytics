import datetime
from os.path import join
from src.database import database as db
import csv
from src.utilities import load_transforms as lt

# Dataset path, TODO: Move this to a config file so it can be changed
from src.utilities.logger import log_data_load

path = "D:\\Documents\\Python Projects\\formula-1-analytics\\data\\external"


def load():
    conn, err = db.get_connection()
    if err:
        print("Error in connecting with database", err)
    curr = conn.cursor()

    curr.execute("""
        TRUNCATE TABLE LAP_TIMES CASCADE 
    """)

    conn.commit()

    with open(join(path, "lap_times.csv"), "r", encoding="utf8") as csvfile:
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
            %(raceId)s,
            %(driverId)s,
            %(lap)s,
            %(position)s,
            %(time)s,
            %(timeInMillis)s 
        )
        """

        start = datetime.datetime.now()
        log_data_load("LAP_TIMES", "START", None, None)
        count = 0

        for row in reader:

            data = {'raceId': row['raceId']
                , 'driverId': row['driverId']
                , 'lap': row['lap']
                , 'position': row['position']
                , 'time': lt.time_transform(row['time']).time()
                , 'timeInMillis': row['milliseconds']
                    }

            curr.execute(query, data)
            count += 1

        log_data_load("LAP_TIMES", "END", start, count)

        conn.commit()
        curr.close()
        conn.close()

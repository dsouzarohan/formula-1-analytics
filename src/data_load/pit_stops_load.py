from os.path import join
from src.migrations.database import database as db
import csv
from src.utilities import load_transforms as lt

# Dataset path, TODO: Move this to a config file so it can be changed
path = "D:\\Documents\\Python Projects\\formula-1-analytics\\data\\external"


def load():
    conn, err = db.get_connection()
    if err:
        print("Error in connecting with database", err)
    curr = conn.cursor()

    curr.execute("""
        TRUNCATE TABLE PIT_STOPS CASCADE 
    """)

    conn.commit()

    with open(join(path, "pit_stops.csv"), "r", encoding="utf8") as csvfile:
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

        for row in reader:
            print(row)

            data = {'raceId': row['raceId']
                , 'driverId': row['driverId']
                , 'stop': row['lap']
                , 'lap': row['lap']
                , 'time': lt.time_of_day_transform(row['time'])
                , 'duration': lt.time_transform(row['duration']).time()
                , 'timeInMillis': row['milliseconds']
                    }

            curr.execute(query, data)

        conn.commit()
        curr.close()
        conn.close()

import datetime
from os.path import join
from src.database import database as db
import csv
from src.utilities.load_transforms import time_of_day_transform, date_transform

# Dataset path, TODO: Move this to a config file so it can be changed
from src.utilities.logger import log_data_load

path = "D:\\Documents\\Python Projects\\formula-1-analytics\\data\\external"


def load():
    conn, err = db.get_connection()
    if err:
        print("Error in connecting with database", err)
    curr = conn.cursor()

    curr.execute("""
        TRUNCATE TABLE RACES CASCADE 
    """)

    conn.commit()

    with open(join(path, "races.csv"), "r", encoding="utf8") as csvfile:
        reader = csv.DictReader(csvfile, delimiter=',')
        query = """
        INSERT INTO RACES (
            raceId,
            year,
            round,
            circuitId,
            name,
            date,
            time,
            url
        ) VALUES (
            %(raceId)s,
            %(year)s,
            %(round)s,
            %(circuitId)s,
            %(name)s,
            %(date)s,
            %(time)s,
            %(url)s         
        )
        """

        start = datetime.datetime.now()
        log_data_load("RACES", "START", None, None)
        count = 0

        for row in reader:

            data = {'raceId': row['raceId']
                , 'year': row['year']
                , 'round': row['round']
                , 'circuitId': row['circuitId']
                , 'name': row['name']
                , 'date': date_transform(row['date'])
                , 'time': time_of_day_transform(row['time'])
                , 'url': row['url']
                    }

            curr.execute(query, data)
            count += 1

        log_data_load("RACES", "END", start, count)

        conn.commit()
        curr.close()
        conn.close()

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
        TRUNCATE TABLE QUALIFYING CASCADE 
    """)

    conn.commit()

    with open(join(path, "qualifying.csv"), "r", encoding="utf8") as csvfile:
        reader = csv.DictReader(csvfile, delimiter=',')
        query = """
        INSERT INTO QUALIFYING (
            qualifyId,
            raceId,
            driverId,
            constructorId,
            number,
            position,
            q1,
            q2,
            q3
        ) VALUES (
            %(qualifyId)s,
            %(raceId)s,
            %(driverId)s,
            %(constructorId)s,
            %(number)s,
            %(position)s,
            %(q1)s,
            %(q2)s,
            %(q3)s
        )
        """

        start = datetime.datetime.now()
        log_data_load("QUALIFYING", "START", None, None)
        count = 0

        for row in reader:

            q1 = lt.time_transform(row['q1'])
            q2 = lt.time_transform(row['q2'])
            q3 = lt.time_transform(row['q3'])

            data = {'qualifyId': row['qualifyId']
                , 'raceId': row['raceId']
                , 'driverId': row['driverId']
                , 'constructorId': row['constructorId']
                , 'number': row['number']
                , 'position': row['position']
                , 'q1': None if q1 is None else q1.time()
                , 'q2': None if q2 is None else q2.time()
                , 'q3': None if q3 is None else q3.time()
                    }

            curr.execute(query, data)
            count += 1

        log_data_load("QUALIFYING", "END", start, count)

        conn.commit()
        curr.close()
        conn.close()

from os.path import join
from src.migrations.database import database as db
import csv
from datetime import datetime as dt

# Dataset path, TODO: Move this to a config file so it can be changed
path = "D:\\Documents\\Python Projects\\formula-1-analytics\\data\\external"

def time_transform (time_string):
    if time_string == '':
        return None
    else:
        return None if time_string.find('N') > 0 else dt.strptime(time_string, '%M:%S.%f').time()

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

        for row in reader:
            print(row)

            data = {'qualifyId': row['qualifyId']
                , 'raceId': row['raceId']
                , 'driverId': row['driverId']
                , 'constructorId': row['constructorId']
                , 'number': row['number']
                , 'position': row['position']
                , 'q1': time_transform(row['q1'])
                , 'q2': time_transform(row['q2'])
                , 'q3': time_transform(row['q3'])
                    }

            curr.execute(query, data)

        conn.commit()
        curr.close()
        conn.close()

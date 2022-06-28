from os.path import join
from src.migrations.database import database as db
import csv
from datetime import datetime as dt

# Dataset path, TODO: Move this to a config file so it can be changed
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

        for row in reader:

            data = {'raceId': row['raceId']
                , 'year': row['year']
                , 'round': row['round']
                , 'circuitId': row['circuitId']
                , 'name': row['name']
                , 'date': dt.strptime(row['date'], '%d/%m/%y').date()
                , 'time': None if row['time'].find('N') > 0 else dt.strptime(row['time'], '%H:%M:%S').time()
                , 'url': row['url']
                    }

            curr.execute(query, data)

        conn.commit()
        curr.close()
        conn.close()

import datetime
from os.path import join
from src.database import database as db
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
        TRUNCATE TABLE DRIVER_STANDINGS CASCADE 
    """)

    conn.commit()

    with open(join(path, "driver_standings.csv"), "r", encoding="utf8") as csvfile:
        reader = csv.DictReader(csvfile, delimiter=',')
        query = """
        INSERT INTO DRIVER_STANDINGS (
            driverStandingsId,
            raceId,
            driverId,
            points,
            position,
            positionText,
            wins
        ) VALUES (
            %(driverStandingsId)s,
            %(raceId)s,
            %(driverId)s,
            %(points)s,
            %(position)s,
            %(positionText)s,         
            %(wins)s         
        )
        """

        start = datetime.datetime.now()
        log_data_load("DRIVER_STANDINGS", "START", None, None)
        count = 0

        for row in reader:

            data = {'driverStandingsId': row['driverStandingsId']
                , 'raceId': row['raceId']
                , 'driverId': row['driverId']
                , 'points': row['points']
                , 'position': row['position']
                , 'positionText': row['positionText']
                , 'wins': row['wins']
                    }

            curr.execute(query, data)
            count += 1

        log_data_load("DRIVER_STANDINGS", "END", start, count)

        conn.commit()
        curr.close()
        conn.close()

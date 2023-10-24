import datetime
from os.path import join
from src.database import database as db
import csv
from src.config.config import DATA_PATH
from src.utilities.logger import log_data_load


def load():
    conn, err = db.get_connection()
    if err:
        print("Error in connecting with database", err)
    curr = conn.cursor()

    curr.execute("""
        TRUNCATE TABLE CONSTRUCTOR_STANDINGS CASCADE 
    """)

    conn.commit()

    with open(join(DATA_PATH, "constructor_standings.csv"), "r", encoding="utf8") as csvfile:
        reader = csv.DictReader(csvfile, delimiter=',')
        query = """
        INSERT INTO CONSTRUCTOR_STANDINGS (
            constructorStandingsId,
            raceId,
            constructorId,
            points,
            position,
            positionText,
            wins
        ) VALUES (
            %(constructorStandingsId)s,
            %(raceId)s,
            %(constructorId)s,
            %(points)s,
            %(position)s,
            %(positionText)s,         
            %(wins)s         
        )
        """

        start = datetime.datetime.now()
        log_data_load("CONSTRUCTOR_STANDINGS", "START", None, None)
        count = 0

        for row in reader:

            data = {'constructorStandingsId': row['constructorStandingsId']
                , 'raceId': row['raceId']
                , 'constructorId': row['constructorId']
                , 'points': row['points']
                , 'position': row['position']
                , 'positionText': row['positionText']
                , 'wins': row['wins']
                    }

            curr.execute(query, data)
            count += 1

        log_data_load("CONSTRUCTOR_STANDINGS", "END", start, count)

        conn.commit()
        curr.close()
        conn.close()

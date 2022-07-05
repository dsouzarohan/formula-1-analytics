import datetime
from os.path import join
from src.migrations.database import database as db
from src.utilities.load_transforms import null_transform
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
        TRUNCATE TABLE CONSTRUCTOR_STANDINGS CASCADE 
    """)

    conn.commit()

    with open(join(path, "constructor_standings.csv"), "r", encoding="utf8") as csvfile:
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

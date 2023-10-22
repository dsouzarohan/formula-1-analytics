import datetime
from os.path import join
from src.database import database as db
from src.utilities.load_transforms import null_transform
import csv
from src.config.config import DATA_PATH
from src.utilities.logger import log_data_load


def load():
    conn, err = db.get_connection()
    if err:
        print("Error in connecting with database", err)
    curr = conn.cursor()

    curr.execute("""
        TRUNCATE TABLE CONSTRUCTOR_RESULTS CASCADE 
    """)

    conn.commit()

    with open(join(DATA_PATH, "constructor_results.csv"), "r", encoding="utf8") as csvfile:
        reader = csv.DictReader(csvfile, delimiter=',')
        query = """
        INSERT INTO CONSTRUCTOR_RESULTS (
            constructorResultId,
            raceId,
            constructorId,
            points,
            status
        ) VALUES (
            %(constructorResultId)s,
            %(raceId)s,
            %(constructorId)s,
            %(points)s,
            %(status)s         
        )
        """

        start = datetime.datetime.now()
        log_data_load("CONSTRUCTOR_RESULTS", "START", None, None)
        count = 0

        for row in reader:
            data = {'constructorResultId': row['constructorResultsId']
                , 'raceId': row['raceId']
                , 'constructorId': row['constructorId']
                , 'points': row['points']
                , 'status': null_transform(row['status'])
                    }

            curr.execute(query, data)
            count += 1

        log_data_load("CONSTRUCTOR_RESULTS", "END", start, count)

        conn.commit()
        curr.close()
        conn.close()


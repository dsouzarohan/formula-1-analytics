import datetime
from os.path import join

from psycopg2.extras import execute_batch

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
            %s,
            %s,
            %s,
            %s,
            %s         
        )
        """

        start = datetime.datetime.now()
        log_data_load("CONSTRUCTOR_RESULTS", "START", None, None)
        insert_data = []

        for row in reader:
            insert_data.append(
                (
                    row["constructorResultsId"],
                    row["raceId"],
                    row["constructorId"],
                    row["points"],
                    null_transform(row['status'])
                )
            )

        execute_batch(curr, query, insert_data)
        log_data_load("CONSTRUCTOR_RESULTS", "END", start, len(insert_data))

        conn.commit()
        curr.close()
        conn.close()


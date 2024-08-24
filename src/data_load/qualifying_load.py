import datetime
from os.path import join

from psycopg2.extras import execute_batch

from src.database import database as db
import csv
from src.utilities import load_transforms as lt
from src.config.config import DATA_PATH
from src.utilities.logger import log_data_load


def load():
    conn, err = db.get_connection()
    if err:
        print("Error in connecting with database", err)
    curr = conn.cursor()

    curr.execute("""
        TRUNCATE TABLE QUALIFYING CASCADE 
    """)

    conn.commit()

    with open(join(DATA_PATH, "qualifying.csv"), "r", encoding="utf8") as csvfile:
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
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s
        )
        """

        start = datetime.datetime.now()
        log_data_load("QUALIFYING", "START", None, None)
        insert_data = []

        for row in reader:

            q1 = lt.time_transform(row['q1'])
            q2 = lt.time_transform(row['q2'])
            q3 = lt.time_transform(row['q3'])

            insert_data.append(
                (
                    row["qualifyId"],
                    row["raceId"],
                    row["driverId"],
                    row["constructorId"],
                    row["number"],
                    row["position"],
                    None if q1 is None else q1.time(),
                    None if q2 is None else q2.time(),
                    None if q3 is None else q3.time(),
                )
            )

        execute_batch(curr, query, insert_data)
        log_data_load("QUALIFYING", "END", start, len(insert_data))

        conn.commit()
        curr.close()
        conn.close()

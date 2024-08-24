import datetime
from os.path import join

from psycopg2.extras import execute_batch

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
        log_data_load("CONSTRUCTOR_STANDINGS", "START", None, None)
        insert_data = []

        for row in reader:
            insert_data.append(
                (
                    row["constructorStandingsId"],
                    row["raceId"],
                    row["constructorId"],
                    row["points"],
                    row["position"],
                    row["positionText"],
                    row["wins"]
                )
            )

        execute_batch(curr, query, insert_data)
        log_data_load("CONSTRUCTOR_STANDINGS", "END", start, len(insert_data))

        conn.commit()
        curr.close()
        conn.close()

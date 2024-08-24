import datetime
from os.path import join

from psycopg2.extras import execute_batch

from src.database import database as db
import csv
from src.utilities.logger import log_data_load
from src.config.config import DATA_PATH


def load():
    conn, err = db.get_connection()
    if err:
        print("Error in connecting with database", err)
    curr = conn.cursor()

    curr.execute("""
        TRUNCATE TABLE DRIVER_STANDINGS CASCADE 
    """)

    conn.commit()

    with open(join(DATA_PATH, "driver_standings.csv"), "r", encoding="utf8") as csvfile:
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
        log_data_load("DRIVER_STANDINGS", "START", None, None)
        insert_data = []

        for row in reader:
            insert_data.append(
                (
                    row["driverStandingsId"],
                    row["raceId"],
                    row["driverId"],
                    row["points"],
                    row["position"],
                    row["positionText"],
                    row["wins"]
                )
            )

        execute_batch(curr, query, insert_data)
        log_data_load("DRIVER_STANDINGS", "END", start, len(insert_data))

        conn.commit()
        curr.close()
        conn.close()

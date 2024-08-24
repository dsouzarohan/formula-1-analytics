import datetime
from os.path import join
from src.database import database as db
from src.utilities.load_transforms import null_transform
from src.utilities.logger import log_data_load
from src.config.config import DATA_PATH
import csv
from psycopg2.extras import execute_batch


def load():
    conn, err = db.get_connection()
    if err:
        print("Error in connecting with database", err)
    curr = conn.cursor()

    curr.execute("""
        TRUNCATE TABLE CIRCUITS CASCADE
    """)

    conn.commit()

    with open(join(DATA_PATH, "circuits.csv"), "r") as csvfile:
        reader = csv.DictReader(csvfile, delimiter=',')
        query = """
        INSERT INTO CIRCUITS (
            circuitId,
            circuitRef,
            name,
            location,
            country,
            lat,
            lng,
            altitude,
            url
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
        log_data_load("CIRCUITS", "START", None, None)
        insert_data = []

        for row in reader:
            insert_data.append(
                (
                    int(row['circuitId']),
                    row['circuitRef'],
                    row['name'],
                    row['location'],
                    row['country'],
                    row['lat'],
                    row['lng'],
                    null_transform(row['alt']),
                    row['url']
                )
            )
        execute_batch(curr, query, insert_data)

        log_data_load("CIRCUITS", "END", start, len(insert_data))

        conn.commit()
        curr.close()
        conn.close()

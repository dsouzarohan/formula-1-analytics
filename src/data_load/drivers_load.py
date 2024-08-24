import datetime
from os.path import join

from psycopg2.extras import execute_batch

from src.database import database as db
from src.utilities.load_transforms import null_transform
import csv
from datetime import datetime as dt
from src.config.config import DATA_PATH
from src.utilities.logger import log_data_load


def load():
    conn, err = db.get_connection()
    if err:
        print("Error in connecting with database", err)
    curr = conn.cursor()

    curr.execute("""
        TRUNCATE TABLE DRIVERS CASCADE 
    """)

    conn.commit()

    with open(join(DATA_PATH, "drivers.csv"), "r", encoding="utf8") as csvfile:
        reader = csv.DictReader(csvfile, delimiter=',')
        query = """
        INSERT INTO DRIVERS (
            driverId,
            refname,
            number,
            code,
            forename,
            surname,
            dob,
            nationality,
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
        log_data_load("DRIVERS", "START", None, None)
        insert_data = []

        for row in reader:
            insert_data.append(
                (
                    row['driverId'],
                    row['driverRef'],
                    null_transform(row['number']),
                    null_transform(row['code']),
                    row['forename'],
                    row['surname'],
                    dt.strptime(row['dob'], '%Y-%m-%d').date(),
                    row['nationality'],
                    row['url']
                )
            )
        execute_batch(curr, query, insert_data)

        log_data_load("DRIVERS", "END", start, len(insert_data))

        conn.commit()
        curr.close()
        conn.close()

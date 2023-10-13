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
        TRUNCATE TABLE STATUS CASCADE 
    """)

    conn.commit()

    with open(join(path, "status.csv"), "r", encoding="utf8") as csvfile:
        reader = csv.DictReader(csvfile, delimiter=',')
        query = """
        INSERT INTO STATUS (
            statusId,
            status
        ) VALUES (
            %(statusId)s,
            %(status)s       
        )
        """

        start = datetime.datetime.now()
        log_data_load("STATUS", "START", None, None)
        count = 0

        for row in reader:

            data = {'statusId': row['statusId']
                , 'status': row['status']
                    }

            curr.execute(query, data)
            count += 1

        log_data_load("STATUS", "END", start, count)

        conn.commit()
        curr.close()
        conn.close()

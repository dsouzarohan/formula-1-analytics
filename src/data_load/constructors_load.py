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
        TRUNCATE TABLE CONSTRUCTORS CASCADE 
    """)

    conn.commit()

    with open(join(DATA_PATH, "constructors.csv"), "r", encoding="utf8") as csvfile:
        reader = csv.DictReader(csvfile, delimiter=',')
        query = """
        INSERT INTO CONSTRUCTORS (
            constructorId,
            constructorRef,
            name,
            nationality,
            url
        ) VALUES (
            %(constructorId)s,
            %(constructorRef)s,
            %(name)s,
            %(nationality)s,
            %(url)s         
        )
        """

        start = datetime.datetime.now()
        log_data_load("CONSTRUCTORS", "START", None, None)
        count = 0

        for row in reader:

            data = {'constructorId': row['constructorId']
                , 'constructorRef': row['constructorRef']
                , 'name': row['name']
                , 'nationality': row['nationality']
                , 'url': row['url']
                    }

            curr.execute(query, data)

            count += 1

        log_data_load("CONSTRUCTORS", "END", start, count)

        conn.commit()
        curr.close()
        conn.close()

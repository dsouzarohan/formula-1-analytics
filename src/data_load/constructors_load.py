import datetime
from os.path import join
from src.migrations.database import database as db
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
        TRUNCATE TABLE CONSTRUCTORS CASCADE 
    """)

    conn.commit()

    with open(join(path, "constructors.csv"), "r", encoding="utf8") as csvfile:
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

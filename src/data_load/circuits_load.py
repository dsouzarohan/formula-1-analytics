import datetime
from os.path import join
from src.migrations.database import database as db
from src.utilities.load_transforms import null_transform
from src.utilities.logger import log_data_load
import csv

# Dataset path, TODO: Move this to a config file so it can be changed
path = "D:\\Documents\\Python Projects\\formula-1-analytics\\data\\external"


def load():
    conn, err = db.get_connection()
    if err:
        print("Error in connecting with database", err)
    curr = conn.cursor()

    curr.execute("""
        TRUNCATE TABLE CIRCUITS CASCADE 
    """)

    conn.commit()

    with open(join(path, "circuits.csv"), "r") as csvfile:
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
            %(circuitId)s,
            %(circuitRef)s,
            %(name)s,
            %(location)s,
            %(country)s,
            %(lat)s,
            %(lng)s,
            %(alt)s,
            %(url)s           
        )
        """

        start = datetime.datetime.now()
        log_data_load("CIRCUITS", "START", None, None)
        count = 0

        for row in reader:
            data = {'circuitId': int(row['circuitId'])
                , 'circuitRef': row['circuitRef']
                , 'name': row['name']
                , 'location': row['location']
                , 'country': row['country']
                , 'lat': row['lat']
                , 'lng': row['lng']
                , 'alt': null_transform(row['alt'])
                , 'url': row['url']
                    }

            curr.execute(query, data)
            count += 1

        log_data_load("CIRCUITS", "END", start, count)

        conn.commit()
        curr.close()
        conn.close()

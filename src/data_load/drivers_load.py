import datetime
from os.path import join
from src.database import database as db
from src.utilities.load_transforms import null_transform
import csv
from datetime import datetime as dt

# Dataset path, TODO: Move this to a config file so it can be changed
from src.utilities.logger import log_data_load

path = "D:\\Documents\\Python Projects\\formula-1-analytics\\data\\external"


def load():
    conn, err = db.get_connection()
    if err:
        print("Error in connecting with database", err)
    curr = conn.cursor()

    curr.execute("""
        TRUNCATE TABLE DRIVERS CASCADE 
    """)

    conn.commit()

    with open(join(path, "drivers.csv"), "r", encoding="utf8") as csvfile:
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
            %(driverId)s,
            %(refname)s,
            %(number)s,
            %(code)s,
            %(forename)s,
            %(surname)s,
            %(dob)s,
            %(nationality)s,
            %(url)s         
        )
        """

        start = datetime.datetime.now()
        log_data_load("DRIVERS", "START", None, None)
        count = 0

        for row in reader:

            data = {'driverId': row['driverId']
                , 'refname': row['driverRef']
                , 'number': null_transform(row['number'])
                , 'code': null_transform(row['code'])
                , 'forename': row['forename']
                , 'surname': row['surname']
                , 'dob': dt.strptime(row['dob'], '%Y-%m-%d').date()
                , 'nationality': row['nationality']
                , 'url': row['url']
                    }

            curr.execute(query, data)
            count += 1

        log_data_load("DRIVERS", "END", start, count)

        conn.commit()
        curr.close()
        conn.close()

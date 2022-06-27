from os.path import join
from src.migrations.database import database as db
import csv
from datetime import datetime as dt

# Dataset path, TODO: Move this to a config file so it can be changed
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

        for row in reader:
            print(row)

            data = {'driverId': row['driverId']
                , 'refname': row['driverRef']
                , 'number': None if row['number'].find('N') > 0 else row['number']
                , 'code': None if row['code'].find('N') > 0 else row['code']
                , 'forename': row['forename']
                , 'surname': row['surname']
                , 'dob': dt.strptime(row['dob'], '%Y-%m-%d').date()
                , 'nationality': row['nationality']
                , 'url': row['url']
                    }

            curr.execute(query, data)

        conn.commit()
        curr.close()
        conn.close()

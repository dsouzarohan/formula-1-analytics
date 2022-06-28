from os.path import join
from src.migrations.database import database as db
import csv

# Dataset path, TODO: Move this to a config file so it can be changed
path = "D:\\Documents\\Python Projects\\formula-1-analytics\\data\\external"


def load():
    conn, err = db.get_connection()
    if err:
        print("Error in connecting with database", err)
    curr = conn.cursor()

    curr.execute("""
        TRUNCATE TABLE SEASONS CASCADE 
    """)

    conn.commit()

    with open(join(path, "seasons.csv"), "r", encoding="utf8") as csvfile:
        reader = csv.DictReader(csvfile, delimiter=',')
        query = """
        INSERT INTO SEASONS (
            year,
            url
        ) VALUES (
            %(year)s,
            %(url)s       
        )
        """

        for row in reader:
            print(row)

            data = {'year': row['year']
                , 'url': row['url']
                    }

            curr.execute(query, data)

        conn.commit()
        curr.close()
        conn.close()

import os
from os import listdir
from os.path import join
from src.migrations.database import database as db
import csv

# Dataset path, TODO: Move this to a config file so it can be changed
path = "D:\\Documents\\Python Projects\\formula-1-analytics\\data\\external"


def read_datasets():
    files = os.listdir(path)

    conn, err = db.get_connection()
    if err:
        print("Error in connecting with database", err)
    curr = conn.cursor()

    with open(join(path, files[0]), "r") as csvfile:
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

        for row in reader:
            print(type(row))

            data = {'circuitId': int(row['circuitId'])
                , 'circuitRef': row['circuitRef']
                , 'name': row['name']
                , 'location': row['location']
                , 'country': row['country']
                , 'lat': row['lat']
                , 'lng': row['lng']
                , 'alt': None if row['alt'].find('N') > 0 else row['alt']
                , 'url': row['url']
            }

            print(data)

            curr.execute(query, data)

        conn.commit()
        curr.close()
        conn.close()


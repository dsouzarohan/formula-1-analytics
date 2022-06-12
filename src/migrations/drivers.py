import psycopg2
from src.migrations.database import database as db


def up():
    query = """create table drivers
    (
        id          integer not null primary key,
        refname     varchar(100),
        number      integer,
        code        varchar(3),
        forename    varchar(20),
        dob         date,
        nationality varchar(20),
        url         varchar(100)
    )"""

    conn, err = db.get_connection()
    if err:
        print("Error in connecting with database", err)
    curr = conn.cursor()
    # TODO: Change query to create table based on DDL

    curr.execute("SELECT VERSION()")
    db_version = curr.fetchone()
    print(db_version)
    conn.close()

    # TODO: Create another function down() for down migrations

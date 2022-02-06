import psycopg2

query= """create table drivers
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

conn = None

try:
    #connect and create table
    conn = psycopg2.connect(host="localhost", database="formula_1_db", user="postgres", password="admin123")
    cur = conn.cursor()

    #Create table 'drivers' in the pgdb
    res = cur.execute(query)
    print(res)
    conn.commit()
except (Exception, psycopg2.DatabaseError) as err:
    print(err)
finally:
    if conn is not None:
        conn.close()


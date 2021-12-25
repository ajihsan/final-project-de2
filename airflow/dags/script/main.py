import json
import pandas as pd

import requests
from sqlalchemy import create_engine
from psycopg2 import connect
from psycopg2.extras import execute_values


def connection_mysql():
    with open ('dags/script/credentials.json', "r") as cred:
        credential = json.load(cred)
        credential = credential['mysql_lake']

    username = credential['username']
    password = credential['password']
    host = credential['host']
    port = credential['port']
    database = credential['database']

    engine = create_engine('mysql+pymysql://{}:{}@{}:{}/{}'
                                .format(username, password, host, port, database))
    engine_conn = engine.connect()
    print("Connect Engine MySQL")
    return engine, engine_conn


def connection_postgresql(username, password, host, port, database):
    with open ('dags/script/credentials.json', "r") as cred:
        credential = json.load(cred)
        credential = credential['postgres_warehouse']

    username = credential['username']
    password = credential['password']
    host = credential['host']
    port = credential['port']
    database = credential['database']
    
    conn = connect(
    user=username,
    password=password,
    host=host,
    port=port,
    database=database
    )
    cursor = conn.cursor()
    print("Connect Cursor Postgresql")
    return conn, cursor




def insert_execute(conn, cursor, data:json):
    column = list(data[0].keys())
    final_list_data = []

    for data in data:
        dat = []
        for col in column:
            dat.append(data[col])
        final_list_data.append(tuple(dat))

    insert_query = """
        INSERT INTO sales ({})
        VALUES %s
        ON CONFLICT (id)
        DO NOTHING
        """.format(','.join(column))

    execute_values(
        cursor, insert_query, final_list_data, template=None, page_size=100
    )
    conn.commit()



def insert_raw_to_mysql():
    response = requests.get('https://covid19-public.digitalservice.id/api/v1/rekapitulasi_v2/jabar/harian?level=kab')
    data = response.json()
    df = pd.DataFrame(data['data']['content']) 
    
    engine, engine_conn = connection_mysql()
    df.to_sql(name='staging_table', con=engine, if_exists="replace", index=False)

    engine.dispose()






import json
from os import name
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

    engine = create_engine('mysql+mysqlconnector://{}:{}@{}:{}/{}'.format(username, password, host, port, database))
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


def insert_raw_to_mysql():
    response = requests.get('https://covid19-public.digitalservice.id/api/v1/rekapitulasi_v2/jabar/harian?level=kab')
    data = response.json()
    df = pd.DataFrame(data['data']['content']) 
    
    engine, engine_conn = connection_mysql()
    df.to_sql(name='rekapitulasi_kasus_harian', con=engine, if_exists="replace", index=False)
    engine.dispose()


def insert_dim_province(data):
    column_start = ["kode_prov", "nama_prov"]
    column_end = ["province_id", "province_name"]

    data = data[column_start]
    data = data.drop_duplicates(column_start)
    data.columns = column_end

    return print(data.head())
    return data


def insert_dim_distict(data):
    column_start = ["kode_kab", "kode_prov", "nama_kab"]
    column_end = ["district_id", "province_id", "province_name"]

    data = data[column_start]
    data = data.drop_duplicates(column_start)
    data.columns = column_end

    return print(data.head())
    return data


def insert_dim_case(data):
    column_start = ["suspect_diisolasi", "suspect_discarded", "closecontact_dikarantina", "closecontact_discarded", "probable_diisolasi", "probable_discarded", "confirmation_sembuh", "confirmation_meninggal", "suspect_meninggal", "closecontact_meninggal", "probable_meninggal"]
    column_end = ["status_name", "status_detail"]

    data = data[column_start]
    data = data[:1]
    data = data.melt(var_name="status", value_name="total")
    data = data.drop_duplicates("status").sort_values("status")
    data[['status_name', 'status_detail']] = data['status'].str.split('_', n=1, expand=True)
    data = data[column_end]

    return print(data.head(10))
    return data


def insert_fact_province_daily(data):
    column_start = ["tanggal", "kode_prov", "suspect_diisolasi", "suspect_discarded", "closecontact_dikarantina", "closecontact_discarded", "probable_diisolasi", "probable_discarded", "confirmation_sembuh", "confirmation_meninggal", "suspect_meninggal", "closecontact_meninggal", "probable_meninggal"]
    column_end = ['date', 'province_id', 'case_id', 'total']

    # AGGREGATE
    data = data[column_start]
    data_sum = data.melt(id_vars=["tanggal", "kode_prov"], var_name="status", value_name="total").sort_values(["tanggal", "kode_prov", "status"])
    data_sum = data_sum.groupby(by=['tanggal', 'kode_prov', 'status']).sum()
    data_sum = data_sum.reset_index()

    # REFORMAT
    data_sum.columns = column_end

    return print(data_sum.head())
    return data_sum


def insert_fact_province_monthly(data):
    column_start = ["tanggal", "kode_prov", "suspect_diisolasi", "suspect_discarded", "closecontact_dikarantina", "closecontact_discarded", "probable_diisolasi", "probable_discarded", "confirmation_sembuh", "confirmation_meninggal", "suspect_meninggal", "closecontact_meninggal", "probable_meninggal"]
    column_end = ['month', 'province_id', 'case_id', 'total']

    # AGGREGATE
    data = data[column_start]
    data['tanggal'] = data['tanggal'].apply(lambda x: x[:7])
    data_sum = data.melt(id_vars=["tanggal", "kode_prov"], var_name="status", value_name="total").sort_values(["tanggal", "kode_prov", "status"])
    data_sum = data_sum.groupby(by=['tanggal', 'kode_prov', 'status']).sum()
    data_sum = data_sum.reset_index()

    # REFORMAT
    data_sum.columns = column_end

    return print(data_sum.head())
    return data_sum


def insert_fact_province_yearly(data):
    column_start = ["tanggal", "kode_prov", "suspect_diisolasi", "suspect_discarded", "closecontact_dikarantina", "closecontact_discarded", "probable_diisolasi", "probable_discarded", "confirmation_sembuh", "confirmation_meninggal", "suspect_meninggal", "closecontact_meninggal", "probable_meninggal"]
    column_end = ['year', 'province_id', 'case_id', 'total']

    # AGGREGATE
    data = data[column_start]
    data['tanggal'] = data['tanggal'].apply(lambda x: x[:4])
    data_sum = data.melt(id_vars=["tanggal", "kode_prov"], var_name="status", value_name="total").sort_values(["tanggal", "kode_prov", "status"])
    data_sum = data_sum.groupby(by=['tanggal', 'kode_prov', 'status']).sum()
    data_sum = data_sum.reset_index()

    # REFORMAT
    data_sum.columns = column_end

    return print(data_sum.head())
    return data_sum


def insert_fact_district_monthly(data):
    column_start = ["tanggal", "kode_kab", "suspect_diisolasi", "suspect_discarded", "closecontact_dikarantina", "closecontact_discarded", "probable_diisolasi", "probable_discarded", "confirmation_sembuh", "confirmation_meninggal", "suspect_meninggal", "closecontact_meninggal", "probable_meninggal"]
    column_end = ['month', 'district_id', 'case_id', 'total']

    # AGGREGATE
    data = data[column_start]
    data['tanggal'] = data['tanggal'].apply(lambda x: x[:7])
    data_sum = data.melt(id_vars=["tanggal", "kode_kab"], var_name="status", value_name="total").sort_values(["tanggal", "kode_kab", "status"])
    data_sum = data_sum.groupby(by=['tanggal', 'kode_kab', 'status']).sum()
    data_sum = data_sum.reset_index()

    # REFORMAT
    data_sum.columns = column_end

    return print(data_sum.head())
    return data_sum


def insert_fact_district_yearly(data):
    column_start = ["tanggal", "kode_kab", "suspect_diisolasi", "suspect_discarded", "closecontact_dikarantina", "closecontact_discarded", "probable_diisolasi", "probable_discarded", "confirmation_sembuh", "confirmation_meninggal", "suspect_meninggal", "closecontact_meninggal", "probable_meninggal"]
    column_end = ['year', 'district_id', 'case_id', 'total']

    # AGGREGATE
    data = data[column_start]
    data['tanggal'] = data['tanggal'].apply(lambda x: x[:4])
    data_sum = data.melt(id_vars=["tanggal", "kode_kab"], var_name="status", value_name="total").sort_values(["tanggal", "kode_kab", "status"])
    data_sum = data_sum.groupby(by=['tanggal', 'kode_kab', 'status']).sum()
    data_sum = data_sum.reset_index()

    # REFORMAT
    data_sum.columns = column_end

    return print(data_sum.head())
    return data_sum


def insert_raw_to_warehouse():
    engine, engine_conn = connection_mysql()
    data = pd.read_sql(sql='rekapitulasi_kasus_harian', con=engine)
    engine.dispose()

    # filter needed column
    column = ["tanggal", "kode_prov", "nama_prov", "kode_kab", "nama_kab", "suspect_diisolasi", "suspect_discarded", "closecontact_dikarantina", "closecontact_discarded", "probable_diisolasi", "probable_discarded", "confirmation_sembuh", "confirmation_meninggal", "suspect_meninggal", "closecontact_meninggal", "probable_meninggal"]
    data = data[column]

    # insert_dim_province(data)
    # insert_dim_distict(data)
    # insert_dim_case(data)

    insert_fact_province_daily(data)
    insert_fact_province_monthly(data)
    insert_fact_province_yearly(data)
    insert_fact_district_monthly(data)
    insert_fact_district_yearly(data)
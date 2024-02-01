'''
    =================================================


    Nama  : Mardhya Malik Nurbani


    Program ini dibuat untuk melakukan automatisasi transform dan load data dari PostgreSQL ke ElasticSearch. 
    Adapun dataset yang dipakai adalah dataset mengenai data penjualan Superstore di Amerika Serikat.
    =================================================
'''


import pandas as pd
import datetime as dt
from datetime import datetime, timedelta
import warnings
import psycopg2
import re
from elasticsearch import Elasticsearch
warnings.filterwarnings("ignore")

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# A.) POSTGRESQL
def fecth_data():
    db_name = 'airflow'
    db_user = 'airflow'
    db_password = 'airflow'
    db_host = 'postgres'
    db_port = '5432'

    #Connect to database
    connection = psycopg2.connect(
        database = db_name,
        user = db_user,
        password = db_password,
        host = db_host,
        port = db_port
    )

    #Get all data
    select_query = 'SELECT * FROM table_m3;'
    df = pd.read_sql(select_query, connection)

    #Close The Connection
    connection.close()

    #Save into CSV
    df.to_csv('/opt/airflow/dags/P2M3_mardhya_data_raw.csv', index=False)
'''
     Fungsi Fetch Data.

        - terhubung ke database PostgreSQL menggunakan modul psycopg2 menggunakan database, user, pass, host, dan port yang sudah disesuikan pada def fecth_data()
        - mengambil semua data dari tabel table_m3 
        - Anda menyimpan data tersebut ke dalam file CSV dengan nama P2M3_mardhya_data_raw.csv
'''

# B.) Data Cleaning
def data_cleaning():
    df = pd.read_csv('/opt/airflow/dags/P2M3_mardhya_data_raw.csv')

    cols = df.columns.tolist()
    new_cols = []

    for col in cols:
        cleaned_col = re.findall(r'[A-Z](?:[a-z]+|[A-Z]*(?=[A-Z]|$))', col)
        cleaned_col = [x.lower() for x in cleaned_col]
        cleaned_col = '_'.join(cleaned_col)
        new_cols.append(cleaned_col)

    df.columns = new_cols

    if df.isnull().any().any():
        df.dropna(inplace=True)
    else:
        print('There are no missing values.')

    if df.duplicated().any():
        df.drop_duplicates(inplace=True)
    else:
        print('There is no duplicated data.')

    # Save to CSV
    df.to_csv('/opt/airflow/dags/P2M3_mardhya_data_clean.csv', index=False)
'''
     Fungsi Data Cleaning.
        - Data yang sudah di ambil dan di save dari Postgree akan di load kembali
        - Membersihkan nama kolom dengan mengubah huruf kapital menjadi huruf kecil dan menggunakan underscoring sebagai pengganti spasi `cleaned_col = re.findall(r'[A-Z](?:[a-z]+|[A-Z]*(?=[A-Z]|$))', col)`
        - menangani nilai-nilai yang hilang dengan menghapus baris pada `if df.isnull().any().any()` dan `if df.duplicated().any()`
        - Hasil data cleaning akan di save kedalam bentuk csv dengan nama P2M3_mardhya_data_clean.csv
'''

# Call the function to execute the data cleaning process
data_cleaning()

# C.) Insert into elastic search - with airflow

def insert_into_elastic_manual():
    df = pd.read_csv('/opt/airflow/dags/P2M3_mardhya_data_clean.csv')

    #Checking Connection
    es = Elasticsearch('http://elasticsearch:9200')
    print('Connection Status : ', es.ping())

    #Insert CSV file to Elastic Search
    for i, r in df.iterrows():
        doc = r.to_json()
        # try:
        #     print(i, r['name']) 
        res = es.index(index="datastore",doc_type="doc",body=doc)
        print(res)
    #     except:
    #         print('Index Gagal : ', r['index'])
    #         failed_insert.append(r['index'])
    #         pass

    # print('DONE')
    # print('Failed Insert : ', failed_insert)
'''
     Fungsi Insert into elastic search - with airflow.
        - Data yang sudah di dicleaning dan di save akan di load kembali
        - Anda menggunakan pustaka elasticsearch untuk berinteraksi dengan Elasticsearch
        - Setelah itu, memasukkan setiap baris data ke dalam Elasticsearch dengan menggunakan es.index()
        - es = Elasticsearch('http://elasticsearch:9200') URL di mana instansi Elasticsearch Anda sedang berjalan. Elasticsearch biasanya berjalan pada port 9200 secara default, dan URL yang Anda berikan menunjukkan bahwa 
        berjalan pada mesin yang sama (elasticsearch) di mana kode Anda berjalan.
'''

# D.) Data Pipeline
default_args ={
    'owner' : 'mardhya',
    'start_date': dt.datetime(2024, 1, 26, 6, 30) - dt.timedelta(hours=7),
    'retrie': 1,
    'retries_delay' : dt.timedelta(minutes=5)
}

with DAG(
    'p2m3',
    default_args = default_args,
    schedule_interval = '@daily', #'*/30 * * * *',
    catchup = False) as dag:

    node_start = BashOperator(
        task_id='starting',
        bash_command='echo "I am reading the csv now ...." ')

    node_fetch_data = PythonOperator(
        task_id='fetch-data',
        python_callable=fecth_data)

    node_data_cleaning = PythonOperator(
        task_id='data-cleaning',
        python_callable=data_cleaning)
    
    node_insert_data_to_elastic = PythonOperator(
        task_id='insert-data-to-elastic',
        python_callable=insert_into_elastic_manual)

node_start >> node_fetch_data >> node_data_cleaning >> node_insert_data_to_elastic

'''
     Fungsi Data Pipeline with Apache Airflow.
        - mendefinisikan  sebuah default_args untuk DAG tersebut seperti pemilik, tanggal mulai, jumlah percobaan, dan penundaan antar percobaan
        - lalu, mendefinisikan sebuah DAG (Directed Acyclic Graph) dengan nama 'p2m3'.
        - DAG dijadwalkan untuk dijalankan setiap 30 menit
            - node_start: Membaca pesan bahwa proses membaca file CSV
            - node_fetch_data: Menjalankan fungsi fecth_data() untuk mengambil data dari PostgreSQL
            - node_data_cleaning: Menjalankan fungsi data_cleaning() untuk membersihkan data yang telah diambil.
            - node_insert_data_to_elastic: Menjalankan fungsi insert_into_elastic_manual() untuk memasukkan data yang telah dibersihkan ke Elasticsearch.
     Setelah mendefinisikan DAG, lalu membuat aliran antara node-node tersebut, dimulai dari node_start dan berakhir di node_insert_data_to_elastic.

    workflow : mengambil data dari database > membersihkan pada Data Cleaning > Menyimpan pada Elasticsearch menggunakan apache airflow
'''
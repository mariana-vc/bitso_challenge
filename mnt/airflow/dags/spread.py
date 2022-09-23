import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sensors.s3_key_sensor import S3KeySensor

import boto3
import requests
import psycopg2
import logging
from datetime import datetime
from pandas import json_normalize

timest = datetime.now()

s3_buckname = ''
s3_locat = ''

def _get_bitso_spread(book):
    url = "https://api.bitso.com/v3/order_book/?book={0}".format(book)
    r = requests.get(url)
    spread = r.json()['payload']
    processed_spread = json_normalize({
        'updated_at': spread['updated_at'],
        'ask_price': spread['asks'][0]['price'],
        'bid_price': spread['bids'][0]['price']
    })
    processed_spread['spread'] = (
        (processed_spread['ask_price'].astype(float) -
         processed_spread['bid_price'].astype(float)) *
        100) / processed_spread['ask_price'].astype(float)
    processed_spread.loc[(processed_spread['spread'].astype(float) > 0.1),
                         'is_above'] = 'True'
    processed_spread.loc[(processed_spread['spread'].astype(float) <= 0.1),
                         'is_above'] = 'False'
    processed_spread['book'] = book
    processed_spread.to_csv(_get_directory(book), index=None, header=False)
    return processed_spread


def _get_directory(book):
    return 'exchange=bitso/book={0}/day={1}/hr={2}/files.csv'.format(
        book, timest.strftime("%Y-%m-%d"), timest.strftime("%H"))


def _store_spread():
    hook = PostgresHook(postgres_conn_id='postgres')
    hook.copy_expert(sql="COPY spreads FROM stdin WITH DELIMITER as ','",
                     filename=_get_directory('btc_mxn'))

def _above_spread(**kwargs):
    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket=s3_buckname, Key=s3_locat)
    lin = obj['Body'].read().decode("utf-8")
    spreads_df = pandas.read_csv(lin)


with DAG(
        'spread_processing',
        start_date=datetime(2022, 9, 22),
        schedule_interval='@hourly',
        #schedule_interval='*/1 * * * *',
        catchup=False) as dag:

    get_bitso_spread = PythonOperator(task_id='get_bitso_spread',
                                      python_callable=_get_bitso_spread,
                                      op_kwargs={"book": 'btc_mxn'})

    store_spread = PythonOperator(task_id='store_spread',
                                  python_callable=_store_spread)

    s3_sensor = S3KeySensor(task_id='s3_file_check',
                            poke_interval=60,
                            timeout=180,
                            soft_fail=False,
                            retries=2,
                            bucket_key=s3_locat,
                            bucket_name=s3_buckname,
                            aws_conn_id='bitso_conn'
                            )

    above_spread = PythonOperator(
        task_id='above_spread',
        python_callable=_above_spread
        )

    get_bitso_spread >> store_spread >> s3_sensor >> above_spread

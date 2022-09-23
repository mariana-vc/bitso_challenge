import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

import json
import requests
import psycopg2
import logging
import numpy as np
from datetime import datetime
from pandas import json_normalize


def _get_bitso_spread(book):
    url = "https://api.bitso.com/v3/order_book/?book={0}".format(book)
    r = requests.get(url)
    spread = r.json()['payload']
    processed_spread = json_normalize({
        'updated_at': spread['updated_at'],
        'ask_price': spread['asks'][0]['price'],
        'bid_price': spread['bids'][0]['price']
         })
    processed_spread['spread'] = ((processed_spread['ask_price'].astype(float) - processed_spread['bid_price'].astype(float)) * 100) / processed_spread['ask_price'].astype(float)
    processed_spread.loc[(processed_spread['spread'].astype(float) > 0.1), 'is_above'] = 'True'
    processed_spread.loc[(processed_spread['spread'].astype(float) <= 0.1), 'is_above'] = 'False'
    processed_spread['book'] = book
    return processed_spread

def _get_directory():
    return

with DAG('spread_processing',
         start_date=datetime(2022, 9, 22),
         schedule_interval='@hourly',
         #schedule_interval='*/1 * * * *',
         catchup=False) as dag:

    get_bitso_spread = PythonOperator(task_id='get_bitso_spread',
                                       python_callable=_get_bitso_spread,
                                       op_kwargs={"book": 'btc_mxn'})

    generate_directory = PythonOperator(task_id='generate_directory',
                                       python_callable=_get_directory)

    get_bitso_spread >> generate_directory

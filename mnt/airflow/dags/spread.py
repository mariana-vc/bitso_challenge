import airflow
from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

import json
import requests
import psycopg2
import logging
from datetime import datetime, timedelta
from json import loads
from requests import get
from psycopg2 import extras

def _get_bitso_tickers():
    url = "https://api.bitso.com/v3/order_book/?book=btc_mxn"
    r = requests.get(url)
    return [ {'base': t.get('base'), 'target': t.get('target')} for t in r.json()['tickers'] ]

with DAG('spread_processing', start_date=datetime(2022, 9, 22), 
        schedule_interval='@daily', catchup=False) as dag:

    get_bitso_tickers = PythonOperator(
        task_id='get_bitso_tickers',
        python_callable=_get_bitso_tickers
    )

    get_bitso_tickers 
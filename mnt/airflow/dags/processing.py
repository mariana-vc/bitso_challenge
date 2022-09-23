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

param_dic = {
    "host": "postgres",
    "database": "airflow_db",
    "user": "airflow",
    "password": "airflow"
}


def connect(params_dic):
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        logging.info('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params_dic)
    except (Exception, psycopg2.DatabaseError) as error:
        logging.info(error)
        sys.exit(1)
    logging.info("Connection successful")
    return conn


def _get_bitso_tickers():
    """This function call the exchanges api to get the tickers from bitso

    Returns:
        list: contains base and target from all bitso markets
    """
    url = "https://api.coingecko.com/api/v3/exchanges/bitso/tickers"
    r = requests.get(url)
    return [{
        'base': t.get('base'),
        'target': t.get('target')
    } for t in r.json()['tickers']]


def _get_exchanges():
    """This function call the exchanges api to return a list of all available exchanges

    Returns:
        list: contains all the exchanges
    """
    url = "https://api.coingecko.com/api/v3/exchanges"
    r = requests.get(url)
    return [e for e in r.json()]


def _get_exchanges_tickers(ti):
    """Get the exchanges that have at least one market similar as bitso.

    Args:
        ti : for xcom pull

    Returns:
        list: contains all the exchanges that have at least one market similar as bitso
    """
    bitso_tickers = ti.xcom_pull(task_ids="get_bitso_tickers")
    exchanges = ti.xcom_pull(task_ids="get_exchanges")
    valid_exchanges = []
    for e in exchanges:
        url = "https://api.coingecko.com/api/v3/exchanges/{0}/tickers".format(
            e.get('id'))
        r = requests.get(url)

        try:
            json_object = json.dumps(r.json())
        except ValueError as err:
            logging.info(e.get('id') + ' json failed to fetch.')
            continue

        tickers_ = [{
            'base': t.get('base'),
            'target': t.get('target')
        } for t in r.json()['tickers']]

        set_a = {tuple(dict_.items()) for dict_ in tickers_}
        set_b = {tuple(dict_.items()) for dict_ in bitso_tickers}
        match_tickers = set_a.intersection(set_b)

        if match_tickers:
            logging.info('This exchange will be stored: ' + e.get('id'))
            valid_exchanges.append(e.get('id'))
            store_exchange(e)
            store_tickers(e.get('id'), list(match_tickers))
        else:
            logging.info('the exchange ' + e.get('id') +
                         ' has not similar markets as bitso')
    return valid_exchanges


def store_exchange(exchanges):
    """Store the exchanges to postgres table:exchanges.

    Args:
        exchanges (dict): contain all the exchanges that have at least one market similar as bitso.
    """
    conn = connect(param_dic)
    cursor = conn.cursor()

    insert_query = "insert into exchanges VALUES(%(id)s, %(name)s, %(trust_score)s, %(trust_score_rank)s)"
    cursor.execute(insert_query, e)
    conn.commit()
    conn.close()


def store_tickers(exchange, tickers):
    """Store the exchanges and markets to postgres table:exchange_market.

    Args:
        exchange (string): exchange_id 
        tickers (list): list of markets from the exchange_id 
    """
    conn = connect(param_dic)
    cursor = conn.cursor()
    for tup in tickers:
        insert_query = "insert into exchange_market VALUES('{0}','{1}','{2}')".format(
            exchange, tup[0][1], tup[1][1])
        cursor.execute(insert_query)
        conn.commit()
    conn.close()


def _get_volume(ti):
    """Store the 30 day volume for all the exchange in BTC to postgres table:exchanges_all.

    Args:
        ti: for xcom_pull
    """
    exchanges = ti.xcom_pull(task_ids="get_exchanges_tickers")
    conn = connect(param_dic)
    cursor = conn.cursor()

    for e in exchanges:
        url = "https://api.coingecko.com/api/v3/exchanges/{0}/volume_chart".format(e)
        r = requests.get(url, params={'days': 30})
        logging.info(r.json())
        for x in r.json():
            vols = iter(x)
            for dt, vol in zip(vols, vols):
                insert_query = "insert into exchanges_all VALUES('{0}','{1}',{2})".format(
                    e,
                    datetime.fromtimestamp(dt / 1000).strftime('%Y-%m-%d'),
                    vol)
                cursor.execute(insert_query)
                conn.commit()

    conn.close()


with DAG('exchanges_processing',
         start_date=datetime(2022, 9, 22),
         schedule_interval='@daily',
         catchup=False) as dag:

    truncate_tables = PostgresOperator(task_id='truncate_tables',
                                       postgres_conn_id='postgres',
                                       sql=[
                                           ' TRUNCATE exchanges; ',
                                           ' TRUNCATE exchange_market; ',
                                           ' TRUNCATE exchanges_all; '
                                       ])

    get_bitso_tickers = PythonOperator(task_id='get_bitso_tickers',
                                       python_callable=_get_bitso_tickers)

    get_exchanges = PythonOperator(task_id='get_exchanges',
                                   python_callable=_get_exchanges)

    get_exchanges_tickers = PythonOperator(
        task_id='get_exchanges_tickers',
        python_callable=_get_exchanges_tickers)

    get_volume = PythonOperator(task_id='get_volume',
                                python_callable=_get_volume)

    truncate_tables >> get_bitso_tickers >> get_exchanges >> get_exchanges_tickers >> get_volume

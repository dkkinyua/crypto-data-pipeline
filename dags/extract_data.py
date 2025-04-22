import os
import json
import requests
from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from pendulum import datetime

API_KEY = os.getenv('CRYPTO_API_KEY')
crypto_data = []

@dag(
    dag_id='crypto_data_dag',
    schedule='@daily',
    start_date=datetime(2025, 4, 22),
    default_args={'owner': 'denzo', 'retries': 3},
    tags = ['Crypto Data Pipeline DAG']
)

def crypto_pipeline():
    @task
    def extract_data():
        url = f"https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&ids=bitcoin&names=Bitcoin&symbols=btc&x_cg_demo_api_key={API_KEY}"

        headers = {
            "accept": "application/json"
            }

        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            print('Lets go!')
        else:
            print(f'There is an error: {response.status_code}')

    extract_task = extract_data()

    return extract_task

crypto_dag = crypto_pipeline()


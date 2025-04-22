import os
import json
import requests
from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from pendulum import datetime

API_KEY = os.getenv('CRYPTO_API_KEY')

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
        crypto_data = []
        url = f"https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&ids=bitcoin&names=Bitcoin&symbols=btc&x_cg_demo_api_key={API_KEY}"

        headers = {
            "accept": "application/json"
            }

        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            data = response.json()
            name = data[0]['name']
            symbol = data[0]['symbol']
            current_price = data[0]['current_price']
            high_24h = data[0]['high_24h']
            low_24h = data[0]['low_24h']
            price_change_24h = data[0]['price_change_24h']
            last_updated = data[0]['last_updated']

            crypto_dict = {
                'name': name,
                'symbol': symbol,
                'current_price': current_price,
                'high_24h': high_24h,
                'low_24h': low_24h,
                'price_change_24h': price_change_24h,
                'last_updated': last_updated
            }
            crypto_data.append(crypto_dict)
            return crypto_data
        else:
            print(f'There is an error: {response.status_code}')

    @task
    def load_data(crypto_data):
        filepath = '/home/deecodes/airflow/data.json'

        # Load data from file
        if os.path.exists(filepath):
            with open(filepath, 'r') as f:
                data = json.load(f)
        else:
            data = []

        data.extend(crypto_data)

        with open(filepath, 'w') as f:
            json.dump(data, f, indent=4)

        print('Data loaded successfully!')

    extract_process = extract_data()
    load_data(extract_process)

crypto_dag = crypto_pipeline()


import os
import json
import requests
import pandas as pd
from dotenv import load_dotenv
from pendulum import datetime
from datetime import timedelta
from sqlalchemy import create_engine
from airflow.decorators import dag, task
from eazyetl import Load, Extract

load_dotenv()

API_KEY = os.getenv('CRYPTO_API_KEY')
DB_CONNECTION_KEY = os.getenv('DB_CONNECTION_KEY')



@dag(
    dag_id='crypto_data_dag',
    schedule='@daily',
    start_date=datetime(2025, 4, 22),
    default_args={'owner': 'denzo', 'retries': 3, 'retry_delay': timedelta(minutes=5)},
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

    @task
    def clean_data(filepath):
        try:
            filepath = '/home/deecodes/airflow/data.json'
            df = pd.read_json(filepath)
            df = df.explode(['name', 'symbol', 'current_price', 'high_24h', 'low_24h', 'price_change_24h', 'last_updated'], ignore_index=True)
            df.index = df.index + 1
            df.to_csv('/home/deecodes/airflow/cleaned_data.csv')
        except Exception as e:
            print(f"Error: {str(e)}")

    @task
    def load_to_db(filepath):
        try:
            filepath = '/home/deecodes/airflow/cleaned_data.csv'
            df = Extract.read_csv(filepath)
            Load.load_to_db(df, name='crypto_data_daily', url=DB_CONNECTION_KEY)
        except Exception as e:
            print(f"Error: {str(e)}")
        

    extract_process = extract_data()
    unclean_file = load_data(extract_process)
    file = clean_data(unclean_file)
    load_to_db(file)

crypto_dag = crypto_pipeline()

# print(DB_CONNECTION_KEY)
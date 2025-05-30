import os
import json
import requests
import pandas as pd
from airflow.decorators import dag, task
from datetime import datetime, timedelta
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()
API_KEY = os.getenv("API_KEY")
DB_URL = os.getenv("DB_URL")

default_args = {
    'owner': 'deecodes',
    'depends_on_past': False,
    'email': ['denzelkinyua11@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False, # wait for all retries to fail.
    'retries': 5,
    'retry_delay': timedelta(minutes=3)
}

@dag(dag_id='new_crypto_pipeline_dag', default_args=default_args, start_date=datetime(2025, 5, 30), schedule='@hourly', catchup=False)
def new_crypto_pipeline_dag():
    # extract data from API
    @task
    def extract_data():
        coin_list = ['cardano', 'polkadot', 'chainlink', 'litecoin', 'uniswap', 'stellar', 'aptos', 'ripple', 'avalanche-2']
        name_list = []
        price_list = []
        cap_list = []
        volume_list = []
        time_list = []

        for coin in coin_list: #loop thru list and enter each coin into url to extract its data
            url = f"https://api.coingecko.com/api/v3/coins/{coin}?localization=false&tickers=false&community_data=false&developer_data=false&sparkline=false&dex_pair_format=symbol"
            headers = {
                "accept": "application/json",
                "x-cg-demo-api-key": API_KEY
            }
            response = requests.get(url, headers=headers)

            if response.status_code == 200:
                data = response.json()

                name_list.append(data["name"])
                price_list.append(data["market_data"]['current_price']['usd'])
                cap_list.append(data['market_data']['market_cap']['usd'])
                volume_list.append(data['market_data']['total_volume']['usd'])
                time_list.append(data['market_data']['last_updated'])

                coin_data = {
                    'name': name_list,
                    'current_price': price_list,
                    'market_cap': cap_list,
                    'total_volume': volume_list,
                    'time': time_list
                }

            else:
                print(f"Requests error: {response.status_code}, {response.text}")
                
        return coin_data

    # transform and load data into db
    @task
    def transform_and_load(data):
        df = pd.DataFrame(data)
        df['time'] = pd.to_datetime(df['time'], utc=True)
        df.index = df.index + 1

        engine = create_engine(DB_URL)
        try:
            df.to_sql(name='hourly_crypto_data', con=engine, schema='crypto', if_exists='append')
            print(f"Data loaded successfully!")
        except Exception as e:
            print("There is an error")
            print(f"Loading error: {e}")
    
    data = extract_data()
    transform_and_load(data)
        
crypto_dag = new_crypto_pipeline_dag()
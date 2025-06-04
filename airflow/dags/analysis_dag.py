# Analysis DAG for tracking price trends over time and signals
import os
import pandas as pd
from airflow.decorators import dag, task
from dotenv import load_dotenv
from datetime import datetime, timedelta
from sqlalchemy import create_engine

load_dotenv()
DB_URL = os.getenv("DB_URL") # dev db
TEST_DB_URL= os.getenv("TEST_DB_URL") # prod db

default_args = {
    'owner': 'deecodes',
    'depends_on_past': False,
    'email': ['denzelkinyua11@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False, # wait for all retries to fail.
    'retries': 5,
    'retry_delay': timedelta(minutes=3)
}

@dag(dag_id='analysis_dag', default_args=default_args, start_date=datetime(2025, 6, 3), schedule='@daily', catchup=False)
def analysis():
    @task
    def analyze_data():
        prod_engine = create_engine(TEST_DB_URL)
        df = pd.read_sql_table(table_name='hourly_crypto_data',con=prod_engine, schema='crypto')
        df['time'] = pd.to_datetime(df['time'], utc=True)
        df.set_index('time', inplace=True)

        names = df['name'].unique() # fetches unique names from dataframe
        final_results = []

        for name in names: # loop thru each unique name and get its data
            new_df = df[df["name"] == name].copy()
            new_df["MA_6"] = new_df["current_price"].rolling(window=6).mean()
            new_df["MA_24"] = new_df["current_price"].rolling(window=24).mean()
            new_df["Buy_Signal"] = new_df["current_price"][
                (new_df["current_price"].shift(1) > new_df["current_price"]) &
                (new_df["current_price"].shift(-1) > new_df["current_price"])
            ]
            new_df["Sell_Signal"] = new_df["current_price"][
                (new_df["current_price"].shift(1) < new_df["current_price"]) &
                (new_df["current_price"].shift(-1) < new_df["current_price"])
            ]
            final_results.append(new_df)
        final_df = pd.concat(final_results) # add the final results of the analysis to df

        today = datetime.today().strftime('%Y:%m:%d')
        filepath = f'/home/deecodes/crypto-data-pipeline/docs/daily_crypto_analysis_for_{today}'
        try:
            final_df.to_csv(filepath, index=False)
            print(f"Data loaded successfully to: {filepath}")
        except Exception as e:
            print(f"Loading to csv error: {e}")

    analyze_data()

analysis_dag = analysis()


from airflow.decorators import dag, task
from datetime import datetime, timedelta

default_args = {
    'owner': 'deecodes',
    'retries': 5,
    'retry_delay': timedelta(minutes=1)
}

@dag(dag_id='test_dag', default_args=default_args, start_date=datetime(2025, 5, 30), schedule='@hourly', catchup=False)
def test_dag():
    @task
    def say_hello():
        print("Hello there, Airflow is running!")

    say_hello()

dag_test = test_dag()
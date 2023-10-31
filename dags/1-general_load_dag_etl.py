from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from etl_modules.load import load


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 10, 28),
    'retries': 1,
}


def run_custom_function():
    load()


with DAG('all_in_one_ETL', default_args=default_args, schedule_interval=None, catchup=False) as dag:
    run_custom_task = PythonOperator(
        task_id='all_in_one_ETL',
        python_callable=run_custom_function,
    )

run_custom_task

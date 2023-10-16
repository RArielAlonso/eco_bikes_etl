from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime
import pandas as pd


default_args = {
    'retries': 3,
}


def print_a():
    print('hi from task a')


def print_c():
    print(pd.DataFrame([['a', 1], ['b', 'Null']], columns=['a', 'b']))


with DAG(
    dag_id='my_dag',
    description="Extract data from APIs, load them to Postgres",
    tags=['ETL_process'],
    start_date=datetime(2023, 10, 11),
    default_args=default_args,
    schedule="@daily",
    catchup=False) as dag:
    task_a = DummyOperator(
        task_id='start_process',
    )
    task_b = PythonOperator(
        task_id='task_b',
        python_callable=print_a
    )
    task_c = PythonOperator(
        task_id='task_c',
        python_callable=print_c
    )

task_a >> task_b >> task_c

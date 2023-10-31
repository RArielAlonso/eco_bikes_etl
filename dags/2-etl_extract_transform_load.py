from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.empty import EmptyOperator
from etl_modules.extract import extract
from etl_modules.transform import transform
from etl_modules.load import load_dim_date, load_station_info_to_database, load_to_postgres_append, transform_scd_station_info
from config.config import extract_list, DB_STR, POSTGRES_SCHEMA

default_args = {
    'owner': 'Ariel Alonso',
    'email': ['rariel.alonso@gmail.com'],
    'start_date': datetime(2023, 10, 28),
    'email_on_failure': False,
    'email_on_retry': False,
}


def extract_task(ti):
    paths_json = extract(extract_list)
    ti.xcom_push(key='paths_json', value=paths_json)


def transform_task(ti):
    path_json = ti.xcom_pull(key='paths_json')
    path_parquet = transform(path_json)
    ti.xcom_push(key='path_parquet', value=path_parquet)


def load_task(ti):
    paths_parquet = ti.xcom_pull(key='path_parquet')
    paths_parquet_append = {k: v for (k, v) in paths_parquet.items() if k not in ["dim_date", "station_info_eco_bikes"]}
    load_dim_date(paths_parquet)
    df_scd2_records_final_replace, df_new_records_final, df_scd2_records_final_append = transform_scd_station_info(paths_parquet, DB_STR, POSTGRES_SCHEMA)
    load_station_info_to_database(df_scd2_records_final_replace, df_new_records_final, df_scd2_records_final_append)
    load_to_postgres_append(paths_parquet_append)


with DAG('2-etl_extract_transform_load', default_args=default_args, schedule_interval="@hourly", catchup=False) as dag:
    start_task = EmptyOperator(task_id="start_etl", trigger_rule='all_success')
    extract_task = PythonOperator(
        task_id='extract',
        python_callable=extract_task,
    )
    transform_task = PythonOperator(
        task_id='transform',
        python_callable=transform_task,
    )
    load_task = PythonOperator(
        task_id='load',
        python_callable=load_task,
    )
    end_task = EmptyOperator(task_id="end_etl", trigger_rule='all_success')


start_task >> extract_task >> transform_task >> load_task >> end_task

from datetime import datetime

from airflow.decorators import dag, task

default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 10, 28),
    "retries": 1,
}


@dag("3-GCP-ETL", default_args=default_args, schedule_interval="@hourly", catchup=False)
def dag_external_general_load():
    @task.external_python(
        task_id="extract",
        python="/home/airflow/.cache/pypoetry/virtualenvs/etl-eco-bikes-9TtSrW0h-py3.9/bin/python",
    )
    def extract_task():
        from config.config import extract_list
        from etl_modules.extract import gcp_extract

        paths_json = gcp_extract(extract_list)
        return paths_json

    @task.external_python(
        task_id="gcp_transform",
        python="/home/airflow/.cache/pypoetry/virtualenvs/etl-eco-bikes-9TtSrW0h-py3.9/bin/python",
    )
    def gcp_transform_task(path_json):
        from etl_modules.gcp_transform import gcp_transform

        path_parquet = gcp_transform(path_json)
        return path_parquet

    @task.external_python(
        task_id="gcp_load",
        python="/home/airflow/.cache/pypoetry/virtualenvs/etl-eco-bikes-9TtSrW0h-py3.9/bin/python",
    )
    def gcp_load_task(paths_parquet):
        from config.config import (
            GCP_BQ_JSON_CREDENTIALS,
            GCP_DATASET_ID,
            GCP_PROJECT_ID,
        )
        from config.constants import GCP_BUCKET_NAME
        from etl_modules.gcp_load import (
            gcp_load_dim_date,
            gcp_load_station_info,
            gcp_transform_scd_station_info,
            load_to_gcp_append,
        )

        paths_parquet_append = {
            k: v
            for (k, v) in paths_parquet.items()
            if k not in ["dim_date", "station_info_eco_bikes"]
        }
        gcp_load_dim_date(paths_parquet)
        (
            df_scd2_records_final_replace,
            df_new_records_final,
            df_scd2_records_final_append,
        ) = gcp_transform_scd_station_info(
            paths_parquet,
            GCP_BQ_JSON_CREDENTIALS,
            GCP_PROJECT_ID,
            GCP_DATASET_ID,
            GCP_BUCKET_NAME,
        )
        gcp_load_station_info(
            df_scd2_records_final_replace,
            df_new_records_final,
            df_scd2_records_final_append,
        )
        load_to_gcp_append(paths_parquet_append)

    @task.external_python(
        task_id="gcp_create_tables",
        python="/home/airflow/.cache/pypoetry/virtualenvs/etl-eco-bikes-9TtSrW0h-py3.9/bin/python",
    )
    def gcp_create_dataset_tables():
        from etl_modules.gcp_load import gcp_create_schema

        gcp_create_schema()

    @task.external_python(
        task_id="gcp_create_bucket_and_folders",
        python="/home/airflow/.cache/pypoetry/virtualenvs/etl-eco-bikes-9TtSrW0h-py3.9/bin/python",
    )
    def gcp_create_bucket_and_folders():
        from etl_modules.gcp_load import gcp_create_bucket

        gcp_create_bucket()

    start_bucket = gcp_create_bucket_and_folders()
    start_dataset = gcp_create_dataset_tables()
    path_jsons = extract_task()
    path_parquets = gcp_transform_task(path_jsons)
    end = gcp_load_task(path_parquets)

    [start_bucket, start_dataset] >> path_jsons >> path_parquets >> end


dag_external_general_load()

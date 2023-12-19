from datetime import datetime
from airflow.decorators import dag, task
from config.constants import PATH_TO_POETRY_ENV


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 10, 28),
    'retries': 1,
}


@dag('2-external_etl_extract_transform_load', default_args=default_args, schedule_interval="@hourly", catchup=False)
def dag_external_general_load():
    @task.external_python(python=PATH_TO_POETRY_ENV)
    def extract_task():
        from etl_modules.extract import extract
        from config.config import extract_list
        paths_json = extract(extract_list)
        return paths_json

    @task.external_python(python=PATH_TO_POETRY_ENV)
    def transform_task(path_json, ds=None, ds_nodash=None, data_interval_start=None, ts=None):
        from etl_modules.transform import transform
        print(ds)
        print(ds_nodash)
        print(ts)
        print(data_interval_start)
        path_parquet = transform(path_json)
        return path_parquet

    @task.external_python(python=PATH_TO_POETRY_ENV)
    def load_task(paths_parquet):
        from etl_modules.load import transform_scd_station_info, load_dim_date, load_station_info_to_database, load_to_postgres_append
        from config.config import DB_STR, POSTGRES_SCHEMA
        paths_parquet_append = {k: v for (k, v) in paths_parquet.items() if k not in ["dim_date", "station_info_eco_bikes"]}
        load_dim_date(paths_parquet)
        df_scd2_records_final_replace, df_new_records_final, df_scd2_records_final_append = transform_scd_station_info(paths_parquet, DB_STR, POSTGRES_SCHEMA)
        load_station_info_to_database(df_scd2_records_final_replace, df_new_records_final, df_scd2_records_final_append)
        load_to_postgres_append(paths_parquet_append)

    load_task(transform_task(extract_task()))


dag_external_general_load()

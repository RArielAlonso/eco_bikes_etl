from datetime import datetime
from airflow.decorators import dag, task
from config.constants import PATH_TO_POETRY_ENV


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 10, 28),
    'retries': 1,
}


@dag('1-external_all_in_one_etl', default_args=default_args, schedule_interval="@hourly", catchup=False)
def dag_external_general_load():
    @task.external_python(python=PATH_TO_POETRY_ENV)
    def load_external():
        from etl_modules.load import load
        load()

    load_external()


dag_external_general_load()

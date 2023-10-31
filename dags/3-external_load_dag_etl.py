from datetime import datetime
from airflow.decorators import dag, task


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 10, 28),
    'retries': 1,
}


@dag('external_all_in_one_etl', default_args=default_args, schedule_interval=None, catchup=False)
def dag_external_general_load():
    @task.external_python(python='/home/airflow/.cache/pypoetry/virtualenvs/etl-eco-bikes-9TtSrW0h-py3.9/bin/python')
    def load_external():
        from etl_modules.load import load
        load()

    load_external()


dag_external_general_load()
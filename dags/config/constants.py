from pathlib import Path
import os

BASE_FILE_DIR = Path("/tmp")
GCP_BUCKET_NAME = 'eco_bikes_bucket'
ROOT_DIR = Path().resolve()
SQL_FILE_DIR = os.path.join(ROOT_DIR, "SQL")
GCP_SQL_FILE_DIR = os.path.join(ROOT_DIR, "SQL/GCP_SQL")
PATH_TO_POETRY_ENV = '/home/airflow/.cache/pypoetry/virtualenvs/etl-eco-bikes-9TtSrW0h-py3.9/bin/python'

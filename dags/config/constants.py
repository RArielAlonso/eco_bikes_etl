import os
from pathlib import Path

BASE_FILE_DIR = Path("/tmp")
GCP_BUCKET_NAME = "eco_bikes_bucket"
ROOT_DIR = Path().resolve()
SQL_FILE_DIR = os.path.join(ROOT_DIR, "SQL")
GCP_SQL_FILE_DIR = os.path.join(ROOT_DIR, "SQL/GCP_SQL")

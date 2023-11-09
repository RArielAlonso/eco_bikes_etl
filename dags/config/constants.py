from pathlib import Path
import os

BASE_FILE_DIR = Path("/tmp")
ROOT_DIR = Path().resolve()
SQL_FILE_DIR = os.path.join(ROOT_DIR, "SQL")
GCP_SQL_FILE_DIR = os.path.join(ROOT_DIR, "GCP_SQL")

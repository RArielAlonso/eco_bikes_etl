import pandas as pd
from sqlalchemy import create_engine
import logging
# from dags.etl_modules.transform import transform
from dags.utlis.utils import df_to_database
from config.config import DB_STR, POSTGRES_SCHEMA


logging.basicConfig(format="%(asctime)s - %(filename)s - %(message)s", level=logging.INFO)


# paths_parquet = transform()
paths_parquet = {'df_dim_date': '/tmp/df_dim_date.parquet',
                 'weather': '/tmp/weather.parquet',
                 'system_info_eco_bikes': '/tmp/system_info_eco_bikes.parquet',
                 'station_status_eco_bikes': '/tmp/station_status_eco_bikes.parquet',
                 'station_info_eco_bikes': '/tmp/station_info_eco_bikes.parquet'}


def load(path_parquet_files):
    logging.info("LOAD PROCESS STARTED".center(80, "-"))
    for key, value in paths_parquet.items():
        logging.info(f"Reading parquet file: {key}.parquet in {value}")
        data = pd.read_parquet(value)
        logging.info(f"Loading dataframe: {key} to Database")
        # data.to_sql(key, create_engine(DB_STR, connect_args={'options': '-csearch_path=public'}), if_exists='append', index=False)
        df_to_database(data, key, DB_STR, POSTGRES_SCHEMA)
    logging.info("LOAD PROCESS ENDED".center(80, "-"))


if __name__ == "__main__":
    load(paths_parquet)

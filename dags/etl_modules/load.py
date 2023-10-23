import pandas as pd
import logging
from dags.etl_modules.transform import extract, transform
from dags.utlis.utils import df_to_database
from config.config import DB_STR, POSTGRES_SCHEMA, extract_list


logging.basicConfig(format="%(asctime)s - %(filename)s - %(message)s", level=logging.INFO)


def load_dim_date(path_parquet):
    logging.info(f"Reading parquet file: {path_parquet['dim_date']}.parquet in {path_parquet['dim_date']}")
    data = pd.read_parquet(path_parquet['dim_date'])
    logging.info(f"Loading dataframe: {path_parquet['dim_date']} to Database")
    df_to_database(data, 'dim_date', DB_STR, POSTGRES_SCHEMA,  "replace")
    logging.info(f"DATAFRAME LOADED: {path_parquet['dim_date']} to Database")


def load_to_postgres_append(path_parquet_files):
    for key, value in path_parquet_files.items():
        logging.info(f"Reading parquet file: {key}.parquet in {value}")
        data = pd.read_parquet(value)
        logging.info(f"Loading dataframe: {key} to Database")
        # data.to_sql(key, create_engine(DB_STR, connect_args={'options': '-csearch_path=public'}), if_exists='append', index=False)
        df_to_database(data, key, DB_STR, POSTGRES_SCHEMA, "append")
        logging.info(f"DATAFRAME LOADED: {key} to Database")


def load_station_info(path_parquet):
    logging.info(f"Reading parquet file: {path_parquet['station_info_eco_bikes']}.parquet in {path_parquet['station_info_eco_bikes']}")
    data_src = pd.read_parquet(path_parquet['station_info_eco_bikes'])
    logging.info(f"Loading dataframe: {path_parquet['station_info_eco_bikes']} to Database")
    df_to_database(data_src, "station_info_eco_bikes", DB_STR, POSTGRES_SCHEMA,  "replace")
    logging.info(f"DATAFRAME LOADED: {path_parquet['station_info_eco_bikes']} to Database")


if __name__ == "__main__":
    try:
        logging.info("Running ONLY LOAD PROCESS".center(80, "-"))
        path_jsons = extract(extract_list)
        paths_parquet = transform(path_jsons)
        paths_parquet_append = {k: v for (k, v) in paths_parquet.items() if k not in ["dim_date", "station_info_eco_bikes"]}
        load_dim_date(paths_parquet)
        load_station_info(paths_parquet)
        load_to_postgres_append(paths_parquet_append)
        logging.info("FINISHED ONLY LOAD PROCESS".center(80, "-"))
    except BaseException as e:
        logging.error("LOAD could not complete", e)

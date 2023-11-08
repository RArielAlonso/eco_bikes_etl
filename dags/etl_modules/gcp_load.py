import pandas as pd
import logging
from etl_modules.extract import extract
from etl_modules.transform import transform_metadata_load, create_dim_date_table, transform_weather
from etl_modules.transform import transform_system_info, transform_station_info, transform_station_status
from utils.utils import gcp_create_dataset_and_tables, gcp_get_max_reload, load_to_parquet, df_to_gcbq
from config.config import extract_list, GCP_DATASET_ID, GCP_JSON_CREDENTIALS, GCP_PROJECT_ID
from config.constants import BASE_FILE_DIR, GCP_SQL_FILE_DIR

logging.basicConfig(format="%(asctime)s - %(filename)s - %(message)s", level=logging.INFO)


def gcp_create_schema():
    gcp_create_dataset_and_tables(GCP_JSON_CREDENTIALS, GCP_PROJECT_ID, GCP_DATASET_ID, GCP_SQL_FILE_DIR)


def gcp_transform(path_jsons):
    try:
        logging.info("Began the TRANSFORM PROCESS".center(80, "-"))
        parquets_path = dict()
        df_metadata = transform_metadata_load()
        reload_id = gcp_get_max_reload(GCP_JSON_CREDENTIALS, GCP_PROJECT_ID, GCP_DATASET_ID)
        df_dim_date = create_dim_date_table()
        df_dim_date['reload_id'] = reload_id
        df_fact_weather = transform_weather(path_jsons)
        df_fact_weather['reload_id'] = reload_id
        df_system_info = transform_system_info(path_jsons)
        df_system_info['reload_id'] = reload_id
        df_station_info = transform_station_info(path_jsons)
        df_station_info['reload_id'] = reload_id
        df_station_status = transform_station_status(path_jsons)
        df_station_status['reload_id'] = reload_id
        list_df = [df_dim_date, df_metadata, df_fact_weather, df_system_info, df_station_info, df_station_status]
        for i in list_df:
            logging.info(f"Generating {i.name}.parquet in {BASE_FILE_DIR}/{i.name}_gcp.parquet")
            parquets_path[i.name] = load_to_parquet(i, f"{i.name}_gcp")
        logging.info("FINISHED the TRANSFORM PROCESS".center(80, "-"))
        return parquets_path
    except BaseException as e:
        logging.error(e)
        raise Exception(e)


def load_dim_date(path_parquet):
    logging.info(f"Reading parquet file: {path_parquet['dim_date']}.parquet in {path_parquet['dim_date']}")
    data = pd.read_parquet(path_parquet['dim_date'])
    logging.info(f"Loading dataframe: {path_parquet['dim_date']} to Database")
    df_to_gcbq(data, 'dim_date', GCP_JSON_CREDENTIALS, GCP_PROJECT_ID,  "replace")
    logging.info(f"DATAFRAME LOADED: {path_parquet['dim_date']} to Database")


def load_to_gcp_append(path_parquet_files):
    for key, value in path_parquet_files.items():
        logging.info(f"Reading parquet file: {key}.parquet in {value}")
        data = pd.read_parquet(value)
        # if key == 'station_status_eco_bikes':
        #    data = add_surrogate_ket_station_status(data, DB_STR, POSTGRES_SCHEMA)
        logging.info(f"Loading dataframe: {key} to Database")
        # data.to_sql(key, create_engine(DB_STR, connect_args={'options': '-csearch_path=public'}), if_exists='append', index=False)
        df_to_gcbq(data, key, GCP_JSON_CREDENTIALS, GCP_PROJECT_ID,  "append")
        logging.info(f"DATAFRAME LOADED: {key} to Database")


if __name__ == "__main__":
    gcp_create_schema()
    path_parquet = gcp_transform(extract(extract_list))
    data = gcp_get_max_reload(GCP_JSON_CREDENTIALS, GCP_PROJECT_ID, GCP_DATASET_ID)
    print(data)
    load_dim_date(path_parquet)
    load_to_gcp_append(path_parquet)

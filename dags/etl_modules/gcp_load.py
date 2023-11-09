import pandas as pd
import logging
from etl_modules.extract import extract
from etl_modules.transform import transform_metadata_load, create_dim_date_table, transform_weather
from etl_modules.transform import transform_system_info, transform_station_info, transform_station_status
from utils.utils import gcp_create_dataset_and_tables, gcp_get_max_reload, load_to_parquet, df_to_gcbq
from utils.utils import gcp_add_surrogate_ket_station_status, gcp_transform_scd_station_info
from config.config import extract_list, GCP_DATASET_ID, GCP_JSON_CREDENTIALS, GCP_PROJECT_ID
from config.constants import BASE_FILE_DIR, GCP_SQL_FILE_DIR
import google.cloud.bigquery.dbapi as bq


logging.basicConfig(format="%(asctime)s - %(filename)s - %(message)s", level=logging.INFO)


def gcp_create_schema():
    gcp_create_dataset_and_tables(GCP_JSON_CREDENTIALS, GCP_PROJECT_ID, GCP_DATASET_ID, GCP_SQL_FILE_DIR)


def gcp_transform(path_jsons):
    try:
        logging.info("Began the TRANSFORM PROCESS".center(80, "-"))
        parquets_path = dict()
        reload_id = gcp_get_max_reload(GCP_JSON_CREDENTIALS, GCP_PROJECT_ID, GCP_DATASET_ID)
        df_metadata = transform_metadata_load()
        df_metadata['reload_id'] = reload_id
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


def gcp_load_station_info(df_scd2_records_final_replace, df_new_records_final, df_scd2_records_final_append):
    try:
        connection = bq.connect()
        cursor = connection.cursor()
        for index, row in df_scd2_records_final_replace.iterrows():
            # Assuming your_table_name is the name of the table you want to update
            # Assuming your_primary_key_column is the primary key column of your table
            # Assuming your_primary_key_value is the value of the primary key for the specific row you want to update
            update_query = "UPDATE eco_bikes_dataset.station_info_eco_bikes SET "
            # Dynamically construct the SET clause of the update query
            set_clauses = ', '.join([f"{col} = %s" for col in df_scd2_records_final_replace.columns])
            update_query += set_clauses
            # Specify the WHERE clause for the specific row to update
            update_query += f" WHERE station_id = '{row['station_id']}' and is_active=1 "
            # Extract values from the DataFrame
            values = tuple([row[col] for col in df_scd2_records_final_replace.columns])
            # Execute the update query with parameterized values
            cursor.execute(update_query, values)
            connection.commit()
        df_to_gcbq(df_new_records_final, 'station_info_eco_bikes', GCP_JSON_CREDENTIALS, GCP_PROJECT_ID, "append")
        df_to_gcbq(df_scd2_records_final_append, 'station_info_eco_bikes', GCP_JSON_CREDENTIALS, GCP_PROJECT_ID, "append")
    except Exception as error:
        raise (f"Error while fetching data from GCP {GCP_PROJECT_ID}", error)
    finally:
        cursor.close()
        connection.close()


def load_dim_date(path_parquet):
    logging.info(f"Reading parquet file: {path_parquet['dim_date']}.parquet in {path_parquet['dim_date']}")
    data = pd.read_parquet(path_parquet['dim_date'])
    logging.info(f"Loading dataframe: {path_parquet['dim_date']} to Database")
    df_to_gcbq(data, 'dim_date', GCP_JSON_CREDENTIALS, GCP_PROJECT_ID,  "replace")
    logging.info(f"DATAFRAME LOADED: {path_parquet['dim_date']} to Database")


def load_to_gcp_append(path_parquet_files):
    for key, value in path_parquet_files.items():
        logging.info(f"Reading parquet file: {key}_gcp.parquet in {value}")
        data = pd.read_parquet(value)
        if key == 'station_status_eco_bikes':
            data = gcp_add_surrogate_ket_station_status(data, GCP_JSON_CREDENTIALS, GCP_PROJECT_ID, GCP_DATASET_ID)
        logging.info(f"Loading dataframe: {key} to Database")
        # data.to_sql(key, create_engine(DB_STR, connect_args={'options': '-csearch_path=public'}), if_exists='append', index=False)
        df_to_gcbq(data, key, GCP_JSON_CREDENTIALS, GCP_PROJECT_ID,  "append")
        logging.info(f"LOADED {key} to DATABASE IN PROJECT {GCP_PROJECT_ID}")


def gcp_load():
    try:
        logging.info("Running ONLY LOAD PROCESS".center(80, "-"))
        gcp_create_schema()
        path_jsons = extract(extract_list)
        paths_parquet = gcp_transform(path_jsons)
        paths_parquet_append = {k: v for (k, v) in paths_parquet.items() if k not in ["dim_date", "station_info_eco_bikes"]}
        load_dim_date(paths_parquet)
        df_scd2_records_final_replace, df_new_records_final, df_scd2_records_final_append = gcp_transform_scd_station_info(paths_parquet,
                                                                                                                           GCP_JSON_CREDENTIALS,
                                                                                                                           GCP_PROJECT_ID,
                                                                                                                           GCP_DATASET_ID)
        print(df_scd2_records_final_replace.shape)
        print(df_new_records_final.shape)
        print(df_scd2_records_final_append.shape)
        breakpoint()
        gcp_load_station_info(df_scd2_records_final_replace, df_new_records_final, df_scd2_records_final_append)
        load_to_gcp_append(paths_parquet_append)
        logging.info("FINISHED ONLY LOAD PROCESS".center(80, "-"))
    except BaseException as e:
        logging.error("LOAD could not complete", e)
        raise BaseException("LOAD could not complete", e)


if __name__ == "__main__":
    gcp_load()

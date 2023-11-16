import logging

from config.config import (
    GCP_BQ_JSON_CREDENTIALS,
    GCP_DATASET_ID,
    GCP_PROJECT_ID,
    GCP_STORAGE_JSON_CREDENTIALS,
    extract_list,
)
from config.constants import GCP_BUCKET_NAME, GCP_SQL_FILE_DIR
from etl_modules.extract import gcp_extract
from etl_modules.gcp_transform import gcp_transform
from utils.utils import gcp_create_dataset_and_tables, df_to_gcbq, gcp_read_parquet, gcp_create_bucket_and_folders
from utils.utils import gcp_add_surrogate_ket_station_status, gcp_transform_scd_station_info
from config.config import GCP_STORAGE_JSON_CREDENTIALS, extract_list, GCP_DATASET_ID, GCP_BQ_JSON_CREDENTIALS, GCP_PROJECT_ID
from config.constants import GCP_BUCKET_NAME, GCP_SQL_FILE_DIR
from google.cloud import bigquery
from utils.utils import (
    df_to_gcbq,
    gcp_add_surrogate_ket_station_status,
    gcp_create_bucket_and_folders,
    gcp_create_dataset_and_tables,
    gcp_read_parquet,
    gcp_transform_scd_station_info,
)

logging.basicConfig(
    format="%(asctime)s - %(filename)s - %(message)s", level=logging.INFO
)


def gcp_create_schema():
    gcp_create_dataset_and_tables(
        GCP_BQ_JSON_CREDENTIALS, GCP_PROJECT_ID, GCP_DATASET_ID, GCP_SQL_FILE_DIR
    )


def gcp_create_bucket():
    gcp_create_bucket_and_folders(
        GCP_PROJECT_ID,
        GCP_BUCKET_NAME,
        ["json", "parquet"],
        GCP_STORAGE_JSON_CREDENTIALS,
    )


def gcp_load_station_info(
    df_scd2_records_final_replace, df_new_records_final, df_scd2_records_final_append
):
    df_to_gcbq(
        df_scd2_records_final_replace,
        "temp_station_info",
        GCP_BQ_JSON_CREDENTIALS,
        GCP_PROJECT_ID,
        "replace",
    )
    try:
        client = bigquery.Client.from_service_account_json(
            GCP_BQ_JSON_CREDENTIALS, project=GCP_PROJECT_ID
        )
        try:
            query = f"""MERGE {GCP_DATASET_ID}.station_info_eco_bikes T
                        USING {GCP_DATASET_ID}.temp_station_info S
                        on T.pk_surrogate_station_info = S.pk_surrogate_station_info
                        WHEN MATCHED THEN
                        UPDATE SET t.is_active = s.is_active,t.end_date = s.end_date;
                        TRUNCATE TABLE {GCP_DATASET_ID}.temp_station_info;"""
            logging.info(f"Executing query: {query}")
            logging.info(
                f"MERGE {GCP_DATASET_ID}.station_info_eco_bikes USING {GCP_DATASET_ID}.temp_station_info "
            )
            logging.info(f"TRUNCATE TABLE {GCP_DATASET_ID}.temp_station_info")
            job_config = bigquery.QueryJobConfig()
            job_config.use_query_cache = False
            query_job = client.query(query, location="US", job_config=job_config)
            query_job.result()
            logging.info("Finished updating the values of the station_info tables")
        except Exception:
            logging.error("Could not update the values of the station_info tables")
            raise ("Could not update the values of the station_info tables")
        df_to_gcbq(
            df_new_records_final,
            "station_info_eco_bikes",
            GCP_BQ_JSON_CREDENTIALS,
            GCP_PROJECT_ID,
            "append",
        )
        df_to_gcbq(
            df_scd2_records_final_append,
            "station_info_eco_bikes",
            GCP_BQ_JSON_CREDENTIALS,
            GCP_PROJECT_ID,
            "append",
        )
    except Exception as error:
        raise (f"Error while fetching data from GCP {GCP_PROJECT_ID}", error)
    finally:
        logging.info(
            "LOADING SC2 TABLE station_info_eco_bikes FINISHED - Function gcp_load_station_info"
        )


def gcp_load_dim_date(path_parquet):
    logging.info(
        f"Reading parquet file: {path_parquet['dim_date']} in {path_parquet['dim_date']}"
    )
    data = gcp_read_parquet(
        path_parquet["dim_date"],
        GCP_PROJECT_ID,
        GCP_STORAGE_JSON_CREDENTIALS,
        GCP_BUCKET_NAME,
    )
    logging.info(f"Loading dataframe: {path_parquet['dim_date']} to Database")
    df_to_gcbq(data, "dim_date", GCP_BQ_JSON_CREDENTIALS, GCP_PROJECT_ID, "replace")
    logging.info(f"DATAFRAME LOADED: {path_parquet['dim_date']} to Database")


def load_to_gcp_append(path_parquet_files):
    for key, value in path_parquet_files.items():
        logging.info(f"Reading parquet file: {key}.parquet in {value}")
        data = gcp_read_parquet(
            value, GCP_PROJECT_ID, GCP_STORAGE_JSON_CREDENTIALS, GCP_BUCKET_NAME
        )
        if key == "station_status_eco_bikes":
            data = gcp_add_surrogate_ket_station_status(
                data, GCP_BQ_JSON_CREDENTIALS, GCP_PROJECT_ID, GCP_DATASET_ID
            )
        logging.info(f"Loading dataframe: {key} to Database")
        # data.to_sql(key, create_engine(DB_STR, connect_args={'options': '-csearch_path=public'}), if_exists='append', index=False)
        df_to_gcbq(data, key, GCP_BQ_JSON_CREDENTIALS, GCP_PROJECT_ID, "append")
        logging.info(f"LOADED {key} to DATABASE IN PROJECT {GCP_PROJECT_ID}")


def gcp_load():
    try:
        logging.info("Running ONLY LOAD PROCESS".center(80, "-"))
        gcp_create_schema()
        gcp_create_bucket()
        path_jsons = gcp_extract(extract_list)
        paths_parquet = gcp_transform(path_jsons)
        paths_parquet_append = {
            k: v
            for (k, v) in paths_parquet.items()
            if k not in ["dim_date", "station_info_eco_bikes"]
        }
        gcp_load_dim_date(paths_parquet)
        (
            df_scd2_records_final_replace,
            df_new_records_final,
            df_scd2_records_final_append,
        ) = gcp_transform_scd_station_info(
            paths_parquet,
            GCP_BQ_JSON_CREDENTIALS,
            GCP_PROJECT_ID,
            GCP_DATASET_ID,
            GCP_BUCKET_NAME,
        )
        gcp_load_station_info(
            df_scd2_records_final_replace,
            df_new_records_final,
            df_scd2_records_final_append,
        )
        load_to_gcp_append(paths_parquet_append)
        logging.info("FINISHED ONLY LOAD PROCESS".center(80, "-"))
    except BaseException as e:
        logging.error("LOAD could not complete", e)
        raise BaseException("LOAD could not complete", e)


if __name__ == "__main__":
    gcp_load()

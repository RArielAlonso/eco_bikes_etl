import pandas as pd
import logging
import psycopg2
from etl_modules.transform import extract, transform
from utils.utils import (
    df_to_database,
    add_surrogate_ket_station_status,
    transform_scd_station_info,
)
from config.config import (
    DB_STR,
    POSTGRES_SCHEMA,
    extract_list,
    POSTGRES_USER,
    POSTGRES_PORT,
    POSTGRES_DB,
    POSTGRES_HOST,
    POSTGRES_PASS,
)

logging.basicConfig(
    format="%(asctime)s - %(filename)s - %(message)s", level=logging.INFO
)


def load_dim_date(path_parquet):
    logging.info(
        f"Reading parquet file: {path_parquet['dim_date']}.parquet in {path_parquet['dim_date']}"
    )
    data = pd.read_parquet(path_parquet["dim_date"])
    logging.info(f"Loading dataframe: {path_parquet['dim_date']} to Database")
    df_to_database(data, "dim_date", DB_STR, POSTGRES_SCHEMA, "replace")
    logging.info(f"DATAFRAME LOADED: {path_parquet['dim_date']} to Database")


def load_to_postgres_append(path_parquet_files):
    for key, value in path_parquet_files.items():
        logging.info(f"Reading parquet file: {key}.parquet in {value}")
        data = pd.read_parquet(value)
        if key == "station_status_eco_bikes":
            data = add_surrogate_ket_station_status(data, DB_STR, POSTGRES_SCHEMA)
        logging.info(f"Loading dataframe: {key} to Database")
        # data.to_sql(key, create_engine(DB_STR, connect_args={'options': '-csearch_path=public'}), if_exists='append', index=False)
        df_to_database(data, key, DB_STR, POSTGRES_SCHEMA, "append")
        logging.info(f"DATAFRAME LOADED: {key} to Database")


def load_station_info_to_database(
    df_scd2_records_final_replace, df_new_records_final, df_scd2_records_final_append
):
    try:
        connection = psycopg2.connect(
            user=POSTGRES_USER,
            password=POSTGRES_PASS,
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            database=POSTGRES_DB,
        )
        cur = connection.cursor()
        for index, row in df_scd2_records_final_replace.iterrows():
            # Assuming your_table_name is the name of the table you want to update
            # Assuming your_primary_key_column is the primary key column of your table
            # Assuming your_primary_key_value is the value of the primary key for the specific row you want to update
            update_query = "UPDATE eco_bikes.station_info_eco_bikes SET "
            # Dynamically construct the SET clause of the update query
            set_clauses = ", ".join(
                [f"{col} = %s" for col in df_scd2_records_final_replace.columns]
            )
            update_query += set_clauses
            # Specify the WHERE clause for the specific row to update
            update_query += (
                f" WHERE station_id = '{row['station_id']}' and is_active=1 "
            )
            # Extract values from the DataFrame
            values = tuple([row[col] for col in df_scd2_records_final_replace.columns])
            # Execute the update query with parameterized values
            cur.execute(update_query, values)
            connection.commit()
        df_to_database(
            df_new_records_final,
            "station_info_eco_bikes",
            DB_STR,
            POSTGRES_SCHEMA,
            "append",
        )
        df_to_database(
            df_scd2_records_final_append,
            "station_info_eco_bikes",
            DB_STR,
            POSTGRES_SCHEMA,
            "append",
        )
    except (Exception, psycopg2.Error) as error:
        raise psycopg2.Error("Error while fetching data from PostgreSQL", error)
    finally:
        cur.close()
        connection.close()


def load():
    try:
        logging.info("Running ONLY LOAD PROCESS".center(80, "-"))
        path_jsons = extract(extract_list)
        paths_parquet = transform(path_jsons)
        paths_parquet_append = {
            k: v
            for (k, v) in paths_parquet.items()
            if k not in ["dim_date", "station_info_eco_bikes"]
        }
        load_dim_date(paths_parquet)
        (
            df_scd2_records_final_replace,
            df_new_records_final,
            df_scd2_records_final_append,
        ) = transform_scd_station_info(paths_parquet, DB_STR, POSTGRES_SCHEMA)
        load_station_info_to_database(
            df_scd2_records_final_replace,
            df_new_records_final,
            df_scd2_records_final_append,
        )
        load_to_postgres_append(paths_parquet_append)
        logging.info("FINISHED ONLY LOAD PROCESS".center(80, "-"))
    except BaseException as e:
        logging.error("LOAD could not complete", e)
        raise Exception("LOAD could not complete", e)


if __name__ == "__main__":
    load()

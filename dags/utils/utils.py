import json
import logging
import requests
import pandas as pd
import datetime as dt
from sqlalchemy import create_engine, text
from config.constants import BASE_FILE_DIR

logging.basicConfig(format="%(asctime)s - %(filename)s - %(message)s", level=logging.INFO)


def retry(func, retries=3):
    def retry_wrapper(*args, **kwargs):
        attempts = 0
        while attempts < retries:
            try:
                return func(*args, **kwargs)
            except BaseException:
                logging.error(f"Retrying from attempt number {attempts} because of error in API or data is not in json format")
                attempts += 1
        logging.exception('Exceed max retry num: {} failed'.format(attempts))
        raise Exception("The request from API is not working or the data its not in json format")
    return retry_wrapper


@retry
def get_request_json(url, params):
    logging.info(f" Getting request from API : {url}")
    s = requests.Session()
    s.headers = {"accept": "application/json", 'Content-Type': 'application/json; charset=utf-8'}
    r = s.get(url, params=params, timeout=30)
    r.raise_for_status()
    logging.info(f"Response 200 to request from {url}")
    data = r.json()
    logging.info(f"Response returned as jsonfrom {url} ")
    return data


def save_json(request_json, filename):
    logging.info(f"Saving request in json format in {BASE_FILE_DIR}/{filename}.json")
    json_path = f'{BASE_FILE_DIR}/{filename}.json'
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(request_json, f)
    return json_path


def load_json(path):
    with open(path, "r") as f:
        d = json.load(f)
    return d


def create_dim_date_table(start='2023-01-01', end='2080-12-31'):
    logging.info("Started generating the dim date table")
    df = pd.DataFrame({"Date": pd.date_range(start, end)})
    df["week_day"] = df.Date.dt.weekday
    df["day_name"] = df.Date.dt.day_name()
    df["day"] = df.Date.dt.day
    df["month"] = df.Date.dt.month
    df["month_name"] = df.Date.dt.month_name()
    df["week"] = df.Date.dt.isocalendar().week
    df["quarter"] = df.Date.dt.quarter
    df["year"] = df.Date.dt.year
    df["is_month_start"] = df.Date.dt.is_month_start
    df["is_month_end"] = df.Date.dt.is_month_end
    df.insert(0, 'date_id', (df.year.astype(str) + df.month.astype(str).str.zfill(2) + df.day.astype(str).str.zfill(2)).astype(int))
    df.name = "dim_date"
    logging.info("Ended generating the dim date table")
    return df


def load_to_parquet(df, filename):
    parquet_path = f'{BASE_FILE_DIR}/{filename}.parquet'
    df.to_parquet(f"{parquet_path}")
    return parquet_path


def df_to_database(df, table_name, connection_string, schema, method):
    try:
        con = create_engine(connection_string, connect_args={'options': f'-csearch_path={schema}'})
        df.to_sql(name=table_name, con=con, if_exists=method, index=False)
    except Exception as e:
        logging.error(f"Connection error {e}")
        raise Exception(f"Connection error {e}")


def get_max_reload(connection_string, schema):
    conn = create_engine(connection_string, connect_args={'options': f'-csearch_path={schema}'})
    if pd.read_sql(text("""select max(reload_id) from metadata_load;"""), con=conn).values[0][0] is None:
        reload_id = 1
    else:
        reload_id = pd.read_sql(text("""select max(reload_id) from metadata_load;"""), con=conn).values[0][0]+1
    return reload_id


def get_table_station_surrogate(connection_string, schema):
    try:
        conn = create_engine(connection_string, connect_args={'options': f'-csearch_path={schema}'})
        data = pd.read_sql(text("""select pk_surrogate_station_info,station_id  from  eco_bikes.station_info_eco_bikes where is_active = 1"""), conn)
    except Exception as e:
        raise BaseException(("Could not get the surrogate key from the stations", e))
    return data


def add_surrogate_ket_station_status(df, connection_string, schema):
    data_surrogate = get_table_station_surrogate(connection_string, schema)
    df = df.merge(data_surrogate, on='station_id', how='inner')
    return df


def transform_scd_station_info(path_parquet, string_connection, schema):
    logging.info(f"Reading parquet file: {path_parquet['station_info_eco_bikes']}.parquet in {path_parquet['station_info_eco_bikes']}")
    data_src = pd.read_parquet(path_parquet['station_info_eco_bikes'])
    conn = create_engine(string_connection, connect_args={'options': f'-csearch_path={schema}'})
    data_target = pd.read_sql_query("SELECT * FROM eco_bikes.station_info_eco_bikes", con=conn)    
    datetime_now = pd.to_datetime(dt.datetime.now(),format="%Y-%m-%d %HH:%MM:%SS")
    data_target_current = data_target[(data_target["is_active"] == 1)]
    df_merge_col = pd.merge(data_src, data_target_current, on='station_id', how='left')
    new_records_filter = pd.isnull(df_merge_col).any(axis=1)

    df_new_records = df_merge_col[new_records_filter]
    df_excluding_new = pd.concat([df_merge_col, df_new_records], axis=0).drop_duplicates(keep=False)
    df_new_records_final = df_new_records.copy()
    df_new_records_final = df_new_records_final[['station_id',
                                                 "station_name_x",
                                                 "physical_configuration_x",
                                                 "lat_x",
                                                 "lon_x",
                                                 "altitude_x",
                                                 "address_x",
                                                 "capacity_x",
                                                 "is_charging_station_x",
                                                 "nearby_distance_x",
                                                 "_ride_code_support_x"]]
    df_new_records_final.columns = ['station_id',
                                    'station_name',
                                    'physical_configuration',
                                    'lat',
                                    'lon',
                                    'altitude',
                                    'address', 'capacity', 'is_charging_station', 'nearby_distance', '_ride_code_support']
    df_new_records_final['start_date'] = datetime_now
    df_new_records_final['end_date'] = "9999-12-30 00:00:00"
    df_new_records_final['is_active'] = 1
    df_scd2_records = df_excluding_new[(df_excluding_new["station_name_x"] != df_excluding_new["station_name_y"]) |
                                       (df_excluding_new["physical_configuration_x"] != df_excluding_new["physical_configuration_y"]) |
                                       (df_excluding_new["lat_x"] != df_excluding_new["lat_y"]) |
                                       (df_excluding_new["lon_x"] != df_excluding_new["lon_y"]) |
                                       (df_excluding_new["altitude_x"] != df_excluding_new["altitude_y"]) |
                                       (df_excluding_new["address_x"] != df_excluding_new["address_y"]) |
                                       (df_excluding_new["capacity_x"] != df_excluding_new["capacity_y"]) |
                                       (df_excluding_new["is_charging_station_x"] != df_excluding_new["is_charging_station_y"]) |
                                       (df_excluding_new["nearby_distance_x"] != df_excluding_new["nearby_distance_y"]) |
                                       (df_excluding_new["_ride_code_support_x"] != df_excluding_new["_ride_code_support_y"])]
    df_scd2_records_final_replace = df_scd2_records.copy()
    df_scd2_records_final_replace = df_scd2_records_final_replace[['station_id',
                                                                   "station_name_y",
                                                                   "physical_configuration_y",
                                                                   "lat_y",
                                                                   "lon_y",
                                                                   "altitude_y",
                                                                   "address_y",
                                                                   "capacity_y",
                                                                   "is_charging_station_y",
                                                                   "nearby_distance_y",
                                                                   "_ride_code_support_y",
                                                                   "start_date"]]
    df_scd2_records_final_replace.columns = ['station_id',
                                             'station_name',
                                             'physical_configuration',
                                             'lat',
                                             'lon',
                                             'altitude',
                                             'address',
                                             'capacity',
                                             'is_charging_station',
                                             'nearby_distance',
                                             '_ride_code_support',
                                             "start_date"]
    df_scd2_records_final_replace['start_date'] = df_scd2_records_final_replace['start_date'].astype(str)
    df_scd2_records_final_replace['end_date'] = datetime_now
    df_scd2_records_final_replace['is_active'] = 0
    df_scd2_records_final_replace
    df_scd2_records_final_append = df_scd2_records.copy()
    df_scd2_records_final_append = df_scd2_records_final_append[['station_id',
                                                                 "station_name_x",
                                                                 "physical_configuration_x",
                                                                 "lat_x",
                                                                 "lon_x",
                                                                 "altitude_x",
                                                                 "address_x",
                                                                 "capacity_x",
                                                                 "is_charging_station_x",
                                                                 "nearby_distance_x", "_ride_code_support_x"]]
    df_scd2_records_final_append.columns = ['station_id',
                                            'station_name',
                                            'physical_configuration',
                                            'lat',
                                            'lon',
                                            'altitude',
                                            'address',
                                            'capacity',
                                            'is_charging_station',
                                            'nearby_distance',
                                            '_ride_code_support']
    df_scd2_records_final_append['start_date'] = datetime_now
    df_scd2_records_final_append['end_date'] = "9999-12-30 00:00:00"
    df_scd2_records_final_append['is_active'] = 1
    df_scd2_records_final_append

    return df_scd2_records_final_replace, df_new_records_final, df_scd2_records_final_append

import json
import logging
import requests
import pandas as pd
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
        print("DF OK")
    except Exception as e:
        logging.info(f"Connection error {e}")


def get_max_reload(connection_string, schema):
    conn = create_engine(connection_string, connect_args={'options': f'-csearch_path={schema}'})
    if pd.read_sql(text("""select max(reload_id) from metadata_load;"""), con=conn).values[0][0] is None:
        reload_id = 1
    else:
        reload_id = pd.read_sql(text("""select max(reload_id) from metadata_load;"""), con=conn).values[0][0]+1
    return reload_id

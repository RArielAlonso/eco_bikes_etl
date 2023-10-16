import json
import logging
import requests
import pandas as pd
from config.constants import BASE_FILE_DIR

logging.basicConfig(format="%(asctime)s - %(filename)s - %(message)s", level=logging.INFO)


def get_request_json(url, params):
    try:
        r = requests.get(url, params=params)
        r.raise_for_status()
        logging.info(f"Response 200 to request of {url}")
    except requests.exceptions.HTTPError as e:
        logging.error(" ERROR ".center(80, "-"))
        logging.error(e)
        logging.error(" ---- VERIFY CLIENT ID AND SECRET TO CONNECT TO API".center(80, "-"))
    except requests.exceptions.RequestException as e:
        logging.error(" ERROR ".center(80, "-"))
        logging.error(e)
    return r.json()


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
    logging.info("Ended generating the dim date table")
    return df

import pandas as pd
import logging
from etl_modules.extract import extract
from utlis.utils import load_json, create_dim_date_table


logging.basicConfig(format="%(asctime)s - %(filename)s - %(message)s", level=logging.INFO)

request_paths = extract()


def transform_dataframe_weather():
    logging.info("Start transforming the weather dataframe")
    data_clouds = pd.json_normalize(load_json(request_paths['weather'])['weather'])
    data_temperature = pd.json_normalize(load_json(request_paths['weather'])['main'])
    data_date = pd.json_normalize(load_json(request_paths['weather']))
    data_date['dt'] = data_date['dt']+data_date['timezone']
    data_date = pd.to_datetime(data_date['dt'], unit='s')
    data_weather = pd.concat([data_clouds, data_temperature, data_date], axis=1)
    data_weather.insert(0,
                        'date_id',
                        (data_weather['dt'].dt.year.astype(str) +
                         data_weather['dt'].dt.month.astype(str).str.zfill(2) + data_weather['dt'].dt.day.astype(str).str.zfill(2)).astype(int))
    logging.info("Finished creating the weather dataframe")
    return data_weather


def transform():
    create_dim_date_table()


if __name__ == "__main__":
    data = transform_dataframe_weather()
    print(data)
    # data.to_csv("asd.csv")
    # transform()

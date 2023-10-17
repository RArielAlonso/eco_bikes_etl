import pandas as pd
import logging
from etl_modules.extract import extract
from utlis.utils import load_json, create_dim_date_table


logging.basicConfig(format="%(asctime)s - %(filename)s - %(message)s", level=logging.INFO)

request_paths = extract()


def transform_weather():
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


def transform_system_info():
    data_system_info = pd.json_normalize(load_json(request_paths['system_info_eco_bikes']))
    data_system_info['last_updated'] = pd.to_datetime(data_system_info['last_updated']-10400, unit='s')
    return data_system_info


def transform_station_status():
    data_station_status = pd.DataFrame.from_dict(pd.json_normalize(load_json(request_paths['station_status_eco_bikes']))['data.stations'][0])
    data_station_status['last_reported'] = pd.to_datetime(data_station_status['last_reported']-10400, unit='s')
    data_station_status.drop(['num_bikes_available_types', 'is_charging_station', 'traffic'], axis=1, inplace=True)
    return data_station_status


def transform_station_info():
    data_station_info = pd.DataFrame.from_dict(pd.json_normalize(load_json(request_paths['station_info_eco_bikes']))['data.stations'][0])
    return data_station_info


def transform():
    create_dim_date_table()
    # transform_weather()
    # transform_system_info()
    # transform_station_status()
    transform_station_info()


if __name__ == "__main__":
    transform()

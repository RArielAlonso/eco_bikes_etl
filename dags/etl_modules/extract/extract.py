import pandas as pd
from dags.etl_modules.extract.extract_eco_bikes import get_bikes_data
from dags.etl_modules.extract.extract_weather import get_weather_data
from config.config import API_CLIENT_ID, API_CLIENT_SECRET, URL_STATION_INFO, URL_STATION_STATUS, URL_SYSTEM_INFORMATION,BASE_URL_WEATHER


request_weather = get_weather_data(BASE_URL_WEATHER)
data = pd.json_normalize(request_weather['main'])
pd.json_normalize(request_weather['weather'])

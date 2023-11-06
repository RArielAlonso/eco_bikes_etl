import os
from dotenv import load_dotenv

load_dotenv()

# URL FOR WEATHER
BASE_URL_WEATHER = os.getenv("BASE_URL_WEATHER")
WEATHER_CITY = os.getenv("WEATHER_CITY")
WEATHER_APP_ID = os.getenv("WEATHER_APP_ID")
WEATHER_UNITS = os.getenv("WEATHER_UNITS")
WEATHER_PARAMS_API = {"q": WEATHER_CITY,
                      "appid": WEATHER_APP_ID,
                      "units": WEATHER_UNITS
                      }

# URL AND CREDENTIALS FOR ECO BIKES
API_CLIENT_ID = os.getenv("API_CLIENT_ID")
API_CLIENT_SECRET = os.getenv("API_CLIENT_SECRET")
URL_SYSTEM_INFORMATION = os.getenv("URL_SYSTEM_INFORMATION")
URL_STATION_STATUS = os.getenv("URL_STATION_STATUS")
URL_STATION_INFO = os.getenv("URL_STATION_INFO")
ECO_BIKES_PARAMS_API = {"client_id": API_CLIENT_ID,
                        "client_secret": API_CLIENT_SECRET
                        }

weather_ds = {'name': "weather",
              "base_url": BASE_URL_WEATHER,
              "params": WEATHER_PARAMS_API}

system_info_eco_bikes_ds = {"name": "system_info_eco_bikes",
                            "base_url": URL_SYSTEM_INFORMATION,
                            "params": ECO_BIKES_PARAMS_API
                            }

station_status_eco_bikes_ds = {"name": "station_status_eco_bikes",
                               "base_url": URL_STATION_STATUS,
                               "params": ECO_BIKES_PARAMS_API}

station_info_eco_bikes_ds = {"name": "station_info_eco_bikes",
                             "base_url": URL_STATION_INFO,
                             "params": ECO_BIKES_PARAMS_API}

extract_list = [weather_ds,
                system_info_eco_bikes_ds,
                station_status_eco_bikes_ds,
                station_info_eco_bikes_ds]

# DB CONNECTIONS

POSTGRES_SCHEMA = os.getenv('POSTGRES_SCHEMA')
POSTGRES_USER = os.getenv('POSTGRES_USER')
POSTGRES_PASS = os.getenv('POSTGRES_PASS')
POSTGRES_HOST = os.getenv('POSTGRES_HOST')
POSTGRES_DB = os.getenv('POSTGRES_DB')
POSTGRES_PORT = os.getenv('POSTGRES_PORT')
POSTGRES_SCHEMA = os.getenv('POSTGRES_SCHEMA')
DB_STR = f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASS}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

import configparser

config = configparser.ConfigParser()
config.read("config/config.ini")

# URL FOR WEATHER
BASE_URL_WEATHER = config["DEFAULT"]["BASE_URL_WEATHER"]
WEATHER_CITY = config["DEFAULT"]["WEATHER_CITY"]
WEATHER_APP_ID = config["DEFAULT"]["WEATHER_APP_ID"]
WEATHER_UNITS = config["DEFAULT"]["WEATHER_UNITS"]
WEATHER_PARAMS_API = {"q": WEATHER_CITY,
                      "appid": WEATHER_APP_ID,
                      "units": WEATHER_UNITS
                      }

# URL AND CREDENTIALS FOR ECO BIKES
API_CLIENT_ID = config["DEFAULT"]["API_CLIENT_ID"]
API_CLIENT_SECRET = config["DEFAULT"]["API_CLIENT_SECRET"]
URL_SYSTEM_INFORMATION = config["DEFAULT"]["URL_SYSTEM_INFORMATION"]
URL_STATION_STATUS = config["DEFAULT"]["URL_STATION_STATUS"]
URL_STATION_INFO = config["DEFAULT"]["URL_STATION_INFO"]
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

station_info_eco_bikes = {"name": "station_info_eco_bikes",
                          "base_url": URL_STATION_INFO,
                          "params": ECO_BIKES_PARAMS_API}

extract_list = [weather_ds,
                system_info_eco_bikes_ds,
                station_status_eco_bikes_ds,
                station_info_eco_bikes]

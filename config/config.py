import configparser
from pathlib import Path


config = configparser.ConfigParser()
config.read("config/config.ini")

BASE_PATH = Path().resolve().parent

# URL FOR WEATHER
BASE_URL_WEATHER = config["DEFAULT"]["BASE_URL_WEATHER"]

# URL AND CREDENTIALS FOR ECO BIKES

API_CLIENT_ID = config["DEFAULT"]["API_CLIENT_ID"]
API_CLIENT_SECRET = config["DEFAULT"]["API_CLIENT_SECRET"]
URL_SYSTEM_INFORMATION = config["DEFAULT"]["URL_SYSTEM_INFORMATION"]
URL_STATION_STATUS = config["DEFAULT"]["URL_STATION_STATUS"]
URL_STATION_INFO = config["DEFAULT"]["URL_STATION_INFO"]

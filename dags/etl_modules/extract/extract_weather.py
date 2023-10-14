import requests
import logging
from config.config import BASE_URL_WEATHER, WEATHER_CITY, WEATHER_APP_ID, WEATHER_UNITS

logging.basicConfig(format="%(name)s - %(asctime)s - %(message)s", level=logging.INFO)


def get_weather_data(url, city, app_id, units):
    try:
        r = requests.get(url, params={"q": city, "appid": app_id, "units": units})
        r.raise_for_status()
        logging.info("Response 200 to request of WEATHER API")
    except requests.exceptions.HTTPError as e:
        logging.error(" ERROR ".center(80, "-"))
        logging.error(e)
        logging.error(" ---- MAKE SURE YOU HAVE THE RIGHT APP ID TO CONNECT TO WEATHER API".center(80, "-"))
    except requests.exceptions.RequestException as e:
        logging.error(" ERROR ".center(80, "-"))
        logging.error(e)
    return r.json()


if __name__ == "__main__":
    data = get_weather_data(BASE_URL_WEATHER, WEATHER_CITY, WEATHER_APP_ID, WEATHER_UNITS)
    print(data)

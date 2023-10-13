import requests
import logging
from config.config import BASE_URL_WEATHER

logging.basicConfig(format="%(asctime)s - %(message)s", level=logging.INFO)


def get_weather_data(url):
    try:
        r = requests.get(url)
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
    data = get_weather_data(BASE_URL_WEATHER)
    print(data)

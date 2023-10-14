import requests
import logging
from config.config import API_CLIENT_ID, API_CLIENT_SECRET, URL_STATION_INFO, URL_STATION_STATUS, URL_SYSTEM_INFORMATION


logging.basicConfig(format="%(name)s - %(asctime)s - %(message)s", level=logging.INFO)


def get_bikes_data(url, client_id, client_secret):
    try:
        r = requests.get(url, params={'client_id': client_id, 'client_secret': client_secret})
        r.raise_for_status()
        logging.info(f"Response 200 to request of BA - ECOBIKES API - {url}")
    except requests.exceptions.HTTPError as e:
        logging.error(" ERROR ".center(80, "-"))
        logging.error(e)
        logging.error(" ---- VERIFY CLIENT ID AND SECRET TO CONNECT TO BA - ECOBIKES API".center(80, "-"))
    except requests.exceptions.RequestException as e:
        logging.error(" ERROR ".center(80, "-"))
        logging.error(e)
    return r.json()


if __name__ == "__main__":
    request_station_info = get_bikes_data(URL_STATION_INFO, API_CLIENT_ID, API_CLIENT_SECRET)
    request_station_status = get_bikes_data(URL_STATION_STATUS, API_CLIENT_ID, API_CLIENT_SECRET)
    request_system_information = get_bikes_data(URL_SYSTEM_INFORMATION, API_CLIENT_ID, API_CLIENT_SECRET)
    print(request_station_info, request_station_status, request_system_information)

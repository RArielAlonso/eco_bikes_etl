import json
import logging
from config.constants import BASE_FILE_DIR
import requests

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
    with open(json_path, 'w') as f:
        json.dump(request_json, f)
    return json_path

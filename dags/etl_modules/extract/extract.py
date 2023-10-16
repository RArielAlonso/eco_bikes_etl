import logging
from utlis.utils import get_request_json, save_json
from config.config import extract_list

logging.basicConfig(format="%(asctime)s - %(filename)s - %(message)s", level=logging.INFO)


def extract():
    for i in extract_list:
        logging.info(f"Extracting began for {i['name']}")
        request_raw_json = get_request_json(i['base_url'], i['params'])
        save_json(request_raw_json, i['name'])


if __name__ == "__main__":
    extract()

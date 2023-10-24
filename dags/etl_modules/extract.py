import logging
from utils.utils import get_request_json, save_json
from config.config import extract_list

logging.basicConfig(format="%(asctime)s - %(filename)s - %(message)s", level=logging.INFO)


def extract(url_list):
    json_paths = dict()
    try:
        for i in url_list:
            logging.info(f"Extracting began for {i['name']}")
            request_raw_json = get_request_json(i['base_url'], i['params'])
            json_paths[i['name']] = save_json(request_raw_json, i['name'])
    except BaseException as e:
        logging.exception("The extract process could not complete".center(80, "-"))
        logging.exception(f"{e}")
    return json_paths


if __name__ == "__main__":
    logging.info("Running ONLY EXTRACT PROCESS FROM THE ETL".center(80, "-"))
    json_paths = extract(extract_list)
    logging.info(f"The path of the files are: {json_paths}")

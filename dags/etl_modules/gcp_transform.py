import datetime as dt
from etl_modules.extract import gcp_extract
from utils.utils import gcp_load_json, create_dim_date_table, gcp_load_to_parquet, gcp_get_max_reload
from config.config import weather_ds, system_info_eco_bikes_ds, station_info_eco_bikes_ds, station_status_eco_bikes_ds, extract_list
from config.config import GCP_STORAGE_JSON_CREDENTIALS, GCP_PROJECT_ID, GCP_DATASET_ID, GCP_BQ_JSON_CREDENTIALS
from config.constants import GCP_BUCKET_NAME
from etl_modules.extract import gcp_extract
from utils.utils import (
    create_dim_date_table,
    gcp_get_max_reload,
    gcp_load_json,
    gcp_load_to_parquet,
)

logging.basicConfig(
    format="%(asctime)s - %(filename)s - %(message)s", level=logging.INFO
)


def gcp_transform_weather(path_jsons):
    logging.info("Start transforming the weather dataframe")
    data_clouds = pd.json_normalize(
        gcp_load_json(
            path_jsons["weather"], GCP_STORAGE_JSON_CREDENTIALS, GCP_BUCKET_NAME
        )["weather"]
    )
    data_temperature = pd.json_normalize(
        gcp_load_json(
            path_jsons["weather"], GCP_STORAGE_JSON_CREDENTIALS, GCP_BUCKET_NAME
        )["main"]
    )
    data_date = pd.json_normalize(
        gcp_load_json(
            path_jsons["weather"], GCP_STORAGE_JSON_CREDENTIALS, GCP_BUCKET_NAME
        )
    )
    data_date["dt"] = data_date["dt"] + data_date["timezone"]
    data_date = pd.to_datetime(data_date["dt"], unit="s")
    data_weather = pd.concat([data_clouds, data_temperature, data_date], axis=1)
    data_weather = data_weather[~data_weather.isnull().any(axis=1)]
    data_weather.insert(
        0, "date_id", (data_weather["dt"].dt.strftime("%Y%m%d").astype(int))
    )
    data_weather.name = weather_ds["name"]
    logging.info("Finished creating the weather dataframe")
    return data_weather


def gcp_transform_system_info(path_jsons):
    logging.info("Start transforming the ecobikes_system_info dataframe")
    data_system_info = pd.json_normalize(
        gcp_load_json(
            path_jsons["system_info_eco_bikes"],
            GCP_STORAGE_JSON_CREDENTIALS,
            GCP_BUCKET_NAME,
        )
    )
    data_system_info["last_updated"] = pd.to_datetime(
        data_system_info["last_updated"] - 10400, unit="s"
    )
    data_system_info.name = system_info_eco_bikes_ds["name"]
    data_system_info.columns = [
        "last_updated",
        "ttl",
        "system_id",
        "language",
        "name",
        "timezone",
        "build_version",
        "build_label",
        "build_hash",
        "build_number",
        "mobile_head_version",
        "mobile_minimum_supported_version",
        "vehicle_count_mechanical_count",
        "vehicle_count_ebike_count",
        "station_count",
    ]
    logging.info("Finished creating the ecobikes_system_info dataframe")
    return data_system_info


def gcp_transform_station_status(path_jsons):
    logging.info("Start transforming the ecobikes_station_status dataframe")
    data_station_status = pd.DataFrame.from_dict(
        pd.json_normalize(
            gcp_load_json(
                path_jsons["station_status_eco_bikes"],
                GCP_STORAGE_JSON_CREDENTIALS,
                GCP_BUCKET_NAME,
            )
        )["data.stations"][0]
    )
    data_station_status["last_reported"] = pd.to_datetime(
        data_station_status["last_reported"] - 10400, unit="s"
    )
    data_station_status.drop(
        ["num_bikes_available_types", "is_charging_station", "traffic"],
        axis=1,
        inplace=True,
    )
    data_station_status.name = station_status_eco_bikes_ds["name"]
    logging.info("Finished creating the ecobikes_station_status dataframe")
    return data_station_status


def gcp_transform_station_info(path_jsons):
    logging.info("Start transforming the ecobikes_station_info dataframe")
    data_station_info = pd.DataFrame.from_dict(
        pd.json_normalize(
            gcp_load_json(
                path_jsons["station_info_eco_bikes"],
                GCP_STORAGE_JSON_CREDENTIALS,
                GCP_BUCKET_NAME,
            )
        )["data.stations"][0]
    )
    data_station_info.rename(columns={"name": "station_name"}, inplace=True)
    data_station_info.drop(
        columns=[
            "rental_uris",
            "rental_methods",
            "groups",
            "obcn",
            "post_code",
            "cross_street",
        ],
        inplace=True,
    )
    data_station_info.name = station_info_eco_bikes_ds["name"]
    logging.info("Finished creating the ecobikes_station_info dataframe")
    return data_station_info


def gcp_transform_metadata_load():
    date_reload = pd.to_datetime(dt.datetime.now())
    date_id = int(
        (
            str(date_reload.year)
            + str(date_reload.month).zfill(2)
            + str(date_reload.day).zfill(2)
        )
    )
    transform_metadata_load = pd.DataFrame(
        [{"date_reload": date_reload, "date_id": date_id}]
    )
    transform_metadata_load.name = "metadata_load"
    return transform_metadata_load


def gcp_transform(path_jsons):
    try:
        logging.info("Began the TRANSFORM PROCESS".center(80, "-"))
        parquets_path = dict()
        reload_id = gcp_get_max_reload(
            GCP_BQ_JSON_CREDENTIALS, GCP_PROJECT_ID, GCP_DATASET_ID
        )
        df_metadata = gcp_transform_metadata_load()
        df_metadata["reload_id"] = reload_id
        df_dim_date = create_dim_date_table()
        df_dim_date["reload_id"] = reload_id
        df_fact_weather = gcp_transform_weather(path_jsons)
        df_fact_weather["reload_id"] = reload_id
        df_system_info = gcp_transform_system_info(path_jsons)
        df_system_info["reload_id"] = reload_id
        df_station_info = gcp_transform_station_info(path_jsons)
        df_station_info["reload_id"] = reload_id
        df_station_status = gcp_transform_station_status(path_jsons)
        df_station_status["reload_id"] = reload_id
        list_df = [
            df_dim_date,
            df_metadata,
            df_fact_weather,
            df_system_info,
            df_station_info,
            df_station_status,
        ]
        for i in list_df:
            logging.info(
                f"Generating {i.name}.parquet in GCP PROJECT: {GCP_PROJECT_ID} - BUCKET: {GCP_BUCKET_NAME}/parquet/{i.name}.parquet"
            )
            parquets_path[i.name] = gcp_load_to_parquet(
                i, f"{i.name}", GCP_PROJECT_ID, GCP_BQ_JSON_CREDENTIALS, GCP_BUCKET_NAME
            )
        logging.info("FINISHED the TRANSFORM PROCESS".center(80, "-"))
        return parquets_path
    except BaseException as e:
        logging.error(e)
        raise Exception(e)


if __name__ == "__main__":
    try:
        logging.info("Running ONLY TRANSFORM PROCESS".center(80, "-"))
        path_jsons = gcp_extract(extract_list)
        paths = gcp_transform(path_jsons)
        logging.info("FINISHED ONLY TRANSFORM PROCESS".center(80, "-"))
    except BaseException as e:
        logging.error("Transform could not complete", e)
        raise Exception("Transform could not complete", e)

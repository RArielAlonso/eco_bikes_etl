# ETL - Process - Bike Station and Weather

The following project is about an ETL process conformed by the next steps:

- Extract (E): The data is extracted from APIs and stored in json files
    * [Bike API](https://buenosaires.gob.ar/apis)
    * [Weather API](https://api.openweathermap.org/data/2.5/weather)

- Transform (T): The data stored in json file is loaded to a Pandas Dataframe to perfom transformations and store it as a parquet file.

- Load (L): The parquet file is loaded to the database

This process will have diffent ways to be executed:

1. [Local by docker compose and Poetry](##-1.-Local-by-docker-compose-and-Poetry) 
1. [Local by docker compose and Poetry](##-2.-Local-by-docker-compose-and-Airflow)

## Initial require configuration
- Have [docker](https://docs.docker.com/get-docker/) and docker compose installed in your machine to run airflow and local postgres database
- Have Linux or [WSL](https://learn.microsoft.com/es-es/windows/wsl/install) installed to execute bash commands
- Have install [Poetry](https://python-poetry.org/docs/), for managing dependencies
- Clone the repository in a new folder and run these commands

``` 
git clone https://github.com/RArielAlonso/eco_bikes_etl.git
cd eco_bikes_etl/
echo -e "AIRFLOW_UID=$(id -u)" > .env
docker compose up airflow-init
poetry install
``` 
After you have runned these commands you have set up Airflow and set the dependencies to run locally

- Create the config.ini file, you can use the example and modify the credentials values
``` 
[DEFAULT]

# OPEN WEATHER API
BASE_URL_WEATHER = https://api.openweathermap.org/data/2.5/weather
WEATHER_CITY=Buenos Aires
WEATHER_APP_ID=VALUE-TO-CHANGE
WEATHER_UNITS=metric

# BA OPEN DATA API
API_CLIENT_ID=VALUE-TO-CHANGE
API_CLIENT_SECRET=VALUE-TO-CHANGE
URL_SYSTEM_INFORMATION=https://datosabiertos-transporte-apis.buenosaires.gob.ar:443/ecobici/gbfs/systemInformation
URL_STATION_STATUS=https://datosabiertos-transporte-apis.buenosaires.gob.ar:443/ecobici/gbfs/stationStatus
URL_STATION_INFO=https://datosabiertos-transporte-apis.buenosaires.gob.ar:443/ecobici/gbfs/stationInformation

# DB STR LOCAL

POSTGRES_USER=VALUE-TO-CHANGE
POSTGRES_PASS=VALUE-TO-CHANGE
POSTGRES_HOST=localhost
POSTGRES_DB=eco_bikes
POSTGRES_PORT=5432
POSTGRES_SCHEMA=eco_bikes
``` 

After executing this commands you have succefully configured airflow and a local postgres database and the configuration files

## 1. Local by docker compose and Poetry

Make sure you have set the previous configuration, after that run the following command that will initialize the airflow and local postgres dabase to store the data.

``` 
docker compose up
``` 



## 2. Local by docker compose and Airflow

















asdasdasdasdad
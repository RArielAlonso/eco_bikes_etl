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

## Access the local database

This is a postgres database from a container image with the following details:

- 13-alpine - [Image Details](https://github.com/docker-library/postgres/blob/6f4ae836406b010948f01fbcb400a31dca4fdf52/13/alpine3.18/Dockerfile)
- The schema and the tables are created when you initialize the docker compose by the scripts in the SQL folders by the following volume

``` ./SQL:/docker-entrypoint-initdb.d ```
- Another volume is created to persist the data after the container gets stopped.

``` $HOME/docker/volumes/postgres:/var/lib/postgresql/data```

After you have run either the option 1 or 2 from docker compose up, you can enter the database by theese options:

- Set a connection through an app like [Dbeaver](https://dbeaver.io/) or [pgadmin](https://www.pgadmin.org/)

- Get inside the container with this command in the terminal and run SQL queries

```
docker exec -it postgres_local psql -U Ariel -D eco_bikes
```


## 1. Local by docker compose and Poetry

Make sure you have set the previous configuration, after that run the following command that will initialize the airflow and local postgres dabase to store the data.

``` 
docker compose up
``` 
Wait to the local database to be initialized and run the following command:

***ATTENTION***: you must change the config.ini ***POSTGRES_HOST*** to ***localhost*** as it will be executed locally

``` 
poetry run python dags/etl_modules/load.py
``` 
Make sure you have install the poetry enviroment

## 2. Local by docker compose and Airflow

Make sure you have set the previous configuration, after that run the following command that will initialize the airflow and local postgres dabase to store the data.

``` 
docker compose up
``` 

After that you can go to your web browser and insert the following command:
``` 
localhost:8080
``` 

This will give acces the web browser of airflow were you can trigger the ETL process manually.

Here I have set up two DAGS:

- ***all_in_one_ETL***: this DAG runs in one task all the ETL process
- ***etl_extract_transform_load***: ETL process grouped in extract, transform and load

## To continue working:
- Add unit tests
- Add github actions
- Test with pull request
- Add triggers in airflow
- Verify the timestamp when running through docker
- Run in google cloud
- Get more detailed in the ETL process, splitting more the tasks
- Add linters and docstrings
- Modify variables in the ETL process
- Generate a makefile

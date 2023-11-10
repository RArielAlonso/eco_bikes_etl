# ETL - Process - Bike Station and Weather

The following project is about an ETL process conformed by the next steps:

- Extract (E): The data is extracted from APIs and stored in json files
    * [Bike API](https://buenosaires.gob.ar/apis)
    * [Weather API](https://api.openweathermap.org/data/2.5/weather)

- Transform (T): The data stored in json file is loaded to a Pandas Dataframe to perfom transformations and store it as a parquet file.

- Load (L): The parquet file is loaded to the database

This process will have diffent ways to be executed:

1. [Local by docker compose and Poetry](##-1.-Local-by-docker-compose-and-Poetry) 
1. [GCP - Persisting in buckets json and parquet](##-2.-Local-by-docker-compose-and-Airflow)

## Initial require configuration
- Have [docker](https://docs.docker.com/get-docker/) and docker compose installed in your machine to run airflow and local postgres database
- Have Linux or [WSL](https://learn.microsoft.com/es-es/windows/wsl/install) installed to execute bash commands
- Have install [Poetry](https://python-poetry.org/docs/), for managing dependencies
- Clone the repository in a new folder and run these commands

``` 
git clone https://github.com/RArielAlonso/eco_bikes_etl.git
cd eco_bikes_etl/
echo -e "AIRFLOW_UID=$(id -u)" > .env
docker build -t airflow-custom . --no-cache 
docker compose up airflow-init
docker compose up --build --force-recreate 
``` 

*By running this commands you have cloned the repo to the directory you have choose (be aware of this), created the variable for airflow user.
The when you started running docker it will build a custom image of airflow and install poetry, it is important for having separeted enviroments for airflow and your app and avoiding conflicts between the dependencies.*

- Create the .env file, you can use the example and modify the credentials values, the file must be in the folder **/dags/config/**
``` 

# OPEN WEATHER API
BASE_URL_WEATHER=https://api.openweathermap.org/data/2.5/weather
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

# GCP

GCP_PROJECT_ID=VALUE-TO-CHANGE
GCP_DATASET_ID=eco_bikes_dataset
GCP_BQ_JSON_CREDENTIALS=dags/config/eco-bikes-gcp.json
GCP_STORAGE_JSON_CREDENTIALS=dags/config/eco-bikes-storage.json
GCP_BUCKET_NAME=eco_bikes_bucket

``` 

After executing this commands you have succefully configured airflow and a local postgres database and the configuration files

## Google Cloud Platform Configuration

In order to run the ETL persisting the data in json and parquet files stored in buckets and after storing it in Big Query, we need some configuration inside the [Google Cloud Console](https://console.cloud.google.com/)

1. You need to create a new project to store the the project_id defined by Google inside the ***.env file*** [Official Documentation](https://cloud.google.com/resource-manager/docs/creating-managing-projects?hl=es-419)

1. Create the servie account for the bucket and big query, storing the json credentials in the following path
``` 
dags/config
``` 
**IMPORTANT**: The files must be with the same names as they are set in the .env file

**eco-bikes-gcp.json** stands for GCP Big query credentials

**eco-bikes-storage.json** stands for GCP Storage credentials
``` 
GCP_BQ_JSON_CREDENTIALS=dags/config/eco-bikes-gcp.json
GCP_STORAGE_JSON_CREDENTIALS=dags/config/eco-bikes-storage.json
``` 
[Link to documentation to create service account credentials in Big Query](https://www.progress.com/tutorials/jdbc/a-complete-guide-for-google-bigquery-authentication)

The same process is for storage and you must select *Storage Admin* and *Owner*, this can be improved to make roles but for the use case of the project selecting the owner makes less possible conflicts.

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
docker exec -it postgres_local psql -U ariel -d eco_bikes
```
After this the password will be asked

For example you can check the diferent reloads:
```
select * from eco_bikes.metadata_load;
```

## 1. Local by docker compose and Poetry

Make sure you have set the previous configuration, after that run the following command that will initialize the airflow and local postgres dabase to store the data.

Wait to the local database (*postgres_local*) to be initialized and run the following command:

**ATTENTION**: you must change the config.ini ***POSTGRES_HOST*** to ***localhost*** as it will be executed locally. This step must be down ***before*** running the ***docker compose up*** command.

``` 
poetry run python dags/etl_modules/load.py
``` 
Make sure you have install the poetry enviroment on your local host, you can use the ***poetry install*** command

## 2. Local by docker compose and Airflow

Make sure you have set the previous configuration, after that run the following command that will initialize the airflow and local postgres dabase to store the data.

After that you can go to your web browser and insert the following command:
``` 
localhost:8080
``` 

This will give access to the web browser of airflow were you can trigger the ETL process manually.

Here I have set up four DAGS:
- **1-external_all_in_one_etl**: same as 1 but running trough the poetry enviroment generated with the Dockerfile. It is set to run with a schedule interval of an hour.
- **2-external_etl_extract_transform_load**: same as 1 but running trough the poetry enviroment generated with the Dockerfile. It is set to run with a schedule interval of an hour.

**IMPORTANT**: *For running the dags from the enviroment generated by poetry you must point the ***path to where the executable lives inside of the worker container*** and you must use the decorators of ***task API***.*

## 3. Google Cloud Platform

Important is that you have run through the configuration process described before.

You can run this part by two ways:
1. Locally by poetry and command line
To run this you can execute the following commands in the root folder of the project:

``` 
poetry shell
python dags/etl_modules/gcp_load.py
``` 
The first command puts you inside the poetry enviroment where you can run the second command to do the ETL process.
The ETL process is logged by [Logging](https://realpython.com/python-logging/) module, to get aware of the step of the ETL process.


2. Locally by Airflow in containers
After you have verified that the containers of Airflow are running safe please select the browser of your choice and get inside the [Airflow UI](https://airflow.apache.org/docs/apache-airflow/stable/ui.html)
``` 
localhost:8080
``` 
Here you will find the ***3-GCP-ETL*** dag that enables you to run the ETL process locally and storing the json and parquet file in the [GCP Storage](https://cloud.google.com/storage/docs). Once this step is finished it will load the data into [GCP Big Query](https://cloud.google.com/bigquery/docs)


## Tests and Github Actions

The project has set up a CI/CD with [Github Actions](https://docs.github.com/es/actions), this CI/CD runs two tests automatically that will be explained next:

1. *Test in dim date table creation*

This test checks that the script that creates the **dim date** table is correct. It is done by the [Pandas Testing Function](https://pandas.pydata.org/docs/reference/api/pandas.testing.assert_frame_equal.html).

2. *Test in the transform of the response of weather API*

The test checks the transformation of the json response of the weather API to the dataframe previous to be created the parquet file.
For this example I used the patch decorator when loading the raw json file of the request.get of the API, by setting the patch we avoid requesting the API.

Both tests can be run locally, by running this command in the main folder of the project. The module used to run theese tests is [Pytest](https://docs.pytest.org/en/7.4.x/)

***ATTENTION***: Be aware that the poetry enviroment is activated, because p

```
pytest dags/tests -vv 
``` 
The -vv flag is for having a more verbose response of the tests in case of failure.

With the example defined in point number 2, we can extend the tests to all of the transformations, as we can mock the response of loading the json file.


## To be continue working:
- Run in cloud composer
- Verify the timestamp when running through docker
- Add linters and docstrings, and add to github actions
- Modify data types in the ETL process
- Generate a makefile
- Add Airflow Operators, such as Big Query
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


## 1. Local by docker compose and Poetry

### Initial require configuration
- Have [docker](https://docs.docker.com/get-docker/) and docker compose installed in your machine
- Have Linux or [WSL](https://learn.microsoft.com/es-es/windows/wsl/install) installed
- Clone the repository and run these commands

``` 
git clone https://github.com/RArielAlonso/eco_bikes_etl.git
cd /eco_bikes_etl
echo -e "AIRFLOW_UID=$(id -u)" > .env
docker compose up airflow-init
``` 
Otherwise you can run after you have cloned the repo
``` 
make configure_airflow
``` 

After executing this commands you have succefully configured airflow and a local postgres database

### Running with poetry



## 2. Local by docker compose and Airflow

















asdasdasdasdad
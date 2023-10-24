configure_airflow_local:
	cd eco_bikes_etl/ && echo -e "AIRFLOW_UID=$(id -u)" > .env && docker compose up airflow-init 

proyect_local_down:
	docker compose down

proyect_local_up: proyect_local_down
	docker compose up -d

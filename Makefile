proyect_local_down:
	docker compose down

proyect_local_up: proyect_local_down
	docker compose up -d

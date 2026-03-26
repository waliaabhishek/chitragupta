.PHONY: up down clean logs

.DEFAULT_GOAL := up

up:
	docker compose up --build -d

down:
	docker compose down

logs:
	docker compose logs -f

clean:
	docker compose down --volumes --rmi local

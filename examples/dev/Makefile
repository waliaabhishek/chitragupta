.PHONY: help up down logs clean

.DEFAULT_GOAL := help

help:
	@echo "Dev Stack (API + Grafana + UI)"
	@echo ""
	@echo "  up     - Build images and start all services"
	@echo "  down   - Stop all services"
	@echo "  logs   - Tail logs from all services"
	@echo "  clean  - Stop all services and remove volumes + images"

up:
	docker compose up --build -d

down:
	docker compose down

logs:
	docker compose logs -f

clean:
	docker compose down --volumes --rmi local

DOCKER_COMPOSE := $(shell docker compose version >/dev/null 2>&1 && echo "docker compose" || echo "docker-compose")

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
	$(DOCKER_COMPOSE) up --build -d

down:
	$(DOCKER_COMPOSE) down

logs:
	$(DOCKER_COMPOSE) logs -f

clean:
	$(DOCKER_COMPOSE) down --volumes --rmi local

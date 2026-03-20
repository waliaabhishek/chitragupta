.PHONY: help setup install sync test lint format typecheck check clean \
        docs docs-serve docs-build dev dev-api dev-ui \
        example-ccloud-grafana-up example-ccloud-grafana-down \
        example-ccloud-full-up example-ccloud-full-down \
        example-self-managed-up example-self-managed-down \
        docker-build docker-up docker-down docker-dev docker-logs docker-push

# Docker registry settings (override with: make docker-push REGISTRY=ghcr.io/myorg)
REGISTRY ?= docker.io/library
PLATFORMS ?= linux/amd64,linux/arm64

.DEFAULT_GOAL := help

help:
	@echo "Available targets:"
	@echo ""
	@echo "  Setup:"
	@echo "    setup        - Create virtual environment and install all dependencies"
	@echo "    install      - Alias for setup"
	@echo "    sync         - Sync dependencies (after pyproject.toml changes)"
	@echo ""
	@echo "  Development:"
	@echo "    dev          - Start backend (API + worker) and frontend together"
	@echo "    dev-api      - Start backend only (API + worker)"
	@echo "    dev-ui       - Start backend (API only) and frontend"
	@echo "    test         - Run tests with coverage"
	@echo "    lint         - Run ruff linter"
	@echo "    format       - Run ruff formatter"
	@echo "    typecheck    - Run mypy type checker"
	@echo "    check        - Run all checks (lint, typecheck, test)"
	@echo ""
	@echo "  Documentation:"
	@echo "    docs         - Serve docs locally at http://127.0.0.1:8000"
	@echo "    docs-serve   - Alias for docs"
	@echo "    docs-build   - Build static documentation site"
	@echo ""
	@echo "  Docker (per-example):"
	@echo "    example-ccloud-grafana-up    - Start ccloud-grafana example (worker + Grafana)"
	@echo "    example-ccloud-grafana-down  - Stop ccloud-grafana example"
	@echo "    example-ccloud-full-up       - Start ccloud-full example (API + Grafana + UI)"
	@echo "    example-ccloud-full-down     - Stop ccloud-full example"
	@echo "    example-self-managed-up      - Start self-managed-full example"
	@echo "    example-self-managed-down    - Stop self-managed-full example"
	@echo ""
	@echo "  Docker (legacy aliases → ccloud-full):"
	@echo "    docker-build - Force rebuild ccloud-full images (local, single arch)"
	@echo "    docker-push  - Build multi-arch images and push to registry"
	@echo "                   Override registry: make docker-push REGISTRY=ghcr.io/myorg"
	@echo "    docker-up    - Alias for example-ccloud-full-up"
	@echo "    docker-down  - Alias for example-ccloud-full-down"
	@echo "    docker-dev   - Alias for example-ccloud-full-up"
	@echo "    docker-logs  - Tail logs from ccloud-full services"
	@echo ""
	@echo "  Cleanup:"
	@echo "    clean        - Remove build artifacts and caches"

# ─────────────────────────────────────────────────────────────────────────────
# Setup
# ─────────────────────────────────────────────────────────────────────────────

setup:
	uv sync --all-groups
	cd frontend && npm install

install: setup

sync:
	uv sync --all-groups
	cd frontend && npm install

# ─────────────────────────────────────────────────────────────────────────────
# Development
# ─────────────────────────────────────────────────────────────────────────────

test:
	uv run pytest --cov=src --cov-report=term-missing

lint:
	uv run ruff check src tests

format:
	uv run ruff format src tests

typecheck:
	uv run mypy src

check: lint typecheck test

dev:
	@echo "Starting backend (API + worker) and frontend..."
	@trap 'kill 0' EXIT; \
	PYTHONPATH=src uv run python -m main --config-file config.yaml --mode both & \
	cd frontend && npx vite

dev-api:
	PYTHONPATH=src uv run python -m main --config-file config.yaml --mode both

dev-ui:
	@echo "Starting backend (API only) and frontend..."
	@trap 'kill 0' EXIT; \
	PYTHONPATH=src uv run python -m main --config-file config.yaml --mode api & \
	cd frontend && npx vite

# ─────────────────────────────────────────────────────────────────────────────
# Documentation
# ─────────────────────────────────────────────────────────────────────────────

docs: docs-serve

docs-serve:
	uv run --group docs mkdocs serve

docs-build:
	uv run --group docs mkdocs build

# ─────────────────────────────────────────────────────────────────────────────
# Docker
# ─────────────────────────────────────────────────────────────────────────────

example-ccloud-grafana-up:
	cd examples/ccloud-grafana && docker compose up -d

example-ccloud-grafana-down:
	cd examples/ccloud-grafana && docker compose down

example-ccloud-full-up:
	cd examples/ccloud-full && docker compose up -d

example-ccloud-full-down:
	cd examples/ccloud-full && docker compose down

example-self-managed-up:
	cd examples/self-managed-full && docker compose up -d

example-self-managed-down:
	cd examples/self-managed-full && docker compose down

# Legacy aliases — point at ccloud-full as the default example
docker-up: example-ccloud-full-up
docker-down: example-ccloud-full-down
docker-dev: example-ccloud-full-up

docker-build:
	cd examples/ccloud-full && docker compose build --no-cache

docker-logs:
	cd examples/ccloud-full && docker compose logs -f

docker-push:
	docker buildx build --platform $(PLATFORMS) -t $(REGISTRY)/chitragupt:latest --push .
	docker buildx build --platform $(PLATFORMS) -t $(REGISTRY)/chitragupt-ui:latest --push frontend/

# ─────────────────────────────────────────────────────────────────────────────
# Cleanup
# ─────────────────────────────────────────────────────────────────────────────

clean:
	rm -rf .pytest_cache .mypy_cache .ruff_cache .coverage htmlcov site
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true

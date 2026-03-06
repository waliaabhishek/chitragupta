.PHONY: help setup install sync test lint format typecheck check clean \
        docs docs-serve docs-build

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
	@echo "  Cleanup:"
	@echo "    clean        - Remove build artifacts and caches"

# ─────────────────────────────────────────────────────────────────────────────
# Setup
# ─────────────────────────────────────────────────────────────────────────────

setup:
	uv sync --all-groups

install: setup

sync:
	uv sync --all-groups

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

# ─────────────────────────────────────────────────────────────────────────────
# Documentation
# ─────────────────────────────────────────────────────────────────────────────

docs: docs-serve

docs-serve:
	uv run --group docs mkdocs serve

docs-build:
	uv run --group docs mkdocs build

# ─────────────────────────────────────────────────────────────────────────────
# Cleanup
# ─────────────────────────────────────────────────────────────────────────────

clean:
	rm -rf .pytest_cache .mypy_cache .ruff_cache .coverage htmlcov site
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true

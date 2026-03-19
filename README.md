# Chitragupt

[![CI](https://github.com/waliaabhishek/chitragupt/actions/workflows/ci.yml/badge.svg)](https://github.com/waliaabhishek/chitragupt/actions/workflows/ci.yml)
[![codecov](https://img.shields.io/codecov/c/github/waliaabhishek/chitragupt)](https://codecov.io/gh/waliaabhishek/chitragupt)
[![Python 3.14+](https://img.shields.io/badge/python-3.14%2B-blue)](https://www.python.org/downloads/)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![mypy](https://img.shields.io/badge/type--checked-mypy-blue)](https://mypy-lang.org/)
[![uv](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/uv/main/assets/badge/v0.json)](https://github.com/astral-sh/uv)

> In Hindu tradition, [Chitragupta](https://en.wikipedia.org/wiki/Chitragupta) is the deity who maintains a complete record of every being's actions; the divine accountant himself. Fitting name for a system that tracks exactly who used what and how much it cost.

Multi-ecosystem infrastructure cost chargeback engine. Allocates costs to teams and service accounts across Confluent Cloud, self-managed Kafka, and any Prometheus-instrumented system.
The goal is to support multiple ecosystems and custom cost allocation strategies. 
This was originally built for Confluent Cloud but has been extended to support other ecosystems. 

## Features

- Pulls billing data from vendor APIs or YAML cost models
- Discovers resources and identities via Prometheus or admin APIs
- Allocates costs using configurable strategies (even split, usage ratio)
- REST API for querying chargeback data and triggering pipeline runs
- CSV emitter built-in; custom emitters via the `Emitter` protocol

## Supported Ecosystems

| Ecosystem | Plugin | Billing Source |
|-----------|--------|----------------|
| Confluent Cloud | `confluent_cloud` | CCloud Billing API |
| Self-managed Kafka | `self_managed_kafka` | YAML cost model + Prometheus |
| Generic metrics | `generic_metrics_only` | YAML cost model + Prometheus |

## Quick Start

```bash
# Install
pip install uv
git clone https://github.com/waliaabhishek/chitragupt.git
cd chitragupt
uv sync

# Copy and configure
cp deployables/config/examples/ccloud-minimal.yaml config.yaml
# Edit config.yaml — set your org ID or use env vars

# Set credentials
export CCLOUD_API_KEY=your-key
export CCLOUD_API_SECRET=your-secret

# Run once
uv run python src/main.py --config-file config.yaml --run-once
```

The [Quickstart guide](docs/getting-started/quickstart.md) covers everything end-to-end: service account creation, permissions, API key setup, configuration, and first run. For a containerized setup with Grafana dashboards, see the [Docker Quickstart](deployables/QUICKSTART.md).

## Architecture

```
AppSettings → WorkflowRunner → ChargebackOrchestrator
                                  ├── EcosystemPlugin
                                  │     ├── ServiceHandler×N → CostAllocator
                                  │     ├── CostInput
                                  │     └── MetricsSource
                                  ├── StorageBackend
                                  └── Emitter×N
```

Each tenant maps to one ecosystem plugin. The orchestrator runs a per-tenant, per-date pipeline: gather resources → resolve identities → fetch costs → allocate → store → emit.

## Documentation

Full documentation is in [`docs/`](docs/):

- [Getting Started](docs/getting-started/index.md) — prerequisites, quickstart, first run
- [Architecture](docs/architecture/index.md) — plugin system, data flow, identity resolution
- [API Reference](docs/api-reference.md) — all REST endpoints, parameters, and response schemas
- [Configuration Reference](docs/configuration/index.md) — all settings and ecosystem options
- [Operations](docs/operations/index.md) — deployment, monitoring, troubleshooting

## Development

```bash
# Install with dev dependencies
uv sync --group dev

# Run tests
uv run pytest

# Lint and type check
uv run ruff check src tests
uv run mypy src
```

## Requirements

- Python 3.14+
- [uv](https://docs.astral.sh/uv/) package manager


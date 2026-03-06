# Chitragupt

Multi-ecosystem infrastructure cost chargeback engine. Allocates costs to teams and service accounts across Confluent Cloud, self-managed Kafka, and any Prometheus-instrumented system.

## Features

- Pulls billing data from vendor APIs or YAML cost models
- Discovers resources and identities via Prometheus or admin APIs
- Allocates costs using configurable strategies (even split, usage ratio)
- REST API for querying chargeback data and triggering pipeline runs
- Pluggable emitters (CSV, custom sinks)

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
export CCLOUD_ORG_ID=org-xxxxx
export CCLOUD_API_KEY=your-key
export CCLOUD_API_SECRET=your-secret

# Run once
uv run python src/main.py --config-file config.yaml --run-once
```

See [Quickstart guide](docs/getting-started/quickstart.md) for step-by-step walkthrough.

## Documentation

Full documentation is in [`docs/`](docs/):

- [Getting Started](docs/getting-started/index.md)
- [Configuration Reference](docs/configuration/index.md)
- [Architecture](docs/architecture/index.md)
- [Operations](docs/operations/index.md)

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


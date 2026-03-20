# Chitragupt — Deployment Assets

Self-contained deployment examples have moved to `examples/`. Pick the topology that fits your setup:

| Directory | Topology | Use when |
|-----------|----------|----------|
| [`examples/ccloud-grafana/`](../examples/ccloud-grafana/) | CCloud pipeline + Grafana | Lightweight dashboards, no API needed |
| [`examples/ccloud-full/`](../examples/ccloud-full/) | CCloud pipeline + API + Grafana + UI | Full stack with REST API and frontend |
| [`examples/self-managed-full/`](../examples/self-managed-full/) | Self-managed Kafka + API + Grafana + UI | On-prem/self-hosted Kafka clusters |

Each example directory is self-contained: it includes a `docker-compose.yml`, a `config.yaml`, a `.env.example`, and a `README.md` with setup instructions.

## Quick start

```bash
# Choose an example, e.g.:
cd examples/ccloud-full
cp .env.example .env
vim .env
docker compose up -d
```

## What remains here

- `assets/prometheus_for_chargeback/collector.sh` — helper script for configuring Prometheus to scrape the JMX metrics required by the self-managed Kafka plugin

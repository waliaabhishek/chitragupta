# Quickstart

Deployment examples have moved to `examples/`. Each is a self-contained directory with everything needed to run the stack.

## Choose a topology

| Example | Services | Best for |
|---------|----------|----------|
| [`examples/ccloud-grafana/`](../examples/ccloud-grafana/README.md) | Pipeline (worker) + Grafana | Lightweight — just dashboards, no API server |
| [`examples/ccloud-full/`](../examples/ccloud-full/README.md) | Pipeline + API + Grafana + UI | Full CCloud stack with REST API and frontend |
| [`examples/self-managed-full/`](../examples/self-managed-full/README.md) | Pipeline + API + Grafana + UI | Self-managed Kafka with Prometheus metrics |

## Steps (same for all examples)

```bash
# 1. Go to the example directory
cd examples/ccloud-full   # or ccloud-grafana, self-managed-full

# 2. Copy and fill in credentials
cp .env.example .env
vim .env

# 3. Start the stack
docker compose up -d
```

See the `README.md` in each example directory for service URLs, configuration options, and troubleshooting.

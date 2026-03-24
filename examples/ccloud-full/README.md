# ccloud-full example

Full stack for Confluent Cloud: chargeback engine (pipeline + REST API), Grafana dashboards, and the interactive frontend UI.

**Use this when:** You want both the dashboards and the REST API for custom integrations or the UI for interactive exploration.

## What this runs

| Service | Port | Description |
|---------|------|-------------|
| chitragupta | 8080 | REST API + pipeline worker |
| grafana | 3000 | Pre-provisioned cost dashboards |
| chitragupta-ui | 8081 | Interactive frontend UI |

## Prerequisites

- Docker Engine 24+ and Docker Compose v2+
- A Confluent Cloud API key with **MetricsViewer** and **BillingAdmin** roles

## Quick start

```bash
# 1. Copy and edit credentials
cp .env.example .env
vim .env   # set CCLOUD_API_KEY and CCLOUD_API_SECRET

# 2. Start the stack
docker compose up -d

# 3. Access services
open http://localhost:8080/health   # API health check
open http://localhost:3000          # Grafana (admin / password)
open http://localhost:8081          # Frontend UI
```

Grafana and the UI wait for the API healthcheck to pass before starting (~10-30s). Dashboards populate after the first pipeline run.

## Configuration

Edit `config.yaml` to tune pipeline behavior:

| Setting | Default | Description |
|---------|---------|-------------|
| `features.refresh_interval` | `3600` | Seconds between pipeline runs |
| `tenants.*.lookback_days` | `90` | Historical data to fetch on first run |
| `tenants.*.cutoff_days` | `5` | Skip most-recent N days (data still settling) |
| `api.cors_origins` | `["http://localhost:8081"]` | Allowed frontend origins |

### Optional: usage-based allocation

By default, costs are allocated evenly. Uncomment the `metrics:` block in `config.yaml` and set `PROMETHEUS_URL` in `.env` to allocate by actual bytes-in/bytes-out per principal.

### Optional: Flink cost gathering

Uncomment the `flink:` block in `config.yaml` and set the Flink credentials in `.env` to include Confluent Flink compute pool costs.

### Multi-tenant setup

Add additional entries under `tenants:` in `config.yaml`. Each tenant can have its own `lookback_days`, `cutoff_days`, and `connection_string`.

## Troubleshooting

**Grafana or UI won't start**
- Both wait for the chitragupta healthcheck — if chitragupta fails to start, they won't come up
- Check: `docker compose logs chitragupta`

**Dashboards show "No data"**
- The pipeline must complete at least one run first
- Verify the Grafana time range covers dates with billing data (use "Last 90 days")
- Test the datasource: Connections > Data Sources > Chargeback SQLite > Test

**Engine exits immediately**
- Check credentials: `docker compose logs chitragupta`
- Common cause: invalid API key or missing BillingAdmin role

**Port conflicts**
- Change host ports in `docker-compose.yml` under `ports:` for each service
- If you change the UI port, also update `api.cors_origins` in `config.yaml`

**UI shows CORS errors**
- Ensure `api.cors_origins` in `config.yaml` matches the URL you're using to access the UI

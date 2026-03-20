# ccloud-grafana example

Runs the Chitragupt chargeback engine in **worker mode** alongside Grafana. No REST API, no frontend — just the pipeline writing to SQLite and Grafana reading from it.

**Use this when:** You want cost dashboards with minimal infrastructure. No API consumers needed.

## What this runs

| Service | Port | Description |
|---------|------|-------------|
| chitragupt | — | Pipeline worker (no HTTP server) |
| grafana | 3000 | Pre-provisioned cost dashboards |

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

# 3. Open Grafana
open http://localhost:3000   # login: admin / password
```

Dashboards populate after the first pipeline run (~1 minute for minimal history, longer for 90-day backfill).

## Configuration

Edit `config.yaml` to tune pipeline behavior:

| Setting | Default | Description |
|---------|---------|-------------|
| `features.refresh_interval` | `3600` | Seconds between pipeline runs |
| `tenants.*.lookback_days` | `90` | Historical data to fetch on first run |
| `tenants.*.cutoff_days` | `5` | Skip most-recent N days (data still settling) |

### Optional: usage-based allocation

By default, costs are allocated evenly. Uncomment the `metrics:` block in `config.yaml` and set `PROMETHEUS_URL` in `.env` to allocate by actual bytes-in/bytes-out per principal.

### Optional: Flink cost gathering

Uncomment the `flink:` block in `config.yaml` and set the Flink credentials in `.env` to include Confluent Flink compute pool costs.

## Troubleshooting

**Dashboards show "No data"**
- The pipeline must complete at least one run first. Check: `docker compose logs chitragupt`
- Verify the Grafana time range covers dates with billing data (use "Last 90 days")
- Test the datasource: Connections > Data Sources > Chargeback SQLite > Test

**Engine exits immediately**
- Check credentials: `docker compose logs chitragupt`
- Common cause: invalid API key or missing BillingAdmin role

**Grafana won't start**
- The `frser-sqlite-datasource` plugin is installed at container startup — needs internet access
- Check: `docker compose logs grafana`

**Port 3000 already in use**
- Change the host port in `docker-compose.yml`: `"3001:3000"`

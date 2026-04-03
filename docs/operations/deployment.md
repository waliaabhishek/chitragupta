# Deployment

## Run modes

| `--mode` | Use case |
|---|---|
| `worker` | Background pipeline only. API served separately. |
| `api` | REST API only. No pipeline. Query existing data. |
| `both` | Pipeline + API in one process. Simplest deployment. |

## Systemd unit (worker)

```ini
[Unit]
Description=Chitragupta Worker
After=network.target

[Service]
Type=simple
WorkingDirectory=/opt/chitragupta
ExecStart=uv run python src/main.py \
    --config-file /etc/chargeback/config.yaml \
    --mode worker
Restart=on-failure
RestartSec=30

[Install]
WantedBy=multi-user.target
```

## Docker

The project includes a multi-stage `Dockerfile` in the repo root (builder stage with `uv` for dependency resolution, slim runtime stage with non-root user).

### Docker Compose (recommended)

The `examples/` directory contains self-contained Docker Compose setups. Each includes a `docker-compose.yml`, `config.yaml`, `.env.example`, and `README.md`:

| Example | Services | Best for |
|---------|----------|----------|
| `examples/ccloud-grafana/` | Pipeline (worker) + Grafana | Lightweight dashboards, no API |
| `examples/ccloud-full/` | Pipeline + API + Grafana + UI | Full CCloud stack |
| `examples/self-managed-full/` | Pipeline + API + Grafana + UI | Self-managed Kafka |

Topic attribution (CCloud only) requires a configured Prometheus metrics source. See the [CCloud configuration reference](../configuration/ccloud-reference.md#topic-attribution).

```bash
cd examples/ccloud-full        # or ccloud-grafana, self-managed-full
cp .env.example .env
vim .env                        # fill in credentials
docker compose up -d
```

See the [Quickstart](../getting-started/quickstart.md) for a step-by-step walkthrough.

### Standalone Docker (no Compose)

Build and run directly if you don't need Grafana or the UI:

```bash
docker build -t chitragupta .
docker run -v ./config:/app/config:ro -v ./data:/app/data chitragupta \
  --config-file /app/config/config.yaml --mode both
```

## Environment variables

Pass secrets via environment — never hardcode in YAML:

```bash
docker run -e CCLOUD_API_KEY=... -e CCLOUD_API_SECRET=... chitragupta
```

## API server

The REST API is a FastAPI application served by uvicorn.

```yaml
api:
  host: 0.0.0.0
  port: 8080
  enable_cors: true
  cors_origins:
    - "https://your-dashboard.example.com"
```

Health endpoint: `GET /health` — returns `{"status": "ok", "version": "..."}`

## Storage

### SQLite (default)

SQLite works well for single-instance deployments with moderate volume:

```yaml
storage:
  backend: sqlmodel
  connection_string: "sqlite:////app/data/tenant-name.db"
```

### PostgreSQL

Use PostgreSQL for multi-instance deployments or high-volume/concurrent-access scenarios where SQLite's single-writer lock becomes a bottleneck.

**Driver:** Requires `psycopg2`. Install via `uv add psycopg2-binary` (or `psycopg2` if you prefer building from source with `libpq-dev`).

**Connection string format:**

```yaml
storage:
  backend: sqlmodel
  connection_string: "postgresql+psycopg2://user:pass@host:5432/dbname"
```

Pass credentials via environment variables to avoid hardcoding secrets:

```yaml
  connection_string: "postgresql+psycopg2://${PG_USER}:${PG_PASS}@${PG_HOST}:5432/dbname"
```

**One database per tenant.** Each tenant's `connection_string` must point to a separate PostgreSQL database. Tables are created automatically on first run — no manual migration needed.

```yaml
tenants:
  prod-org:
    storage:
      connection_string: "postgresql+psycopg2://${PG_USER}:${PG_PASS}@pg:5432/chargeback_prod_org"
  staging-org:
    storage:
      connection_string: "postgresql+psycopg2://${PG_USER}:${PG_PASS}@pg:5432/chargeback_staging_org"
```

### When to choose PostgreSQL over SQLite

| Consideration | SQLite | PostgreSQL |
|---|---|---|
| Concurrent writers | Single writer (locks on write) | Multiple concurrent writers |
| Multi-instance | Not safe across processes/containers | Designed for it |
| Operational overhead | Zero — file on disk | Requires running PostgreSQL server |
| Data volume | Good up to ~10 GB per tenant | Scales further |
| Backups | Copy the `.db` file | `pg_dump` / replication |

**Rule of thumb:** Start with SQLite. Switch to PostgreSQL when you need multiple application instances or observe lock contention under write-heavy workloads.

## Prometheus collector script

When using the [Prometheus emitter](../configuration/index.md#prometheus-emitter), chargeback data is held in memory and served at `/metrics`. To persist it into a Prometheus TSDB (for long-term retention and historical queries), use the bundled collector script:

```
examples/shared/scripts/collector.sh
```

The script scrapes `/metrics` in OpenMetrics format and writes TSDB blocks via `promtool tsdb create-blocks-from openmetrics`. It requires `promtool` on `PATH` (ships with the Prometheus distribution).

**Required environment variables:**

| Variable | Description |
|---|---|
| `CHITRAGUPTA_METRICS_URL` | URL of the `/metrics` endpoint, e.g. `http://localhost:9090/metrics` |
| `CHITRAGUPTA_HEALTH_URL` | URL of the `/health` endpoint, e.g. `http://localhost:8080/health` |
| `TSDB_OUT_DIR` | Output directory for TSDB blocks (default: `/data/prometheus`) |

**Optional environment variables:**

| Variable | Default | Description |
|---|---|---|
| `CHITRAGUPTA_METRICS_FORMAT` | `openmetrics` | Must be `openmetrics`. Setting `text` causes immediate exit — Prometheus text format uses millisecond timestamps that `promtool` misinterprets. |

**Polling modes:**

| Mode | Interval | Trigger |
|---|---|---|
| Catch-up (fast) | 1 second | Most recent metric timestamp is older than 5 days |
| Current (slow) | 600 seconds | Most recent metric timestamp is recent |

The script waits for the health endpoint to return HTTP 200 before scraping. Run it as a sidecar alongside the Chitragupta worker.

## Upgrading

See [Upgrading](upgrading.md) for backup procedures, upgrade steps, database migration behavior, and rollback instructions.

# Deployment

## Run modes

| `--mode` | General behavior | FOCUS Mapping Preview |
|---|---|---|
| `worker` | Background pipeline only. API served separately. | Runs scheduled monthly publication and revision retention. Exposes no Preview HTTP routes. |
| `api` | REST API only. No periodic pipeline. | Serves ad-hoc requests, history, revisions, and downloads. Does not publish scheduled revisions. |
| `both` | Pipeline + API in one process. Simplest deployment. | Serves the same Preview HTTP contract as `api` and runs scheduled publication and retention. |

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

## Operational logging configuration

Use `INFO` as the production baseline. It records bounded lifecycle transitions,
completion summaries, degraded fallbacks, and terminal failures without
per-record output.

```yaml
logging:
  level: INFO
  format: "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
```

During an investigation, enable `DEBUG` only for the boundary involved. These
settings expose request/provider lifecycle events while leaving the rest of the
process at `INFO`:

```yaml
logging:
  level: INFO
  per_module_levels:
    core.api.app: DEBUG
    core.engine.orchestrator: DEBUG
    core.metrics.prometheus: DEBUG
    plugins.confluent_cloud.connections: DEBUG
```

Return temporary module overrides to their normal level after the incident.
Global `DEBUG` increases event volume across tenants and providers.

The default message contains canonical `key=value` context such as request,
pipeline run, calculation, revision, repair, date/month, stage, operation,
outcome, and retry position when those fields apply. Preserve the full message
in the log collector so these correlations remain searchable. If the collector
expects JSON, wrap the already-redacted message rather than attempting to
reconstruct fields from application objects:

```yaml
logging:
  level: INFO
  format: '{"time":"%(asctime)s","logger":"%(name)s","level":"%(levelname)s","message":"%(message)s"}'
```

Configure the collector and alerting policy to:

- retain `INFO` events long enough to connect API requests and scheduled runs
  to later warnings or errors;
- alert on terminal `ERROR` events and repeated `WARNING` events where
  `retryable=false` or attempts reach `max_attempts`;
- group related events by `request_id`, `pipeline_run_id`, `calculation_id`,
  `revision_id`, or `repair_id`, rather than message text alone;
- preserve percent-escaped values and `traceback_frames` as emitted;
- restrict log access because tenant and resource identifiers are operational
  data even though secrets and raw payloads are excluded.

Do not add credentials, tokens, authentication headers, connection strings,
provider payloads, database queries or parameters, response bodies, raw URLs
with query strings, or raw exception messages in sidecar, proxy, or wrapper
logs. Chitragupta emits sanitized error type, root type/code, and bounded frame
names instead.

For response procedures and representative event sequences, see
[Operational logging](troubleshooting.md#operational-logging).

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

### FOCUS Mapping Preview boundary

FOCUS Mapping Preview is opt-in per tenant. When no tenant has a
`focus_preview` block, API and worker startup do not create or access
`preview.artifact_root`, initialize Preview recovery, gather Preview
organization authority, or require Preview evidence/lineage storage. The
ordinary pipeline, emitters, and generic export keep their existing deployment
requirements.

Chitragupta does not provide Preview-specific users, roles, API keys, or
tokens. Protect the complete
`/api/v1/tenants/{tenant_name}/focus-preview` prefix with the deployment's
existing authenticated reverse proxy or API gateway, and configure credentials
at that external boundary.

Do not expose `preview.artifact_root` as a static directory or public volume.
Preview downloads must go through the API so artifact identity and checksums
are verified before delivery. When `api` and `worker` run separately, configure
both with the same tenant database and the same durable artifact root. Use a
database deployment suitable for the expected process and write concurrency.

### Preview capacity and storage sizing

Preview generation limits are process-local. In `both` mode, requested packages
and scheduled tenant-month publication share one bounded scheduler. In split
mode, the API process enforces its requested-generation limits and the worker
process enforces its scheduled-generation limits independently. Every
additional API or worker replica has its own configured running and queued
capacity; replicas do not combine their counters into a distributed limit.

`preview.max_generation_spool_bytes` defaults to 2 GiB for each running
generation. Reserve at least:

```text
preview.max_workers × preview.max_generation_spool_bytes
```

of temporary-disk headroom per process, in addition to retained immutable
packages, the tenant database, filesystem metadata, and operating-system safety
margin. The defaults therefore permit up to 4 GiB of concurrent generation
temporary-disk use in one process. Split API and worker processes or multiple
replicas can each consume that amount. Place `preview.artifact_root` on storage
sized for the sum of all processes that share it.

Generation and artifact delivery are bounded so package size does not require
equivalent process memory. Operators must still budget ordinary application and
API memory in addition to the temporary-disk calculation above.

Use the queue defaults for burst absorption. Both queue limits may instead be
set to zero to require immediate starts; requested submissions then return
retryable HTTP 429 when no slot is available, and scheduled tenant-months defer
to a later periodic cycle.

Mixed deployments are supported. Only enabled tenants use the shared Preview
artifact root and evidence schema; disabled tenants do not acquire Preview
organization/source/lineage data. An enabled tenant's Preview storage,
bootstrap, or evidence failure is reported through Preview diagnostics while
generic chargeback collection and calculation continue.

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

Standard URL percent-encoding is supported in `storage.connection_string` for credentials and query values. Use normal single `%` escape sequences in application configuration; do not double `%` signs.

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

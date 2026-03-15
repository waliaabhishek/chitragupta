# Docker Quickstart

Run the full Chitragupt stack locally with Docker Compose. No Python install required.

## Prerequisites

- Docker Engine 24+ and Docker Compose v2+
- API credentials for your ecosystem (see Step 2)

## Step 1: Choose a config template

Chitragupt ships example configs for different ecosystems. Pick one and copy it into place:

```bash
cd deployables

# Confluent Cloud — pulls billing data from CCloud Billing API
cp config/examples/ccloud-minimal.yaml config/config.yaml

# OR Self-managed Kafka — uses YAML cost model + Prometheus metrics
cp config/examples/self-managed-minimal.yaml config/config.yaml
```

**When to use which:**

| Template | You have... |
|----------|-------------|
| `ccloud-minimal.yaml` | A Confluent Cloud organization with an API key that has billing access |
| `self-managed-minimal.yaml` | Self-hosted Kafka with a Prometheus instance scraping JMX exporter metrics |

## Step 2: Create the `.env` file

Each config template has a matching `.env.example` with the required credentials:

```bash
# Confluent Cloud
cp config/examples/ccloud-minimal.env.example config/.env

# OR Self-managed Kafka
cp config/examples/self-managed-minimal.env.example config/.env
```

Edit `config/.env` with your actual values:

### Confluent Cloud variables

| Variable | Description |
|----------|-------------|
| `CCLOUD_ORG_ID` | Your Confluent Cloud organization ID (e.g., `org-abc123`). Found in Cloud Console under Settings. |
| `CCLOUD_API_KEY` | Cloud API key with Organization Admin or billing read access. |
| `CCLOUD_API_SECRET` | Secret for the above API key. |

### Self-managed Kafka variables

| Variable | Description |
|----------|-------------|
| `PROMETHEUS_URL` | Prometheus base URL reachable from the Docker network (e.g., `http://prometheus:9090`). |

### Optional variables (both ecosystems)

| Variable | Default | Description |
|----------|---------|-------------|
| `LOG_LEVEL` | `INFO` | Log verbosity: `CRITICAL`, `ERROR`, `WARNING`, `INFO`, `DEBUG`. |
| `GF_ADMIN_PASSWORD` | `password` | Grafana admin password. |

## Step 3: Create the data directory

```bash
mkdir -p data
```

This directory holds the SQLite database(s). It's mounted read-write into the container.

## Step 4: Start the stack

```bash
docker compose up --build -d
```

This builds the backend image and starts two services:
- **chitragupt** — API server + pipeline worker
- **grafana** — pre-provisioned dashboards reading from the SQLite database

First build takes 1-2 minutes (cached after that). The backend healthcheck runs before Grafana starts, so Grafana may take ~30 seconds to become available.

## Step 5: Verify it's running

### API (port 8080)

```bash
curl http://localhost:8080/health
# Expected: {"status": "healthy"}
```

### Grafana (port 3000)

Open http://localhost:3000 in your browser.

- Login: `admin` / `password` (or whatever you set `GF_ADMIN_PASSWORD` to)
- Go to Dashboards — you should see **Chargeback Overview** and **Chargeback Details**
- Dashboards will show data after the first pipeline run completes (usually within a few minutes of startup)

## Step 6 (optional): Enable the UI

The frontend UI is disabled by default. To include it:

```bash
docker compose --profile ui up --build -d
```

This adds:
- **chitragupt-ui** — a React frontend served by nginx, proxying API calls to the backend

Open http://localhost:8081 to access the UI. It connects to the backend API automatically.

## Service summary

| Service | URL | What it does |
|---------|-----|--------------|
| chitragupt | http://localhost:8080 | REST API + periodic pipeline worker |
| grafana | http://localhost:3000 | Pre-built cost dashboards |
| chitragupt-ui | http://localhost:8081 | Interactive UI (optional, `--profile ui`) |

## Tear down

Stop all services and remove containers:

```bash
docker compose --profile ui down
```

Your data is preserved in `./data/`. To also remove the database and start fresh:

```bash
docker compose --profile ui down
rm -rf data/
```

## Troubleshooting

**`docker compose up` fails to build**
- Ensure Docker is running: `docker info`
- Check you're in the `deployables/` directory (the compose file uses relative paths)

**Backend exits immediately**
- Check logs: `docker compose logs chitragupt`
- Common cause: missing or invalid credentials in `config/.env`
- Test with a single run: edit `docker-compose.yml` command to add `--run-once`

**Grafana shows "No data"**
- The pipeline must complete at least one run to populate the database
- Check backend logs for errors: `docker compose logs chitragupt`
- Verify the Grafana time range covers dates with billing data
- Test the datasource: Connections > Data Sources > Chargeback SQLite > Test

**Permission denied on data directory**
- The container runs as uid 1000. Fix with: `chown 1000:1000 data`

**Port conflicts**
- Another service is using 8080, 3000, or 8081
- Change the host port in `docker-compose.yml` under `ports:` (e.g., `"9090:8080"`)

## What's next

- Edit `config/config.yaml` to tune `lookback_days`, `refresh_interval`, or add more tenants
- See [Run Modes](README.md#run-modes) for API-only or worker-only deployments
- See the [Configuration Reference](../docs/configuration/index.md) for all available settings

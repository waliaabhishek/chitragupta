# Quickstart — Confluent Cloud

Get Chitragupt running against your Confluent Cloud organization in under 10 minutes.

## Prerequisites

- **Docker Engine 24+** and **Docker Compose v2+**
- A Confluent Cloud organization
- **Confluent CLI** installed ([install guide](https://docs.confluent.io/confluent-cli/current/install.html))

## 1. Create a Service Account

The chargeback engine needs a dedicated service account to access Confluent Cloud APIs.

```bash
confluent login
confluent iam sa create chargeback_handler \
  --description "Chargeback handler user"
```

Save the Service Account ID (`sa-*******`) from the output — you'll need it in the next steps.

## 2. Assign permissions

The service account needs three role bindings:

```bash
# Replace <sa_id> with your Service Account ID from above
confluent iam rbac role-binding create --principal User:<sa_id> --role MetricsViewer
confluent iam rbac role-binding create --principal User:<sa_id> --role OrganizationAdmin
confluent iam rbac role-binding create --principal User:<sa_id> --role BillingAdmin
```

| Role | Why it's needed |
|------|----------------|
| **MetricsViewer** | Base permission for metrics access |
| **OrganizationAdmin** | Objects API, Metrics API, and viewing connector/ksqlDB principals |
| **BillingAdmin** | Pull billing data from the Billing API |

!!! note
    OrganizationAdmin is broader than ideal. Confluent Cloud RBAC doesn't currently offer more granular scoping for the APIs this tool requires.

## 3. Create an API key

```bash
confluent api-key create --resource cloud --service-account <sa_id>
```

Save the **API Key** and **API Secret** from the output.

## 4. Start with Docker Compose

Clone the repo and go to the full-stack CCloud example:

```bash
git clone https://github.com/waliaabhishek/chitragupt.git
cd chitragupt/examples/ccloud-full
```

Create your `.env` file from the template and fill in the credentials from steps 1–3:

```bash
cp .env.example .env
```

Edit `.env`:

```bash
CCLOUD_API_KEY=your-api-key-from-step-3
CCLOUD_API_SECRET=your-api-secret-from-step-3
CCLOUD_TENANT_ID=my-org        # any string — internal partition key, NOT the CCloud org ID
```

Start the stack:

```bash
docker compose up -d
```

This builds and starts three services:

| Service | URL | What it does |
|---------|-----|--------------|
| **chitragupt** | `http://localhost:8080` | REST API + periodic pipeline worker |
| **grafana** | `http://localhost:3000` | Pre-built chargeback dashboards |
| **chitragupt-ui** | `http://localhost:8081` | Interactive frontend UI |

First build takes 1–2 minutes (cached after that). The backend healthcheck runs before Grafana and the UI start, so they may take ~30 seconds to become available.

## 5. Verify it's running

### API

```bash
curl http://localhost:8080/health
# {"status": "healthy"}
```

### Grafana

Open [http://localhost:3000](http://localhost:3000).

- Login: `admin` / `password`
- Go to Dashboards — you should see **Chargeback Overview** and **Chargeback Details**
- Dashboards show data after the first pipeline run completes (usually within a few minutes)

### Frontend UI

Open [http://localhost:8081](http://localhost:8081) for interactive cost exploration.

## 6. Check logs

```bash
docker compose logs chitragupt -f
```

You should see the pipeline running — discovering resources, fetching billing data, allocating costs.

## Other example setups

| Example | What it runs | When to use |
|---------|-------------|-------------|
| [`ccloud-grafana/`](../../examples/ccloud-grafana/) | Pipeline (worker mode) + Grafana only | Lightweight — just dashboards, no API or UI |
| [`ccloud-full/`](../../examples/ccloud-full/) | Pipeline + API + Grafana + UI | Full stack (what you just set up) |
| [`self-managed-full/`](../../examples/self-managed-full/) | Pipeline + API + Grafana + UI | Self-managed/on-prem Kafka with Prometheus |

Each example directory is self-contained with its own `docker-compose.yml`, `config.yaml`, `.env.example`, and `README.md`.

## Tear down

```bash
docker compose down
```

Data is preserved in a Docker volume. To also remove the database and start fresh:

```bash
docker compose down -v
```

## Troubleshooting

**Backend exits immediately**

- Check logs: `docker compose logs chitragupt`
- Common cause: missing or invalid credentials in `.env`
- Test with a single run: change `--mode both` to `--run-once` in `docker-compose.yml`

**Grafana shows "No data"**

- The pipeline must complete at least one run to populate the database
- Verify the time range in Grafana covers dates with billing data
- Test the datasource: Connections > Data Sources > Chargeback SQLite > Test

**Port conflicts**

- Another service is using 8080, 3000, or 8081
- Change the host port in `docker-compose.yml` under `ports:` (e.g., `"9090:8080"`)

## Alternative: local development without Docker

If you prefer running directly with Python instead of Docker:

```bash
# Install
pip install uv
git clone https://github.com/waliaabhishek/chitragupt.git
cd chitragupt
uv sync

# Configure
cp examples/ccloud-full/config.yaml config.yaml
# Edit config.yaml — update connection_string to a local path:
#   connection_string: "sqlite:///data/chargeback.db"

# Set credentials
export CCLOUD_API_KEY=your-key
export CCLOUD_API_SECRET=your-secret
export CCLOUD_TENANT_ID=my-org

# Run once
uv run python src/main.py --config-file config.yaml --run-once

# Or run continuously with API:
uv run python src/main.py --config-file config.yaml --mode both
```

## Next steps

- [Full configuration reference](../configuration/ccloud-reference.md) — all available settings
- [How costs work](../architecture/cost-model.md) — allocation strategies and billing models
- [Deployment](../operations/deployment.md) — production deployment options (systemd, Docker, PostgreSQL)
- [API Reference](../api-reference.md) — all REST endpoints

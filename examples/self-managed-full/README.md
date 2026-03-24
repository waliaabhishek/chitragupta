# self-managed-full example

Full stack for self-managed Kafka: chargeback engine (pipeline + REST API), Grafana dashboards, and the interactive frontend UI. The engine reads Prometheus metrics (via Kafka JMX exporter) to derive usage and compute costs.

**Use this when:** You run your own Kafka clusters (on-prem, cloud VMs, or Kubernetes) and want cost attribution without a managed billing API.

## What this runs

| Service | Port | Description |
|---------|------|-------------|
| chitragupta | 8080 | REST API + pipeline worker |
| grafana | 3000 | Pre-provisioned cost dashboards |
| chitragupta-ui | 8081 | Interactive frontend UI |

## Prerequisites

- Docker Engine 24+ and Docker Compose v2+
- A Prometheus instance reachable from the Docker network, scraping Kafka brokers via [prometheus-jmx-exporter](https://github.com/prometheus/jmx_exporter)
- See `examples/shared/scripts/collector.sh` for a helper that configures the required JMX metrics

## Quick start

```bash
# 1. Copy and edit credentials
cp .env.example .env
vim .env   # set PROMETHEUS_URL

# 2. Edit config.yaml to set your cost model rates and broker count
vim config.yaml

# 3. Start the stack
docker compose up -d

# 4. Access services
open http://localhost:8080/health   # API health check
open http://localhost:3000          # Grafana (admin / password)
open http://localhost:8081          # Frontend UI
```

Grafana and the UI wait for the API healthcheck to pass before starting (~10-30s). Dashboards populate after the first pipeline run.

## Configuration

### Cost model rates

Edit the `cost_model` section in `config.yaml` to match your infrastructure costs:

| Setting | Default | Description |
|---------|---------|-------------|
| `compute_hourly_rate` | `0.50` | USD per broker per hour |
| `storage_per_gib_hourly` | `0.000125` | USD per GiB of storage per hour |
| `network_ingress_per_gib` | `0.01` | USD per GiB of data ingested |
| `network_egress_per_gib` | `0.09` | USD per GiB of data consumed |

### Identity → team mapping

Add your principal-to-team mapping under `identity_source.principal_to_team` in `config.yaml`:

```yaml
identity_source:
  source: prometheus
  default_team: UNASSIGNED
  principal_to_team:
    User:alice: platform-team
    User:bob: data-team
```

### Using Admin API for resource discovery

Change `resource_source.source` to `admin_api` in `config.yaml` and set `KAFKA_BOOTSTRAP_SERVERS` in `.env` to query Kafka directly instead of deriving resources from Prometheus labels.

### Multi-cluster setup

Add additional entries under `tenants:` in `config.yaml`. Each cluster entry gets its own `cluster_id`, `broker_count`, `cost_model`, and `connection_string`.

### Pipeline frequency

| Setting | Default | Description |
|---------|---------|-------------|
| `features.refresh_interval` | `900` | Seconds between pipeline runs (15 min) |
| `tenants.*.lookback_days` | `30` | Historical data range on first run |
| `tenants.*.cutoff_days` | `3` | Skip most-recent N days |

## Prometheus requirements

The engine queries these JMX metrics from Prometheus:

- `kafka_server_brokertopicmetrics_bytesin_total` — bytes produced per topic
- `kafka_server_brokertopicmetrics_bytesout_total` — bytes consumed per topic
- `kafka_log_log_size` — storage per topic/partition

Ensure your JMX exporter configuration exposes these metrics with `topic` and `partition` labels.

## Troubleshooting

**Grafana or UI won't start**
- Both wait for the chitragupta healthcheck — check: `docker compose logs chitragupta`

**Dashboards show "No data"**
- The pipeline must complete at least one run first
- Check Prometheus is reachable: `docker compose exec chitragupta python -c "import urllib.request; print(urllib.request.urlopen('$PROMETHEUS_URL/-/healthy').status)"`
- Verify the Grafana time range covers dates with data

**No principals discovered**
- Confirm Prometheus has JMX metrics with principal labels: query `kafka_server_brokertopicmetrics_bytesin_total` in Prometheus UI
- Try `identity_source.source: static` with an explicit `principal_to_team` mapping

**Cost model looks wrong**
- Adjust `compute_hourly_rate`, `storage_per_gib_hourly`, etc. in `config.yaml`
- Restart the stack: `docker compose restart chitragupta`

**Port conflicts**
- Change host ports in `docker-compose.yml` under `ports:`
- If you change the UI port, also update `api.cors_origins` in `config.yaml`

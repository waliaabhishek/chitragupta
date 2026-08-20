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

# 2. Edit config.yaml to set cost model rates, broker count, and Prometheus target scope
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

### Prometheus target scope

`cluster_id` is the logical resource ID shown in Chitragupta. It is separate from
the required `metrics_identifier`, the operator-defined value used to select this
cluster's Prometheus targets. `metrics_identifier_label` names that target label
and defaults to `kafka_cluster_id`.

Every scraped broker target must carry the exact configured label/value, even when
brokers or JMX exporters use separate endpoints. For the supplied configuration,
the target health selector is `up{kafka_cluster_id="kafka-dc1"}`.

### Quota evidence and static policy allocation

BrokerTopicMetrics supplies cluster cost and broker/topic discovery data; it does
not supply principal identity for allocation. Quota telemetry records principal
evidence for each billing window. Static identities are the visible allocation
policy and produce chargeback rows with `measured_usage=false`.

```yaml
identity_source:
  source: both
  static_identities:
    - identity_id: platform-team
      identity_type: team
    - identity_id: data-team
      identity_type: team
```

If quota byte-rate telemetry is absent, its status is `not_observed`; allocation
does not infer an owner from topic traffic. Structurally valid non-finite throttle
samples mean no positive throttling was observed, not measured usage.

### Using Admin API for resource discovery

Change `resource_source.source` to `admin_api` in `config.yaml` and set `KAFKA_BOOTSTRAP_SERVERS` in `.env` to query Kafka directly instead of deriving resources from Prometheus labels.

### Multi-cluster setup

Add additional entries under `tenants:` in `config.yaml`. Each cluster entry gets
its own `cluster_id`, `metrics_identifier`, `broker_count`, `cost_model`, and
`connection_string`.

### Pipeline frequency

| Setting | Default | Description |
|---------|---------|-------------|
| `features.refresh_interval` | `900` | Seconds between pipeline runs (15 min) |
| `tenants.*.lookback_days` | `30` | Historical data range on first run |
| `tenants.*.cutoff_days` | `3` | Skip most-recent N days |

## Prometheus requirements

The engine requires these metrics from Prometheus:

- `up` — target health for each billing window
- `kafka_server_brokertopicmetrics_bytesin_total` — cluster ingress cost and broker/topic discovery
- `kafka_server_brokertopicmetrics_bytesout_total` — cluster egress cost
- `kafka_log_log_size` — cluster storage cost

When `identity_source.source` is `prometheus` or `both`, it also evaluates
`kafka_server_quota_byte_rate` and `kafka_server_quota_throttle_time_ms` as quota
evidence.

Ensure every target carries the configured `metrics_identifier_label` and
`metrics_identifier` value. The `up` selector must be healthy and complete for the
full billing window. Topic and quota metrics may appear only when active, so they
cannot prove target scope.

## Scope blocking and recovery

If the target selector is missing, mismatched, incomplete, or unhealthy, the
pipeline fails closed: it does not commit billing or progress for that window. A
breaker remains open and blocks both until a later run sends one targeted probe
before starting normal work. Once healthy, it recovers available windows in
chronological order within the configured lookback range; older unavailable windows
remain visible as a retention gap rather than receiving fabricated billing.

## Troubleshooting

**Grafana or UI won't start**
- Both wait for the chitragupta healthcheck — check: `docker compose logs chitragupta`

**Dashboards show "No data"**
- The pipeline must complete at least one run first
- Check Prometheus is reachable: `docker compose exec chitragupta python -c "import urllib.request; print(urllib.request.urlopen('$PROMETHEUS_URL/-/healthy').status)"`
- Verify the Grafana time range covers dates with data

**No quota evidence**
- Confirm quota telemetry is available with the configured target label/value
- Use `identity_source.source: static` or `both` and list the policy identities
  that should receive the static split

**Scope blocked**
- Query the exact `up{<metrics_identifier_label>="<metrics_identifier>"}` selector
- Confirm the label/value is injected on every broker target, including separate endpoints

**Cost model looks wrong**
- Adjust `compute_hourly_rate`, `storage_per_gib_hourly`, etc. in `config.yaml`
- Restart the stack: `docker compose restart chitragupta`

**Port conflicts**
- Change host ports in `docker-compose.yml` under `ports:`
- If you change the UI port, also update `api.cors_origins` in `config.yaml`

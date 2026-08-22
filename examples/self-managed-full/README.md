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

### Quota-backed principal attribution

BrokerTopicMetrics supplies cluster cost and broker/topic discovery data; it does
not supply principal ownership. Principal attribution is optional and disabled by
default. To allocate the network pools from Kafka quota telemetry, configure:

```yaml
principal_attribution:
  enabled: true
  scrape_interval_seconds: 30
  max_gap_seconds: 90
  compute_policy: unattributed
  storage_policy: unattributed
identity_source:
  source: both
  principal_to_team:
    "User:service-account": data-platform
  default_team: UNASSIGNED
  static_identities:
    - identity_id: platform-team
      identity_type: team
    - identity_id: data-team
      identity_type: team
```

The feature requires `identity_source.source: prometheus` or `both`. Produce quota
rates allocate ingress, Fetch quota rates allocate egress, and the two directions
remain independent. The Kafka-authenticated user label is represented as
`User:<user>` with case preserved; client-only weight remains `UNALLOCATED`.
After target scope is valid, missing, invalid, or incomplete quota evidence fails
closed and leaves the affected network pool unallocated. A target-scope failure
stops calculation before quota queries or allocation and creates no business rows.
`static_even_v1` is available only for the fixed compute/storage policies and is
not measured usage. Its empty `static_identities` list is valid at startup and
preserves the entire fixed pool as `UNALLOCATED`.

When the `principal_attribution` block is omitted or disabled, `prometheus` and
`both` retain their byte-rate/throttle readiness probes (`observed`, `not_observed`,
`invalid`, or `transient_failure`) without measured allocation. `static` remains
policy-only and makes no quota calls.

See the [configuration reference](../../docs/configuration/self-managed-reference.md#quota-backed-principal-attribution)
for the exporter labels, state model, exact reconciliation, retention, and
recalculation contract.

### Optional topic attribution

The default is cluster-level chargeback only. To add a topic-level analytical view,
enable the independent overlay:

```yaml
topic_attribution:
  enabled: true
  compute_policy: shared_even_v1
  # Omit this key, or use [], to exclude no topics.
  # exclude_topic_patterns:
  #   - "internal-*"
```

Ingress, egress, and storage use topic-labelled evidence. `shared_even_v1`
allocates the full fixed compute pool evenly across complete active topics; it is a
shared policy, not measured usage. Omit the block to leave the overlay disabled,
or leave `compute_policy: disabled` to keep compute as `__UNATTRIBUTED__`.

Exclusion patterns are reporting-only. Allocation and stored amounts keep the real
topic names. After the service's normal configuration reload or restart, changed
patterns reclassify both current and historical results without rerunning the
pipeline. Analytics collapse matching topics into `Excluded topics`; the table and
CSV retain each topic name and show its derived exclusion status.

### Using Admin API for resource discovery

Change `resource_source.source` to `admin_api` in `config.yaml` and set `KAFKA_BOOTSTRAP_SERVERS` in `.env` to query Kafka directly instead of deriving resources from Prometheus labels.

For an authenticated cluster, set the matching `security_protocol`,
`sasl_mechanism`, `sasl_username`, and `sasl_password` in `resource_source`. These
credentials are for Kafka resource discovery only; they are separate from the
Kafka-authenticated users represented by quota telemetry and from Prometheus
credentials.

### Multi-cluster setup

Add additional entries under `tenants:` in `config.yaml`. Each cluster entry gets
its own `cluster_id`, `metrics_identifier`, `broker_count`, `cost_model`, and
`connection_string`.

### Pipeline frequency

| Setting | Default | Description |
|---------|---------|-------------|
| `features.refresh_interval` | `900` | Seconds between pipeline runs (15 min) |
| `tenants.*.lookback_days` | `30` | Historical data range on first run |
| `tenants.*.cutoff_days` | `2` | Skip the most-recent 1–2 days so a full UTC day closes |

## Prometheus requirements

The engine requires these metrics from Prometheus on every broker target:

- `up` — target health for each billing window
- `kafka_server_brokertopicmetrics_alltopics_bytesin_total` — broker-wide client ingress pool
- `kafka_server_brokertopicmetrics_alltopics_bytesout_total` — broker-wide client egress pool
- `kafka_log_log_size` — storage pool and per-topic partition storage evidence

The `alltopics` counters build the network cost pools and exclude replication
traffic. The Prometheus resource source also uses
`kafka_server_brokertopicmetrics_bytesin_total` with a `topic` label for discovery.
When topic attribution is enabled, export both that counter and
`kafka_server_brokertopicmetrics_bytesout_total`. They divide the client pools
among topics; they do not construct the pools themselves.

`kafka_log_log_size` must carry topic and partition evidence. A reported zero is a
valid storage value. A missing storage family leaves the day retryable unless a
successful Admin API inventory proves the cluster has no topics or partitions.

Every one of those series must carry the configured `metrics_identifier_label`.
The `alltopics` counters need a `broker` label and no `topic` label; topic counters
need `broker` and `topic`; for topic attribution `kafka_log_log_size` needs
`broker`, `topic`, and `partition`.

When `principal_attribution.enabled` is `true`, also export
`kafka_server_quota_byte_rate` as a gauge. It must carry `broker`, the configured
cluster label, `quota_type` (`Produce` or `Fetch`), `quota_scope` (`user`,
`user-client`, or `client-id`), `user`, and `client_id`. Export the corresponding
Kafka JMX quota MBeans for user, client-ID, and user/client scopes. Configure Kafka
quotas for the authenticated users and client IDs you intend to observe;
Chitragupta reads those metrics and does not configure broker quotas.

Ensure every target carries the configured `metrics_identifier_label` and
`metrics_identifier` value. Chitragupta applies that selector to `up`, resource
discovery, all broker-wide pools, and topic evidence. The `up` selector must be
healthy and complete for the full billing window. Topic and quota metrics may appear
only when active, so they cannot prove target scope.

All pool and topic queries use the same `[00:00 UTC, 00:00 UTC)` day. Start with a
1–2 day cutoff so the pipeline does not gather an incomplete current day.

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
- Confirm `principal_attribution.enabled` is set and the identity source is
  `prometheus` or `both`
- Confirm `kafka_server_quota_byte_rate` has the configured target label, broker,
  quota type/scope, user, and client-ID labels for both Produce and Fetch
- Check that the declared scrape interval and maximum gap match Prometheus, then
  explicitly recalculate retained affected dates after correcting the source

**Scope blocked**
- Query the exact `up{<metrics_identifier_label>="<metrics_identifier>"}` selector
- Confirm the label/value is injected on every broker target, including separate endpoints

**Cost model looks wrong**
- Adjust `compute_hourly_rate`, `storage_per_gib_hourly`, etc. in `config.yaml`
- Restart the stack: `docker compose restart chitragupta`

**Port conflicts**
- Change host ports in `docker-compose.yml` under `ports:`
- If you change the UI port, also update `api.cors_origins` in `config.yaml`

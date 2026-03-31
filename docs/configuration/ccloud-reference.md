# Confluent Cloud Configuration Reference

!!! tip "New to Confluent Cloud configuration?"
    Read the [Configuration Guide](guide.md#configuring-confluent-cloud) first
    for a walkthrough of the decisions you'll make, then come back here for the
    full field reference.

## ecosystem key

```yaml
ecosystem: confluent_cloud
```

## Full example

```yaml
tenants:
  my-ccloud-org:
    ecosystem: confluent_cloud
    tenant_id: my-ccloud-org       # internal partition key (not the CCloud org ID)
    lookback_days: 200
    cutoff_days: 5
    retention_days: 250
    storage:
      connection_string: "sqlite:///data/ccloud.db"
    plugin_settings:
      ccloud_api:
        key: ${CCLOUD_API_KEY}
        secret: ${CCLOUD_API_SECRET}
      billing_api:
        days_per_query: 15
      metrics:
        type: prometheus
        url: https://api.telemetry.confluent.cloud
        auth_type: basic
        username: ${METRICS_API_KEY}
        password: ${METRICS_API_SECRET}
      flink:
        - region_id: us-east-1
          key: ${FLINK_API_KEY}
          secret: ${FLINK_API_SECRET}
      emitters:
        - type: csv
          aggregation: daily
          params:
            output_dir: ./output
      chargeback_granularity: daily
      topic_attribution:
        enabled: true
        exclude_topic_patterns:
          - "__consumer_offsets"
          - "_schemas"
          - "_confluent-*"
        missing_metrics_behavior: even_split
        retention_days: 90
```

## TenantConfig fields

| Field | Type | Default | Description |
|---|---|---|---|
| `ecosystem` | string | required | Must be `confluent_cloud` |
| `tenant_id` | string | required | Unique partition key for DB records. Can be any string (e.g. `prod`, `acme-corp`). This is **not** your Confluent Cloud Organization ID — it is an internal label used to isolate data across tenants in the database. |
| `lookback_days` | int | 200 | Days of billing history to fetch (max 364). Must be > `cutoff_days`. |
| `cutoff_days` | int | 5 | Skip dates within this many days of today (billing lag, max 30) |
| `retention_days` | int | 250 | Delete data older than this (max 730) |
| `allocation_retry_limit` | int | 3 | Max identity resolution retries before fallback (max 10) |
| `gather_failure_threshold` | int | 5 | Consecutive gather failures before tenant suspension |
| `tenant_execution_timeout_seconds` | int | 3600 | Per-tenant run timeout (0 = no timeout) |
| `metrics_prefetch_workers` | int | 4 | Parallel metrics query threads (1–20) |
| `zero_gather_deletion_threshold` | int | -1 | Mark resources deleted after N zero-gather cycles (-1 = disabled) |

## plugin_settings fields (CCloud)

| Field | Type | Default | Description |
|---|---|---|---|
| `ccloud_api.key` | string | required | CCloud API key |
| `ccloud_api.secret` | secret | required | CCloud API secret |
| `billing_api.days_per_query` | int | 15 | Days per billing API request (max 30) |
| `metrics.url` | string | optional | Prometheus/Telemetry API URL |
| `metrics.auth_type` | enum | `none` | `basic`, `bearer`, or `none` |
| `metrics.username` | string | optional | For `auth_type: basic` |
| `metrics.password` | secret | optional | For `auth_type: basic` |
| `metrics.bearer_token` | secret | optional | For `auth_type: bearer` |
| `flink` | list | optional | Per-region Flink API credentials |
| `chargeback_granularity` | enum | `daily` | `hourly`, `daily`, or `monthly` |
| `metrics_step_seconds` | int | 3600 | Prometheus query step (lower = finer granularity) |
| `min_refresh_gap_seconds` | int | 1800 | Minimum time between pipeline runs for this tenant |

## Handled product types

Each product type from the CCloud billing API is routed to a handler that knows
how to resolve identities and allocate costs for that service. The allocation
strategy reflects the nature of the cost — usage-driven costs are split by
measured consumption, shared costs are split evenly.

| Handler | Product types | Allocation strategy | Why |
|---|---|---|---|
| `kafka` | `KAFKA_NUM_CKU`, `KAFKA_NUM_CKUS` | Hybrid: 70% usage ratio (bytes), 30% even split | CKUs are the main Kafka compute cost. Part of the cost is driven by traffic volume (usage), part is base infrastructure overhead (shared). The 70/30 default is configurable via `allocator_params`. |
| `kafka` | `KAFKA_NETWORK_READ`, `KAFKA_NETWORK_WRITE` | Usage ratio (bytes in/out per principal) | Network transfer is directly attributable to the principal that produced or consumed the data. Requires Telemetry API metrics. |
| `kafka` | `KAFKA_BASE`, `KAFKA_PARTITION`, `KAFKA_STORAGE` | Even split | Base fees, partition counts, and storage are cluster-level costs with no per-principal usage metric. |
| `schema_registry` | `SCHEMA_REGISTRY`, `GOVERNANCE_BASE`, `NUM_RULES` | Even split | Schema Registry is a shared service — all principals benefit equally from schema validation. |
| `connector` | `CONNECT_CAPACITY`, `CONNECT_NUM_TASKS`, `CONNECT_THROUGHPUT`, `CUSTOM_CONNECT_*` | Even split per connector | Connectors are typically owned by teams. Costs are split among identities active on the connector's resource. |
| `ksqldb` | `KSQL_NUM_CSU`, `KSQL_NUM_CSUS` | Even split | ksqlDB compute units are application-level — split across active identities. |
| `flink` | `FLINK_NUM_CFU`, `FLINK_NUM_CFUS` | Usage ratio by statement owner CFU consumption | Flink CFU costs are directly traceable to the user who created the SQL statement. Uses a 4-tier chain: statement owner → active identities → period identities → resource. |
| `org_wide` | `AUDIT_LOG_READ`, `SUPPORT` | Even split across tenant, then to UNALLOCATED | Org-wide costs have no resource or principal — they apply to the whole organization. |
| `default` | `TABLEFLOW_*` | Shared (to resource) | New product types without a dedicated handler fall back to resource-level allocation. |
| `default` | `CLUSTER_LINKING_*` | Usage (to resource) | Cluster linking costs are attributed to the linked resource. |

Unknown product types are allocated to UNALLOCATED. Check the `allocation_detail`
field on chargeback rows to understand which fallback tier was used.

See [How Costs Work](../architecture/cost-model.md) for the complete allocation
model including the fallback chain and composite CKU allocation.

## Allocator params

Override default allocation ratios for Kafka CKU costs:

```yaml
allocator_params:
  kafka_cku_usage_ratio: 0.70   # fraction allocated by bytes (default 0.70)
  kafka_cku_shared_ratio: 0.30  # fraction allocated evenly (default 0.30)
```

`kafka_cku_usage_ratio` + `kafka_cku_shared_ratio` must sum to 1.0 (tolerance: 0.0001). Startup fails if they don't.

!!! note "How to think about the ratio"
    The usage portion is allocated proportionally to `bytes_in + bytes_out` per
    principal. The shared portion is split evenly across all active identities.

    - **High usage ratio (0.90/0.10):** Heavy producers/consumers pay proportionally
      more. Good when your cluster is right-sized and traffic volume drives cost.
    - **Balanced (0.70/0.30):** Default. Acknowledges that the cluster has a base
      cost regardless of traffic.
    - **High shared ratio (0.50/0.50):** Spreads cost more evenly. Good when the
      cluster is over-provisioned and most cost is fixed overhead.

    If metrics are unavailable for a billing window, the usage portion falls back
    to even-split anyway — so at 1.0/0.0, you effectively get even-split when
    Telemetry API data is missing. See [How Costs Work](../architecture/cost-model.md#composite-allocation-cku-model)
    for a worked example.

## Topic attribution

Topic attribution is an optional overlay stage that breaks Kafka cluster costs
down to individual topics using Prometheus metrics. It runs after chargeback
calculation and writes results to a separate star-schema table.

**Prerequisite:** Topic-level CCloud metrics (`received_bytes`, `sent_bytes`,
`retained_bytes` per topic) must be scraped into the Prometheus instance
configured under `plugin_settings.metrics`.

```yaml
topic_attribution:
  enabled: true                           # off by default
  exclude_topic_patterns:
    - "__consumer_offsets"                # default exclusions
    - "_schemas"
    - "_confluent-*"
  missing_metrics_behavior: even_split   # even_split | skip
  retention_days: 90                     # 1–365, independent of tenant retention_days
  cost_mapping_overrides:                # override per product type
    KAFKA_PARTITION: even_split
    KAFKA_BASE: disabled
  metric_name_overrides:                 # override Prometheus metric names
    topic_bytes_in: custom_received_bytes
  emitters: []                           # same format as top-level emitters
```

!!! warning "Requires `metrics` to be configured"
    `topic_attribution.enabled: true` requires a `metrics` section in `plugin_settings`.
    Config validation rejects the combination of `enabled: true` and no `metrics` source — startup
    fails with a `ValidationError` rather than silently producing even-split attribution from zero data.

### `topic_attribution` fields

| Field | Type | Default | Description |
|---|---|---|---|
| `enabled` | bool | `false` | Enable the topic overlay stage |
| `exclude_topic_patterns` | list[string] | `["__consumer_offsets", "_schemas", "_confluent-*"]` | Glob patterns for topics to skip. Matched with `fnmatch`. |
| `missing_metrics_behavior` | enum | `even_split` | What to do when metrics are all-zero or unavailable: `even_split` distributes evenly; `skip` omits the cluster from output. |
| `retention_days` | int | `90` | Days to keep topic attribution rows (1–365). Independent of the tenant-level `retention_days`. |
| `cost_mapping_overrides` | dict[string, string] | `{}` | Override the attribution method per CCloud product type. Valid methods: `bytes_ratio`, `retained_bytes_ratio`, `even_split`, `disabled`. |
| `metric_name_overrides` | dict[string, string] | `{}` | Override Prometheus metric names. Valid keys: `topic_bytes_in`, `topic_bytes_out`, `topic_retained_bytes`. |
| `emitters` | list | `[]` | Emitter specs for topic attribution output. Same format as top-level `emitters`. |

### Default cost mappings

| Product type | Attribution method | Metric used |
|---|---|---|
| `KAFKA_NETWORK_WRITE` | bytes_ratio | `topic_bytes_in` |
| `KAFKA_NETWORK_READ` | bytes_ratio | `topic_bytes_out` |
| `KAFKA_STORAGE` | retained_bytes_ratio | `topic_retained_bytes` |
| `KAFKA_PARTITION` | even_split | — |
| `KAFKA_BASE` | even_split | — |
| `KAFKA_NUM_CKU` / `KAFKA_NUM_CKUS` | bytes_ratio | `topic_bytes_in` + `topic_bytes_out` |

For bytes_ratio product types, if Prometheus returns all-zero values, the
`missing_metrics_behavior` setting determines the fallback.

## Emitters

```yaml
emitters:
  - type: csv
    aggregation: daily       # rows aggregated to daily before writing
    params:
      output_dir: /data/csv
      filename_template: "{tenant_id}_{date}.csv"
```

`aggregation` options: `null` (as-is), `hourly`, `daily`, `monthly`.

# Confluent Cloud Configuration Reference

## ecosystem key

```yaml
ecosystem: confluent_cloud
```

## Full example

```yaml
tenants:
  my-ccloud-org:
    ecosystem: confluent_cloud
    tenant_id: t-abc123
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
```

## TenantConfig fields

| Field | Type | Default | Description |
|---|---|---|---|
| `ecosystem` | string | required | Must be `confluent_cloud` |
| `tenant_id` | string | required | CCloud org ID (e.g. `t-abc123`) |
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

| Handler | Product types | Allocation strategy |
|---|---|---|
| `kafka` | `KAFKA_NUM_CKU`, `KAFKA_NUM_CKUS` | Hybrid: 70% usage ratio (bytes), 30% even split |
| `kafka` | `KAFKA_NETWORK_READ`, `KAFKA_NETWORK_WRITE` | Usage ratio (bytes in/out) |
| `kafka` | `KAFKA_BASE`, `KAFKA_PARTITION`, `KAFKA_STORAGE` | Even split |
| `schema_registry` | `SCHEMA_REGISTRY`, `GOVERNANCE_BASE`, `NUM_RULES` | Even split |
| `connector` | `CONNECT_CAPACITY`, `CONNECT_NUM_TASKS`, `CONNECT_THROUGHPUT`, `CUSTOM_CONNECT_PLUGIN`, `CUSTOM_CONNECT_NUM_TASKS`, `CUSTOM_CONNECT_THROUGHPUT` | Even split per connector |
| `ksqldb` | `KSQL_NUM_CSU`, `KSQL_NUM_CSUS` | Even split |
| `flink` | `FLINK_NUM_CFU`, `FLINK_NUM_CFUS` | Usage ratio by statement owner CFU consumption (fallback: even split) |
| `org_wide` | `AUDIT_LOG_READ`, `SUPPORT` | Even split |
| `default` | `TABLEFLOW_*` | Shared (to resource) |
| `default` | `CLUSTER_LINKING_*` | Usage (to resource) |

Unknown product types are allocated to UNALLOCATED.

## Allocator params

Override default allocation ratios for Kafka CKU costs:

```yaml
allocator_params:
  kafka_cku_usage_ratio: 0.70   # fraction allocated by bytes (default 0.70)
  kafka_cku_shared_ratio: 0.30  # fraction allocated evenly (default 0.30)
```

`kafka_cku_usage_ratio` + `kafka_cku_shared_ratio` must sum to 1.0 (tolerance: 0.0001). Startup fails if they don't.

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

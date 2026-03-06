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
| `lookback_days` | int | 200 | Days of billing history to fetch (max 364) |
| `cutoff_days` | int | 5 | Skip dates within this many days of today (billing lag) |
| `retention_days` | int | 250 | Delete data older than this (max 730) |
| `max_dates_per_run` | int | 15 | Limit dates processed per pipeline run |
| `allocation_retry_limit` | int | 3 | Max identity resolution retries before fallback |
| `gather_failure_threshold` | int | 5 | Consecutive gather failures before tenant suspension |
| `tenant_execution_timeout_seconds` | int | 3600 | Per-tenant run timeout (0 = no timeout) |

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
| `kafka` | `KAFKA_*` | Usage ratio (bytes in/out) |
| `schema_registry` | `SCHEMA_REGISTRY_*` | Even split |
| `connector` | `CONNECT_*` | Even split per connector |
| `ksqldb` | `KSQL_*` | Even split |
| `flink` | `FLINK_*` | Even split per statement |
| `org_wide` | `SUPPORT_*`, `GOVERNANCE_*` | Even split |
| `default` | All others | Even split |

## Allocator params

Override default ratio splits for org-wide costs:

```yaml
allocator_params:
  support_ratio: 0.8   # fraction of support cost charged to teams
```

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

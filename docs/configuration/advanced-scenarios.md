# Advanced Scenarios

## Multiple tenants

Each tenant must have a unique `storage.connection_string`. Shared databases are rejected at startup.

```yaml
tenants:
  team-alpha:
    storage:
      connection_string: "sqlite:///data/alpha.db"
    ...
  team-beta:
    storage:
      connection_string: "sqlite:///data/beta.db"
    ...
```

## Custom granularity durations

Override chargeback period length (minimum 1 hour):

```yaml
plugin_settings:
  granularity_durations:
    "4h": 4
    "weekly": 168
```

## Allocator overrides

Replace a built-in allocator for a specific product type:

```yaml
plugin_settings:
  allocator_overrides:
    KAFKA_NETWORK_READ: mymodule.custom_allocator
```

The value is a dotted import path to a callable matching the `CostAllocator` protocol.

## Identity resolution overrides

Replace identity resolution for a specific product type:

```yaml
plugin_settings:
  identity_resolution_overrides:
    KAFKA_NETWORK_READ: mymodule.custom_resolver
```

## Validation constraints

These cross-field constraints are enforced at startup:

| Constraint | Error if violated |
|---|---|
| `lookback_days` must be > `cutoff_days` | `lookback_days must be > cutoff_days` |
| CKU ratios must sum to 1.0 (CCloud) | `kafka_cku_usage_ratio + kafka_cku_shared_ratio must equal 1.0` |
| Each tenant must have a unique `storage.connection_string` | Names the conflicting tenants |
| `discovery_query` required when `identity_source.source` is `prometheus` or `both` | `discovery_query required` |

## Tuning parameters

These TenantConfig fields have sensible defaults but can be overridden. See the
[Configuration Guide — Pipeline Tuning](guide.md#pipeline-tuning) for guidance on
when and how to adjust these.

| Field | Type | Default | Description | When to change |
|---|---|---|---|---|
| `metrics_prefetch_workers` | int | 4 | Parallel threads for metrics queries (1–20) | Increase for 100+ resources with fast Prometheus. Decrease if Prometheus is rate-limited. |
| `zero_gather_deletion_threshold` | int | -1 | Mark resources deleted after N consecutive zero-gather cycles (-1 = disabled) | Enable (e.g., 3) if you want automatic cleanup of decommissioned resources. Leave disabled if gather cycles are unreliable. |
| `gather_failure_threshold` | int | 5 | Consecutive gather failures before tenant is permanently suspended | Increase if transient API errors are common (rate limiting, network blips). Decrease to fail fast on bad credentials. |
| `tenant_execution_timeout_seconds` | int | 3600 | Per-tenant pipeline run timeout in seconds (0 = no timeout) | Increase for large backfills (200+ lookback days on first run). Decrease for alerting on stuck pipelines. |
| `allocation_retry_limit` | int | 3 | Max identity resolution retries before allocating to UNALLOCATED (1–10) | Increase if identity data arrives with a delay (eventual consistency). Rarely needs changing. |

## API server configuration

```yaml
api:
  host: 0.0.0.0
  port: 8080                    # 1–65535
  request_timeout_seconds: 30   # 1–300, returns HTTP 504 on timeout
  enable_cors: true
  cors_origins:
    - "https://your-dashboard.example.com"
```

## Metrics authentication

### Basic auth

```yaml
metrics:
  url: https://prometheus.example.com
  auth_type: basic
  username: ${PROM_USER}
  password: ${PROM_PASS}
```

### Bearer token

```yaml
metrics:
  url: https://prometheus.example.com
  auth_type: bearer
  bearer_token: ${PROM_TOKEN}
```

**Validation rules:** `basic` requires both `username` and `password`. `bearer` requires `bearer_token`. `none` rejects any credentials — don't mix auth_type with unrelated credential fields.

## Custom plugins path

```yaml
plugins_path: /opt/custom_plugins
```

Path is resolved relative to CWD if relative. Must contain directories with
`__init__.py` exporting a class implementing `EcosystemPlugin`.

## Emitter aggregation

Emitters receive rows aggregated to the requested period:

```yaml
emitters:
  - type: csv
    aggregation: monthly     # one file per month
    params:
      output_dir: ./monthly-output
  - type: csv
    aggregation: daily       # one file per day (same data, different granularity)
    params:
      output_dir: ./daily-output
```

Emitters may not request finer granularity than `chargeback_granularity` produces.

## Per-module log levels

```yaml
logging:
  level: INFO     # CRITICAL | ERROR | WARNING | INFO | DEBUG
  per_module_levels:
    core.metrics.prometheus: DEBUG
    plugins.confluent_cloud: DEBUG
```

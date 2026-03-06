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
  level: INFO
  per_module_levels:
    core.metrics.prometheus: DEBUG
    plugins.confluent_cloud: DEBUG
```

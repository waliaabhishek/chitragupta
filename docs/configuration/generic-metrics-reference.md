# Generic Metrics Configuration Reference

The `generic_metrics_only` plugin allocates costs for any Prometheus-instrumented system
using a YAML-defined cost model. No vendor API calls are made — you define everything:
cost types, quantities (from Prometheus queries or fixed counts), and how costs are
split across identities.

!!! tip "New to generic metrics configuration?"
    Read the [Configuration Guide](guide.md#configuring-generic-metrics) first
    for a walkthrough of defining cost types and allocation strategies, then come
    back here for the full field reference.

## ecosystem key

```yaml
ecosystem: generic_metrics_only
```

## Full example (self-managed PostgreSQL)

```yaml
tenants:
  my-postgres:
    ecosystem: generic_metrics_only
    tenant_id: postgres-prod
    storage:
      connection_string: "sqlite:///data/postgres-prod.db"
    plugin_settings:
      ecosystem_name: self_managed_postgres
      cluster_id: pg-prod-cluster
      display_name: "Production PostgreSQL"
      metrics:
        type: prometheus
        url: http://prometheus:9090
        auth_type: none
      identity_source:
        source: prometheus
        label: principal
        discovery_query: >
          sum by (principal) (pg_stat_activity_count)
        principal_to_team:
          "user:alice": team-data
        default_team: UNASSIGNED
      cost_types:
        - name: PG_COMPUTE
          product_category: postgres
          rate: "0.50"
          cost_quantity:
            type: fixed
            count: 3
          allocation_strategy: even_split
        - name: PG_STORAGE
          product_category: postgres
          rate: "0.0001"
          cost_quantity:
            type: storage_gib
            query: "avg(pg_database_size_bytes)"
          allocation_strategy: even_split
        - name: PG_NETWORK
          product_category: postgres
          rate: "0.05"
          cost_quantity:
            type: network_gib
            query: "sum(increase(pg_stat_bgwriter_buffers_alloc_total[1h]))"
          allocation_strategy: usage_ratio
          allocation_query: >
            sum by (principal) (increase(pg_stat_activity_count[1h]))
          allocation_label: principal
      emitters:
        - type: csv
          aggregation: daily
          params:
            output_dir: ./output
```

## plugin_settings fields (generic metrics)

| Field | Type | Default | Description |
|---|---|---|---|
| `ecosystem_name` | string | required | Used as ecosystem label in billing output |
| `cluster_id` | string | required | Resource identifier |
| `display_name` | string | optional | Human-readable name for the cluster |
| `metrics.url` | string | required | Prometheus URL |
| `identity_source.source` | enum | `prometheus` | `prometheus`, `static`, or `both` |
| `identity_source.label` | string | `principal` | Prometheus label used as identity ID |
| `identity_source.discovery_query` | string | required if source=`prometheus` or `both` | PromQL to discover identities |
| `identity_source.principal_to_team` | dict | `{}` | Map label value → team name |
| `identity_source.static_identities` | list | `[]` | Hard-coded identities |
| `allocator_overrides` | dict | `{}` | Replace allocator for specific product types (see [Advanced Scenarios](advanced-scenarios.md)) |
| `identity_resolution_overrides` | dict | `{}` | Replace identity resolver for specific product types |

## cost_types fields

| Field | Type | Required | Description |
|---|---|---|---|
| `name` | string | yes | `product_type` in billing lines |
| `product_category` | string | yes | Grouping label |
| `rate` | Decimal | yes | Unit price |
| `cost_quantity.type` | enum | yes | `fixed`, `storage_gib`, or `network_gib` |
| `cost_quantity.count` | int | if fixed | Instance count (e.g. broker count) |
| `cost_quantity.query` | string | if storage_gib/network_gib | Cluster-wide PromQL (no `{}` placeholder) |
| `allocation_strategy` | enum | yes | `even_split` or `usage_ratio` |
| `allocation_query` | string | if usage_ratio | Per-identity PromQL |
| `allocation_label` | string | if usage_ratio | Label to extract from `allocation_query` rows |

## Cost quantity types

| Type | Query | Formula | Rate unit | When to use |
|---|---|---|---|---|
| `fixed` | none | `count × rate × 24h` | $/instance/hour | Fixed infrastructure: server instances, fixed-size clusters, license seats |
| `storage_gib` | cluster-wide avg PromQL | `avg(query) ÷ 2^30 × rate × 24h` | $/GiB/hour | Gauge metrics: disk usage, memory, database size — anything measured as "how much right now" |
| `network_gib` | cluster-wide sum PromQL | `sum(increase(query)) ÷ 2^30 × rate` | $/GiB | Counter metrics: bytes transferred, I/O throughput — anything measured as "how much total" |

!!! note "Storage vs. network: the math is different because the metrics are different"
    **Storage** queries return a gauge (current value at a point in time). Averaging
    gives the representative size held over the day. The rate is per GiB per hour
    because you're paying for storage *over time*.

    **Network** queries return a counter (cumulative total). Summing `increase()`
    values gives total bytes transferred. The rate is per GiB (flat) because you're
    paying for data *moved*, not data held.

    If you accidentally use `storage_gib` for a counter metric (or vice versa), your
    costs will be wrong — the engine applies the wrong aggregation function.

See [How Costs Work](../architecture/cost-model.md#constructed-cost-math) for
detailed examples of how each quantity type is computed.

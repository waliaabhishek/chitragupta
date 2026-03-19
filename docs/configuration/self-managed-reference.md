# Self-Managed Kafka Configuration Reference

!!! tip "New to self-managed Kafka configuration?"
    Read the [Configuration Guide](guide.md#configuring-self-managed-kafka) first
    for a walkthrough of the decisions you'll make, then come back here for the
    full field reference.

## ecosystem key

```yaml
ecosystem: self_managed_kafka
```

## Full example

```yaml
tenants:
  my-kafka-cluster:
    ecosystem: self_managed_kafka
    tenant_id: kafka-prod
    storage:
      connection_string: "sqlite:///data/kafka-prod.db"
    plugin_settings:
      cluster_id: kafka-prod-cluster
      broker_count: 3
      region: us-east-1
      cost_model:
        compute_hourly_rate: "0.50"
        storage_per_gib_hourly: "0.0001"
        network_ingress_per_gib: "0.01"
        network_egress_per_gib: "0.05"
        region_overrides:
          eu-west-1:
            compute_hourly_rate: "0.60"
      identity_source:
        source: prometheus
        principal_to_team:
          "User:alice": team-data-eng
          "User:bob": team-platform
        default_team: UNASSIGNED
      resource_source:
        source: prometheus
      metrics:
        type: prometheus
        url: http://prometheus:9090
        auth_type: none
      emitters:
        - type: csv
          aggregation: daily
          params:
            output_dir: ./output
```

## plugin_settings fields (self-managed Kafka)

| Field | Type | Default | Description |
|---|---|---|---|
| `cluster_id` | string | required | Logical cluster identifier (used as resource_id) |
| `broker_count` | int | required | Number of brokers (for compute cost) |
| `region` | string | optional | Region for cost override lookup |
| `cost_model.compute_hourly_rate` | Decimal | required | Per broker-hour cost |
| `cost_model.storage_per_gib_hourly` | Decimal | required | Per GiB-hour storage cost |
| `cost_model.network_ingress_per_gib` | Decimal | required | Per GiB ingress cost |
| `cost_model.network_egress_per_gib` | Decimal | required | Per GiB egress cost |
| `cost_model.region_overrides` | dict | `{}` | Override any rate field per region |
| `identity_source.source` | enum | `prometheus` | `prometheus`, `static`, or `both` |
| `identity_source.principal_to_team` | dict | `{}` | Map principal ID → team name |
| `identity_source.default_team` | string | `UNASSIGNED` | Team for unmapped principals |
| `identity_source.static_identities` | list | `[]` | Hard-coded identities (for `static` / `both`) |
| `resource_source.source` | enum | `prometheus` | `prometheus` or `admin_api` |
| `resource_source.bootstrap_servers` | string | optional | Required for `admin_api` source |
| `resource_source.sasl_mechanism` | enum | optional | `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512` |
| `resource_source.sasl_username` | string | optional | SASL username (required when `sasl_mechanism` is set) |
| `resource_source.sasl_password` | secret | optional | SASL password (required when `sasl_mechanism` is set) |
| `resource_source.security_protocol` | enum | `PLAINTEXT` | `PLAINTEXT`, `SSL`, `SASL_PLAINTEXT`, `SASL_SSL` |
| `identity_source.discovery_window_hours` | int | 1 | Hours of Prometheus data to scan for identity discovery (must be > 0) |
| `metrics.url` | string | required | Prometheus URL |
| `metrics.auth_type` | enum | `none` | `basic`, `bearer`, or `none` |
| `allocator_overrides` | dict | `{}` | Replace allocator for specific product types (see [Advanced Scenarios](advanced-scenarios.md)) |
| `identity_resolution_overrides` | dict | `{}` | Replace identity resolver for specific product types |

## Required Prometheus metrics

The cost model derives costs from these JMX exporter metrics:

| Metric | Type | Used for |
|---|---|---|
| `kafka_server_brokertopicmetrics_bytesin_total` | counter | Network ingress cost, identity discovery (principal label), CKU-equivalent usage attribution |
| `kafka_server_brokertopicmetrics_bytesout_total` | counter | Network egress cost, identity discovery |
| `kafka_log_log_size` | gauge | Storage cost (cluster-wide average) |

Network metrics are queried with `sum(increase(...[1h]))` per step (summing hourly
deltas gives total bytes transferred). Storage is averaged across all samples in the
day (since it's a point-in-time gauge, not a cumulative counter).

!!! note "Labels matter"
    For identity discovery via Prometheus, the `principal` label must be present on
    `kafka_server_brokertopicmetrics_bytesin_total`. If your JMX exporter doesn't
    include this label, set `identity_source.source: static` and list identities
    manually.

    The engine runs a combined discovery query at gather time:
    ```promql
    group by (broker, topic, principal) (kafka_server_brokertopicmetrics_bytesin_total{})
    ```
    This single query extracts brokers, topics, and principals in one round-trip.

## Produced product types

| Product type | Cost formula | Allocation strategy | Why this strategy |
|---|---|---|---|
| `SELF_KAFKA_COMPUTE` | `broker_count × 24h × compute_hourly_rate` | Even split | Compute is shared infrastructure — every team benefits equally from broker availability regardless of their traffic volume. |
| `SELF_KAFKA_STORAGE` | `avg_gib × 24h × storage_per_gib_hourly` | Even split | Storage is cluster-wide; individual principal contribution to log size is not directly measurable from JMX metrics. |
| `SELF_KAFKA_NETWORK_INGRESS` | `sum_bytes_in ÷ 2^30 × network_ingress_per_gib` | Usage ratio (bytes in per principal) | Ingress is directly attributable — the `principal` label on `bytesin_total` tells you exactly who produced the data. |
| `SELF_KAFKA_NETWORK_EGRESS` | `sum_bytes_out ÷ 2^30 × network_egress_per_gib` | Usage ratio (bytes out per principal) | Same as ingress — `bytesout_total` by principal measures actual consumption. |

See [How Costs Work](../architecture/cost-model.md) for the complete math with
worked examples.

## Identity discovery via Prometheus

With `identity_source.source: prometheus`, principals are extracted from metric labels
during the discovery phase and again during identity resolution for each billing window:

```promql
# Discovery (gather phase) — find all principals with any traffic
group by (broker, topic, principal) (kafka_server_brokertopicmetrics_bytesin_total{})

# Billing resolution (calculate phase) — per-principal bytes in a specific window
sum by (principal) (increase(kafka_server_brokertopicmetrics_bytesin_total[1h]))
```

The first query runs once per gather cycle and populates the resource and identity
inventory. The second runs per billing window and determines which principals were
active during that specific period (stored as `metrics_derived` identities).

### Fallback behavior

If the engine discovers zero principals from Prometheus (e.g., the metric exists but
has no `principal` label), the allocation chain falls through:

1. **Usage ratio** — skipped (no per-principal data)
2. **Even split across `resource_active`** — uses static identities if configured
3. **Even split across `tenant_period`** — all identities seen during the billing period
4. **Terminal** — allocates to `UNALLOCATED`

Check `allocation_detail` on chargeback rows to see which tier fired. If you see
`NO_METRICS_LOCATED` on network costs, your principal labels are likely missing.

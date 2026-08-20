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
      metrics_identifier: kafka-prod-target
      metrics_identifier_label: kafka_cluster_id
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
        source: static
        static_identities:
          - identity_id: team-data-eng
            identity_type: team
          - identity_id: team-platform
            identity_type: team
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
| `cluster_id` | string | required | Logical Chitragupta resource ID for the cluster; it is not the Prometheus target selector |
| `metrics_identifier` | string | required | Operator-defined value used to select this cluster's Prometheus targets |
| `metrics_identifier_label` | string | `kafka_cluster_id` | Prometheus target-label name carrying `metrics_identifier` |
| `broker_count` | int | required | Number of brokers (for compute cost) |
| `region` | string | optional | Region for cost override lookup |
| `cost_model.compute_hourly_rate` | Decimal | required | Per broker-hour cost |
| `cost_model.storage_per_gib_hourly` | Decimal | required | Per GiB-hour storage cost |
| `cost_model.network_ingress_per_gib` | Decimal | required | Per GiB ingress cost |
| `cost_model.network_egress_per_gib` | Decimal | required | Per GiB egress cost |
| `cost_model.region_overrides` | dict | `{}` | Override any rate field per region |
| `identity_source.source` | enum | `prometheus` | `prometheus`, `static`, or `both`; controls quota-evidence collection and static policy identities |
| `identity_source.principal_to_team` | dict | `{}` | Optional identity metadata; it does not make BrokerTopicMetrics principal evidence |
| `identity_source.default_team` | string | `UNASSIGNED` | Optional identity metadata; it does not change static-policy allocation |
| `identity_source.static_identities` | list | `[]` | Visible policy identities for `static` / `both`; allocations are marked `measured_usage=false` |
| `resource_source.source` | enum | `prometheus` | `prometheus` or `admin_api` |
| `resource_source.bootstrap_servers` | string | optional | Required for `admin_api` source |
| `resource_source.sasl_mechanism` | enum | optional | `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512` |
| `resource_source.sasl_username` | string | optional | SASL username (required when `sasl_mechanism` is set) |
| `resource_source.sasl_password` | secret | optional | SASL password (required when `sasl_mechanism` is set) |
| `resource_source.security_protocol` | enum | `PLAINTEXT` | `PLAINTEXT`, `SSL`, `SASL_PLAINTEXT`, `SASL_SSL` |
| `discovery_window_hours` | int | 1 | Hours of Prometheus data to scan for broker/topic discovery (must be > 0) |
| `metrics.url` | string | required | Prometheus URL |
| `metrics.auth_type` | enum | `none` | `basic`, `bearer`, or `none` |
| `allocator_overrides` | dict | `{}` | Replace allocator for specific product types (see [Advanced Scenarios](advanced-scenarios.md)) |
| `identity_resolution_overrides` | dict | `{}` | Replace identity resolver for specific product types |

## Required Prometheus metrics

The cost model and target-scope check require these Prometheus metrics:

| Metric | Type | Used for |
|---|---|---|
| `kafka_server_brokertopicmetrics_bytesin_total` | counter | Network ingress cost and broker/topic discovery; it is not principal identity evidence |
| `kafka_server_brokertopicmetrics_bytesout_total` | counter | Network egress cost; it is not principal identity evidence |
| `kafka_log_log_size` | gauge | Storage cost (cluster-wide average) |
| `up` | gauge | Required target-scope validation for every billing window |

When `identity_source.source` is `prometheus` or `both`, the plugin also evaluates
these optional quota-evidence series:

| Metric | Type | Used for |
|---|---|---|
| `kafka_server_quota_byte_rate` | gauge | Principal-attribution evidence when quota telemetry is configured and observed |
| `kafka_server_quota_throttle_time_ms` | gauge | Supplemental quota-throttling evidence |

Each quota row must include the `quota_type`, `quota_scope`, `user`, and
`client_id` labels. Accepted `quota_scope` values are `user`, `client-id`, and
`user-client`; the associated user and/or client ID must be a real identity, not
an empty or `not_applicable` value. Byte-rate values must be finite. Throttle
rows use the same label validation, but a non-finite throttle value is reported
as no finite positive throttling observed rather than as invalid quota evidence.

Network metrics are queried with `sum(increase(...[1h]))` per step (summing hourly
deltas gives total bytes transferred). Storage is averaged across all samples in the
day (since it's a point-in-time gauge, not a cumulative counter).

## Prometheus target scope

`cluster_id` remains the logical resource ID used in Chitragupta output. It does
not select a Prometheus target. Set the required `metrics_identifier` to the
operator-controlled value that identifies this cluster's metrics, and use
`metrics_identifier_label` when your target label is not the default
`kafka_cluster_id`.

Every scraped broker target must carry the exact configured label/value, including
separate JMX exporter or broker endpoints. For example, this configuration requires
the scope selector:

```promql
up{kafka_cluster_id="kafka-prod-target"}
```

When scope validation fails, the runtime diagnostic is:
`expected Prometheus target label <metrics_identifier_label>=<metrics_identifier>`.
Query `up{<metrics_identifier_label>="<metrics_identifier>"}` and correct target
relabeling or metric injection on every broker endpoint before retrying.

The target must be present and healthy across each billing window. A missing target,
label mismatch, incomplete coverage, or unhealthy target fails closed: billing and
pipeline progress do not advance for that window. Topic metrics and quota metrics
can appear lazily, so neither proves target scope.

### Scope blocking and recovery

When scope validation fails, Chitragupta opens a breaker before billing work is
committed. While the breaker is open, billing and pipeline progress remain blocked.
A later run performs one targeted health probe rather than starting the normal gather
workload. Once healthy, it recovers the still-available windows in chronological
order, bounded by the configured lookback range. If the earliest blocked windows are
no longer available, the retained gap is reported as a retention gap; the engine does
not fabricate billing for it.

## Produced product types

| Product type | Cost formula | Allocation strategy | Why this strategy |
|---|---|---|---|
| `SELF_KAFKA_COMPUTE` | `broker_count × 24h × compute_hourly_rate` | Static policy split | Shared infrastructure is allocated across visible static policy identities. |
| `SELF_KAFKA_STORAGE` | `avg_gib × 24h × storage_per_gib_hourly` | Static policy split | Storage is cluster-wide and is not inferred from a principal label. |
| `SELF_KAFKA_NETWORK_INGRESS` | `sum_bytes_in ÷ 2^30 × network_ingress_per_gib` | Static policy split | BrokerTopicMetrics supplies the cluster total, not measured per-principal usage. |
| `SELF_KAFKA_NETWORK_EGRESS` | `sum_bytes_out ÷ 2^30 × network_egress_per_gib` | Static policy split | BrokerTopicMetrics supplies the cluster total, not measured per-principal usage. |

See [How Costs Work](../architecture/cost-model.md) for the complete math with
worked examples.

## Principal evidence and static policy allocation

BrokerTopicMetrics is used for cluster costs and broker/topic discovery; it does not
carry principal identity for allocation. With `identity_source.source: prometheus`
or `both`, Chitragupta evaluates the configured quota telemetry for each billing
window. Missing quota byte-rate telemetry is reported as `not_observed`, rather
than being treated as a principal identity. Structurally valid throttle rows with
non-finite values report that no positive throttling was observed; they do not turn
the source into measured usage.

Static identities are the allocation policy visible to operators. For `static` and
`both`, costs are split across those identities and chargeback rows state
`measured_usage=false`. Quota telemetry supplies evidence status only; it does not
change that policy split. If no static policy identities are configured, the normal
unallocated outcome makes the missing policy visible instead of inventing an owner.

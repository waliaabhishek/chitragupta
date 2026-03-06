# Self-Managed Kafka Configuration Reference

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
| `resource_source.security_protocol` | enum | `PLAINTEXT` | `PLAINTEXT`, `SSL`, `SASL_PLAINTEXT`, `SASL_SSL` |
| `metrics.url` | string | required | Prometheus URL |
| `metrics.auth_type` | enum | `none` | `basic`, `bearer`, or `none` |

## Required Prometheus metrics

The cost model derives costs from these JMX exporter metrics:

| Metric | Used for |
|---|---|
| `kafka_server_brokertopicmetrics_bytesin_total` | Network ingress (per topic/principal) |
| `kafka_server_brokertopicmetrics_bytesout_total` | Network egress (per topic/principal) |
| `kafka_log_log_size` | Storage (cluster-wide average) |

All metrics are summed cluster-wide with `sum(increase(...[1h]))` per step.

## Identity discovery via Prometheus

With `identity_source.source: prometheus`, principals are extracted from metric labels:

```promql
sum by (principal) (kafka_server_brokertopicmetrics_bytesin_total)
```

# Chitragupta

Multi-ecosystem infrastructure cost chargeback engine. Allocates costs to teams and
service accounts across Confluent Cloud, self-managed Kafka, and any Prometheus-instrumented system.

## What it does

- Pulls billing data (vendor API or YAML cost model) per billing period
- Discovers resources and identities via Prometheus or admin APIs
- Allocates costs to identities using configurable strategies (even split, usage ratio)
- Emits results to CSV or custom sinks
- Exposes a REST API for querying chargeback data and triggering pipeline runs

## Supported ecosystems

| Ecosystem | Plugin key | Billing source |
|---|---|---|
| Confluent Cloud | `confluent_cloud` | CCloud Billing API |
| Self-managed Kafka | `self_managed_kafka` | YAML cost model + Prometheus |
| Generic metrics | `generic_metrics_only` | YAML cost model + Prometheus |

## Quick links

- [Quickstart](getting-started/quickstart.md)
- [Configuration reference — CCloud](configuration/ccloud-reference.md)
- [Configuration reference — Self-managed Kafka](configuration/self-managed-reference.md)
- [Configuration reference — Generic metrics](configuration/generic-metrics-reference.md)
- [Troubleshooting](operations/troubleshooting.md)

# Identity Resolution

## UNALLOCATED identity

If a billing line item cannot be resolved to any identity, cost is attributed to the
system `UNALLOCATED` identity (type=`system`). This is created automatically by the
orchestrator on first run.

## Confluent Cloud

Identity types: `service_account`, `user`, `identity_pool`

Resolved via CCloud API — principals linked to Kafka clusters via ACLs and metrics.
Identity resolution uses metrics data (bytes in/out per principal) to weight splits.

## Self-managed Kafka

Identity types: `principal`, `team`

Three discovery modes:

| `identity_source.source` | How identities are found |
|---|---|
| `prometheus` | Extracted from Prometheus metric label `principal` |
| `static` | Loaded from `static_identities` list in config |
| `both` | Union of Prometheus-discovered + static |

`principal_to_team` maps raw principal IDs to team names.

## Generic metrics

Identity types: configurable via `identity_source.label`

Same three modes as self-managed Kafka. Label name is configurable — not fixed to
`principal`.

## Allocation strategies

| Strategy | When to use |
|---|---|
| `even_split` | Infrastructure costs that benefit all users equally |
| `usage_ratio` | Usage-driven costs (network, compute by workload) |

Even split: cost ÷ identity count.
Usage ratio: each identity gets `their_usage / total_usage × cost`.

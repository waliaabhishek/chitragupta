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
      # Omit topic_attribution to leave the topic-level overlay disabled.
      # topic_attribution:
      #   enabled: true
      #   compute_policy: shared_even_v1
      #   # exclude_topic_patterns:
      #   #   - "internal-*"
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
| `principal_attribution.enabled` | bool | `false` | Enable quota-backed allocation of the two network pools. |
| `principal_attribution.scrape_interval_seconds` | int | required when enabled | Declared Prometheus scrape interval; must be greater than zero. |
| `principal_attribution.max_gap_seconds` | int | required when enabled | Largest permitted gap between quota samples and between the window boundary and its guard sample; must be greater than zero. |
| `principal_attribution.compute_policy` | enum | `unattributed` | `unattributed` or `static_even_v1` for the fixed compute pool while principal attribution is enabled. |
| `principal_attribution.storage_policy` | enum | `unattributed` | `unattributed` or `static_even_v1` for the fixed storage pool while principal attribution is enabled. |
| `identity_source.source` | enum | `prometheus` | `prometheus`, `static`, or `both`. Principal attribution requires `prometheus` or `both`. |
| `identity_source.principal_to_team` | dict | `{}` | Maps canonical measured principal IDs such as `User:service-account` to a team snapshot. |
| `identity_source.default_team` | string | `UNASSIGNED` | Team snapshot used for a measured user with no `principal_to_team` entry. |
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
| `topic_attribution.enabled` | bool | `false` | Enable the independent topic-level analytical overlay. It does not change principal chargebacks. |
| `topic_attribution.compute_policy` | enum | `disabled` | `disabled` keeps compute as `__UNATTRIBUTED__`; `shared_even_v1` uses the fixed, shared-even policy described below. |
| `topic_attribution.exclude_topic_patterns` | list[string] | `[]` | Optional shell-style topic patterns for read-time reporting classification. Omit it, or set `[]`, to exclude no topics. |
| `topic_attribution.retention_days` | int | `90` | Days to retain topic-attribution rows (1–365). |
| `topic_attribution.emitters` | list | `[]` | Optional emitter specs for topic-attribution output. |
| `allocator_overrides` | dict | `{}` | Replace allocator for specific product types (see [Advanced Scenarios](advanced-scenarios.md)) |
| `identity_resolution_overrides` | dict | `{}` | Replace identity resolver for specific product types |

## Required Prometheus metrics

The cost model and target-scope check require these Prometheus metrics on every
scraped broker target:

| Metric | Type | Used for |
|---|---|---|
| `kafka_server_brokertopicmetrics_alltopics_bytesin_total` | counter | Broker-wide client ingress cost pool; it excludes replication traffic. |
| `kafka_server_brokertopicmetrics_alltopics_bytesout_total` | counter | Broker-wide client egress cost pool; it excludes replication traffic. |
| `kafka_log_log_size` | gauge | Cluster storage cost pool and, when topic attribution is enabled, per-topic storage evidence. |
| `up` | gauge | Required target-scope validation for every billing window |

With `resource_source.source: prometheus`, export the topic-labelled ingress
counter for resource discovery. When `topic_attribution.enabled` is `true`,
export both topic-labelled counters. They are allocation evidence, not the cost
pools themselves:

| Metric | Type | Used for |
|---|---|---|
| `kafka_server_brokertopicmetrics_bytesin_total` | counter | Topic ingress ratio |
| `kafka_server_brokertopicmetrics_bytesout_total` | counter | Topic egress ratio |

Each series must carry the configured `metrics_identifier_label`. The broker-wide
`alltopics` counters also need `broker` and must not carry `topic`; topic counters
need `broker` and `topic`. For topic attribution, `kafka_log_log_size` also needs
`broker`, `topic`, and `partition`. `up` must carry the configured selector label
on every target.

When `principal_attribution.enabled` is `true`, the plugin requires this quota
metric for measured network allocation:

| Metric | Type | Used for |
|---|---|---|
| `kafka_server_quota_byte_rate` | gauge | Quota-backed principal weights for ingress and egress |

Every quota series must carry `broker`, the configured
`metrics_identifier_label`, `quota_type`, `quota_scope`, `user`, and `client_id`.
The runtime accepts only finite, non-negative byte-rate samples. `Produce` is
ingress and `Fetch` is egress. The accepted scope/label combinations are:

| `quota_scope` | `user` | `client_id` |
|---|---|---|
| `user` | a real user | `not_applicable` |
| `user-client` | a real user | a real client ID |
| `client-id` | `not_applicable` | a real client ID |

The JMX exporter must expose all three Kafka quota MBean forms for both Produce
and Fetch: `user=*`, `client-id=*`, and `user=*,client-id=*`. Preserve the
Kafka-authenticated user label exactly; Chitragupta makes a measured user
identity by prepending `User:` and preserves the suffix case. The telemetry lab's
exporter rules are a working label contract for these MBean forms.

All Prometheus queries inject the configured
`metrics_identifier_label=metrics_identifier` selector. That includes target
health, discovery, broker-wide pools, and topic evidence, so multiple configured
clusters can safely share one Prometheus endpoint.

Network pools use the broker-wide `alltopics` counters over a closed UTC day.
Storage is averaged across all `kafka_log_log_size` samples in that same day
(since it is a point-in-time gauge, not a cumulative counter).

## Optional topic attribution

Topic attribution is a self-managed Kafka overlay on top of the existing cluster
chargeback. It is opt-in and independent of principal evidence and principal
allocation policy. Enabling it writes a topic breakdown; it never uses
BrokerTopicMetrics as principal identity evidence.

```yaml
plugin_settings:
  topic_attribution:
    enabled: true
    compute_policy: shared_even_v1
```

Ingress and egress use topic-labelled bytes-in and bytes-out evidence against
their respective broker-wide client pools. Storage uses partition log size,
summed by topic and averaged across the day. A present zero value is valid. If
the storage metric family is absent, the day remains retryable unless successful
Admin API inventory proves that the cluster has no topics or partitions.

Every processed pool is represented by topic rows or by `__UNATTRIBUTED__`. When
topic evidence is incomplete, its unmatched cost remains `__UNATTRIBUTED__`;
topic amounts and that residual reconcile to the cluster pool. The overlay uses
the same closed `[00:00 UTC, 00:00 UTC)` day for the pool and all topic evidence.
Start with a 1–2 day tenant cutoff so only fully closed metric days are processed.

`shared_even_v1` is an explicit fixed-cost policy, not measured usage. It assigns
100% of compute evenly across the complete active-topic universe before any
reporting exclusions are applied. `disabled` is the default and makes the compute
topic row `__UNATTRIBUTED__`. Future compute algorithms use new policy names;
the meaning of `shared_even_v1` does not change.

`exclude_topic_patterns` is optional reporting policy. Omission and `[]` both
mean that no topics are excluded; there are no implicit internal-topic patterns.
Actual topic names and amounts are persisted. After the application's normal
configuration reload or restart, changed patterns reclassify both current and
historical results at read time without a Prometheus query, allocation rerun, or
storage rewrite. Aggregate analytics collapse matching topics into `Excluded topics`;
detailed API rows and CSV retain each actual name with its derived exclusion status.

Confluent Cloud topic attribution keeps its existing configuration and behavior.

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
| `SELF_KAFKA_COMPUTE` | `broker_count × 24h × compute_hourly_rate` | Existing configured policy, or the opt-in fixed policy | Shared infrastructure has no measured principal signal. |
| `SELF_KAFKA_STORAGE` | `avg_gib × 24h × storage_per_gib_hourly` | Existing configured policy, or the opt-in fixed policy | Storage has no measured principal signal. |
| `SELF_KAFKA_NETWORK_INGRESS` | `sum_bytes_in ÷ 2^30 × network_ingress_per_gib` | Existing configured policy, or quota-backed principal allocation | BrokerTopicMetrics supplies the cluster pool. |
| `SELF_KAFKA_NETWORK_EGRESS` | `sum_bytes_out ÷ 2^30 × network_egress_per_gib` | Existing configured policy, or quota-backed principal allocation | BrokerTopicMetrics supplies the cluster pool. |

See [How Costs Work](../architecture/cost-model.md) for the complete math with
worked examples.

## Quota-backed principal attribution

BrokerTopicMetrics supplies the cluster pools and topic evidence; it never supplies
principal ownership. Quota-backed attribution is therefore an explicit opt-in. When
it is disabled, the existing identity-source allocation behavior is unchanged.

With attribution disabled or omitted, `identity_source.source: prometheus` and
`both` keep their existing byte-rate and throttle readiness probes. They report
`observed`, `not_observed`, `invalid`, or `transient_failure`, but do not create
measured principal allocations. `static` reports `policy_only_configured` and makes
no quota calls.

```yaml
plugin_settings:
  principal_attribution:
    enabled: true
    scrape_interval_seconds: 30
    max_gap_seconds: 90
    compute_policy: unattributed
    storage_policy: unattributed
  identity_source:
    source: prometheus  # `both` is also valid when fixed-policy identities are needed
    principal_to_team:
      "User:service-account": data-platform
    default_team: UNASSIGNED
```

The declared scrape interval and maximum gap are independent operational inputs;
choose values that match the Prometheus scrape configuration and its expected
delays. The feature rejects `identity_source.source: static`. A target-scope failure
stops calculation before quota queries or allocation and creates no business rows.
After target scope is valid, missing, invalid, or incomplete quota evidence fails
closed for the affected network direction.

### Kafka quota and exporter setup

Configure Kafka quota entities for the authenticated users, client IDs, and/or
user/client pairs that you want to observe. The exporter must publish the matching
Kafka JMX quota MBeans with the labels above and apply the same cluster target label
as the other Kafka metrics. Chitragupta queries Prometheus; it does not create or
change Kafka quotas.

Kafka Admin API credentials are only for optional resource discovery. If
`resource_source.source: admin_api`, provide the bootstrap servers and the matching
SSL or SASL settings (`security_protocol`, `sasl_mechanism`, username, and password)
for a client permitted to describe the cluster. This client configuration is
separate from broker quota enforcement and from Prometheus authentication.

### Allocation states and reconciliation

Ingress and egress are evaluated independently after target scope is valid. A
complete positive user-only matrix is `ready`; a complete positive matrix that also
contains client-only weight is `degraded`; a complete zero matrix is `zero_usage`.
Missing, invalid, incomplete, or cadence-mismatched quota evidence is `unavailable`
and sends that direction's whole network pool to `UNALLOCATED` rather than guessing
an owner. A target-scope failure occurs before this evaluation and creates no
business rows.

For a direction, Chitragupta integrates each valid quota gauge over the billing
window. User and user/client weights are combined per canonical `User:` identity;
client-only weight is never assigned to a user. If `M` is the network pool, `q_i`
are user weights, `c` is client-only weight, and `W = sum(q_i) + c`, user amount
`i` is `M * q_i / W` and the client-only amount is `M * c / W`. Amounts round down
to four decimal places. The client-only amount and any remaining fractional amount
are separate `UNALLOCATED` rows, so every direction reconciles exactly to its pool.

`principal_to_team` is an exact, case-sensitive canonical-ID map. A measured user
without an entry receives `default_team`; client-only and rounding-residual rows do
not acquire a team. Team values are retained with each completed chargeback row.
Changing the map does not rewrite history. Recalculate a date explicitly to replace
that date with the then-current mapping, and retain the source data required for
every date that may be recalculated.

### Fixed pools, topic independence, and operations

Compute and storage do not gain a measured principal signal. With attribution
enabled, `unattributed` sends each full fixed pool to `UNALLOCATED`.
`static_even_v1` divides it evenly across the unique configured
`identity_source.static_identities`; it remains a fixed policy, not measured usage,
and any rounding remainder is `UNALLOCATED`. An empty `static_identities` list is
valid at startup: `static_even_v1` then preserves the entire fixed pool as
`UNALLOCATED`.

Topic attribution and principal attribution are independent marginals. They can
each reconcile to the same cluster-level network pool, but the system does not
derive principal-by-topic ownership or a topic owner from either metric family.

Retain Prometheus data for the billing window, the leading guard sample, normal
calculation delay, and the complete intended recalculation horizon. Quota gauges
are monitoring-resolution estimates rather than byte-exact Kafka accounting. For a
blocked direction, first verify the configured `up` selector on every broker target,
then verify the `Produce` or `Fetch` quota MBeans and required labels. Correct the
source and explicitly recalculate affected retained dates; do not fill missing
evidence with a static owner.

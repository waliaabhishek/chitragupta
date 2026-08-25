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
      historical_acquisition_chunk_days: 5
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
| `metrics_identifier` | string | required | Unique per-cluster Prometheus target value using only letters, digits, `.`, `_`, `-`, or `:` |
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
| `historical_acquisition_chunk_days` | int | 5 | Maximum closed UTC days in one historical response and reduced workload chunk; valid range `1..30`, with `30` an explicit operator-selected maximum |
| `metrics.url` | string | required | Prometheus URL |
| `metrics.auth_type` | enum | `none` | `basic`, `bearer`, or `none` |
| `metric_name_overrides` | dict[string, string] | `{}` | Optional tenant-wide mapping from canonical metric-family names to their physical Prometheus metric names |
| `label_name_overrides` | dict[string, dict[string, string]] | `{}` | Optional per-family mapping from canonical labels to their physical Prometheus label names |
| `topic_attribution.enabled` | bool | `false` | Enable the independent topic-level analytical overlay. It does not change principal chargebacks. |
| `topic_attribution.compute_policy` | enum | `disabled` | `disabled` keeps compute as `__UNATTRIBUTED__`; `shared_even_v1` uses the fixed, shared-even policy described below. |
| `topic_attribution.exclude_topic_patterns` | list[string] | `[]` | Optional shell-style topic patterns for read-time reporting classification. Omit it, or set `[]`, to exclude no topics. |
| `topic_attribution.retention_days` | int | `90` | Days to retain topic-attribution rows (1–365). |
| `topic_attribution.emitters` | list | `[]` | Optional emitter specs for topic-attribution output. |
| `allocator_overrides` | dict | `{}` | Replace allocator for specific product types (see [Advanced Scenarios](advanced-scenarios.md)) |
| `identity_resolution_overrides` | dict | `{}` | Replace identity resolver for specific product types |

## Cost-pool value ranges and fail-closed behavior

Self-managed Kafka infrastructure rates are finite decimal values greater than
or equal to zero. This applies to every base rate and every regional override:
`compute_hourly_rate`, `storage_per_gib_hourly`, `network_ingress_per_gib`, and
`network_egress_per_gib`. Negative values, `NaN`, and positive or negative
infinity are rejected during configuration validation. `-0`, `0`, and `+0`
are valid zero rates.

The broker-wide ingress and egress counters and the storage gauge must provide
finite, non-negative samples after the normal family-presence and closed UTC-day
selection rules. Signed zero is valid telemetry. A selected negative or
non-finite sample fails that entire UTC day closed before any billing line is
written. The independent days in the same backfill continue, and the failed day
is retryable on a later refresh. The existing storage rule remains unchanged:
an absent storage family is retryable unless the authoritative inventory check
confirms a partitionless cluster, in which case it is measured zero.

This validation is private to self-managed constructed cost pools. Shared
billing models and other providers may still represent explicit negative credits
or adjustments.

## Prometheus metric and label aliases

Self-managed Kafka telemetry uses canonical names in configuration, code, and
chargeback output. Prometheus may expose different names after JMX exporter
configuration, relabeling, or recording rules. Use the two optional mappings to
describe that physical naming without changing the canonical data model:

```yaml
plugin_settings:
  metric_name_overrides:
    kafka_log_log_size: company_kafka_partition_size
  label_name_overrides:
    kafka_log_log_size:
      broker: node
      topic: topic_name
      partition: partition_number
```

Canonical names are keys and physical Prometheus names are values. The mappings
are tenant-wide, optional, and partial: omitted families and labels resolve to
their canonical names. Each canonical family maps to exactly one physical metric
name, and each canonical label maps to exactly one physical label name. Names
must be valid Prometheus identifiers. The mappings are used consistently for
target scope, discovery, cost input, historical acquisition, topic evidence, and
quota/principal readiness; returned rows are converted back to canonical labels
before allocation and persistence.

The global selector is a separate boundary. `metrics_identifier` is the value
that identifies the cluster and `metrics_identifier_label` is the physical label
used to select it. That selector label is required on every family and is never
configured inside `label_name_overrides`; aliases cannot give different families
different selector labels. For example, with
`metrics_identifier_label: deployment` and `metrics_identifier: kafka-prod`, all
queries include `deployment="kafka-prod"`.

### Supported canonical telemetry catalog

The following eight canonical families are supported. The listed labels are
non-selector labels; the configured global selector is required in addition to
them. A disabled feature produces no query for its family in the live checker and
is reported as `skipped`.

| Canonical metric family | Type | Canonical non-selector labels | Queried when | Production feature(s) |
|---|---|---|---|---|
| `up` | gauge | none | always | target scope |
| `kafka_server_brokertopicmetrics_alltopics_bytesin_total` | counter | `broker` | always | cluster ingress |
| `kafka_server_brokertopicmetrics_alltopics_bytesout_total` | counter | `broker` | always | cluster egress |
| `kafka_log_log_size` | gauge | `broker`, `topic`, `partition` | always | cluster storage, Prometheus discovery, topic storage |
| `kafka_server_brokertopicmetrics_bytesin_total` | counter | `broker`, `topic` | Prometheus resource discovery or topic attribution | Prometheus discovery, topic ingress |
| `kafka_server_brokertopicmetrics_bytesout_total` | counter | `broker`, `topic` | topic attribution | Prometheus discovery, topic egress |
| `kafka_server_quota_byte_rate` | gauge | `broker`, `quota_type`, `quota_scope`, `user`, `client_id` | `identity_source.source` is `prometheus` or `both` | principal readiness, principal attribution |
| `kafka_server_quota_throttle_time_ms` | gauge | `broker`, `quota_type`, `quota_scope`, `user`, `client_id` | `identity_source.source` is `prometheus` or `both` and principal attribution is disabled | principal readiness |

Aliases change names only. They do not change metric type, units, values,
temporality, query operators, selector values, UTC windows, allocation policy,
reconciliation, persistence, API responses, or exports.

## Live self-managed telemetry checker

Run the explicit Prometheus check without starting the worker, API, storage,
Kafka Admin client, or normal pipeline:

```console
chitragupta --config-file config.yaml --check-self-managed-telemetry
```

The checker examines every configured `self_managed_kafka` tenant in mapping
order. It validates all selected tenant configurations before opening any
Prometheus source, captures one UTC end time for all tenants, and checks the
families above over each tenant's `discovery_window_hours`. Each enabled family
is queried independently, so a failed family does not erase the other results.
The report is JSON Lines on stdout: one record per family, in tenant/catalog
order, followed by one summary record. Every family record includes the tenant,
canonical and resolved physical metric names, the selector, expected physical
labels, observed label-name union (never label values), affected production
features, a deterministic corrective override when applicable, and a warning.

The five possible states are:

| State | Meaning | Exit impact |
|---|---|---:|
| `valid` | At least one selected series has the required selector and labels; `up` samples are finite and equal to `1`. | 0 |
| `invalid` | A selected series has a missing/blank selector or required label, or `up` is unhealthy. | 1 |
| `not_observed` | The query succeeded but returned no selected series. This is a historical-coverage warning only. | 0 |
| `inconclusive` | Prometheus returned an error, parsing failed, a result key was absent, or the source could not be constructed. | 1 |
| `skipped` | The family is disabled by the tenant's feature configuration. | 0 |

An empty result reports a `metric_name_overrides` placeholder so an operator can
provide the physical metric name. Missing family labels report deterministic
`label_name_overrides` placeholders. The checker never guesses a mapping from
unrelated labels. Source or query failures have no corrective override. Historical
gaps remain warnings and do not mutate scope state, block normal reconciliation,
or fabricate chargeback data. A complete report is emitted even when the exit
code is `1`.

Configuration errors exit `1` with `Telemetry check configuration failed` on
stderr. If no self-managed Kafka tenant is configured, the command exits `2` with
`error: no self-managed Kafka tenant is configured`. Missing configuration or an
invalid checker option combination keeps the existing argparse exit-`2` contract.

### Alias boundaries

Aliases are name mappings only. They are not a label-value mapping, and they do
not support arbitrary PromQL, metric discovery, fuzzy or inferred semantic
matching, multiple physical names for one family, value/unit conversion, metric
type or temporality changes, or per-family selector overrides. Configure the
exporter and source data to expose the required canonical dimensions and values;
use the checker to identify missing physical names or labels.

## Prometheus metric inventory

Prometheus scrapes Kafka through JMX Exporter. Chitragupta queries Prometheus;
it does not scrape brokers directly.

### Always queried

| Metric | Type | Used for |
|---|---|---|
| `kafka_server_brokertopicmetrics_alltopics_bytesin_total` | counter | Broker-wide client ingress cost pool; it excludes replication traffic. |
| `kafka_server_brokertopicmetrics_alltopics_bytesout_total` | counter | Broker-wide client egress cost pool; it excludes replication traffic. |
| `kafka_log_log_size` | gauge | Cluster storage cost pool and, when topic attribution is enabled, per-topic storage evidence. |
| `up` | gauge | Required target-scope validation for every billing window |

### Queried when a feature is enabled

| Metric | Type | Queried when | Used for |
|---|---|---|---|
| `kafka_server_brokertopicmetrics_bytesin_total` | counter | `resource_source.source` is `prometheus`, or topic attribution is enabled | Topic discovery and ingress allocation evidence |
| `kafka_server_brokertopicmetrics_bytesout_total` | counter | Topic attribution is enabled | Topic discovery and egress allocation evidence |
| `kafka_server_quota_byte_rate` | gauge | `identity_source.source` is `prometheus` or `both` | Quota readiness; measured ingress and egress weights when principal attribution is enabled |
| `kafka_server_quota_throttle_time_ms` | gauge | Principal attribution is disabled and `identity_source.source` is `prometheus` or `both` | Quota readiness diagnostic |

Topic-labelled counters are allocation evidence, not cluster cost pools.

Each series must carry the configured `metrics_identifier_label`. The broker-wide
`alltopics` counters also need `broker` and must not carry `topic`; topic counters
need `broker` and `topic`. For topic attribution, `kafka_log_log_size` also needs
`broker`, `topic`, and `partition`. `up` must carry the configured selector label
on every target.

Every quota series must carry `broker`, the configured
`metrics_identifier_label`, `quota_type`, `quota_scope`, `user`, and `client_id`.
Measured allocation accepts only finite, non-negative byte-rate samples.
`Produce` is ingress and `Fetch` is egress. The accepted scope/label combinations
are:

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

## Bounded historical acquisition and recovery behavior

`historical_acquisition_chunk_days` is a self-managed Kafka setting. It controls the
maximum closed-UTC-day span requested from Prometheus for scope and workload history:

- Omit the setting to use `5` days.
- Values from `1` through `30` are valid. `30` is an explicit operator-selected
  maximum, not the default.
- Smaller values reduce response size and the amount of reduced evidence retained in
  memory, but increase the number of Prometheus requests.
- Every cluster or topic workload response spans at most `H` days, where `H` is the
  configured value. Only one reduced workload chunk is retained while the next chunk
  is acquired; the chunk is released after success, failure, or shutdown.

The engine reuses exact scope evidence within one run when tenant, target identifier,
target-label name, step, start, and end are identical. A failed open breaker performs
one newest-point health probe and no workload, billing, topic, write, or progress
activity. A successful probe is persisted as recovery state, then the complete
bounded scope is validated again before workload queries, progress, or writes start.
Billing windows remain owned by calculation; acquisition does not invent or widen
them.

Counters are evaluated at each UTC day end and own the preceding half-open day
`[day_start, day_end)`. Gauge samples retain the daily start-anchored grid and the
same half-open filter, including when the configured step does not divide a day.
Missing and present-zero evidence remain distinct. A failed family or day is isolated
from successful families and later days; residual rows, monetary reconciliation,
retryable dates, and terminal topic visibility are preserved.

### Measured request and transport bounds

The following cold-cache measurements use the default one-hour divisor step and the
existing retry ceiling `R=4`. A logical query is one PromQL family; a transport
attempt is one HTTP request. The normal one-day path batches three cluster families
in one client call, but still has three logical cluster families.

| Scenario | H and date sets | Logical families by source | Cold HTTP attempts | Retry ceiling |
|---|---|---|---:|---:|
| Normal one day | `H=5`, one cluster/topic day | target `1`; cluster ingress/egress/storage `1` each; topic ingress/egress/storage `1` each (`L=7`) | 7 | `R×L=28` |
| 31-day backfill | `H=5`, `C_cluster=C_topic=7` | target `7`; each of the six workload families `7` (`L=49`) | 49 | `R×L=196` |
| Unequal, gapped recovery workload | `H=5`; cluster `9` days (`C_cluster=2`), topic runs `2+1+1` days (`C_topic=3`) | target `2`; each cluster family `2`; each topic family `3` (`L=17`) | 17 | `R×L=68` |
| Family-local fallback | `H=5`, six cluster/topic days; one five-day cluster ingress chunk fails | target `2`; cluster ingress `7`; other cluster and topic families `2` (`L=19`, workload `B+F=17`) | 22 (`10` ingress attempts plus first-attempt remainder) | `R×L=76` |
| Failed open probe | `H=5`, newest point only | target `1`; no workload families (`L=1`) | 4 when all transient attempts are exhausted | `R×L=4` |
| Successful recovery | `H=5`, six cluster and six topic days (`C_cluster=C_topic=2`) | target `3` (point probe plus two bounded validation requests); each workload family `2` (`L=15`) | 15 | `R×L=60` |
| Warm raw-response cache | same logical target query twice | target `2` logical families | 1 | cache hits can reduce attempts, never logical count |

For independent cluster and topic date sets, let `C_cluster` and `C_topic` be the
sum of `ceil(run_length / H)` over each contiguous run. If the step divides one UTC
day, successful workload counts are:

```
B_cluster = 3 × C_cluster
B_topic   = 3 × C_topic
B_total   = B_cluster + B_topic
```

For exact gauge bounds, use `D = 86,400` seconds per UTC day, `S` equal to the
positive `metrics_step_seconds`, and `h` equal to the number of days in one actual
chunk. For one actual chunk `j`, `g(h_j, S)` is the exact number of residue-group
requests for that gauge family:

```
g(h, S) = min(h, S / gcd(S, D))
G_family = sum_j g(h_j, S)  # sum once over the actual chunks j
```

`G_cluster` and `G_topic` are the corresponding sums for their independent date
sets. Each returned series contains at most `floor(H × D / S) + 1` evaluation
timestamps before ordinary daily filtering; the half-open day filter may retain
fewer. The shorthand `6C` is valid only when cluster and topic date sets have the
same contiguous runs, use the same `H`, and `S` divides `D`, so each side is exactly
`3C`.

For the documented twelve-day coincident example with the default `H=5`, both sides
have `C=3`, so the workload is `9 + 9 = 18` logical families. With explicit `H=30`,
the same workload is `3 + 3 = 6`.

For a non-divisor step, use `G_cluster` and `G_topic` from the exact per-family sums
above:

```
B_cluster = 2 × C_cluster + G_cluster
B_topic   = 2 × C_topic   + G_topic
```

For example, six days with `H=5` and a 3,601-second step have `C=2`, `G=6`, and
`B=10` workload families per side. If a bounded family request fails, `F` is the
number of days retried with daily fallback queries, so the workload is `B + F`; a
successful family adds no fallback. In the measured fallback row above,
`B_cluster=6`, `F_cluster=5`, and `B_topic=6`, giving `17` workload families before
the two target-scope families. In all cases, cold attempts are at most `R×L`,
transient recovery uses between two and `R` attempts, and a warm cache hit uses zero
transport attempts for that logical query.

These bounded-history settings apply only to `self_managed_kafka`. Confluent Cloud
keeps its existing configuration, query, progress, and topic-attribution behavior;
the `generic_metrics_only` path is likewise unchanged.

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
They are not re-resolved from the current configuration when a row is read.
Changing the map does not rewrite history. Recalculate a date explicitly to replace
that date with the then-current mapping, and retain the source data required for
every date that may be recalculated. For manual preparation or downgrade of the
self-managed storage, follow the [self-managed Kafka storage migration
procedure](../operations/upgrading.md#self-managed-kafka-storage-migrations).

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

# Configuration Guide

This guide walks you through building a configuration from scratch. Rather than
listing every field (that's what the [reference pages](index.md) are for), it
explains the decisions you need to make and why they matter.

## Start with the question: where does your cost data come from?

This is the single most important decision. It determines which ecosystem plugin
you use, which in turn determines everything else — what credentials you need,
how costs are calculated, and how identities are discovered.

**If you use Confluent Cloud** and have billing API access, use `confluent_cloud`.
The engine fetches actual invoiced costs from the CCloud billing API. You don't
define rates — the vendor already computed what you owe.

**If you run your own Kafka** (on-prem, EC2, Kubernetes, etc.) and have JMX
metrics flowing to Prometheus, use `self_managed_kafka`. The engine queries
Prometheus for usage data and applies cost rates that *you* define in YAML. You
are constructing the bill yourself.

**If you run something else entirely** (PostgreSQL, Redis, Elasticsearch, a
custom service) and it exposes Prometheus metrics, use `generic_metrics_only`.
Same idea as self-managed Kafka, but fully configurable — you define the cost
types, the queries, and the allocation strategy.

```mermaid
graph TD
    A{Where do costs come from?} -->|Vendor billing API| B[confluent_cloud]
    A -->|Prometheus + your rates| C{Is it Kafka?}
    C -->|Yes| D[self_managed_kafka]
    C -->|No| E[generic_metrics_only]
```

!!! note "The two billing paradigms"
    Understanding this distinction is critical. With `confluent_cloud`, the vendor
    already computed `quantity × unit_price = total_cost` for you. The engine's
    job is purely to *allocate* that bill across your teams.

    With `self_managed_kafka` and `generic_metrics_only`, the engine *constructs*
    the bill by querying Prometheus for usage metrics and multiplying by the rates
    you configure. If your rates are wrong, your chargebacks are wrong.

---

## Tenants: one per billing boundary

A tenant represents a single billing boundary — typically one CCloud organization,
one Kafka cluster, or one database cluster. Each tenant:

- Has its own database (enforced: no two tenants can share a `connection_string`)
- Runs independently (a failure in one tenant does not affect others)
- Has its own plugin configuration, cost model, and emitters

**When to use multiple tenants:**

- You have two CCloud orgs → two tenants, both using `confluent_cloud`
- You have a CCloud org *and* an on-prem Kafka cluster → two tenants, different ecosystems
- You have a Kafka cluster and a PostgreSQL cluster → two tenants, different ecosystems

**Don't create multiple tenants for teams within the same cluster.** That's what
identity resolution and allocation do — they split costs *within* a tenant across
teams and users.

```yaml
tenants:
  # One tenant per billing boundary
  ccloud-prod:
    ecosystem: confluent_cloud
    tenant_id: ccloud-prod         # internal partition key (not the CCloud org ID)
    storage:
      connection_string: "sqlite:///data/ccloud-prod.db"
    plugin_settings: ...

  kafka-onprem:
    ecosystem: self_managed_kafka
    tenant_id: kafka-dc1
    storage:
      connection_string: "sqlite:///data/kafka-dc1.db"
    plugin_settings: ...
```

---

## Choosing a storage backend

Each tenant needs a database. Two options:

**SQLite** (default) — Zero setup. One file per tenant. Good for single-node
deployments, development, and small-to-medium workloads.

```yaml
storage:
  connection_string: "sqlite:///data/ccloud-prod.db"
```

**PostgreSQL** — Required when multiple processes need concurrent access (e.g.,
separate worker and API server processes). Also better for large tenants with
hundreds of resources and years of history.

```yaml
storage:
  connection_string: "postgresql://user:pass@localhost:5432/chargeback_prod"  # pragma: allowlist secret
```

!!! warning "When SQLite won't work"
    If you run the worker and API server as separate processes (`chitragupt worker`
    and `chitragupt api` in different containers), they both need to write to the
    same database. SQLite doesn't handle concurrent writers well. Use PostgreSQL.

    If everything runs in a single process (`chitragupt worker --with-api`), SQLite
    is fine.

---

## Time windows: lookback, cutoff, and retention

Three settings control which dates the engine processes and how long data is kept.
They interact with each other, so it helps to understand them together.

```
←─── retention_days ───────────────────────────────────────→
←─── lookback_days ──────────────────────→
                                          ←── cutoff_days ──→
├──────────────────────────────────────────┼────────────────┼──→
oldest data                              skip zone        today
(deleted after                    (billing not yet
 retention_days)                   finalized by vendor)
```

**`lookback_days`** (default: 200) — How far back to fetch billing data. On the
first run, the engine backfills this many days. On subsequent runs, it only
fetches new data, but will re-fetch and recalculate dates within the cutoff
window if the billing data changed.

**`cutoff_days`** (default: 5) — Skip dates this close to today. Vendors
(especially CCloud) don't finalize billing data immediately. If you process a
date too early, you get partial costs. The cutoff gives the vendor time to
settle. For self-managed Kafka, this is less critical since you control the
metrics, but a small cutoff (1–2 days) still avoids processing incomplete
Prometheus scrapes.

**`retention_days`** (default: 250) — Delete data older than this. Runs
automatically after each pipeline cycle. Set this higher than `lookback_days`
to keep historical chargebacks visible in the API after the engine stops
re-fetching them.

!!! note "Constraint: `lookback_days` must be greater than `cutoff_days`"
    Otherwise the engine has no valid date range to process. This is enforced at
    startup.

**Practical guidance:**

| Scenario | lookback | cutoff | retention |
|---|---|---|---|
| CCloud (billing lag ~3 days) | 200 | 5 | 365 |
| Self-managed Kafka | 90 | 2 | 180 |
| Testing / development | 30 | 1 | 60 |

---

## Configuring Confluent Cloud

### What you need before you start

1. **CCloud API key** with `OrganizationAdmin` role — the engine calls the billing
   API and the environments/clusters APIs to discover resources and identities.
2. **Metrics API key** (optional but recommended) — enables usage-based allocation
   for Kafka network costs and Flink CFUs. Without this, network costs fall back
   to even-split across active identities.
3. **Flink API credentials** (optional) — only needed if you use Confluent Flink
   and want per-statement-owner allocation.

### The CKU ratio: the one tuning knob you should care about

Kafka CKU costs (the main compute cost in CCloud) are allocated using a hybrid
model: part usage-based, part shared. By default, 70% is allocated proportionally
to bytes produced/consumed, and 30% is split evenly.

The reasoning: a Kafka cluster has a base cost whether anyone uses it or not
(the shared portion), but teams that produce/consume more data drive more of
the compute load (the usage portion).

```yaml
allocator_params:
  kafka_cku_usage_ratio: 0.70    # 70% by bytes in + bytes out
  kafka_cku_shared_ratio: 0.30   # 30% even split
```

These must sum to 1.0. Adjust based on your organization's philosophy:

- **More usage-driven (e.g., 0.90 / 0.10):** Heavy producers/consumers pay more.
  Fair if your cluster is right-sized and usage directly drives cost.
- **More shared (e.g., 0.50 / 0.50):** Spreads the base infrastructure cost more
  evenly. Fair if the cluster is over-provisioned and most cost is fixed overhead.
- **Fully usage-driven (1.0 / 0.0):** Only works if you have reliable per-principal
  metrics. If metrics are missing for a billing window, costs fall back to even-split
  anyway.

### What happens without metrics

If you don't configure `metrics` (the Prometheus/Telemetry API connection), the
engine can still allocate costs — but falls back to even-split for everything.
The allocation chain works like this:

1. **Try usage ratio** (bytes per principal from metrics) → needs metrics
2. **Try even split across active identities** (from API key discovery) → needs identities
3. **Try even split across all tenant identities** (from the full billing period)
4. **Allocate to the resource itself** (terminal fallback)
5. **Allocate to UNALLOCATED** (for org-wide costs with no resource)

Each tier fires only when the previous one has no data. The `allocation_detail`
field on every chargeback row tells you which tier was used, so you can audit why
a cost was allocated the way it was.

---

## Configuring Self-Managed Kafka

### The cost model: you define the rates

Unlike CCloud, there's no billing API. You tell the engine what your infrastructure
costs, and it constructs billing lines from Prometheus metrics.

```yaml
cost_model:
  compute_hourly_rate: "0.50"           # $/broker/hour
  storage_per_gib_hourly: "0.0001"      # $/GiB/hour
  network_ingress_per_gib: "0.01"       # $/GiB
  network_egress_per_gib: "0.05"        # $/GiB
```

**Where do these numbers come from?** Typically from your cloud provider or
internal cost accounting:

- **Compute:** Divide your monthly EC2/GKE bill for broker instances by
  `(broker_count × hours_in_month)`.
- **Storage:** EBS/PD cost per GiB-month, divided by hours in a month. Use
  `Decimal` strings for precision — `"0.0001"` not `0.0001`.
- **Network:** Cloud provider's network egress/ingress pricing per GiB. Ingress
  is often free or cheaper than egress.

!!! note "Region overrides"
    If your cluster spans regions with different pricing (or you want to model
    "what if we moved to eu-west-1"), use `region_overrides`. Only fields you
    specify are overridden; the rest inherit from the base cost model.

    ```yaml
    cost_model:
      compute_hourly_rate: "0.50"
      ...
      region_overrides:
        eu-west-1:
          compute_hourly_rate: "0.60"   # Only compute is more expensive
          # storage, network rates inherited from base
    ```

### How the engine computes daily costs

For each day in the lookback window, the engine queries Prometheus and applies
your rates. Here is the exact math (from `ConstructedCostInput`):

| Cost type | Formula | Example (3 brokers, 24h) |
|---|---|---|
| Compute | `broker_count × 24 × compute_hourly_rate` | 3 × 24 × $0.50 = **$36.00** |
| Storage | `avg_storage_gib × 24 × storage_per_gib_hourly` | 100 GiB × 24 × $0.0001 = **$0.24** |
| Network ingress | `total_bytes_in ÷ 1,073,741,824 × network_ingress_per_gib` | 50 GiB × $0.01 = **$0.50** |
| Network egress | `total_bytes_out ÷ 1,073,741,824 × network_egress_per_gib` | 50 GiB × $0.05 = **$2.50** |

Storage uses the **average** of all Prometheus samples in the day (because storage
is a point-in-time measurement, not a cumulative counter). Network uses the **sum**
of all hourly increases (because bytes are a cumulative counter).

See [How Costs Work](../architecture/cost-model.md) for the complete mathematical
model including allocation.

### Choosing a resource source

How does the engine discover your brokers and topics?

| Source | How it works | Tradeoffs |
|---|---|---|
| `prometheus` (default) | Extracts broker, topic, and principal labels from `kafka_server_brokertopicmetrics_bytesin_total` | Zero additional credentials. Only discovers resources that have traffic. A topic with zero bytes in the discovery window won't appear. |
| `admin_api` | Queries the Kafka AdminClient for cluster metadata | Discovers *all* topics including idle ones. Requires bootstrap server credentials. Does **not** discover principals (no ACL info). |

**When to use `admin_api`:** If you need a complete inventory of topics regardless
of traffic. Combine with `identity_source: prometheus` or `both` to still get
principal data from metrics.

```yaml
resource_source:
  source: admin_api
  bootstrap_servers: kafka-1:9092,kafka-2:9092,kafka-3:9092
  security_protocol: SASL_SSL
  sasl_mechanism: SCRAM-SHA-512
  sasl_username: ${KAFKA_USER}
  sasl_password: ${KAFKA_PASS}
```

### Choosing an identity source

How does the engine discover who is producing/consuming?

| Source | How it works | Tradeoffs |
|---|---|---|
| `prometheus` (default) | Extracts `principal` label from JMX metrics | Requires JMX exporter configured to expose principal labels. Only finds principals with recent traffic. |
| `static` | You list identities in YAML | Works without Prometheus principal labels. You maintain the list manually. |
| `both` | Combines Prometheus + static | Prometheus principals go to `metrics_derived` (dynamic, per-window). Static identities go to `resource_active` (always present). Good when Prometheus has partial coverage. |

**Which one should you use?**

- If your JMX exporter includes `principal` labels on `kafka_server_brokertopicmetrics_*` → use `prometheus`
- If JMX doesn't expose principal labels (common with older exporters) → use `static`
- If some principals appear in metrics but you also want to include service accounts
  that rarely produce traffic → use `both`

```yaml
# Static identities example
identity_source:
  source: static
  static_identities:
    - identity_id: "User:alice"
      identity_type: principal
      display_name: Alice
      team: data-eng
    - identity_id: "User:bob"
      identity_type: service_account
      display_name: Bob (ETL service)
      team: platform
```

### Principal-to-team mapping

Regardless of identity source, you can map raw principal IDs to team names. This
is purely cosmetic — it doesn't affect allocation — but makes chargeback reports
readable.

```yaml
identity_source:
  source: prometheus
  principal_to_team:
    "User:alice": team-data-eng
    "User:bob": team-platform
    "User:etl-service": team-platform
  default_team: UNASSIGNED    # Principals not in the map get this
```

---

## Configuring Generic Metrics

The generic plugin is the most flexible but requires the most configuration. You
define everything: what the cost types are, how quantities are measured, and how
costs are allocated.

### Defining cost types

Each entry in `cost_types` becomes a separate billing line per day. Think of each
one as answering: "what does this infrastructure cost, and how do I measure usage?"

```yaml
cost_types:
  - name: PG_COMPUTE              # Product type in billing output
    product_category: postgres     # Grouping label
    rate: "0.50"                   # $/unit (depends on quantity type)
    cost_quantity:
      type: fixed                  # Fixed instance count
      count: 3                     # 3 nodes
    allocation_strategy: even_split
```

### Three ways to measure quantity

**`fixed`** — A constant. Use for infrastructure with a known, static count:
server instances, fixed-size clusters. The daily cost is `count × rate × 24`
(hours in a day).

**`storage_gib`** — Query Prometheus for a storage metric. The engine averages
all samples over the day, converts bytes to GiB, and multiplies by rate and
hours. Use for databases, object stores, anything measured in "how much data
is stored."

```yaml
cost_quantity:
  type: storage_gib
  query: "avg(pg_database_size_bytes)"    # Cluster-wide, no {} placeholder
```

**`network_gib`** — Query Prometheus for a throughput metric. The engine sums
all hourly increases, converts bytes to GiB, and multiplies by rate. Use for
network transfer, I/O throughput.

```yaml
cost_quantity:
  type: network_gib
  query: "sum(increase(pg_stat_bgwriter_buffers_alloc_total[1h]))"
```

!!! note "Storage vs. network: why the math differs"
    Storage is a gauge (current value). Averaging gives GiB-hours. Network is a
    counter (cumulative). Summing increases gives total GiB transferred. The rate
    units differ: `$/GiB/hour` for storage, `$/GiB` for network.

### Choosing an allocation strategy

Once the engine computes the daily cost for a cost type, it needs to split it
across identities. Two strategies:

**`even_split`** — Divide equally among all discovered identities. Use for shared
infrastructure costs where there's no meaningful way to attribute usage
(compute nodes, base storage).

**`usage_ratio`** — Divide proportionally to a usage metric. Requires an
`allocation_query` (PromQL returning per-identity values) and `allocation_label`
(which label identifies the identity). Use when a Prometheus metric directly
measures per-user consumption.

```yaml
# Example: network cost split by query activity per user
- name: PG_NETWORK
  rate: "0.05"
  cost_quantity:
    type: network_gib
    query: "sum(increase(pg_stat_bgwriter_buffers_alloc_total[1h]))"
  allocation_strategy: usage_ratio
  allocation_query: "sum by (usename) (increase(pg_stat_activity_count[1h]))"
  allocation_label: usename      # Prometheus label → identity_id
```

!!! warning "The allocation_query must have a `by (label)` clause"
    The query must return one series per identity. If it returns a single
    aggregated value, every identity gets the same ratio and you've effectively
    written a more expensive even-split.

---

## Emitters: where do chargeback results go?

After costs are allocated, emitters write the results to external destinations.
Each tenant can have multiple emitters — for example, a CSV file for finance and
a Prometheus endpoint for dashboards.

### CSV emitter

Writes one file per billing date. Good for finance teams, spreadsheet workflows,
and archival.

```yaml
emitters:
  - type: csv
    aggregation: daily
    params:
      output_dir: ./output/chargebacks
      filename_template: "{tenant_id}_{date}.csv"   # optional
```

### Prometheus emitter

Exposes chargeback data as Prometheus gauge metrics on an HTTP `/metrics` endpoint.
The timestamps on the samples are the billing date (midnight UTC), not the current
wall clock — this makes the data suitable for TSDB backfill.

```yaml
emitters:
  - type: prometheus
    aggregation: daily
    params:
      port: 9090       # default: 8000
```

!!! note "Aggregation controls output granularity"
    `aggregation: daily` collapses hourly chargeback rows into one row per day
    before emitting. `aggregation: monthly` collapses further. `null` (or omit)
    emits rows at their native granularity. An emitter cannot request finer
    granularity than `chargeback_granularity` produces — if your granularity is
    `daily`, requesting `hourly` aggregation has no effect.

---

## Pipeline tuning

These settings control how aggressively the engine runs. The defaults are
conservative — they work for most deployments without tuning.

### `metrics_step_seconds` (default: 3600)

Controls the Prometheus range query step interval. Lower values mean more data
points per query, finer-grained usage attribution, but higher Prometheus load.

| Value | Effect |
|---|---|
| `3600` (1h) | One data point per hour. Good balance of precision and load. |
| `900` (15m) | Four data points per hour. Better for short-lived workloads. |
| `300` (5m) | Twelve data points per hour. Prometheus must retain this resolution. |

!!! warning "Match your Prometheus retention"
    If Prometheus downsamples to 1h resolution after 7 days, setting
    `metrics_step_seconds: 300` only helps for recent data. Older queries
    return interpolated values, not real 5-minute granularity.

### `metrics_prefetch_workers` (default: 4)

Parallel threads for Prometheus queries during the calculate phase. The engine
batches metrics queries across all billing lines for a date and runs them in
parallel.

Increase if: your Prometheus server is fast and you have many resources
(100+ topics). Decrease if: Prometheus is under heavy load or rate-limited.

### `max_parallel_tenants` (default: 4)

How many tenants run concurrently within a single pipeline cycle. Each tenant
gets its own thread. Increase if you have many small tenants. Decrease if
tenants are large (hundreds of resources, thousands of billing lines) and you're
hitting memory limits.

### `min_refresh_gap_seconds` (default: 1800)

Minimum time between pipeline runs for a tenant. If the engine runs in loop
mode (`enable_periodic_refresh: true`), it skips a tenant if the last run was
less than this many seconds ago. Prevents hammering external APIs when the
loop interval is shorter than the actual pipeline duration.

### `gather_failure_threshold` (default: 5)

After this many consecutive gather failures, the tenant is permanently suspended
for the lifetime of the process. Prevents infinite retry loops when credentials
expire or an API is permanently down. Restart the process to reset the counter.

---

## Putting it all together: a complete example

Here's a configuration for a team running both Confluent Cloud and an on-prem
Kafka cluster, with CSV output for finance and Prometheus metrics for dashboards.

```yaml
logging:
  level: INFO

features:
  enable_periodic_refresh: true
  refresh_interval: 3600                 # run once per hour
  max_parallel_tenants: 2

api:
  port: 8080

tenants:
  # Confluent Cloud organization
  ccloud-prod:
    ecosystem: confluent_cloud
    tenant_id: ccloud-prod         # internal partition key (not the CCloud org ID)
    lookback_days: 200
    cutoff_days: 5
    retention_days: 365
    storage:
      connection_string: "sqlite:///data/ccloud-prod.db"
    plugin_settings:
      ccloud_api:
        key: ${CCLOUD_API_KEY}
        secret: ${CCLOUD_API_SECRET}
      billing_api:
        days_per_query: 15
      metrics:
        type: prometheus
        url: https://api.telemetry.confluent.cloud
        auth_type: basic
        username: ${METRICS_API_KEY}
        password: ${METRICS_API_SECRET}
      allocator_params:
        kafka_cku_usage_ratio: 0.70
        kafka_cku_shared_ratio: 0.30
      emitters:
        - type: csv
          aggregation: daily
          params:
            output_dir: ./output/ccloud
        - type: prometheus
          aggregation: daily
          params:
            port: 9090

  # On-prem Kafka cluster
  kafka-dc1:
    ecosystem: self_managed_kafka
    tenant_id: kafka-dc1
    lookback_days: 90
    cutoff_days: 2
    retention_days: 180
    storage:
      connection_string: "sqlite:///data/kafka-dc1.db"
    plugin_settings:
      cluster_id: kafka-dc1-cluster
      broker_count: 5
      cost_model:
        compute_hourly_rate: "0.45"
        storage_per_gib_hourly: "0.00008"
        network_ingress_per_gib: "0.00"
        network_egress_per_gib: "0.09"
      identity_source:
        source: both
        principal_to_team:
          "User:etl-service": team-platform
          "User:analytics": team-data
        default_team: UNASSIGNED
        static_identities:
          - identity_id: "User:batch-job"
            identity_type: service_account
            display_name: Nightly Batch
            team: team-platform
      resource_source:
        source: prometheus
      metrics:
        type: prometheus
        url: http://prometheus.internal:9090
        auth_type: none
      emitters:
        - type: csv
          aggregation: daily
          params:
            output_dir: ./output/kafka-dc1
```

This configuration:

- Runs the pipeline hourly, processing both tenants in parallel
- CCloud: fetches billing from the API, allocates CKUs 70/30, uses Telemetry API
  metrics for per-principal network attribution
- On-prem: constructs billing from Prometheus metrics and your rates, discovers
  principals from JMX labels plus a static entry for a batch job that rarely
  appears in metrics
- Both tenants emit to CSV and (CCloud only) Prometheus

# Data Flow

## Pipeline overview

```mermaid
flowchart TD
    subgraph inputs["Data Sources"]
        BILLING[("Billing API<br/>(CCloud)")]
        YAML[("YAML Cost Model<br/>(Self-managed)")]
        PROM[("Prometheus")]
        API[("Resource APIs")]
    end

    subgraph gather["Phase 1: Gather"]
        G1["Gather Billing"]
        G2["Gather Resources"]
        G3["Gather Identities"]
    end

    subgraph resolve["Phase 2: Resolve & Allocate"]
        M["Fetch Metrics"]
        R["Resolve Identities"]
        A["Allocate Costs"]
    end

    subgraph output["Phase 3: Output"]
        DB[("Storage<br/>SQLite")]
    end

    subgraph emit["Phase 4: Emit (post-pipeline)"]
        ER["EmitterRunner"]
        E["Emitters<br/>(CSV, Prometheus, etc.)"]
    end

    BILLING --> G1
    YAML --> G1
    API --> G2
    API --> G3
    PROM --> M

    G1 --> |BillingLineItem| M
    G2 --> |Resource| R
    G3 --> |Identity| R
    M --> |MetricRow| R
    R --> |IdentityResolution| A
    A --> |ChargebackRow| DB
    DB --> ER
    ER --> E
```

## Pipeline steps per date

1. **Gather billing** — `CostInput.gather(tenant_id, start, end, uow)`
   Returns `BillingLineItem` objects. CCloud fetches from billing API. Self-managed/generic
   constructs from YAML cost model + Prometheus.

2. **Gather resources** — `handler.gather_resources(tenant_id, uow)`
   discovers ordinary infrastructure resources. A separate plugin supplemental
   gather acquires the Confluent organization used as Preview billing-account
   authority. All are stored in `resources`.

3. **Gather identities** — `handler.gather_identities(tenant_id, uow)`
   Discovers principals, service accounts, teams.
   Stored in `identities` table.

4. **Detect deletions** — resource deletion authority is tracked per declared
   resource type. A type is scanned only when every handler declaring that type
   succeeded; IDs yielded under an undeclared type may be persisted but are
   never deletion authority. Identity deletion remains skipped after any
   handler failure. Supplemental organization reconciliation is isolated from
   both paths. Consecutive zero-gather thresholds prevent transient bulk
   deletion.

5. **Fetch metrics** — `metrics_source.query_range(...)` per handler
   Prometheus range queries for the billing period. Returns `MetricRow` objects.

6. **Resolve identities** — `handler.resolve_identities(tenant_id, resource_id, ...)`
   Maps billing line items to identities using metrics data.
   Returns `IdentityResolution` (list of `(identity_id, weight)` pairs).

7. **Allocate** — `allocator(AllocationContext) → AllocationResult`
   Splits cost across identities using configured strategy.
   UNALLOCATED identity used for unresolved costs.

8. **Commit** — `ChargebackRow` records written to storage.

The pipeline loop ends at step 8. Topic overlay (step 9) is a separate pass over completed dates.

9. **Topic overlay** *(CCloud only, optional)* — `TopicAttributionPhase.run(uow, date)`
   Runs after chargeback calculation. For each Kafka billing line item, queries
   Prometheus for per-topic byte metrics and splits the cluster cost across
   active topics. Results are written to `topic_attribution_facts`. Enabled via
   `plugin_settings.topic_attribution.enabled: true`. If Prometheus returns
   all-zero data, the `missing_metrics_behavior` setting controls the fallback
   (even-split or skip). If Prometheus is unreachable (infrastructure failure),
   the date stays pending and the pipeline retries on the next run. After
   `topic_attribution_retry_limit` consecutive failures for a cluster, sentinel
   rows are written (`topic_name=__UNATTRIBUTED__`, `attribution_method=ATTRIBUTION_FAILED`)
   preserving full cost, and the date is marked calculated.

10. **Emit (post-pipeline)** — `EmitterRunner` runs after each pipeline cycle completes.
   It queries storage for pending dates (not yet emitted, or previously failed, within
   each emitter's `lookback_days` window) and dispatches to each configured emitter.
   Outcome records (`emitted`, `failed`, `skipped`) are persisted per tenant/emitter/date,
   so already-emitted dates are not re-sent on the next cycle.

## Storage schema

| Table | Purpose |
|---|---|
| `billing` | Raw billing line items (composite PK: ecosystem, tenant_id, timestamp, resource_id, product_type, product_category) |
| `resources` | Discovered infrastructure resources with `created_at`, `deleted_at`, `last_seen_at` |
| `identities` | Discovered principals/service accounts with lifecycle timestamps |
| `chargeback_dimensions` | Unique (identity, resource, product, cost_type) combinations — the "what" |
| `chargeback_facts` | Cost amounts linked to dimensions via `dimension_id` — the "how much" |
| `pipeline_state` | Per-date progress flags plus the successful chargeback calculation ID, completion time, and optional owning-run provenance used by Preview |
| `topic_attribution_dimensions` | Unique (cluster, topic, product_type, attribution_method) combinations |
| `topic_attribution_facts` | Per-topic cost amounts linked to dimensions via `dimension_id` |
| `pipeline_runs` | Audit trail: run start/end, status, rows written, errors |
| `preview_requests` | Tenant-scoped Daily Full Preview lifecycle, diagnostics, source snapshot, and public artifact metadata (never server paths) |
| `custom_tags` | User-defined key/value tags attached to chargeback dimensions |
| `emission_records` | Per-tenant/emitter/date emission outcome tracking (emitted, failed) with attempt count |

Each row is scoped to `(ecosystem, tenant_id)`. No cross-tenant data access.

## Pipeline state tracking

The `pipeline_state` table enables resumption and prevents re-processing. The calculate
phase only processes dates where billing and resources are gathered but chargebacks not
yet calculated. When new billing data arrives for recent dates, the recalculation window
re-clears the `chargeback_calculated` flag so those dates get reprocessed.

The calculate phase writes `calculation_id`, `calculation_completed_at`, and
optional `calculation_run_id` in the same per-date transaction as the chargeback
rows. Preview uses the per-date identity and completion time as success authority;
the global `pipeline_runs` status is audit provenance and does not invalidate a
date that already committed.

## FOCUS Mapping Preview read path

```mermaid
flowchart LR
    PS[(Persisted pipeline state)] --> PR[Preview read transaction]
    EV[(Persisted source and allocation evidence)] --> PR
    ORG[(Persisted provider organization)] --> PR
    PR --> READY[Classify and apply native-line readiness]
    READY --> MAP[Reconcile, map, and validate Daily Full v3]
    MAP --> ART[(Atomic local artifact package)]
    ART --> API[Protected Preview API]
    API --> UI[Web UI]
    API --> CLI[Remote CLI]
```

Preview is read-only with respect to collected business data. It does not call a
provider, start a gather/calculation run, infer missing historical calculation
metadata, or expose an edit/backfill path. Migrated calculated dates without
usable correlation remain unchanged and produce a non-retryable metadata
diagnostic. Only the ordinary collector and calculation lifecycle can later
replace persisted data.

At submission, Preview samples `created_at` once and derives an immutable policy
from tenant `focus_preview` configuration plus `lookback_days`/`cutoff_days`.
The worker checks, in order: calculation correlation, acquisition/cutoff
lifecycle, Direct-billed PAYG effective containment, configured USD, complete
streamed structural/classification/financial evaluation, and source-issue
precedence. It then rejects organization-wide sources with
`PreviewMappingScopeError`, TABLEFLOW with
`PreviewProviderContextIncompleteError`, and TASK-254.05 lineage-deferred native
types with `PreviewMappingScopeError`. Only production-ready resource-specific
types continue through complete source/aggregate coverage, one-source
cardinality, bounded aggregate/allocation candidates, currency compatibility,
reconciliation, immutable organization binding, identity/environment/provider
context, 65/12 v3 row validation, and atomic artifact finalization. All evidence
reads occur in one read-only transaction. Complete source and aggregate reads
stream in stable origin order; only the selected source's candidate queries
retain their two-row ambiguity bounds.

Expected failures travel through the initialized diagnostic path and atomically
mark the request failed without a source snapshot or package. Source diagnostics
can persist up to 20 sorted, unique, opaque tenant-scoped correlations; raw
provider identities and payload fields never enter the public diagnostic.

Confluent's Costs API currently omits per-record ISO currency. Configured/default
USD establishes the eligible commercial contract, but it does not become source
evidence: mapped `BillingCurrency` remains null and the manifest records
`provider_billing_currency_field_unavailable`. No currency conversion occurs.
The maximum 364-day `lookback_days` is an acquisition/recalculation boundary,
not retention or a reconstruction promise. TASK-256 owns any independent
longer-term completed-chargeback archive.

The readiness table has a closed 16-ready/13-deferred partition. TASK-254.05
lineage defers `KAFKA_REST_PRODUCE`, `KAFKA_STREAMS`,
`CONNECT_NUM_RECORDS`, all Cluster Linking types, `USM_CONNECTED_NODE`, and
every `PROMO_CREDIT` row. Organization-wide `AUDIT_LOG_READ`/`SUPPORT` and all
TABLEFLOW types also remain non-ready, with the distinct typed routes described
above. Complete semantic mapping is not a conformance or allocation-readiness
claim.

## Concurrency

Multiple tenants run concurrently (bounded by `features.max_parallel_tenants`).
One orchestrator per tenant. Thread-safe via per-tenant `TenantRuntime` isolation.

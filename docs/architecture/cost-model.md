# How Costs Work

This page explains the complete cost lifecycle: how raw billing data enters the
system, how costs are computed, how they're allocated across identities, and how
rounding is handled. Every formula shown here maps directly to code in the engine.

## Two billing paradigms

The engine supports two fundamentally different ways of obtaining cost data.

### API-billed (Confluent Cloud)

The vendor has already computed the bill. Each billing line item arrives with
`quantity`, `unit_price`, and `total_cost` pre-calculated. The engine's job
is purely to *allocate* that cost across identities — it never modifies the
total.

```
Vendor Billing API
    → BillingLineItem(product_type="KAFKA_NUM_CKU", total_cost=$100.00)
    → Engine allocates $100.00 across identities
    → Sum of all ChargebackRows = $100.00 (guaranteed)
```

### Constructed (Self-Managed Kafka, Generic Metrics)

There is no billing API. The engine *constructs* billing lines by:

1. Querying Prometheus for usage metrics
2. Computing quantities from those metrics
3. Multiplying by rates you define in YAML

```
Prometheus + YAML rates
    → Engine computes: quantity × rate = total_cost
    → BillingLineItem(product_type="SELF_KAFKA_COMPUTE", total_cost=$36.00)
    → Engine allocates $36.00 across identities
```

---

## Constructed cost math

### Compute costs (fixed quantity)

Compute is a fixed cost based on the number of brokers (or instances). There is
no Prometheus query — the quantity comes from your config.

```
daily_cost = broker_count × 24 hours × compute_hourly_rate
```

| Input | Value |
|---|---|
| `broker_count` | 3 |
| `compute_hourly_rate` | $0.50 |

```
daily_cost = 3 × 24 × $0.50 = $36.00
```

One `BillingLineItem` per day with `product_type = SELF_KAFKA_COMPUTE`.

### Storage costs (gauge metric, averaged)

Storage is measured as a point-in-time value (a Prometheus gauge). The engine
queries `kafka_log_log_size` across the day, averages all samples, converts
bytes to GiB, and charges per GiB-hour.

```
avg_bytes = mean(all prometheus samples for the day)
avg_gib   = avg_bytes ÷ 1,073,741,824
daily_cost = avg_gib × 24 hours × storage_per_gib_hourly
```

| Input | Value |
|---|---|
| Prometheus samples (3 in this example) | 107,374,182,400 bytes (100 GiB), 112,742,891,520 bytes (105 GiB), 107,374,182,400 bytes (100 GiB) |
| `storage_per_gib_hourly` | $0.0001 |

```
avg_bytes  = (107,374,182,400 + 112,742,891,520 + 107,374,182,400) ÷ 3
           = 109,163,752,107 bytes
avg_gib    = 109,163,752,107 ÷ 1,073,741,824 = 101.67 GiB
daily_cost = 101.67 × 24 × $0.0001 = $0.2440
```

!!! note "Why average, not sum?"
    Storage is not cumulative. A disk holding 100 GiB at hour 0 and 100 GiB at
    hour 1 does not mean you used 200 GiB — you used 100 GiB for 2 hours.
    Averaging across samples gives the representative GiB held over the day.

### Network costs (counter metric, summed)

Network bytes are cumulative counters. The engine queries
`kafka_server_brokertopicmetrics_bytesin_total` (and `bytesout`) using
`increase()` over 1-hour windows, then sums all the hourly deltas to get the
total bytes transferred in a day.

```
total_bytes = sum(all hourly increase values for the day)
total_gib   = total_bytes ÷ 1,073,741,824
daily_cost  = total_gib × network_per_gib
```

| Input | Value |
|---|---|
| Hourly increases (24 values) | Sum = 53,687,091,200 bytes |
| `network_ingress_per_gib` | $0.01 |

```
total_gib  = 53,687,091,200 ÷ 1,073,741,824 = 50.0 GiB
daily_cost = 50.0 × $0.01 = $0.50
```

!!! note "Why sum, not average?"
    Network bytes are a running total. `increase()` gives bytes transferred in
    each hour. Summing gives total daily transfer. This is the actual volume
    that crossed the network.

### Generic metrics: the three quantity types

The generic plugin generalizes the above patterns:

| Quantity type | Math | Rate unit |
|---|---|---|
| `fixed` | `count × 24 × rate` | $/instance/hour |
| `storage_gib` | `avg(query) ÷ 2^30 × 24 × rate` | $/GiB/hour |
| `network_gib` | `sum(increase(query)) ÷ 2^30 × rate` | $/GiB |

---

## Cost allocation

Once a billing line exists (either from the vendor API or constructed), the
engine allocates it across identities. This section explains the strategies
and the fallback chain.

### Even split

Divides the cost equally among a set of identities. Used for shared
infrastructure costs where no per-identity usage metric exists.

```
per_identity = total_cost ÷ identity_count
```

**Rounding:** All amounts are quantized to 4 decimal places ($0.0001). After
dividing, the engine distributes any rounding remainder one unit at a time
across the leading identities to guarantee the sum equals the total.

**Example:** $10.00 across 3 identities

```
base = $10.0000 ÷ 3 = $3.3333 (quantized to 4 decimals)
sum  = $3.3333 × 3 = $9.9999
diff = $10.0000 - $9.9999 = $0.0001

Final:
  identity-a: $3.3334  (+$0.0001 remainder)
  identity-b: $3.3333
  identity-c: $3.3333
  total:      $10.0000 ✓
```

### Usage ratio

Divides the cost proportionally to a usage metric (e.g., bytes produced).
Used for network costs and other usage-driven costs.

```
ratio_i    = usage_i ÷ total_usage
amount_i   = total_cost × ratio_i
```

**Rounding:** Same approach — quantize each amount, then distribute remainder.

**Example:** $100.00 split by bytes

| Identity | Bytes | Ratio | Raw amount | After rounding |
|---|---|---|---|---|
| team-a | 500 GiB | 0.500 | $50.0000 | $50.0000 |
| team-b | 300 GiB | 0.300 | $30.0000 | $30.0000 |
| team-c | 200 GiB | 0.200 | $20.0000 | $20.0000 |
| **Total** | **1000 GiB** | **1.000** | **$100.0000** | **$100.0000** |

When ratios don't divide evenly:

| Identity | Bytes | Ratio | Raw amount | Quantized | Adjusted |
|---|---|---|---|---|---|
| team-a | 333 | 0.3333... | $33.3333... | $33.3333 | $33.3334 |
| team-b | 333 | 0.3333... | $33.3333... | $33.3333 | $33.3333 |
| team-c | 334 | 0.3340... | $33.4000... | $33.3334 | $33.3333 |
| **Total** | **1000** | | **$100.0000** | **$99.9999** | **$100.0000** |

The $0.0001 remainder goes to the first identity. The sum always equals the
input.

---

## The allocation chain (CostType tagging)

Every chargeback row carries a `cost_type` field: either `USAGE` or `SHARED`.
This isn't just a label — it indicates *why* the cost was allocated that way.

- **USAGE** — Allocated based on actual measured consumption (bytes, CFUs, queries)
- **SHARED** — Allocated because no per-identity usage metric was available, so the
  cost was spread evenly as a shared infrastructure charge

### Tiered fallback (ChainModel)

Most allocators use a tiered fallback chain. The engine tries each tier in order
and uses the first one that succeeds (has data to work with).

```
Tier 0: Usage ratio (metrics available?)
    ↓ no metrics
Tier 1: Even split across active identities (identities known?)
    ↓ no active identities
Tier 2: Even split across all tenant identities for the period
    ↓ no identities at all
Tier 3: Terminal — allocate to the resource ID itself
```

Each chargeback row records which tier was used in the `allocation_detail` field.
Common values:

| Detail | Meaning |
|---|---|
| `USAGE_RATIO_ALLOCATION` | Tier 0 succeeded — proportional allocation by metrics |
| `NO_METRICS_LOCATED` | Tier 0 failed — no metrics available, fell back to even split |
| `NO_ACTIVE_IDENTITIES_LOCATED` | Tier 1 failed — no active identities, used period-wide set |
| `NO_IDENTITIES_LOCATED` | All tiers failed — allocated to resource or UNALLOCATED |
| `NO_USAGE_FOR_ACTIVE_IDENTITIES` | Metrics exist but all values are zero |

### Composite allocation (CKU model)

Some costs use a hybrid model that splits the cost into portions before allocating.
The Kafka CKU model is the primary example:

```
Total CKU cost: $100.00
    ├── 70% ($70.00) → Usage ratio chain (bytes in + bytes out)
    │   ├── Tier 0: proportional by bytes → USAGE rows
    │   ├── Tier 1: even split (active) → SHARED rows
    │   └── ...
    └── 30% ($30.00) → Shared chain (even split)
        ├── Tier 0: even split (active) → SHARED rows
        └── ...
```

The ratios are configurable via `allocator_params.kafka_cku_usage_ratio` and
`kafka_cku_shared_ratio`. Each portion runs its own fallback chain independently.

**Example output** (3 identities, $100 CKU cost, 70/30 split):

| Identity | Usage portion (70%) | Shared portion (30%) | Total |
|---|---|---|---|
| team-a (50% of bytes) | $35.00 (USAGE) | $10.00 (SHARED) | $45.00 |
| team-b (30% of bytes) | $21.00 (USAGE) | $10.00 (SHARED) | $31.00 |
| team-c (20% of bytes) | $14.00 (USAGE) | $10.00 (SHARED) | $24.00 |
| **Total** | **$70.00** | **$30.00** | **$100.00** |

Each row's `metadata` includes `composition_index` (0 = usage portion, 1 = shared
portion) and `composition_ratio` (0.70 or 0.30) for auditability.

---

## The UNALLOCATED identity

When the engine cannot attribute a cost to any known identity, it allocates to
a synthetic identity called `UNALLOCATED` with `identity_type = "system"`.

This happens when:

- **No identities exist** for a resource in any scope (resource_active, metrics_derived, tenant_period)
- **Allocation fails after retries** — the allocation retry limit was exceeded
- **Org-wide costs** — costs like `AUDIT_LOG_READ` and `SUPPORT` that apply to the
  entire organization, not a specific resource

UNALLOCATED is automatically excluded from tenant-period identity sets, so it
never receives costs via even-split. It only receives costs through the terminal
tier of a chain or through explicit org-wide allocation.

!!! note "UNALLOCATED is not an error"
    For org-wide costs, allocating to UNALLOCATED is the correct behavior. For
    resource-specific costs, it indicates missing identity data — check your
    identity source configuration and the `allocation_detail` field on the
    chargeback rows.

---

## Active fraction adjustment

Before allocation, costs are adjusted for resource lifecycle. If a resource was
only active for part of the billing window (e.g., it was created mid-day or
deleted mid-day), the engine computes an `active_fraction` and adjusts the
cost proportionally.

```
active_fraction = active_hours ÷ billing_window_hours
split_amount    = total_cost × active_fraction
```

A resource created at noon on a daily billing window has `active_fraction = 0.5`,
so only half the daily cost is allocated. The remaining half stays with the
billing line (it was incurred but not attributable to this resource for the
full window).

---

## Precision guarantees

All cost arithmetic uses Python `Decimal` with 4-decimal precision (`$0.0001`).
Floating-point is never used for money.

**Guarantees:**

1. **Sum-to-total** — The sum of all ChargebackRows for a billing line always
   equals the input `split_amount` (which is `total_cost × active_fraction`).
   Rounding remainders are distributed deterministically.

2. **Deterministic** — Given the same inputs (billing lines, identities, metrics),
   allocation produces identical output. The remainder distribution is
   order-dependent (first identity gets the extra cent), and identity lists are
   sorted before allocation.

3. **Auditable** — Every row includes `allocation_method` (even_split, usage_ratio,
   terminal), `allocation_detail` (which tier fired), and `metadata` (chain_tier,
   composition_index, composition_ratio, usage ratio per identity).

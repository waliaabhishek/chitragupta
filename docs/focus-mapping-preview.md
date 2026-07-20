# FOCUS Mapping Preview

FOCUS Mapping Preview creates an immutable, explicitly non-conforming FOCUS 1.4
Cost and Usage package from data Chitragupta has already collected and
calculated. The current tracer supports Confluent Cloud, Daily grain, and the
Full column profile.

Preview is an exposition path, not a data-editing path. Submitting a request:

- reads the latest persisted successful calculation for every requested date;
- does not call Confluent Cloud or trigger the pipeline;
- does not edit, backfill, approve, or repair collected data; and
- produces no partial package when the persisted scope is unsupported or
  incomplete.

Run the API in `api` or `both` mode and configure durable artifact storage before
using Preview. See [Configuration](configuration/index.md#focus-mapping-preview)
and the [API Reference](api-reference.md#focus-mapping-preview).

## Tenant eligibility configuration

The tenant-level `focus_preview` block is optional so existing configurations
continue to load. Omission does not make a tenant eligible: a submitted request
fails asynchronously with `preview_commercial_profile_unavailable`.

```yaml
tenants:
  production:
    focus_preview:
      commercial_profile: direct_payg
      billing_currency: USD
      effective_start_date: 2026-01-01
      effective_end_date: 2027-01-01
```

When the block is present, `commercial_profile` must be `direct_payg` and the
effective dates define a non-empty half-open interval that contains the complete
request. `billing_currency` defaults to `USD`; surrounding whitespace and case
are normalized. A valid non-USD value can load configuration, but Preview fails
closed with `preview_billing_currency_unsupported`. There is no currency
conversion or relabeling.

Confluent's Costs API currently does not return a per-record ISO currency value.
Configured/default USD is therefore the supported commercial contract, not a
claim that the provider supplied record-level currency. The generated FOCUS
`BillingCurrency` remains null and the manifest reports
`provider_billing_currency_field_unavailable`. A future provider currency field
can close that limitation without silently rewriting historical values.

## Request scope

A request uses inclusive-start, exclusive-end UTC dates and must contain 1–31
days within one UTC calendar month. The exclusive end may be the first day of
the following month. A ready package may contain multiple persisted billing
origins and every actual allocation portion produced for those origins. Each
origin must have exactly one accepted raw Cost source, complete calculation
lineage, and exact cost and quantity reconciliation. Multiple native/tier Cost
sources mapped to one billing origin remain outside the current scope.

The package contains:

- `manifest.json`, a Chitragupta manifest declaring target FOCUS version 1.4,
  `non_conforming` status, source coverage, validation, reconciliation, file
  metadata, and known gaps; and
- `cost-and-usage.csv`, one deterministic UTF-8 Daily Full data file.

The manifest and CSV are stored as immutable bytes. API responses expose public
filenames, sizes, SHA-256 values, and download URLs, but never the artifact root,
opaque storage key, or server filesystem path.

## Daily Full v4 mapping

`focus-1.4-daily-full-v4` is the single code-owned mapping profile. It defines
all 65 FOCUS 1.4 Full columns and 12 ordered custom evidence columns, including
feature level, applicability, nullability, source, transformation, allowed
values, validator, and gap ownership. Startup validation rejects incomplete,
overlapping, reordered, or inconsistent profile/readiness definitions. Every
row is validated against the complete profile before artifact finalization.
Passing this validation means only that the row matches this declared Preview
profile; the manifest remains `non_conforming`.

Version 4 resolves the prior `allocation_lineage_and_tag_projection_pending`,
`allocation_ratio_deferred`, and `allocation_method_version_deferred` gaps.
Persisted method details, method version, realized ratio, origin tags, and
Allocation Target tags are now mapped and validated.

The ordinary pipeline gathers the Confluent organization from
`GET /org/v2/organizations` as isolated supplemental inventory. Preview reads
the persisted immutable binding: `BillingAccountId` is the provider
organization ID, `BillingAccountName` is its optional display name, and
`BillingAccountType` is `Organization`. The tenant ID is only Chitragupta's
partition key and is never used as a billing account. Missing or conflicting
organization authority fails the request; Preview does not call the provider.
The containing UTC month supplies `BillingPeriodStart` and
`BillingPeriodEnd`.

Charge and financial behavior is fail-closed:

- ordinary metered usage is `Usage` / `Usage-Based`;
- Support is `Purchase` / `Recurring`;
- a non-refund native `PROMO_CREDIT` is `Credit` / `One-Time`, including when
  the provider supplies null product, unit, price, and quantity; those native
  nulls remain null in source evidence and output;
- an unambiguous refund retains the original native product's `Usage` /
  `Usage-Based` or `Purchase` / `Recurring` semantics, including signed cost,
  price, and quantity values that reconcile exactly; and
- ambiguous classification, invalid signs, non-finite numbers, or failed exact
  `Decimal` arithmetic blocks the whole request.

For each ready allocation portion, its persisted cost supplies `BilledCost` and
`EffectiveCost`; source `original_amount` multiplied by the persisted realized
allocation ratio supplies `ListCost` and `ContractedCost`. Price, allocated
quantity, unit, and tier evidence are emitted only when their arithmetic and
charge semantics are valid. `SkuId` and
`SkuPriceId` are deterministic namespaced v1 hashes over closed canonical
component schemas; `SkuPriceDetails` and `x_ChitraguptaSkuComponents` preserve
the exact canonical components. These values are Chitragupta-derived, not
provider-issued SKU authority.

`InvoiceId`, `InvoiceDetailId`, and `InvoiceIssuerName` remain null. Provider
cloud and region inventory is retained twice: normalized operational fields
continue to serve allocation code, while Preview copies the exact raw
`provider_cloud` into `HostProviderName` and raw `provider_region` into
`RegionId`. It does not trim, title-case, canonicalize, or synthesize display
names. `RegionName` remains null.

The persisted `CCloudBillingLineItem` is the sole allocation origin.
`CalculatePhase` records lineage from that existing billing natural key to the
actual chargeback portions it produced, including exact cost, derived quantity,
realized ratio, target kind, method, calculation identity, and completion time.
It does not reconstruct billing from chargebacks, redistribute costs, synthesize
a remainder, or change handlers, allocators, metrics, identity resolution, or
generic exports.

Raw Confluent Cost rows remain the classification and coverage authority. Each
row stores a lossless association to its already mapped billing key while
retaining native values independently. Migration 021 adds this association and
Confluent-owned lineage run/portion storage. Migrated rows with no association
or calculation lineage are not guessed or backfilled: regather the ordinary
provider source and run the ordinary calculation before retrying Preview.

All currently accepted native line types can consume persisted lineage.
Organization-wide `AUDIT_LOG_READ`, `SUPPORT`, and promotional credits retain
null origin resource/provider fields. `TABLEFLOW_DATA_PROCESSED`,
`TABLEFLOW_NUM_TOPICS`, and `TABLEFLOW_STORAGE` still fail
`preview_provider_context_incomplete` because current inventory cannot prove
provider-authoritative TABLEFLOW context. Multiple native/tier sources under one
billing origin still fail `preview_mapping_scope_unsupported`; Preview never
invents a native-origin allocation matrix.

An actual `UNALLOCATED` portion is emitted with null `AllocatedResourceId`,
`AllocatedResourceName`, and `AllocatedTags`. Other actual identity/resource
targets retain their typed IDs and names. `Tags` contains package-time origin
resource tags, while `AllocatedTags` contains package-time target tags; the two
sets are never overlaid. They are frozen into the stored CSV bytes, so later tag
edits cannot change an already-ready package.

## Web UI

Open **FOCUS Mapping Preview** from the sidebar (`/focus-preview`). The page:

- uses the currently selected tenant;
- fixes the request to Daily grain and the Full column profile;
- initially selects the complete current UTC month, from its first day through
  the exclusive first day of the next month;
- submits one asynchronous request and polls queued/running state;
- displays the persisted diagnostic message and retryability on failure; and
- downloads the stored manifest and CSV through the configured API origin.

The page declares these current authority gaps before submission:

| Code | Gap | Owner |
|---|---|---|
| `provider_billing_currency_field_unavailable` | The provider Costs API omits a per-record ISO currency; `BillingCurrency` remains null | TASK-254.03 |
| `invoice_identity_unavailable` | Post-issuance invoice identity | TASK-254.04 |
| `invoice_issuer_name_unavailable` | Provider legal invoice-issuer evidence is unavailable | TASK-254.04 |
| `provider_host_display_name_unavailable` | `HostProviderName` contains the raw provider cloud code, not a provider display name | TASK-254.04 |
| `provider_region_display_name_unavailable` | Confluent inventory does not provide a distinct region display name; `RegionName` remains null | TASK-254.04 |
| `derived_sku_identity_not_provider_authoritative` | Deterministic SKU values are Chitragupta-derived, not provider-issued identifiers | TASK-254.04 |

Request, polling, and download transport failures produce a generic safe UI
error. Cancelling or leaving the page aborts polling without showing an error.
The UI does not generate CSV, map fields, run the collector, or offer a data
editing/repair control.

## Remote CLI

`chitragupta-preview` is an HTTP client for a remote Chitragupta API. Include the
API version prefix in `--api-url`:

```bash
chitragupta-preview daily-full \
  --api-url https://chitragupta.example/api/v1 \
  --tenant production \
  --start-date 2026-07-01 \
  --end-date 2026-08-01 \
  --output-dir ./focus-preview \
  --header 'Authorization=Bearer <token>'
```

Repeat `--header NAME=VALUE` for every header required by the deployment's
reverse proxy or API gateway. Duplicate names are preserved. The CLI forwards
all supplied headers on submission, every status poll, and every artifact
download. Header values and response bodies are omitted from HTTP errors.

The CLI polls once per second until the request is ready or failed. On success it
creates the local output directory and writes `manifest.json` and
`cost-and-usage.csv`. On a failed request it writes:

```text
Preview failed [<code>]: <message>
Source correlation: src:v1:<hash>
```

and exits non-zero. It does not automatically resubmit, run the pipeline, or
change server data. API-provided cross-origin download URLs are rejected before
credentials are forwarded. The correlation line appears only when the API
returns safe source correlations; raw provider identifiers are never printed.

## Lifecycle and diagnostics

The persisted lifecycle is:

```text
queued -> running -> ready
                  -> failed
queued ----------------> failed
```

`ready` is recorded only after the complete artifact directory has been
atomically finalized. The status response includes the exact per-date
calculation identity and completion time used, plus `source_through`, which is
the end of the provider collection window for the accepted source evidence.

Calculation coverage failures are:

| Code | Retryable | Meaning |
|---|---:|---|
| `calculation_metadata_unavailable` | no | A calculated date lacks usable persisted calculation identity/completion metadata. No edit or repair action is implied. |
| `calculation_before_acquisition_lookback` | no | Required retained calculation evidence is outside the current acquisition window. Increasing lookback or reconstructing provider data is not promised. |
| `calculation_pending_cutoff_window` | yes | At least one missing date is still inside the recent cutoff window; wait for it to enter the acquisition window, run the ordinary pipeline, and retry. |
| `calculation_unavailable` | yes | No requested date has a usable persisted calculation; run the ordinary pipeline and retry. |
| `calculation_coverage_incomplete` | yes | Some, but not all, requested dates have usable persisted calculations; run the ordinary pipeline and retry. |

Commercial/currency policy precedes a complete streamed source scan. Structural,
classification, and financial source issues are resolved globally before keyed
TABLEFLOW provider context, complete source/aggregate coverage, and the current
one-source-per-billing-origin cardinality gate. Aggregate currency checks and
source/aggregate equality precede complete lineage run/portion structure; every
origin is structurally valid before any allocation cost/quantity total is
reconciled. Input ordering cannot change the winning diagnostic.

Missing or invalid persisted lineage fails non-retryably with
`preview_allocation_lineage_incomplete`. A source/aggregate mismatch or any
allocation cost/quantity shortfall or overage fails non-retryably with
`preview_source_reconciliation_failed`. A legacy source without the complete
billing association fails `preview_source_coverage_incomplete`.
Where a source can be implicated, the API/UI/CLI can display up to 20 sorted,
unique safe `src:v1:<64 lowercase hex>` correlations. Correlations are
tenant-scoped hashes and contain no provider IDs or raw source values. Failed
requests have no source snapshot, package, staging path, or downloadable
artifact.

`lookback_days` remains capped at 364. It controls the current provider
acquisition/recalculation window and Preview's lifecycle classification; it is
not retention, an archive, or a guarantee that billing plus Metrics API evidence
can be reconstructed. `retention_days` is a separate current-data cleanup
setting. Independent multi-year completed-chargeback retention/archive is owned
by TASK-256 and is outside this release.

## Current boundaries

This release does not provide Preview data editing, approval, manual metadata
correction, request-triggered collection, historical correlation backfill,
partial output, request listing, automatic expiry, or a Download All archive.
It does not add TABLEFLOW provider authority, support native tier/multi-source
projection under one billing origin, or claim FOCUS 1.4 conformance. The generic
chargeback export remains a separate API and is unchanged.

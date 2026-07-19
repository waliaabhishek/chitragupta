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
the following month. The current tracer requires exactly one supported ordinary
metered source, one compatibility aggregate, and one non-`UNALLOCATED`
Allocation Target that reconcile exactly.

The package contains:

- `manifest.json`, a Chitragupta manifest declaring target FOCUS version 1.4,
  `non_conforming` status, source coverage, validation, reconciliation, file
  metadata, and known gaps; and
- `cost-and-usage.csv`, one deterministic UTF-8 Daily Full data file.

The manifest and CSV are stored as immutable bytes. API responses expose public
filenames, sizes, SHA-256 values, and download URLs, but never the artifact root,
opaque storage key, or server filesystem path.

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
| `billing_account_and_issuer_mapping_pending` | Billing account and issuer mapping | TASK-254.04 |
| `billing_period_authority_pending` | Authoritative provider billing period | TASK-254.04 |
| `provider_billing_currency_field_unavailable` | The provider Costs API omits a per-record ISO currency; `BillingCurrency` remains null | TASK-254.03 |
| `provider_authoritative_sku_identity_unavailable` | Provider-authoritative SKU identity | TASK-254.04 |
| `invoice_identity_unavailable` | Post-issuance invoice identity | TASK-254.04 |
| `allocation_lineage_and_tag_projection_pending` | Allocation lineage and tag projection | TASK-254.05 |
| `task_254_04_applicability_and_provider_mapping_pending` | Provider applicability and remaining provider mapping | TASK-254.04 |

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

Commercial/currency failures then precede a complete streamed source scan.
Source diagnostics distinguish malformed, out-of-scope, ambiguous charge,
unknown/unsupported line type, mapping-unavailable, incomplete, unsupported
economics, reconciliation, coverage, and current mapping-cardinality failures.
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
The generic chargeback export remains a separate API and is unchanged.

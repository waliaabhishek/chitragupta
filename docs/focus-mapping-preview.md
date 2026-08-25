# FOCUS Mapping Preview

FOCUS Mapping Preview turns persisted Confluent Cloud billing and allocation
data into an immutable Cost and Usage package using the FOCUS 1.4 vocabulary.
Packages are marked `non_conforming` because some provider-authoritative fields
are unavailable.

Preview reads data already stored by Chitragupta. Package generation does not
call Confluent Cloud, run the pipeline, or recalculate allocations. Run the
ordinary pipeline first, then create a requested package or use a published
monthly revision.

For a runnable setup and representative output, see the
[FOCUS Preview example](https://github.com/waliaabhishek/chitragupta/tree/main/examples/focus-preview).

## Supported scope

| Capability | Current support |
|---|---|
| Provider | Confluent Cloud |
| Commercial profile | Direct-billed PAYG |
| Billing currency | USD; no currency conversion |
| Target vocabulary | FOCUS 1.4 |
| Conformance | `non_conforming` |
| Reporting grain | Daily or Monthly |
| Column profiles | Full, Summary, or Custom |
| Delivery | Requested packages and published monthly revisions |

Preview is opt-in per tenant. Omitting `focus_preview` leaves the tenant's
ordinary billing, chargeback, export, and emitter workflows unchanged.

## Configure Preview

Configure a durable artifact root and declare the eligible commercial interval
for each enabled tenant:

```yaml
preview:
  artifact_root: /var/lib/chitragupta/focus-preview

tenants:
  production:
    ecosystem: confluent_cloud
    focus_preview:
      commercial_profile: direct_payg
      billing_currency: USD
      effective_start_date: 2026-01-01
      # Optional exclusive end:
      # effective_end_date: 2027-01-01
```

`effective_start_date` is inclusive. `effective_end_date` is exclusive; when it
is omitted, each request, publication cycle, or repair resolves its own end from
the operation's UTC date. The commercial interval does not widen acquisition,
cutoff, or retention windows.

When API and worker processes are separate, both must use the same tenant
database and artifact root. Serve downloads through the API rather than exposing
the artifact directory directly.

Configuration ownership is split by subject:

- [Process-wide Preview settings](configuration/index.md#focus-mapping-preview)
  defines artifact storage, workers, queues, spool limits, and CSV part sizing.
- [Confluent Cloud Preview eligibility](configuration/ccloud-reference.md#focus-mapping-preview-eligibility)
  defines tenant fields and validation.
- [Deployment](operations/deployment.md#focus-mapping-preview-boundary) covers
  authentication, shared storage, process modes, and capacity sizing.

## Prepare source data

Run one pipeline cycle before requesting a package:

```bash
uv run python src/main.py --config-file config.yaml --run-once
```

For a continuously running worker and API:

```bash
uv run python src/main.py --config-file config.yaml --mode both
```

Preview requires successful persisted calculation metadata, raw Confluent Cost
records, billing rows, allocation lineage, organization inventory, and the
resource or identity evidence needed by the selected interval. Missing or
inconsistent evidence fails the complete Preview operation; it does not produce
a partial package.

The periodic worker also publishes eligible monthly revisions. `--run-once` and
ad-hoc requests do not publish revisions.

## Create a requested package

Requested packages are asynchronous and remain downloadable for seven days
after they become ready.

### Web UI

Open **FOCUS Mapping Preview** at `/focus-preview`, select a tenant, reporting
period, and column profile, then submit the request. The page shows request
status, source freshness, diagnostics, expiry, and package downloads.

The UI also lists recent requests and published monthly revisions. Existing
ready packages remain downloadable while a repair or retention cleanup needs
attention.

### Remote CLI

`chitragupta-preview` is an HTTP client. Include `/api/v1` in `--api-url` and
use repeatable `--header NAME=VALUE` options for credentials supplied by your
reverse proxy or API gateway.

Create and download a Monthly Summary package:

```bash
uv run chitragupta-preview request \
  --api-url https://chitragupta.example/api/v1 \
  --tenant production \
  --month 2026-07 \
  --column-profile summary \
  --output-dir ./focus-preview \
  --header 'Authorization=Bearer <token>'
```

Use `--no-wait` to print the request ID immediately. Later, use `status` to
inspect it or `download` to retrieve an output directory, one declared file, or
the complete ZIP. Run `chitragupta-preview --help` for the complete option set.

### Request rules

| Choice | Rule |
|---|---|
| Daily | Inclusive start, exclusive end, 1–31 UTC dates, within one UTC month |
| Monthly | One `YYYY-MM` value |
| Full | Complete supported column set |
| Summary | Fixed 20-column subset |
| Custom | Ordered supported columns returned by the profile API |

An ad-hoc Monthly package is `provisional` when the complete month is not yet
settlement-ready. It keeps the requested calendar-month bounds but aggregates
only the frozen effective evidence interval available to that request. The
package remains immutable. Submit a later request after the 72-hour post-month
threshold and acquisition cutoff have passed to obtain a `settled` package.

The API has no built-in authentication. Protect the complete Preview route
prefix at the deployment boundary. For exact endpoints, request and response
fields, status codes, pagination, and diagnostics, use the
[API reference](api-reference.md#focus-mapping-preview).

## Repair retained dates

Dates calculated before Preview evidence was available may fail with
`calculation_metadata_unavailable`. If the provider and metrics history still
exists, submit a historical repair for an eligible retained interval:

```bash
curl -i -X POST \
  https://chitragupta.example/api/v1/tenants/production/focus-preview/repairs \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer <token>' \
  -d '{"start_date":"2026-01-01","end_date":"2026-02-01"}'
```

Repair is available only in `both` mode. It reacquires provider billing data and
runs the normal calculation for each selected date, so it can replace billing,
including with an authoritative empty provider result, plus chargebacks,
pipeline state, and related exports within that tenant/date range. It does not
create a package or revision. Submit a normal request after the needed dates
succeed.

The range must fit inside the tenant's commercial, acquisition, cutoff, and
retention intervals. Configuration cannot guarantee that Confluent Cloud or
metrics history is still available. See the
[repair API](api-reference.md#post-apiv1tenantstenant_namefocus-previewrepairs)
for the exact contract and
[troubleshooting](operations/troubleshooting.md#focus-mapping-preview-is-upgrading-degraded-or-unavailable)
for recovery actions.

## Published monthly revisions

After a successful periodic cycle, the worker evaluates complete eligible
months. Publication waits until at least 72 hours after month end and until the
configured acquisition cutoff covers the month. A valid candidate becomes the
current Settled Full revision; a failed candidate leaves the current revision
unchanged.

Each revision is a complete replacement for its month. Consumers must replace a
superseded revision and must never aggregate current and superseded revisions.

List and download revisions with the CLI:

```bash
uv run chitragupta-preview revisions \
  --api-url https://chitragupta.example/api/v1 \
  --tenant production \
  --month 2026-07

uv run chitragupta-preview revision <revision_id> \
  --api-url https://chitragupta.example/api/v1 \
  --tenant production \
  --output-dir ./focus-revision
```

Current revision download URLs contain a revision guard. If a replacement is
published after metadata is fetched, the guarded URL returns
`focus_preview_current_changed`; fetch current metadata and retry. Retained
revision URLs remain immutable until retention removes them.

## Package contents and lifecycle

| Package type | Scope | Profile | Lifecycle | Consumer action |
|---|---|---|---|---|
| Requested package | Daily or Monthly | Full, Summary, or Custom | Downloadable for seven days | Treat as a one-off immutable snapshot |
| Published revision | Complete eligible month | Full | Retained by billing-data retention | Replace the prior revision; never aggregate revisions |

Every package exposes:

- `manifest.json`, using `chitragupta.preview-manifest.v1`;
- one `cost-and-usage.csv`, or ordered CSV parts when a byte limit is configured;
- `focus-metadata.json`.

The API can also stream a ZIP containing the manifest followed by its declared
files.

CSV files use UTF-8 and LF line endings. Each part repeats the selected header,
rows are never split, and names, order, sizes, and SHA-256 checksums are recorded
in the manifest. The ZIP is a transport wrapper, not a manifest data artifact.

The manifest is authoritative for package identity, selected columns, source
coverage, validation, reconciliation, lifecycle, file order, sizes, and
checksums. `focus-metadata.json` supplies nonstandard
`x_ChitraguptaPreview...` import metadata for the generated dataset and emitted
schema. It targets the FOCUS 1.4 vocabulary but is not official conforming FOCUS
metadata. Importers that require a conforming FOCUS Export must reject the
package.

Importers should process `dataset_artifacts` in order, apply the exact ordered
columns in `x_ChitraguptaPreviewSchema`, and use the referenced dataset, schema,
and recency IDs for correlation. The manifest remains the authority for file
sizes, checksums, known gaps, and lifecycle.

Requested packages declare `not_a_correction_series`. Published revisions use
FOCUS Replacement handling with Overwrite delivery. API, CLI, UI, individual
file downloads, and ZIP downloads return the same stored immutable bytes.

Retention and recovery belong to the operations guide:

- [Requested package and revision retention](operations/data-retention.md#requested-focus-preview-packages)
- [Preview artifact recovery](operations/data-retention.md#preview-artifact-recovery)
- [Preview readiness monitoring](operations/monitoring.md#readiness-check)

## Supported customization

| Need | Supported control |
|---|---|
| Reporting period | Daily dates or Monthly month |
| Columns | Full, Summary, or Custom profile |
| CSV part size | `preview.max_csv_file_bytes` |
| Storage and capacity | Process-wide `preview` settings |
| Commercial interval | Tenant `focus_preview` settings |
| Allocation inputs | Existing Confluent allocator and identity settings, applied by the ordinary pipeline or repair |
| Automatic monthly publication | `features.enable_periodic_refresh` and `features.refresh_interval` |

The mapping profile is not configurable. YAML cannot override FOCUS field
mappings, classification, derived SKU rules, row order, manifest schema,
validation, reconciliation, the Summary profile, or package lifetime.

## Current output boundaries

- `BillingAccountId` comes from the persisted Confluent organization ID.
- `BillingCurrency` comes from the eligible tenant scope. Only USD is supported,
  Preview performs no conversion, and the value is not inferred from individual
  provider records.
- `ServiceProviderName` and `InvoiceIssuerName` are `Confluent Cloud`, supplied
  by the mapping profile rather than individual provider records.
- Metered Usage and supported Usage refunds use the persisted allocated quantity
  and normalized billing unit. Purchase, Support, Support refunds, and Credit
  rows keep consumed quantity and unit null.
- `ContractedCost` equals `ListCost`. For priced rows,
  `ContractedUnitPrice` equals `ListUnitPrice`, and
  `PricingCurrencyContractedUnitPrice` equals
  `PricingCurrencyListUnitPrice`. Credit and promotional rows without
  `SkuPriceId` keep these unit-price fields null.
- Provider discounts remain represented by final billed/effective costs and
  `x_ConfluentDiscountAmount`; they are not presented as negotiated unit-price
  savings.
- Supported promotional credits and refunds preserve their source
  classification and signed financial values.
- `HostProviderName` and `RegionId` preserve provider values. A separate provider
  region display name is unavailable, so `RegionName` is null.
- `InvoiceId` and `InvoiceDetailId` are unavailable. Preview does not fabricate
  them or perform invoice reconciliation.
- SKU identities are deterministic Chitragupta-derived values, not
  provider-issued identifiers.
- TABLEFLOW rows fail closed when provider context cannot be proven.
- The generic chargeback export is separate and unchanged.

The package reports these authority gaps in `known_gaps`. Passing Preview
validation, publishing a Settled revision, or using deterministic derived keys
does not claim FOCUS conformance.

## When this can become FOCUS Export

Preview must not be presented as a conforming FOCUS Export until it has
provider-authoritative SKU and invoice identities, complete applicable field
coverage, official conforming FOCUS metadata, and conformance validation for the
target version.

## Related documentation

- [Configuration reference](configuration/index.md#focus-mapping-preview)
- [Confluent Cloud eligibility](configuration/ccloud-reference.md#focus-mapping-preview-eligibility)
- [Deployment](operations/deployment.md#focus-mapping-preview-boundary)
- [Monitoring](operations/monitoring.md#readiness-check)
- [Troubleshooting](operations/troubleshooting.md#focus-mapping-preview-is-upgrading-degraded-or-unavailable)
- [Data retention](operations/data-retention.md#focus-preview-evidence)
- [Upgrading](operations/upgrading.md#focus-mapping-preview-upgrades)
- [API reference](api-reference.md#focus-mapping-preview)

# FOCUS Mapping Preview

FOCUS Mapping Preview turns Confluent Cloud billing, inventory, and calculated
allocation data already stored by Chitragupta into an immutable FOCUS 1.4 Cost
and Usage package. Packages are explicitly marked `non_conforming` because
some provider-authoritative FOCUS fields are unavailable.

Preview does not call Confluent Cloud, gather data, calculate allocations, edit
stored records, or recreate historical evidence. Run the ordinary pipeline
first. You can then create an ad-hoc package from the web UI, CLI, or request
API, while the periodic worker automatically publishes the current validated
Full monthly revision for eligible months.

## 1. Configure the tenant and package storage

Preview currently supports Confluent Cloud tenants with a Direct-billed PAYG
commercial profile and USD billing contract. The complete requested interval
must fit inside the configured half-open effective interval.

```yaml
preview:
  artifact_root: data/focus-preview
  max_workers: 2
  max_csv_file_bytes: null

tenants:
  production:
    ecosystem: confluent_cloud
    tenant_id: production
    lookback_days: 200
    cutoff_days: 5
    retention_days: 250
    focus_preview:
      commercial_profile: direct_payg
      billing_currency: USD
      effective_start_date: 2026-01-01
      effective_end_date: 2027-01-01
    storage:
      connection_string: sqlite:///data/ccloud.db
    plugin_settings:
      ccloud_api:
        key: ${CCLOUD_API_KEY}
        secret: ${CCLOUD_API_SECRET}
      billing_api:
        days_per_query: 15
```

`tenant_id` is Chitragupta's storage partition key. It is not used as the FOCUS
billing account. Preview obtains the Confluent organization through the normal
inventory pipeline and uses that provider organization ID as
`BillingAccountId`.

`preview.artifact_root` must be durable and writable by both the API and
periodic worker. When those run as separate processes, configure the same
mounted path for both. The database holds request and package metadata; the
immutable manifest and CSV bytes live under this root and are served only
through the Preview API.

The process-wide Preview settings are:

| Setting | Default | Valid values | Effect |
|---|---:|---|---|
| `preview.artifact_root` | `data/focus-preview` | Writable local path | Stores immutable requested packages and published monthly revisions. Changing it does not move existing packages. |
| `preview.max_workers` | `2` | 1–16 | Maximum concurrent ad-hoc Preview request jobs in one API process. |
| `preview.max_csv_file_bytes` | `null` | `null` or a positive integer | `null` produces one CSV. A byte limit splits output into deterministic parts without splitting rows. |

See the [Confluent Cloud configuration reference](configuration/ccloud-reference.md)
for the remaining collection and allocation settings.

## 2. Gather and calculate source data

Run the worker before requesting output:

```bash
uv run python src/main.py --config-file config.yaml --run-once
```

For the continuously running worker and API backend:

```bash
uv run python src/main.py --config-file config.yaml --mode both
```

The backend command does not start the frontend. For local development, the
repository Makefile starts the worker/API backend and Vite frontend together:

```bash
make dev
```

For the deployed full stack, use the repository Docker Compose setup:

In `examples/ccloud-full/config.yaml`, set the top-level Preview artifact root
to `/app/data/focus-preview`. The Compose service mounts its persistent named
volume at `/app/data`:

```yaml
preview:
  artifact_root: /app/data/focus-preview
  max_workers: 2
  max_csv_file_bytes: null
```

```bash
cd examples/ccloud-full
docker compose up -d
```

The default Compose URLs are API `http://localhost:8080` and frontend UI
`http://localhost:8081`. See the [Quickstart](getting-started/quickstart.md) and
[Deployment](operations/deployment.md) guides for environment-specific setup.

Preview requires persisted successful calculation metadata, raw Confluent Cost
records, billing rows, allocation lineage, organization inventory, and relevant
resource/identity inventory for the requested evidence interval. It fails the
whole request when required evidence is missing or inconsistent; it never emits
a partial package.

Automatic monthly publication runs only after a successful cycle of the
continuously running periodic worker. `--run-once`, direct tenant runs, and
ad-hoc Daily or Monthly Preview requests do not publish revisions.

## 3. Generate and download from the web UI

Open **FOCUS Mapping Preview** at `/focus-preview` and select a tenant. The page:

- defaults to Monthly and the current UTC month;
- offers Daily with an inclusive start date and exclusive end date;
- offers Full, Summary, and Custom column profiles;
- loads the supported Custom column allowlist from the API;
- submits and polls the asynchronous request;
- lists recent requests and supports cursor-based **Load more**;
- shows calculation time, source-through time, Monthly provisional/settled
  state, completion time, and expiry; and
- downloads `manifest.json`, any individual CSV part, or the complete ZIP.

Ready packages show download controls. Expired requests remain in history but
show no downloads. Failed requests show their persisted diagnostic and whether
retrying can succeed after the underlying data condition changes.

## 4. Use the remote CLI

`chitragupta-preview` is an HTTP client. The examples below run it from a source
checkout with `uv run`. Include `/api/v1` in `--api-url` and repeat
`--header NAME=VALUE` for deployment-specific authentication or proxy headers.
Duplicate header names are preserved on submission, polling, and downloads.

Chitragupta's REST API has no built-in authentication. Deployments must protect
the complete Preview route prefix—including submission, history, status,
manifest, file, and archive routes—behind an authenticated reverse proxy or API
gateway. The CLI forwards every supplied `--header` on submission, status polls,
and artifact downloads so it can use that external authentication boundary.

### Request and download a package

Monthly Summary package as individual files:

```bash
uv run chitragupta-preview request \
  --api-url https://chitragupta.example/api/v1 \
  --tenant production \
  --month 2026-07 \
  --column-profile summary \
  --output-dir ./focus-preview \
  --header 'Authorization=Bearer <token>'
```

Daily Custom package as a ZIP:

```bash
uv run chitragupta-preview request \
  --api-url https://chitragupta.example/api/v1 \
  --tenant production \
  --start-date 2026-07-01 \
  --end-date 2026-07-08 \
  --column-profile custom \
  --column BilledCost \
  --column ResourceId \
  --archive ./focus-preview.zip
```

Daily dates are UTC, inclusive-start/exclusive-end, 1–31 days, and must stay
within one UTC calendar month; the exclusive end may be the first day of the
next month. Monthly accepts one `YYYY-MM`. `--column-profile` defaults to
`full`; repeat `--column` only with `custom`. The `daily-full` command remains a
compatibility alias that requires Daily dates and `--output-dir`.

Without an output option, `request` waits and prints `<request_id> ready`.
`--json` prints the exact terminal API status document.

### Submit now and retrieve later

```bash
request_id=$(uv run chitragupta-preview request \
  --api-url https://chitragupta.example/api/v1 \
  --tenant production \
  --month 2026-07 \
  --no-wait)

uv run chitragupta-preview status \
  --api-url https://chitragupta.example/api/v1 \
  --tenant production \
  --wait \
  --json \
  "$request_id"
```

`--no-wait` performs only the POST. It prints the request ID, or the complete
queued response with `--json`, and cannot be combined with a download output.
`status` performs one GET unless `--wait` is present.

Download an existing ready request:

```bash
# Manifest plus every CSV part
uv run chitragupta-preview download \
  --api-url https://chitragupta.example/api/v1 \
  --tenant production \
  "$request_id" \
  --output-dir ./focus-preview

# One file enumerated by the package
uv run chitragupta-preview download \
  --api-url https://chitragupta.example/api/v1 \
  --tenant production \
  "$request_id" \
  --file cost-and-usage.csv \
  --output ./cost-and-usage.csv

# Complete archive to a local file
uv run chitragupta-preview download \
  --api-url https://chitragupta.example/api/v1 \
  --tenant production \
  "$request_id" \
  --archive ./focus-preview.zip

# Complete verified archive to stdout
uv run chitragupta-preview download \
  --api-url https://chitragupta.example/api/v1 \
  --tenant production \
  "$request_id" \
  --archive - > focus-preview.zip
```

For `--file`, `--output` is required and the file name must be present in the
manifest. Output paths are local-only. `--json` cannot be combined with archive
stdout because both use stdout. On other successful `download` modes, `--json`
prints the ready status document after the verified local output is published.

The CLI verifies the manifest, file metadata, byte sizes, SHA-256 checksums,
archive member order, and every archived file before publishing local output.
An individual file or local archive replaces its target atomically with
`os.replace` only after verification. Directory mode verifies the complete
package in a same-parent staging directory, moves an existing target directory
to a backup, swaps in the verified staging directory, and restores the backup
if publication fails. API-provided cross-origin URLs are rejected before
headers are forwarded.

CLI exit codes are:

| Code | Meaning |
|---:|---|
| 0 | Success |
| 1 | HTTP, request-state, generation, or expiry failure |
| 2 | Invalid CLI usage |
| 3 | Manifest, checksum, or archive-integrity failure |

## 5. Use the API

All paths are under `/api/v1/tenants/{tenant_name}/focus-preview`.

| Method and path | Purpose |
|---|---|
| `GET /profile` | Return the mapping profile version and ordered Full/Summary column allowlists. |
| `POST /requests` | Create an asynchronous Daily or Monthly request. |
| `GET /requests?limit=20&cursor={request_id}` | List requests newest first. `limit` is 1–100; `next_cursor` continues the list. |
| `GET /requests/{request_id}` | Read status, freshness, diagnostics, expiry, and ready package metadata. |
| `GET /requests/{request_id}/manifest` | Download exact `manifest.json` bytes. |
| `GET /requests/{request_id}/files/{file_name}` | Download one enumerated CSV part. |
| `GET /requests/{request_id}/archive` | Stream the complete deterministic ZIP. |
| `GET /revisions/current?month=YYYY-MM` | Return the current published monthly revision and guarded artifact URLs. |
| `GET /revisions/current/manifest?month=YYYY-MM&revision_id=...` | Download the guarded current revision manifest. |
| `GET /revisions/current/files/{file_name}?month=YYYY-MM&revision_id=...` | Download one guarded current revision CSV part. |
| `GET /revisions/current/archive?month=YYYY-MM&revision_id=...` | Stream the guarded current revision ZIP. |
| `GET /revisions?month=YYYY-MM&limit=20&cursor={revision_id}` | List current and superseded revisions newest first. |
| `GET /revisions/{revision_id}` | Return one retained revision and its direct artifact URLs. |
| `GET /revisions/{revision_id}/manifest` | Download a retained revision manifest. |
| `GET /revisions/{revision_id}/files/{file_name}` | Download one retained revision CSV part. |
| `GET /revisions/{revision_id}/archive` | Stream a retained revision ZIP. |

Daily request:

```json
{
  "grain": "daily",
  "start_date": "2026-07-01",
  "end_date": "2026-07-08",
  "column_profile": "full"
}
```

Monthly Custom request:

```json
{
  "grain": "monthly",
  "month": "2026-07",
  "column_profile": "custom",
  "columns": ["BilledCost", "ResourceId"]
}
```

Unknown and duplicate Custom columns are logged and ignored. Supported columns
retain first-occurrence order. A Custom request fails when no supported columns
remain. Full and Summary reject `columns`.

Every status/list item contains `request_id`, `tenant_name`, `grain`,
`start_date`, `end_date`, nullable `month`, `column_profile`, ordered
`effective_columns`, `status`, `created_at`, nullable `started_at`, nullable
`completed_at`, nullable `expires_at`, nullable `diagnostic`, nullable
`source_snapshot`, and nullable `package`.

Ready `package` contains `manifest`, ordered `files`, `download_all_name`, and
`download_all_url`. Each artifact has `name`, `media_type`, `size_bytes`,
`sha256`, optional `order`, and `download_url`. Expired responses keep their
snapshot and expiry but return `package: null`.

See the [API reference](api-reference.md#focus-mapping-preview) for response
fields, pagination, status behavior, errors, and diagnostic codes.

## 6. Browse and retrieve published monthly revisions

The periodic worker evaluates every calendar-month scope whose start is inside
both the tenant's current `lookback_days` acquisition window and the configured
`focus_preview` effective interval. On the first successful pass it publishes
every month that validates, including a valid header-only month with no cost
rows. A failure for one month publishes nothing for that month and does not
replace its current revision.

Published revisions use the Full profile. A month can have these transitions:

- no current revision to an initial `provisional` or `settled` revision;
- `provisional` to another `provisional` revision when logical report content
  or mapping semantics change;
- `provisional` to the first validated `settled` revision, even when logical
  content is unchanged; and
- `settled` to another `settled` revision when a later correction changes
  logical content or mapping semantics.

A settled month never regresses to provisional. CSV part size, part names,
source-row counts, timestamps, provenance, and other physical package layout do
not by themselves create a replacement. Each revision is a complete replacement
for the month: use the current revision for reporting and never add revisions
together.

The web UI's **Published monthly revisions** section has an independent month
selector. It lists current and superseded revisions newest first, including
publication state, calculation and source freshness, validation results, and
predecessor/successor links. Select **View and download** to retrieve the
manifest, an individual CSV part, or the complete ZIP for that retained
revision.

The remote CLI offers the same history and retrieval workflow:

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

`revisions` accepts `--limit`, `--cursor`, and `--json`. `revision` shows the
lifecycle, publication time, source freshness, and validation result when no
output option is supplied. Use exactly one output option to retrieve content:
`--output-dir PATH`, `--manifest PATH_OR_DASH`, `--file NAME --output PATH`, or
`--archive PATH_OR_DASH`. A dash writes a verified manifest or archive to
stdout. Local targets are published only after identity and checksum
verification.

Fetch current metadata first:

```bash
curl -sS \
  'https://chitragupta.example/api/v1/tenants/production/focus-preview/revisions/current?month=2026-07' \
  -H 'Authorization: Bearer <token>'
```

The response includes `revision_id`, `month`, `monthly_status`, `published_at`,
`supersedes_revision_id`, `material_sha256`, the validated source snapshot, and
guarded manifest, file, and archive URLs. Follow those returned URLs rather
than constructing an unguarded download URL.

Every artifact URL includes the month and current `revision_id`. If publication
replaces the revision after metadata discovery, the old URL returns 409 with
`focus_preview_current_changed`. Fetch current metadata again and retry with its
new URLs. Missing months and months belonging to another configured storage
owner return the same 404 response.

For example, use the `revision_id` returned above to download the current ZIP:

```bash
curl -fSs \
  'https://chitragupta.example/api/v1/tenants/production/focus-preview/revisions/current/archive?month=2026-07&revision_id=<revision_id>' \
  -H 'Authorization: Bearer <token>' \
  -o focus-mapping-preview-2026-07.zip
```

Published revisions are separate from ad-hoc Preview requests: they do not
appear in request history and do not have a seven-day request expiry. To browse
retained revisions through the API, call
`GET /revisions?month=YYYY-MM&limit=20`; continue with `next_cursor`, then follow
the selected item's direct URL. Direct revision URLs are immutable and do not
use the current-revision guard. A revision that has passed the billing-scope
retention cutoff is no longer listed or retrievable.

## Package contents and lifecycle

Every package contains:

- `manifest.json`, using schema `chitragupta.preview-manifest.v2`; and
- one `cost-and-usage.csv` by default, or ordered files named
  `cost-and-usage-part-00001-of-00003.csv` and so on when the configured byte
  limit requires partitioning.

Each CSV part is UTF-8 with LF line endings and repeats the same selected
header. Rows are never split between parts. Part ordering, names, bytes, sizes,
and SHA-256 values are deterministic for the same source snapshot and request
parameters.

The manifest's `package_type` distinguishes `requested_preview_package` from
`published_preview_revision`. Both record mapping profile, effective columns,
source/calculation coverage, validation, reconciliation, and ordered file
metadata. Requested manifests include request scope and seven-day lifecycle;
revision manifests include revision identity, publication time, monthly status,
and the superseded revision ID. The `files` list contains data files only. The
status or revision metadata response separately supplies the manifest's own size
and checksum. The ZIP is a transport wrapper containing `manifest.json` followed
by the CSV files in manifest order; it is not another data artifact in the
manifest.

A requested package is downloadable for exactly seven days from durable ready
publication. At `expires_at`, status becomes `expired` and all downloads return
410 before filesystem cleanup. The request and audit metadata remain visible.
Creating a new request after expiry reads the then-current persisted source
snapshot; it does not recreate the expired bytes. Published monthly revisions
use the separate billing-scope retention lifecycle described above.

This fixed seven-day package lifecycle is independent of tenant
`retention_days`, topic-attribution retention, and `lookback_days`.

## Supported customization

| Need | Supported control | Notes |
|---|---|---|
| Choose reporting period | Daily `start_date`/`end_date` or Monthly `month` | Daily cannot span more than one UTC calendar month. |
| Choose columns | Full, Summary, or Custom profile | Full emits 65 FOCUS columns plus 12 evidence columns; Summary emits its fixed 20-column subset; Custom uses only names returned by `GET /profile`. |
| Choose physical part size | `preview.max_csv_file_bytes` | Changes filenames and part boundaries only; rows and totals are unchanged. |
| Choose package storage/concurrency | `preview.artifact_root`, `preview.max_workers` | The root stores both package kinds; worker count applies to ad-hoc request jobs. |
| Declare Preview commercial scope | Tenant `focus_preview` block | Currently Direct-billed PAYG and USD only. |
| Change allocation inputs | Existing Confluent allocator/identity settings | Takes effect through a later ordinary calculation; Preview reads the persisted result and never recalculates ratios. |
| Enable automatic monthly publication | `features.enable_periodic_refresh` | Publication occurs after successful periodic cycles only. |
| Control when periodic cycles run | `features.refresh_interval` | Interval in seconds; this is not a separate revision schedule. |
| Control eligible publication months | Tenant `lookback_days`, `cutoff_days`, and `focus_preview` effective dates | These bound source acquisition and eligible calendar months; they are not archival-retention settings. |

The mapping profile itself is code-owned. Changing FOCUS field mappings,
service/charge classification, derived SKU rules, canonical row ordering,
manifest schema, validation, reconciliation, the Summary column set, the
seven-day lifetime, or adding another provider/commercial profile requires a
code change and a new release. There is no YAML mapping override or client-side
remapping hook.

Automatic monthly revisions always use Full. Summary and Custom remain ad-hoc
request choices. Changing `preview.max_csv_file_bytes` alone does not publish a
replacement; the new partition setting is used when a later logically material
revision is published.

## Current output boundaries

- `BillingAccountId` comes from the persisted Confluent organization ID.
- Native promotional-credit rows are retained as `Credit` / `One-Time`, even
  when provider product, unit, price, and quantity fields are null. Supported
  refunds retain their source classification and signed financial values.
- Preview projects the persisted allocation portions produced by the ordinary
  calculation. It does not reconstruct billing rows from chargebacks or
  recalculate allocation ratios.
- Confluent Cost records do not provide per-record ISO currency, so
  `BillingCurrency` is null even though USD is the required commercial
  contract. No currency conversion occurs.
- `HostProviderName` and `RegionId` preserve the provider values; a separate
  provider region display name is unavailable, so `RegionName` is null.
- Invoice identity and issuer fields are unavailable.
- SKU identities are deterministic Chitragupta-derived values, not
  provider-issued identifiers.
- TABLEFLOW rows currently fail closed when provider context cannot be proven.
- The package declares `conformance_status: non_conforming`; passing Preview's
  validation does not claim FOCUS conformance.

The generic chargeback export is a separate API and is not changed by Preview.

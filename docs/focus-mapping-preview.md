# FOCUS Mapping Preview

FOCUS Mapping Preview turns Confluent Cloud billing, inventory, and calculated
allocation data already stored by Chitragupta into an immutable FOCUS 1.4 Cost
and Usage package. Packages are explicitly marked `non_conforming` because
some provider-authoritative FOCUS fields are unavailable.

Preview does not call Confluent Cloud, gather data, calculate allocations, edit
stored records, or recreate historical evidence. Run the ordinary pipeline
first, then request a Preview from the web UI, CLI, or API.

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

`preview.artifact_root` must be durable and writable by the API process. Mount
it into the same persistent volume as application data in container
deployments. The database holds request and package metadata; the immutable
manifest and CSV bytes live under this root and are served only through the
Preview API.

The process-wide Preview settings are:

| Setting | Default | Valid values | Effect |
|---|---:|---|---|
| `preview.artifact_root` | `data/focus-preview` | Writable local path | Stores immutable requested packages. Changing it does not move existing packages. |
| `preview.max_workers` | `2` | 1–16 | Maximum concurrent Preview jobs in one API process. |
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

The manifest records request scope, mapping profile, effective columns, source
and calculation coverage, known gaps, validation counts, cost and quantity
reconciliation, lifecycle timestamps, and ordered file metadata. Its `files`
list contains data files only. The status response separately supplies the
manifest's own size and checksum. The ZIP is a transport wrapper containing
`manifest.json` followed by the CSV files in manifest order; it is not another
data artifact in the manifest.

A requested package is downloadable for exactly seven days from durable ready
publication. At `expires_at`, status becomes `expired` and all downloads return
410 before filesystem cleanup. The request and audit metadata remain visible.
Creating a new request after expiry reads the then-current persisted source
snapshot; it does not recreate the expired bytes.

This fixed seven-day package lifecycle is independent of tenant
`retention_days`, topic-attribution retention, and `lookback_days`.

## Supported customization

| Need | Supported control | Notes |
|---|---|---|
| Choose reporting period | Daily `start_date`/`end_date` or Monthly `month` | Daily cannot span more than one UTC calendar month. |
| Choose columns | Full, Summary, or Custom profile | Full emits 65 FOCUS columns plus 12 evidence columns; Summary emits its fixed 20-column subset; Custom uses only names returned by `GET /profile`. |
| Choose physical part size | `preview.max_csv_file_bytes` | Changes filenames and part boundaries only; rows and totals are unchanged. |
| Choose package storage/concurrency | `preview.artifact_root`, `preview.max_workers` | Process-wide operational settings. |
| Declare Preview commercial scope | Tenant `focus_preview` block | Currently Direct-billed PAYG and USD only. |
| Change allocation inputs | Existing Confluent allocator/identity settings | Takes effect through a later ordinary calculation; Preview reads the persisted result and never recalculates ratios. |

The mapping profile itself is code-owned. Changing FOCUS field mappings,
service/charge classification, derived SKU rules, canonical row ordering,
manifest schema, validation, reconciliation, the Summary column set, the
seven-day lifetime, or adding another provider/commercial profile requires a
code change and a new release. There is no YAML mapping override or client-side
remapping hook.

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

# FOCUS Mapping Preview

FOCUS Mapping Preview turns Confluent Cloud billing, inventory, and calculated
allocation data already stored by Chitragupta into an immutable FOCUS 1.4 Cost
and Usage package. Packages are explicitly marked `non_conforming` because
some provider-authoritative FOCUS fields are unavailable.

Package generation does not call Confluent Cloud, gather data, calculate
allocations, edit stored records, or recreate historical evidence. Run the
ordinary pipeline first. A separate operator-requested historical repair can
reacquire and recalculate eligible retained dates that predate Preview evidence.
You can then create an ad-hoc package from the web UI, CLI, or request API, while
the periodic worker automatically publishes the current validated Settled Full
monthly revision for settlement-ready months.

Preview is opt-in per tenant. If every tenant omits `focus_preview`, Chitragupta
does not create or access the Preview artifact root, gather the provider
organization for Preview, capture raw source evidence, or store allocation
lineage. Billing collection, chargeback calculation, generic CSV export, and
emitters continue with their existing requirements. A deployment can mix
enabled and disabled tenants; only enabled tenants use Preview storage and
evidence.

## 1. Configure the tenant and package storage

Preview currently supports Confluent Cloud tenants with a Direct-billed PAYG
commercial profile and USD billing contract. The complete requested interval
must fit inside the configured half-open effective interval.

```yaml
preview:
  artifact_root: data/focus-preview
  max_workers: 2
  max_queued_repairs: 8
  max_queued_generations: 8
  max_running_generations_per_tenant: 1
  max_queued_generations_per_tenant: 2
  max_generation_spool_bytes: 2147483648
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

Artifact directories use an opaque v1 storage identity derived from ecosystem,
provider tenant ID, and the tenant's configured storage backend. Renaming the
display tenant does not move its packages. Separate tenant databases remain
isolated even when they use the same ecosystem and provider tenant ID. Treat
all directory and lock names below `preview.artifact_root` as internal.

Preview artifact settings and a writable artifact root are operational
requirements only when at least one tenant has `focus_preview` enabled. To
leave a tenant disabled, omit its `focus_preview` block. Preview routes for that
tenant return HTTP 409 with
`preview_commercial_profile_unavailable` before opening Preview storage. No
disabled-tenant organization, source-evidence, or allocation-lineage work is
performed.

The process-wide Preview settings are:

| Setting | Default | Valid values | Effect |
|---|---:|---|---|
| `preview.artifact_root` | `data/focus-preview` | Writable local path | Stores immutable requested packages and published monthly revisions. Changing it does not move existing packages. |
| `preview.max_workers` | `2` | 1–16 | Process-local running-generation limit shared by requested packages and scheduled publication; also the separate repair-runtime worker count. |
| `preview.max_queued_repairs` | `8` | Zero or a positive integer | Maximum historical repairs waiting across all tenants in one process. Zero permits only repairs that can occupy a running position. |
| `preview.max_queued_generations` | `8` | Zero or a positive integer | Maximum requested and scheduled generations waiting across all tenants in one process. |
| `preview.max_running_generations_per_tenant` | `1` | Positive and no greater than `max_workers` | Running-generation limit for one tenant. |
| `preview.max_queued_generations_per_tenant` | `2` | Zero or a positive integer | Waiting-generation limit for one tenant. |
| `preview.max_generation_spool_bytes` | `2147483648` (2 GiB) | Positive integer | Hard temporary-disk ceiling for one running generation. |
| `preview.max_csv_file_bytes` | `null` | `null` or a positive integer | `null` produces one CSV. A byte limit splits output into deterministic parts without splitting rows. |

The global and per-tenant queue limits must both be zero or both be positive.
When positive, the per-tenant queue limit must be lower than the global queue
limit. Zero disables waiting: an ad-hoc request must start immediately or
returns retryable HTTP 429, while a scheduled month is deferred. Positive
queues provide fair capacity across tenants. Limits are per application
process, not distributed across replicas.

Historical repair has a separate process-local admission bound:
`preview.max_workers` repairs can run and `preview.max_queued_repairs`
additional repairs can wait. The existing one-active-repair-per-tenant rule
still applies. A full repair limit returns retryable HTTP 429 before creating a
repair. Generation and repair limits are independent, so admitted repair and
package-generation work can run concurrently. Replicas have independent
limits.

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

### Choose a run mode

| Mode | FOCUS Mapping Preview behavior |
|---|---|
| `worker` | Runs the ordinary pipeline, scheduled monthly publication, and revision retention. It exposes no Preview HTTP routes. |
| `api` | Serves ad-hoc requests, request history, published revision history, downloads, and retained repair status. It cannot submit repair or run periodic publication. |
| `both` | Serves the same Preview HTTP contract as `api`, runs the periodic worker, and is the only mode that accepts repair submissions. |

When API and worker run as separate processes, they must use the same tenant
database and the same durable `preview.artifact_root`. The database identifies
requests and revisions; the shared artifact root holds the immutable bytes that
the API serves.

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
  max_queued_repairs: 8
  max_queued_generations: 8
  max_running_generations_per_tenant: 1
  max_queued_generations_per_tenant: 2
  max_generation_spool_bytes: 2147483648
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

For accepted metered Usage and Usage refunds, `ConsumedQuantity` equals the
exact persisted allocated quantity for that portion and `ConsumedUnit` equals
the retained normalized Billing API unit. `PricingQuantity` and `PricingUnit`
use the same portion quantity and unit. Purchase/Support, Support refunds, and
Credit rows keep both consumed fields null. Distinct tiers retain their own
quantity, unit, and tier identity.

For enabled tenants, the ordinary pipeline captures that evidence and provider
organization authority once as part of its normal refresh/calculation work.
Package requests and scheduled revisions reuse the persisted values; generation
does not call Confluent Cloud or recalculate allocations. A Preview evidence or
authority failure makes Preview fail closed with a Preview-specific diagnostic,
but does not turn an otherwise successful generic chargeback cycle into a
failure.

Automatic monthly publication runs only after a successful cycle of the
continuously running periodic worker. `--run-once`, direct tenant runs, and
ad-hoc Daily or Monthly Preview requests do not publish revisions. Active,
sub-72-hour, and acquisition-cutoff-incomplete months are excluded before
package generation. On-demand Monthly requests remain available and can produce
Provisional packages for those months. Scheduled and ad-hoc generation share
the same bounded process-local capacity. If a tenant-month cannot be admitted,
the worker defers it without creating a revision or changing the current
pointer; a later periodic cycle reevaluates it.

### Repair retained upgraded dates

Retained calculations upgraded without Preview calculation correlation, native
source evidence, or allocation lineage cannot be made Preview-ready by copying
legacy aggregate rows. If such dates fail with
`calculation_metadata_unavailable`, use the historical repair REST operation.
There is no repair command in the CLI or web UI.

Submit an inclusive-start/exclusive-end UTC interval in `both` mode:

```bash
curl -i -X POST \
  https://chitragupta.example/api/v1/tenants/production/focus-preview/repairs \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer <token>' \
  -d '{"start_date":"2026-01-01","end_date":"2026-02-01"}'
```

A valid submission returns 202, a durable queued document, and a `Location`
header. Poll that URL:

```bash
curl \
  https://chitragupta.example/api/v1/tenants/production/focus-preview/repairs/{repair_id} \
  -H 'Authorization: Bearer <token>'
```

The submitted range must be contained in the intersection of the configured
`focus_preview` effective interval, current `lookback_days` and `cutoff_days`
window, and complete `retention_days` interval. Future dates, disabled tenants,
unsupported ecosystems, and ranges outside that scope are rejected before
pipeline data changes. The operation also requires active Confluent Cloud
billing credentials, retained provider billing history, and any historical
Telemetry, Prometheus, or Flink metrics required by the configured allocators.
Configuration does not guarantee that those external histories still exist.

Repair processes dates in ascending order and continues after expected
per-date failures. Each date has durable status, timestamps, optional
calculation result, failure stage, and diagnostic. `daily_validated` is
nonterminal: Daily Full validation passed, but validation of a wholly selected
UTC month has not completed. A process interruption marks the operation and
unfinished dates for that configured tenant owner failed, including work
created in the same whole second as restart; work is not automatically resumed.
After successful restart recovery, the repair is `degraded` and requires an
explicit resubmission. If recovery for that tenant cannot complete at startup,
the next repair submission retries recovery for that tenant first. A failed
retry returns
`503 FOCUS Mapping Preview repair worker is unavailable` without creating a new
repair.

For each date, the authoritative provider result replaces the exact
tenant/date billing scope, including an authoritative empty result. The
canonical calculation then writes mutually consistent chargebacks, pipeline
state, source evidence, and allocation lineage. It does not fabricate
calculation identity, timestamps, source evidence, or lineage from legacy
records. The selected range may therefore change billing, chargebacks, and
generic exports, while other dates and tenants remain unchanged.

Repair creates no requested package or published revision. After all required
dates succeed, submit a normal Daily or Monthly Preview request. Retrying a
partial or interrupted range creates a new operation and safely performs the
same exact-date replacements without duplicate current lineage. API-only mode
can read retained repair status but returns 503 for POST; disabled tenants
cannot execute or read repair.

Only one repair may be active for a tenant. Across all tenants in one process,
up to `preview.max_workers` repairs run and up to
`preview.max_queued_repairs` additional repairs wait. If that capacity is full,
submission returns retryable HTTP 429 with
`focus_preview_repair_capacity_exhausted`; no repair was created. Wait for
current repair work to finish and submit again. There is no automatic retry.

### Understand Preview readiness and date progress

`GET /api/v1/readiness`, the Preview navigation item, and the Preview page use
five tenant-scoped states:

| State | Customer behavior |
|---|---|
| `disabled` | Preview is not configured for this tenant. |
| `ready` | Preview is available and no repair is active. |
| `upgrading` | A repair is queued or running. Existing valid packages and revisions remain available. |
| `degraded` | A repair failed, completed with failed dates, or was interrupted. Existing valid packages remain available; retry failed dates with a new bounded repair. |
| `unavailable` | Preview readiness or storage cannot be read safely. Restore availability before retrying. |

During and after repair, **Date progress** is shown as completed repair dates
out of total requested dates. Both succeeded and failed dates count as
completed lifecycle progress; failed dates make the feature `degraded`. This is
not data-volume progress and no percentage is implied.

Only FOCUS Mapping Preview receives these states. Billing, chargeback,
inventory, the ordinary pipeline, and unrelated navigation remain available.
Readiness polls every five seconds while a tenant is `upgrading` and returns to
the normal fifteen-second cadence after the repair reaches a terminal state.

## 3. Generate and download from the web UI

Open **FOCUS Mapping Preview** at `/focus-preview` and select a tenant. The page:

- displays disabled, upgrading, degraded, and unavailable feature status;
- labels repair lifecycle counts as **Date progress**;
- defaults to Monthly and the current UTC month;
- offers Daily with an inclusive start date and exclusive end date;
- offers Full, Summary, and Custom column profiles;
- loads the supported Custom column allowlist from the API;
- renders the target FOCUS 1.4 warning, `non_conforming` status, and ordered
  authority gaps from the active tenant's `/profile` response without a
  component-owned fallback;
- submits and polls the asynchronous request;
- lists recent requests and supports cursor-based **Load more**;
- shows calculation time, source-through time, Monthly provisional/settled
  state, completion time, and expiry; and
- downloads `manifest.json`, any individual CSV part, or the complete ZIP.

Ready packages show download controls. Expired requests remain in history but
show no downloads. Failed requests show their persisted diagnostic and whether
retrying can succeed after the underlying data condition changes.
Upgrading and degraded states preserve package generation, history, revision,
and download actions because existing valid Preview data remains available.
Disabled or unavailable states block new Preview generation and refresh work;
already loaded immutable downloads remain available.

## 4. Use the remote CLI

`chitragupta-preview` is an HTTP client. The examples below run it from a source
checkout with `uv run`. Include `/api/v1` in `--api-url` and repeat
`--header NAME=VALUE` for deployment-specific authentication or proxy headers.
Duplicate header names are preserved on submission, polling, and downloads.
Top-level help and help for `daily-full`, `request`, `status`, `download`,
`revisions`, and `revision` identify FOCUS Mapping Preview, target FOCUS 1.4,
and state that generated data is non-conforming.

Chitragupta's REST API has no built-in authentication. Deployments must protect
the complete Preview route prefix—including repair, package submission,
history, status, manifest, file, and archive routes—behind an authenticated
reverse proxy or API gateway. The CLI forwards every supplied `--header` on
submission, status polls, and artifact downloads so it can use that external
authentication boundary.

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

If submission receives HTTP 429
`preview_capacity_exhausted`, no request was created. The CLI exits with code 1
and does not retry automatically. Wait for running or queued Preview work to
drain, then repeat the same command.

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
| `GET /profile` | Return target FOCUS 1.4, `non_conforming` status, the mapping profile version, ordered Full/Summary column allowlists, and canonical ordered public `known_gaps`. |
| `POST /repairs` | In `both` mode, create a durable historical repair for an eligible retained UTC interval. |
| `GET /repairs/{repair_id}` | Read durable operation and per-date repair status. |
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

The pre-submission UI renders its target-version warning, non-conformance
status, and **Current authority gaps** from the existing tenant-scoped
`/profile` response. Its `known_gaps` value is the same canonical ordered public
value used by Requested Preview Package and Published Preview Revision
manifests. Each gap contains exactly `code`, customer-facing `description`, and
ordered affected `columns`; internal ownership and delivery-process metadata
are excluded. The complete current catalog remains documented once in the
manifest contract below.

Profile, request creation/status/detail/list metadata, current and retained
revision summary/detail/list metadata, and both manifest types consume one
code-owned capability authority for exact `target_focus_version: "1.4"` and
`conformance_status: "non_conforming"` values. Clients forward those API values
without reinterpreting them.

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
`focus_preview` effective interval. It generates a revision candidate only after
the month has ended by at least 72 hours and the configured acquisition cutoff
covers the full month. The first automatic revision publishes only when complete
full-month calculation and source coverage, reconciliation, and mapping
validation produce a Settled result. This includes a valid settlement-ready
header-only month with no cost rows. A failure for one month publishes nothing
for that month and does not replace its current revision.

Published revisions use the Full profile. A month can have these transitions:

- no current revision to the first validated `settled` revision;
- an existing `provisional` revision to the first validated `settled` revision,
  even when logical content is unchanged; and
- `settled` to another `settled` revision when a later correction changes
  logical content or mapping semantics.

Existing Provisional revisions are not deleted during upgrade. They remain
retrievable under the ordinary supersession and retention rules until the first
valid Settled revision supersedes them or retention removes them. A settled
month never regresses to provisional. CSV part size, part names,
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

Persisted financial-period keys and Preview lifecycle state use aware UTC
whole-second timestamps on both SQLite and PostgreSQL. This includes the
timestamps embedded in persisted request calculation coverage and revision
source snapshots. Public API fields and manifest shapes remain unchanged, and
strict validation of retained manifests and artifact sizes, checksums, and bytes
is unchanged. Timestamp canonicalization does not alter financial amounts,
correlations, reconciliation, or totals.

Every package contains:

The mapping profile `focus-1.4-preview-v1` and manifest schema
`chitragupta.preview-manifest.v1` are the first release contracts. Pre-release
development packages are unsupported and fail closed instead of being treated
as current packages.

- `manifest.json`, using schema `chitragupta.preview-manifest.v1`; and
- one `cost-and-usage.csv` by default, or ordered files named
  `cost-and-usage-part-00001-of-00003.csv` and so on when the configured byte
  limit requires partitioning.

Each CSV part is UTF-8 with LF line endings and repeats the same selected
header. Rows are never split between parts. Part ordering, names, bytes, sizes,
and SHA-256 values are deterministic for the same source snapshot and request
parameters.

The manifest's `package_type` distinguishes `requested_preview_package` from
`published_preview_revision`. Both declare `target_focus_version: "1.4"` and
`conformance_status: "non_conforming"` from the same capability authority as
request and revision API metadata, and record mapping profile, effective
columns, source/calculation coverage, validation, reconciliation, and ordered
file metadata. Requested manifests include request scope and seven-day
lifecycle; revision manifests include revision identity, publication time,
monthly status, and the superseded revision ID. The `files` list contains data
files only. The status or revision metadata response separately supplies the
manifest's own size and checksum. The ZIP is a transport wrapper containing
`manifest.json` followed by the CSV files in manifest order; it is not another
data artifact in the manifest. Requested manifests validate the complete
current request identity, tenant, interval, effective columns, target, status,
canonical gaps, snapshot, evidence, lifecycle, validation, reconciliation, and
file checks. Revision manifests validate current revision identity, snapshot,
Full-profile authority, material digest, validation summary, and file
correlation. Both use the same current schema and mapping authority and enforce
canonical gaps plus file order, size, and checksums where applicable before
delivery.

Both manifest types contain the same complete ordered `known_gaps` array. Each
object contains exactly the stable `code`, durable customer-facing
`description`, and ordered affected `columns`:

```json
[
  {
    "code": "provider_billing_currency_field_unavailable",
    "description": "Confluent Costs records do not carry a per-record billing currency.",
    "columns": ["BillingCurrency"]
  },
  {
    "code": "invoice_identity_unavailable",
    "description": "Post-issuance invoice identity is unavailable.",
    "columns": ["InvoiceDetailId", "InvoiceId"]
  },
  {
    "code": "invoice_issuer_name_unavailable",
    "description": "Provider legal invoice-issuer evidence is unavailable.",
    "columns": ["InvoiceIssuerName"]
  },
  {
    "code": "provider_host_display_name_unavailable",
    "description": "HostProviderName contains the raw provider cloud code, not a provider display name.",
    "columns": ["HostProviderName"]
  },
  {
    "code": "provider_region_display_name_unavailable",
    "description": "Confluent inventory does not provide a distinct region display name.",
    "columns": ["RegionName"]
  },
  {
    "code": "derived_sku_identity_not_provider_authoritative",
    "description": "SKU values are deterministic Chitragupta-derived evidence, not provider-issued identifiers.",
    "columns": [
      "SkuId",
      "SkuMeter",
      "SkuPriceDetails",
      "SkuPriceId",
      "x_ChitraguptaSkuComponents"
    ]
  }
]
```

The manifest contract has no public ownership or delivery-process field or
alias. These provider-authority gaps remain unresolved, and both package types
retain `conformance_status: non_conforming`.

A requested package is downloadable for exactly seven days from durable ready
publication. At `expires_at`, status becomes `expired` and all downloads return
410 before filesystem cleanup. The request and audit metadata remain visible.
Creating a new request after expiry reads the then-current persisted source
snapshot; it does not recreate the expired bytes. Published monthly revisions
use the separate billing-scope retention lifecycle described above.

This fixed seven-day package lifecycle is independent of tenant
`retention_days`, topic-attribution retention, and `lookback_days`.

Both requested packages and revisions are written to synchronized staging,
atomically finalized, and protected by one stable package lock until their
metadata transaction commits. If publication is interrupted, same-process or
restart recovery removes abandoned staging work and retries incomplete request
terminalization, expiry cleanup, and revision retention.

Request recovery uses durable worker ownership and lease state, so an unowned
incomplete request remains recoverable even when it shares startup's whole
second. Revision-retention retries use persisted retry count, original pending
time, and revision identity; a failed attempt increments the count without
changing the pending timestamp. Compare-and-set checks include the retry count
and candidate ownership/identity fields, preserving current-pointer safety and
restart convergence when multiple events have tied timestamps.

Package generation uses bounded temporary disk rather than retaining a complete
package in memory. `preview.max_generation_spool_bytes` defaults to a 2 GiB
hard ceiling for each running generation. Exceeding it fails an ad-hoc request
with non-retryable
`preview_generation_spool_limit_exceeded`; scheduled publication creates no
revision and preserves the current revision.

Artifact retrieval incrementally validates the manifest and verifies stored
sizes and SHA-256 values before response delivery, then streams verified
manifest, CSV, and ZIP bytes in fixed chunks. Large stored artifacts therefore
do not require a matching whole-body response allocation.

Recovery does not infer ownership from a directory name alone. It takes an
owner-scoped snapshot of every non-null request and revision storage reference,
then rechecks each apparent finalized orphan against authoritative metadata
while holding the package lock. Live publishers, referenced packages, and
reference checks that cannot be completed are preserved. Referenced packages
that satisfy the current release contract remain available. Pre-release
development packages are unsupported and fail closed; their physical paths are
not converted into current packages. Unverifiable finalized paths are left for
operator inspection instead of being automatically deleted. Deletion or
filesystem-synchronization failures are logged and retried.

Requested-package submission, history, status, and artifact retrieval perform
owner recovery after their existing tenant, input, feature-enablement, runtime,
and storage checks and before requested-package metadata actions. While that
recovery is unavailable, the requested-package operation fails closed with HTTP
503 detail `FOCUS Mapping Preview recovery is unavailable`. The operator should
inspect the Preview recovery log, restore database or artifact-root
availability, and retry the same operation; successful recovery is idempotent.
Published-revision history, detail, and artifact routes do not invoke this
requested-package owner-recovery step.

## Supported customization

| Need | Supported control | Notes |
|---|---|---|
| Choose reporting period | Daily `start_date`/`end_date` or Monthly `month` | Daily cannot span more than one UTC calendar month. |
| Choose columns | Full, Summary, or Custom profile | Full emits 65 FOCUS columns plus 12 evidence columns; Summary emits its fixed 20-column subset; Custom uses only names returned by `GET /profile`. |
| Choose physical part size | `preview.max_csv_file_bytes` | Changes filenames and part boundaries only; rows and totals are unchanged. |
| Choose package storage/capacity | `preview.artifact_root`, `preview.max_workers`, queue/per-tenant limits, `preview.max_generation_spool_bytes` | Requested and scheduled generation share process-local capacity; the spool ceiling applies to each running generation. |
| Declare Preview commercial scope | Tenant `focus_preview` block | Currently Direct-billed PAYG and USD only. |
| Change allocation inputs | Existing Confluent allocator/identity settings | Takes effect through a later ordinary recalculation or explicit historical repair; Preview package generation reads the persisted result and never recalculates ratios. |
| Enable automatic monthly publication | `features.enable_periodic_refresh` | Publication occurs after successful periodic cycles only. |
| Control when periodic cycles run | `features.refresh_interval` | Interval in seconds; this is not a separate revision schedule. |
| Control eligible publication months | Tenant `lookback_days`, `cutoff_days`, and `focus_preview` effective dates | Automatic generation also waits at least 72 hours after month end and requires the acquisition cutoff to cover the complete month. These controls are not archival-retention settings. |

The mapping profile itself is code-owned. Changing FOCUS field mappings,
service/charge classification, derived SKU rules, canonical row ordering,
manifest schema, validation, reconciliation, the Summary column set, the
seven-day lifetime, or adding another provider/commercial profile requires a
code change and a new release. There is no YAML mapping override or client-side
remapping hook.

Automatic monthly revisions are always Settled and use Full. Summary and Custom
remain ad-hoc request choices. Changing `preview.max_csv_file_bytes` alone does
not publish a replacement; the new partition setting is used when a later
logically material Settled revision is published.

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

## When this can become FOCUS Export

FOCUS Mapping Preview remains non-conforming and must not be presented as a
FOCUS Export until all of these gates are met:

- provider-authoritative `SkuId` and `SkuPriceId` values, including their
  authoritative price-list relationship;
- invoice-issuer-assigned `InvoiceId` and `InvoiceDetailId` values after the
  source charges have been associated with an invoice;
- authoritative invoice-issuer semantics;
- complete coverage of every applicable FOCUS field and its native source
  semantics;
- official FOCUS metadata; and
- conformance validation appropriate to the target FOCUS version.

Passing Preview mapping validation, publishing a Settled revision, or using
deterministic Chitragupta-derived keys does not satisfy those gates.

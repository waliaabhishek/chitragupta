# API Reference

Base URL: `http://localhost:8080` (configurable via `api.host` and `api.port`)

All endpoints except `/health` are prefixed with `/api/v1`.

## General behavior

- **Authentication:** None built-in. Use a reverse proxy for auth.
- **CORS:** Configurable via `api.enable_cors` and `api.cors_origins`. Allowed methods: GET, POST, PATCH, DELETE.
- **Request timeout:** Requests exceeding `api.request_timeout_seconds` (default 30, max 300) return HTTP 504.
- **Content type:** JSON for API data. Export and Preview artifact endpoints
  return their declared CSV or manifest media type.

### Pagination

List endpoints accept:

| Parameter | Type | Default | Constraints |
|---|---|---|---|
| `page` | int | 1 | >= 1 |
| `page_size` | int | 100 | 1–1000 |

Response includes: `items`, `total`, `page`, `page_size`, `pages`.

### Date range defaults

When `start_date` / `end_date` are omitted on list endpoints, the API uses the
tenant's configured lookback window. FOCUS Mapping Preview requires both dates.

### Error responses

| Status | Meaning |
|---|---|
| 400 | Invalid parameters (bad filter, unknown column, etc.) |
| 404 | Tenant or resource not found |
| 409 | Pipeline already running (trigger endpoint) |
| 504 | Request timeout |

---

## Health & Readiness

### `GET /health`

Lightweight liveness check.

**Response:** `{"status": "ok", "version": "<version>"}`

### `GET /api/v1/readiness`

Per-tenant readiness with pipeline state. TTL-cached for 2 seconds.

**Response fields:**

| Field | Type | Description |
|---|---|---|
| `status` | string | `ready`, `initializing`, `no_data`, or `error` |
| `version` | string | Package version |
| `mode` | string | Run mode (`api`, `worker`, `both`) |
| `tenants` | list | Per-tenant status (see below) |

**Per-tenant fields:** `tenant_name`, `tables_ready`, `has_data`, `pipeline_running`, `pipeline_stage`, `pipeline_current_date`, `last_run_status`, `last_run_at`, `permanent_failure`, `topic_attribution_status`, `topic_attribution_error`.

`topic_attribution_status` is one of `"disabled"` | `"enabled"` | `"config_error"`. `topic_attribution_error` is a string describing the validation failure when `topic_attribution_status` is `"config_error"`, otherwise null.

---

## Tenants

### `GET /api/v1/tenants`

List all configured tenants with summary pipeline state.

**Response fields per tenant:** `tenant_name`, `tenant_id`, `ecosystem`, `dates_pending`, `dates_calculated`, `last_calculated_date`, `topic_attribution_status`, `topic_attribution_error`.

`topic_attribution_status` is one of `"disabled"` | `"enabled"` | `"config_error"`. See `GET /api/v1/readiness` above for field semantics.

### `GET /api/v1/tenants/{tenant_name}/status`

Detailed per-date pipeline state for a tenant.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `start_date` | date | no | Filter from this date |
| `end_date` | date | no | Filter to this date |

**Response:** `{tenant_name, tenant_id, ecosystem, topic_attribution_status, topic_attribution_error, states}` where `states` is a list of `{tracking_date, billing_gathered, resources_gathered, chargeback_calculated, topic_overlay_gathered, topic_attribution_calculated}` per date. The `topic_overlay_gathered` and `topic_attribution_calculated` fields are `false` when topic attribution is disabled or in `config_error` state.

---

## Billing

### `GET /api/v1/tenants/{tenant_name}/billing`

List raw billing line items. Paginated.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `start_date` | date | no | Filter start |
| `end_date` | date | no | Filter end |
| `timezone` | string | no | IANA timezone for date boundaries (e.g. `America/Denver`). Defaults to UTC. |
| `product_type` | string | no | Filter by product type |
| `resource_id` | string | no | Filter by resource |

**Response fields per item:** `ecosystem`, `tenant_id`, `timestamp`, `resource_id`, `product_category`, `product_type`, `quantity`, `unit_price`, `total_cost`, `currency`, `granularity`, `metadata`.

---

## Chargebacks

### `GET /api/v1/tenants/{tenant_name}/chargebacks`

List allocated chargeback rows. Paginated.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `start_date` | date | no | Filter start |
| `end_date` | date | no | Filter end |
| `timezone` | string | no | IANA timezone for date boundaries (e.g. `America/Denver`). Defaults to UTC. |
| `identity_id` | string | no | Filter by identity |
| `product_type` | string | no | Filter by product type |
| `resource_id` | string | no | Filter by resource |
| `cost_type` | string | no | Filter by cost type |
| `tag_key` | string | no | Filter by tag key |
| `tag_value` | string | no | Filter by tag value (requires tag_key) |

**Response fields per item:** `dimension_id`, `ecosystem`, `tenant_id`, `timestamp`, `resource_id`, `product_category`, `product_type`, `identity_id`, `cost_type`, `amount`, `allocation_method`, `allocation_detail`, `tags`, `metadata`.

`tags` is a `dict[str, str]` mapping tag keys to tag values (e.g. `{"team": "platform", "env": "prod"}`). Tags are resolved at query time from the linked resource or identity.

### `GET /api/v1/tenants/{tenant_name}/chargebacks/{dimension_id}`

Get a single chargeback dimension.

**Response:** Dimension fields (tags not resolved at this level — use `/chargebacks` list endpoint for tag-enriched rows).

### `GET /api/v1/tenants/{tenant_name}/chargebacks/dates`

List all distinct dates with chargeback data.

**Response:** `{"dates": ["2026-01-01", "2026-01-02", ...]}`

### `GET /api/v1/tenants/{tenant_name}/chargebacks/allocation-issues`

Aggregated view of failed or problematic allocations. Paginated. Same filters as chargebacks list.

**Response fields per item:** `ecosystem`, `resource_id`, `product_type`, `identity_id`, `allocation_detail`, `row_count`, `usage_cost`, `shared_cost`, `total_cost`.

---

## Aggregation

### `GET /api/v1/tenants/{tenant_name}/chargebacks/aggregate`

Multi-dimensional aggregation with time bucketing. Returns up to 10,000 buckets.

| Parameter | Type | Default | Description |
|---|---|---|---|
| `group_by` | list[string] | `["identity_id"]` | Columns or tag keys to group by (repeatable). Use `tag:{key}` for tag-based grouping. |
| `time_bucket` | string | `day` | `hour`, `day`, `week`, or `month` |
| `start_date` | date | no | Filter start |
| `end_date` | date | no | Filter end |
| `timezone` | string | no | IANA timezone for date boundaries (e.g. `America/Denver`). Defaults to UTC. |
| `identity_id` | string | no | Filter by identity |
| `product_type` | string | no | Filter by product type |
| `resource_id` | string | no | Filter by resource |
| `cost_type` | string | no | Filter by cost type |
| `tag:{key}` | string | no | Filter to rows where the tag `{key}` matches the given value. Repeatable for AND semantics. Comma-separated values in a single param are OR-matched. |

**Valid `group_by` columns:** `identity_id`, `resource_id`, `product_type`, `product_category`, `cost_type`, `allocation_method`, `environment_id`.

**Tag-based grouping (`group_by=tag:{key}`):**

Use `tag:` prefix to group by an entity tag key instead of a dimension column. Examples:

- `group_by=tag:owner` — group by the `owner` tag; rows with no `owner` tag land in an `UNTAGGED` bucket.
- `group_by=tag:owner&group_by=tag:department` — group by two tag keys; each bucket has both keys in `dimensions`.
- `group_by=tag:owner&group_by=product_type` — mix tag and dimension grouping in one query.

Tag values are resolved by joining the `entity_tags` table. When a resource and its linked identity both carry the same tag key, the **resource tag wins** (mirrors the behavior on the list endpoint).

**Tag-based filtering (`tag:{key}={value}`):**

Dynamic query parameters prefixed `tag:` are treated as tag filters and are independent of `group_by`.

- `tag:department=eng` — only rows where `department` tag equals `eng`.
- `tag:team=platform,commerce` — `team` IN (`platform`, `commerce`) — comma-separated values are OR-matched within one key.
- `tag:owner=alice&tag:department=eng` — multiple tag params are AND-matched across keys.
- Untagged rows (no matching tag key) are excluded from filtered results.

Tag key format: must start with an alphanumeric character, then alphanumeric, `_`, or `-`, up to 63 characters total. Invalid keys return HTTP 400.

**Response:**

```json
{
  "buckets": [
    {
      "dimensions": {"tag:owner": "team-commerce", "product_type": "kafka"},
      "time_bucket": "2026-01-01",
      "total_amount": "150.00",
      "usage_amount": "120.00",
      "shared_amount": "30.00",
      "row_count": 42
    },
    {
      "dimensions": {"tag:owner": "UNTAGGED", "product_type": "kafka"},
      "time_bucket": "2026-01-01",
      "total_amount": "30.00",
      "usage_amount": "30.00",
      "shared_amount": "0.00",
      "row_count": 8
    }
  ],
  "total_amount": "180.00",
  "usage_amount": "150.00",
  "shared_amount": "30.00",
  "total_rows": 50
}
```

Tag dimensions appear in `dimensions` with the `tag:` prefix (e.g. `"tag:owner": "team-commerce"`). `usage_amount` is cost attributed by actual usage metrics. `shared_amount` is cost split evenly.

---

## Resources

### `GET /api/v1/tenants/{tenant_name}/resources`

List discovered resources. Paginated. Supports three temporal query modes:

| Parameter | Type | Default | Description |
|---|---|---|---|
| `resource_type` | string | — | Filter by type |
| `status` | string | — | Filter by status (`active` or `deleted`) |
| `active_at` | datetime | — | Resources active at this point in time |
| `period_start` + `period_end` | datetime | — | Resources active during this period |
| `search` | string | — | Case-insensitive substring match on `resource_id` and `display_name` |
| `sort_by` | string | — | Column to sort by: `resource_id`, `display_name`, `resource_type`, `status`. Falls back to `resource_id` if invalid. |
| `sort_order` | string | `asc` | Sort direction: `asc` or `desc`. Applied when no temporal params are set. |
| `tag_key` | string | — | Filter to resources that have this tag key |
| `tag_value` | string | — | Narrow `tag_key` filter to this value (requires `tag_key`) |

`search`, `sort_by`, `sort_order`, `tag_key`, and `tag_value` apply only when no temporal params (`active_at`, `period_start`/`period_end`) are set.

If no temporal params: returns all resources. Cannot combine `active_at` with `period_start`/`period_end`.

**Response fields per item:** `ecosystem`, `tenant_id`, `resource_id`, `resource_type`, `display_name`, `parent_id`, `owner_id`, `status`, `created_at`, `deleted_at`, `last_seen_at`, `metadata`.

---

## Identities

### `GET /api/v1/tenants/{tenant_name}/identities`

List discovered identities. Paginated. Same temporal query modes as resources.

| Parameter | Type | Default | Description |
|---|---|---|---|
| `identity_type` | string | — | Filter by type |
| `active_at` | datetime | — | Identities active at this point |
| `period_start` + `period_end` | datetime | — | Identities active during period |
| `search` | string | — | Case-insensitive substring match on `identity_id` and `display_name` |
| `sort_by` | string | — | Column to sort by: `identity_id`, `display_name`, `identity_type`. Falls back to `identity_id` if invalid. |
| `sort_order` | string | `asc` | Sort direction: `asc` or `desc`. Applied when no temporal params are set. |
| `tag_key` | string | — | Filter to identities that have this tag key |
| `tag_value` | string | — | Narrow `tag_key` filter to this value (requires `tag_key`) |

`search`, `sort_by`, `sort_order`, `tag_key`, and `tag_value` apply only when no temporal params are set.

**Response fields per item:** `ecosystem`, `tenant_id`, `identity_id`, `identity_type`, `display_name`, `created_at`, `deleted_at`, `last_seen_at`, `metadata`.

---

## Inventory

### `GET /api/v1/tenants/{tenant_name}/inventory/summary`

Counts of resources and identities grouped by type.

**Response:** `{"resource_counts": {"cluster": {"total": 3, "active": 3, "deleted": 0}}, "identity_counts": {"service_account": {"total": 12, "active": 10, "deleted": 2}}}`

---

## Tags

Tags attach to entities (resources or identities) and propagate to chargeback rows at query time. `entity_type` must be `"resource"` or `"identity"`.

### `GET /api/v1/tenants/{tenant_name}/entities/{entity_type}/{entity_id}/tags`

List all tags on an entity.

**Response:** Array of tag objects with fields `tag_id`, `tenant_id`, `entity_type`, `entity_id`, `tag_key`, `tag_value`, `created_by`, `created_at`.

### `POST /api/v1/tenants/{tenant_name}/entities/{entity_type}/{entity_id}/tags`

Create a tag on an entity. Returns 201. Returns 409 if a tag with the same key already exists on this entity.

**Body:** `{"tag_key": "team", "tag_value": "platform", "created_by": "admin"}`

### `PUT /api/v1/tenants/{tenant_name}/entities/{entity_type}/{entity_id}/tags/{tag_key}`

Update a tag's value.

**Body:** `{"tag_value": "new-value"}`

### `DELETE /api/v1/tenants/{tenant_name}/entities/{entity_type}/{entity_id}/tags/{tag_key}`

Delete a tag by key. Returns 204.

### `GET /api/v1/tenants/{tenant_name}/tags`

List all tags for a tenant. Paginated.

| Parameter | Type | Description |
|---|---|---|
| `entity_type` | string | Filter by entity type (`"resource"` or `"identity"`) |
| `tag_key` | string | Filter by tag key |
| `page` | int | Page number (default 1) |
| `page_size` | int | Page size 1–1000 (default 100) |

**Response:** `{"items": [...], "total": N, "page": 1, "page_size": 100, "pages": N}`

### `GET /api/v1/tenants/{tenant_name}/tags/keys`

List distinct tag keys for a tenant, sorted alphabetically.

| Parameter | Type | Description |
|---|---|---|
| `entity_type` | string | Filter by entity type (`"resource"` or `"identity"`) |

**Response:** `{"keys": ["env", "team", "owner"]}`

### `GET /api/v1/tenants/{tenant_name}/tags/keys/{tag_key}/values`

List distinct values for a tag key, sorted alphabetically. Returns 400 if `tag_key` format is invalid.

| Parameter | Type | Description |
|---|---|---|
| `entity_type` | string | Filter by entity type (`"resource"` or `"identity"`) |
| `q` | string | Prefix filter for autocomplete (case-insensitive) |

**Response:** `{"values": ["prod", "staging"]}`

### `POST /api/v1/tenants/{tenant_name}/tags/bulk`

Bulk create/update tags on explicit entity IDs.

**Body:**

```json
{
  "items": [
    {"entity_type": "resource", "entity_id": "cluster-1", "tag_key": "team", "tag_value": "platform"},
    {"entity_type": "identity", "entity_id": "sa-abc", "tag_key": "team", "tag_value": "data"}
  ],
  "created_by": "admin",
  "override_existing": false
}
```

**Response:** `{"created_count": 2, "updated_count": 0, "skipped_count": 0}`

When `override_existing` is true, existing tags with the same key are updated instead of skipped.

### `POST /api/v1/tenants/{tenant_name}/tags/bulk-by-filter`

Bulk tag all unique resources/identities found in chargebacks matching the given filters. Resolves entities server-side.

**Body:**

```json
{
  "start_date": "2026-01-01",
  "end_date": "2026-01-31",
  "timezone": "America/Denver",
  "identity_id": "sa-abc",
  "tag_key": "team",
  "display_name": "Platform",
  "created_by": "admin",
  "override_existing": false
}
```

`display_name` is stored as the tag value. `identity_id` narrows which chargebacks are scanned.

**Response:** `{"created_count": 2, "updated_count": 0, "skipped_count": 0}`

---

## Pipeline

### `POST /api/v1/tenants/{tenant_name}/pipeline/run`

Trigger a pipeline run for a tenant. Returns 202 (accepted).

Returns HTTP 409 if a run is already in progress. Requires `both` mode — API-only mode cannot trigger runs.

### `GET /api/v1/tenants/{tenant_name}/pipeline/status`

Get latest pipeline run status.

**Response:**

```json
{
  "tenant_name": "my-org",
  "is_running": false,
  "last_run": "2026-03-17T12:00:00Z",
  "last_result": {
    "dates_gathered": 5,
    "dates_calculated": 5,
    "chargeback_rows_written": 142,
    "errors": [],
    "completed_at": "2026-03-17T12:00:00Z"
  }
}
```

`last_result` is `null` if no completed or failed runs exist.

---

## Export

### `POST /api/v1/tenants/{tenant_name}/export`

Stream chargeback data as CSV. Returns `text/csv` with `Content-Disposition: attachment`.

**Body:**

```json
{
  "columns": ["timestamp", "resource_id", "product_type", "identity_id", "amount"],
  "start_date": "2026-01-01",
  "end_date": "2026-01-31",
  "timezone": "America/Denver",
  "filters": {
    "identity_id": "sa-12345",
    "product_type": "KAFKA_NUM_CKU"
  }
}
```

| Field | Type | Default | Description |
|---|---|---|---|
| `columns` | list[string] | 9 default columns | Columns to include |
| `start_date` | date | no | Filter start |
| `end_date` | date | no | Filter end |
| `timezone` | string | no | IANA timezone for date boundaries (e.g. `America/Denver`). Defaults to UTC. |
| `filters` | dict | no | Key-value filters (`identity_id`, `product_type`, `resource_id`, `cost_type`) |

**All available columns:** `ecosystem`, `tenant_id`, `timestamp`, `resource_id`, `product_category`, `product_type`, `identity_id`, `cost_type`, `amount`, `allocation_method`, `allocation_detail`, `tags`, `metadata`.

The `tags` column is serialized as `key=value;key=value` pairs (e.g. `team=platform;env=prod`).

**Default columns:** `timestamp`, `resource_id`, `product_category`, `product_type`, `identity_id`, `cost_type`, `amount`, `allocation_method`, `tags`.

---

## FOCUS Mapping Preview

FOCUS Mapping Preview is an asynchronous, Confluent Cloud-only exposition API.
It reads persisted calculation and source evidence; it never calls the provider,
triggers the pipeline, reruns allocation, or edits/backfills data. Requests
support Daily or Monthly grain and Full, Summary, or Custom column profiles. The
tenant's optional `focus_preview`
block must establish a `direct_payg` commercial profile over the complete
request interval. `billing_currency` defaults to normalized `USD`; any other
configured currency fails closed without conversion.

For setup and complete UI/CLI workflows, start with
[FOCUS Mapping Preview](focus-mapping-preview.md). This section is the HTTP
contract reference.

The API has no built-in authentication. Protect the entire Preview route prefix
behind an authenticated reverse proxy or API gateway, including profile,
submission, recent history, status, manifest, individual-file, and archive
routes. The remote CLI's repeatable `--header NAME=VALUE` option forwards the
deployment's external authentication headers on every request.

### `GET /api/v1/tenants/{tenant_name}/focus-preview/profile`

Return static metadata for the current `focus-1.4-preview-v5` mapping profile:
`mapping_profile_version`, the ordered `full_columns` allowlist, and the ordered
20-column `summary_columns` subset. This endpoint validates tenant existence and
the Confluent Cloud ecosystem but does not initialize Preview storage or the
worker runtime.

### `POST /api/v1/tenants/{tenant_name}/focus-preview/requests`

Create a request. Returns HTTP 202 with a queued status document.

```json
{
  "grain": "daily",
  "start_date": "2026-07-01",
  "end_date": "2026-08-01",
  "column_profile": "full"
}
```

Dates are UTC and use inclusive-start/exclusive-end semantics. The request must
contain 1–31 days within the UTC calendar month containing `start_date`; the
exclusive end may be the first day of the following month.

Monthly requests supply one exact ASCII `YYYY-MM`; public start/end dates are
not accepted for Monthly submission:

```json
{
  "grain": "monthly",
  "month": "2026-07",
  "column_profile": "summary"
}
```

`column_profile` defaults to `full`. Summary uses the fixed profile subset.
Custom supplies `columns`, for example
`{"column_profile":"custom","columns":["BilledCost","ResourceId"]}`.
Supported names retain first-occurrence caller order; unknown and duplicate
entries are logged and ignored. Full and Summary reject `columns`, and Custom
rejects a selection with no supported Full-profile columns.

### `GET /api/v1/tenants/{tenant_name}/focus-preview/requests`

Return recent requests newest first, including queued, running, ready, failed,
and expired requests. Ordering is descending by the immutable
`(created_at, request_id)` pair.

| Query | Default | Constraints | Meaning |
|---|---:|---|---|
| `limit` | `20` | 1–100 | Maximum items returned. |
| `cursor` | none | Non-empty request ID | Continue after the prior page's `next_cursor`. |

The response is `{"items":[...],"next_cursor":"..."}`. `next_cursor` is null
when no later page exists. A missing cursor and a cursor owned by another tenant
both return 400 `Preview request cursor is invalid`.

### `GET /api/v1/tenants/{tenant_name}/focus-preview/requests/{request_id}`

Return the tenant-scoped request. The lifecycle is `queued`, `running`, then
`ready` or `failed`; a ready request becomes `expired` at its fixed cutoff.

Common response fields are `request_id`, `tenant_name`, `grain`, `start_date`,
`end_date`, derived nullable `month`, `column_profile`, ordered
`effective_columns`, `status`, `created_at`, `started_at`,
`completed_at`, `expires_at`, `diagnostic`, `source_snapshot`, and `package`.

- Queued/running responses have no diagnostic, source snapshot, or package.
- Failed responses contain `{code, message, retryable}` and no package. Source-
  related failures can also contain `source_correlation_ids`, a sorted, unique,
  maximum-20 list of opaque `src:v1:<64 lowercase hex>` values. These values do
  not reveal provider record IDs, tenant IDs, raw fields, secrets, or paths.
- Ready responses contain a source snapshot and package metadata.
- Expired responses retain source snapshot and expiry metadata but return
  `package: null`.

The source snapshot contains nullable `calculation_timestamp`, date-ordered
`calculation_coverage` entries, nullable `source_through`,
`effective_coverage_start_date`, `effective_coverage_end_date`, nullable
`evidence_through_date`, nullable `availability_cutoff_end_date`, and nullable
`monthly_status`. Each coverage entry has
`tracking_date`, `calculation_id`, `calculation_completed_at`, and optional
`calculation_run_id`. A ready package contains `manifest`, ordered `files`,
`download_all_name`, and `download_all_url`. Each artifact contains public
`name`, `media_type`, `size_bytes`, `sha256`, optional `order`, and
`download_url`. It contains no artifact root, storage key, or server path.

A ready package stores `focus-1.4-preview-v5` output using the exact ordered
effective columns. Full is the legacy-compatible 65 ordered FOCUS columns plus
12 custom evidence columns; Summary and Custom are projections of those same
validated rows. The stored manifest declares `conformance_status:
non_conforming`, profile version, grain, requested bounds, derived month,
column profile, effective columns,
calculation/source coverage, exact known gaps, validation status/counts, cost
and quantity reconciliation, seven-day lifecycle, and ordered file checksums.
Mapping/profile validation completes before atomic publication. Manifest and
CSV downloads return verified stored bytes; the API, UI, and CLI do not remap
them.

With `preview.max_csv_file_bytes: null`, the package has one
`cost-and-usage.csv`. A positive byte limit may produce ordered names such as
`cost-and-usage-part-00001-of-00003.csv`. Every part repeats the header, no row
is split, and the limit includes the header and LF record terminators. A header
or single row that cannot fit fails generation with
`preview_csv_row_exceeds_file_size_limit`.

The package may contain multiple persisted billing origins and multiple actual
allocation portions per origin. `CCloudBillingLineItem` remains the sole
allocation origin; raw Cost rows remain classification/coverage evidence linked
to that existing billing key. Actual `UNALLOCATED` portions have null allocated
resource/name/tag fields. Origin `Tags` and target `AllocatedTags` are resolved
separately at package time and frozen into the stored bytes.

Monthly requests preserve the full requested calendar bounds while aggregating
only the frozen effective daily evidence interval. They remain `provisional`
until both the 72-hour post-month threshold and configured acquisition cutoff
permit complete full-month evidence; only then can a fully validated package be
`settled`. Monthly aggregation preserves non-additive allocation ratio/method,
target, classification, tier, pricing, tag, SKU, and provenance distinctions.
It does not invoke the provider or allocation code.

### `GET /api/v1/tenants/{tenant_name}/focus-preview/requests/{request_id}/manifest`

Return the exact stored `manifest.json` bytes for a ready request.

### `GET /api/v1/tenants/{tenant_name}/focus-preview/requests/{request_id}/files/{file_name}`

Return the exact stored bytes for a file enumerated by a ready request. The
package contains one or more ordered CSV files declared by `package.files`.

### `GET /api/v1/tenants/{tenant_name}/focus-preview/requests/{request_id}/archive`

Stream a deterministic ZIP for a ready request. The archive contains
`manifest.json` followed by data files in manifest order and is returned as
`application/zip` with filename
`focus-mapping-preview-{request_id}.zip`. The archive is a transport wrapper and
is not listed as a manifest data artifact.

Requested packages are available from ready publication until, but not
including, `expires_at`, exactly seven days later. At the cutoff, status remains
available as `expired`, while manifest, file, and archive endpoints return 410.

### Preview errors

| Condition | Status | Detail |
|---|---:|---|
| Invalid JSON, date, grain, or profile | 422 | FastAPI validation body |
| Unknown tenant | 404 | `Tenant '<tenant_name>' not found` |
| Non-Confluent Cloud tenant | 400 | `FOCUS Mapping Preview currently supports only Confluent Cloud tenants` |
| Start is not before end | 400 | `start_date must be before end_date` |
| Range crosses the allowed UTC month | 400 | `Daily preview range must stay within one UTC calendar month` |
| Invalid or unrepresentable Monthly value | 400 | `month must use YYYY-MM` |
| `columns` supplied for Full or Summary | 400 | `columns may be supplied only when column_profile is custom` |
| Custom has no supported columns | 400 | `Custom column selection must contain at least one supported Full-profile column` |
| Runtime unavailable | 503 | `FOCUS Mapping Preview runtime is unavailable` |
| Storage unavailable | 503 | `FOCUS Mapping Preview storage is unavailable` |
| Recovery unavailable | 503 | `FOCUS Mapping Preview recovery is unavailable` |
| Worker scheduling unavailable | 503 | `FOCUS Mapping Preview worker is unavailable` |
| Invalid or foreign recent-list cursor | 400 | `Preview request cursor is invalid` |
| Request absent or owned by another tenant | 404 | `Preview request '<request_id>' not found` |
| Manifest/file/archive requested before ready or after failure | 409 | Status-specific not-ready detail |
| Manifest/file/archive requested after expiry | 410 | `Preview request '<request_id>' expired at <UTC timestamp>` |
| File not enumerated by a ready package | 404 | File-specific not-found detail |
| Stored bytes unavailable | 500 | `Stored preview artifact is unavailable` |

Calculation diagnostics use these exact public meanings:

The message strings below remain unchanged for compatibility. For Daily they
refer to the explicit requested interval; for Monthly they apply only to the
frozen effective evidence interval, which can be shorter than the requested
calendar month while provisional. An empty early-month effective interval
performs no calculation lookup and therefore does not emit an unavailable or
incomplete calculation diagnostic.

| Code | Retryable | Message |
|---|---:|---|
| `calculation_metadata_unavailable` | false | `One or more requested dates lack preview calculation metadata.` |
| `calculation_before_acquisition_lookback` | false | `Required retained calculation evidence is unavailable outside the current acquisition window.` |
| `calculation_pending_cutoff_window` | true | `One or more requested dates are still inside the configured acquisition cutoff window; wait for the dates to enter the acquisition window, run the pipeline, and retry.` |
| `calculation_unavailable` | true | `No successful persisted calculation is available for the requested dates; run the pipeline and retry.` |
| `calculation_coverage_incomplete` | true | `No successful persisted calculation covers every requested date; run the pipeline and retry.` |

Eligibility and source diagnostics are:

| Code | Retryable | Meaning |
|---|---:|---|
| `preview_commercial_profile_unavailable` | false | The optional tenant block is absent or its Direct-billed PAYG effective interval does not contain the request. |
| `preview_billing_currency_unsupported` | false | The configured or selected currency is not USD. Preview performs no currency conversion. |
| `preview_billing_currency_unknown` | false | Selected persisted aggregate currency evidence is blank. |
| `preview_source_record_malformed` | false | Persisted provider source evidence is malformed. |
| `preview_source_scope_unsupported` | false | Source evidence is not fully contained in the effective Daily or Monthly evidence interval. |
| `preview_charge_classification_ambiguous` | false | Credit/refund/adjustment/correction-like semantics are not authoritative. |
| `preview_source_line_type_unknown` | false | A source record has no line type. |
| `preview_source_line_type_unsupported` | false | A provider line type is unknown to this release. |
| `preview_source_mapping_unavailable` | false | A known line type lacks required mapping evidence, such as a returned unit for `KAFKA_STREAMS`. |
| `preview_source_record_incomplete` | false | Required Preview evidence is absent. |
| `preview_source_economics_unsupported` | false | Monetary or quantity values are outside the supported tracer. |
| `preview_source_reconciliation_failed` | false | Source, aggregate, or allocation evidence does not reconcile. |
| `preview_source_coverage_incomplete` | false | Complete source and aggregate origin coverage does not match. |
| `preview_mapping_scope_unsupported` | false | The complete source set exceeds the current v5 Full-row mapping scope before profile projection, including multiple native/tier Cost rows associated with one billing origin. |
| `preview_allocation_lineage_incomplete` | false | Persisted calculation lineage is missing, incomplete, corrupt, or structurally inconsistent for one or more billing origins. |
| `preview_billing_account_unavailable` | false | No authoritative persisted Confluent organization binding is available. |
| `preview_billing_account_conflicting` | false | Persisted Confluent organization evidence conflicts for the tenant partition. |
| `preview_provider_context_incomplete` | false | Authoritative resource context is absent or incompatible; all TABLEFLOW rows use this failure because current inventory cannot prove their provider context. |
| `preview_mapping_validation_failed` | false | A generated Daily Full row or Monthly Full aggregate does not satisfy the current v5 Full-row mapping profile before Full/Summary/Custom projection. |

All accepted native line types can use persisted calculation lineage, including
organization-wide rows, provider-null promotional allowances, and signed
refunds. TABLEFLOW still returns `preview_provider_context_incomplete` because
current inventory cannot prove its provider context. Missing legacy billing
association returns `preview_source_coverage_incomplete`; recovery requires an
ordinary provider regather followed by ordinary calculation. Missing or invalid
lineage returns `preview_allocation_lineage_incomplete`; exact cost or quantity
shortfall/overage returns `preview_source_reconciliation_failed`. These failures
are non-retryable, carry at most 20 sorted safe correlations, and persist null
source snapshot and package fields; manifest/file retrieval remains unavailable.

Incomplete persisted calculation metadata has precedence over acquisition,
commercial, currency, source, and mapping diagnostics. `lookback_days` is capped
at 364 and classifies the current acquisition/recalculation window; it is not a
retention or reconstruction promise. The API provides no data-editing,
historical reconstruction, or correlation-repair endpoint.

---

## Topic Attributions

Topic attribution rows are produced by the optional `topic_overlay` pipeline
stage (Confluent Cloud only). Each row represents the cost portion attributed to
one topic for one billing line item. Requires `topic_attribution.enabled: true`
in plugin settings.

### `GET /api/v1/tenants/{tenant_name}/topic-attributions`

List topic attribution rows. Paginated.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `start_date` | date | no | Filter start |
| `end_date` | date | no | Filter end |
| `timezone` | string | no | IANA timezone for date boundaries. Defaults to UTC. |
| `cluster_resource_id` | string | no | Filter by cluster (e.g. `lkc-abc123`) |
| `topic_name` | string | no | Filter by topic name |
| `product_type` | string | no | Filter by product type |
| `attribution_method` | string | no | Filter by method (`bytes_ratio`, `retained_bytes_ratio`, `even_split`) |
| `tag_key` | string | no | Filter by tag key (exact match) |
| `tag_value` | string | no | Filter by tag value (requires tag_key, exact match) |
| `page` | int | no | Page number (default 1) |
| `page_size` | int | no | Page size 1–1000 (default 100) |

**Response fields per item:** `dimension_id`, `ecosystem`, `tenant_id`, `timestamp`, `env_id`, `cluster_resource_id`, `topic_name`, `product_category`, `product_type`, `attribution_method`, `amount`.

### `GET /api/v1/tenants/{tenant_name}/topic-attributions/aggregate`

Multi-dimensional aggregation of topic attribution rows.

| Parameter | Type | Default | Description |
|---|---|---|---|
| `group_by` | list[string] | `["topic_name"]` | Columns or tag keys to group by (repeatable). Use `tag:{key}` for tag-based grouping. |
| `time_bucket` | string | `day` | `hour`, `day`, `week`, or `month` |
| `start_date` | date | no | Filter start |
| `end_date` | date | no | Filter end |
| `timezone` | string | no | IANA timezone for date boundaries. Defaults to UTC. |
| `cluster_resource_id` | string | no | Filter by cluster |
| `topic_name` | string | no | Filter by topic |
| `product_type` | string | no | Filter by product type |
| `tag:{key}` | string | no | Filter to rows where the tag `{key}` matches the given value. Repeatable for AND semantics. Comma-separated values in a single param are OR-matched. |

**Valid `group_by` columns:** `topic_name`, `cluster_resource_id`, `env_id`, `product_type`, `product_category`, `attribution_method`. Invalid dimension names are silently dropped (no 400).

**Tag-based grouping (`group_by=tag:{key}`):**

Use `tag:` prefix to group by a resource tag key instead of a dimension column. Examples:

- `group_by=tag:owner` — group by the `owner` tag; rows with no `owner` tag land in an `UNTAGGED` bucket.
- `group_by=tag:owner&group_by=tag:department` — group by two tag keys; each bucket has both keys in `dimensions`.
- `group_by=tag:owner&group_by=topic_name` — mix tag and dimension grouping in one query.

Topic attribution is resource-only — tags are resolved from the resource entity (`cluster_resource_id:topic:topic_name`) only. There is no identity join and no resource/identity precedence rule.

**Tag-based filtering (`tag:{key}={value}`):**

Dynamic query parameters prefixed `tag:` are treated as tag filters and are independent of `group_by`.

- `tag:department=eng` — only rows where the resource's `department` tag equals `eng`.
- `tag:team=platform,commerce` — `team` IN (`platform`, `commerce`) — comma-separated values are OR-matched within one key.
- `tag:owner=alice&tag:department=eng` — multiple tag params are AND-matched across keys.
- Untagged rows (no matching tag key on the resource) are excluded from filtered results.

Tag key format: must start with an alphanumeric character, then alphanumeric, `_`, or `-`, up to 63 characters total. Invalid keys return HTTP 400.

**Response:**

```json
{
  "buckets": [
    {
      "dimensions": {"tag:owner": "alice", "topic_name": "payments-events"},
      "time_bucket": "2026-01-01",
      "total_amount": "10.00",
      "row_count": 1
    },
    {
      "dimensions": {"tag:owner": "UNTAGGED", "topic_name": "untagged-events"},
      "time_bucket": "2026-01-01",
      "total_amount": "25.00",
      "row_count": 1
    }
  ],
  "total_amount": "35.00",
  "total_rows": 2
}
```

Tag dimensions appear in `dimensions` with the `tag:` prefix (e.g. `"tag:owner": "alice"`). Rows with no matching tag appear under `"UNTAGGED"`.

### `GET /api/v1/tenants/{tenant_name}/topic-attributions/dates`

List distinct dates for which topic attribution rows exist.

**Response:** `{"dates": ["2026-01-01", "2026-01-02", ...]}`

### `POST /api/v1/tenants/{tenant_name}/topic-attributions/export`

Stream topic attribution data as CSV. Returns `text/csv` with
`Content-Disposition: attachment; filename=topic_attributions.csv`.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `start_date` | date | no | Filter start |
| `end_date` | date | no | Filter end |
| `timezone` | string | no | IANA timezone for date boundaries. Defaults to UTC. |

**CSV columns:** `ecosystem`, `tenant_id`, `timestamp`, `env_id`, `cluster_resource_id`, `topic_name`, `product_category`, `product_type`, `attribution_method`, `amount`.


---

## Graph

Returns cost topology as a graph (nodes + edges) for use by the cost explorer graph frontend. One endpoint covers three views depending on the `focus` parameter.

### `GET /api/v1/tenants/{tenant_name}/graph`

Return a neighborhood of nodes and directed edges centred on a focus entity.

| Parameter | Type | Default | Description |
|---|---|---|---|
| `focus` | string | — | Entity ID to focus on. Omit for root (tenant) view. |
| `depth` | int | 1 | Hierarchy hops from focus (1–3). |
| `at` | datetime | now | Point-in-time lifecycle filter (ISO 8601 with timezone, e.g. `2026-03-15T00:00:00Z`). |
| `start_date` | date | — | Cost period start date. Overrides `at`-derived month when provided. |
| `end_date` | date | — | Cost period end date. Overrides `at`-derived month when provided. |
| `timezone` | string | UTC | IANA timezone for `start_date`/`end_date` boundaries. |

**Views:**

- **Root view** (`focus` omitted): returns a synthetic tenant node plus one environment node per active environment. Edges are `parent` type, directed tenant → environment.
- **Environment focus** (`focus=env-abc`): returns the environment node plus all direct child resources up to `depth` hops (clusters, connectors, flink pools, schema registries). Edges are `parent` type, directed parent → child.
- **Cluster focus** (`focus=lkc-abc`): returns the cluster node, its child topic nodes, and any identity (service account / pool) nodes charged to the cluster via chargeback. Edges are `parent` (cluster → topic) and `charge` (cluster → identity).

**Billing period:** when `start_date`/`end_date` are omitted, the cost window defaults to the full calendar month containing `at` (e.g. `at=2026-03-15` → March 1–April 1).

**Response:** `GraphResponse`

```json
{
  "nodes": [
    {
      "id": "env-abc",
      "resource_type": "environment",
      "display_name": "Production",
      "cost": "1234.56",
      "created_at": "2025-01-01T00:00:00Z",
      "deleted_at": null,
      "tags": {"team": "platform"},
      "parent_id": null,
      "cloud": "aws",
      "region": "us-east-1",
      "status": "active",
      "cross_references": []
    }
  ],
  "edges": [
    {
      "source": "org-123",
      "target": "env-abc",
      "relationship_type": "parent",
      "cost": null
    }
  ]
}
```

**Node fields:**

| Field | Type | Description |
|---|---|---|
| `id` | string | Entity ID (`resource_id` or `identity_id`) |
| `resource_type` | string | Entity type: `tenant`, `environment`, `kafka_cluster`, `kafka_topic`, `service_account`, etc. |
| `display_name` | string\|null | Human-readable name |
| `cost` | decimal string | Aggregated cost for the billing period |
| `created_at` | datetime\|null | Lifecycle start |
| `deleted_at` | datetime\|null | Lifecycle end (null = still active) |
| `tags` | object | Tag key→value dict resolved from `entity_tags` |
| `parent_id` | string\|null | Parent entity ID |
| `cloud` | string\|null | Cloud provider |
| `region` | string\|null | Cloud region |
| `status` | string | `active` or `deleted` |
| `cross_references` | list[CrossReferenceGroup] | For identity nodes: other resources this identity is charged in (excluding the focus cluster), grouped by resource type. Each group has `resource_type` (string), `total_count` (int, full DB count before cap), and `items` (list of up to 5 `CrossReferenceItem` objects sorted by cost descending). Each item has `id`, `resource_type`, `display_name` (string\|null), and `cost` (decimal string). |

**Edge `relationship_type` values:** `parent` (hierarchy), `charge` (identity charged to cluster).

**Error codes:** 400 (tz-naive `at` value), 404 (unknown tenant or unknown `focus` entity), 422 (unparseable parameters).

### `GET /api/v1/tenants/{tenant_name}/graph/search`

Search resources and identities by partial name or ID match. Intended for the jump-to-entity feature in the cost explorer graph frontend.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `q` | string | yes | Case-insensitive partial match against `resource_id`, `display_name`, and `identity_id`. Minimum length 1. |

Results are ordered by relevance: exact match first, then prefix match, then substring match. Returns at most 20 results. Returns an empty list when no entities match (never 404).

**Response:** `GraphSearchResponse`

```json
{
  "results": [
    {
      "id": "lkc-abc123",
      "resource_type": "kafka_cluster",
      "display_name": "Production Cluster",
      "parent_id": "env-abc",
      "parent_display_name": "Production Environment",
      "status": "active"
    }
  ]
}
```

**Result fields:**

| Field | Type | Description |
|---|---|---|
| `id` | string | Entity ID (`resource_id` for resources, `identity_id` for identities) |
| `resource_type` | string | `resource_type` for resources; `identity_type` for identities (e.g. `service_account`) |
| `display_name` | string\|null | Human-readable name |
| `parent_id` | string\|null | Parent entity ID. Always `null` for identity results (identities have no parent in the graph model) |
| `parent_display_name` | string\|null | Human-readable name of the parent entity. `null` when parent has no display name or result has no parent |
| `status` | string | `active` or `deleted` |

**Error codes:** 404 (unknown tenant), 422 (missing or empty `q`).

### `GET /api/v1/tenants/{tenant_name}/graph/diff`

Compare costs between two time periods for a neighborhood. Returns a flat list of nodes annotated with before/after costs and a diff status, enabling the "what changed?" view in the cost explorer.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `from_start` | date | yes | Start of the "before" period (inclusive). |
| `from_end` | date | yes | End of the "before" period (inclusive). |
| `to_start` | date | yes | Start of the "after" period (inclusive). |
| `to_end` | date | yes | End of the "after" period (inclusive). |
| `focus` | string | no | Entity ID to focus on. Omit for root (tenant/environment) view. |
| `depth` | int | no (default 1) | Hierarchy hops from focus (1–3). |
| `timezone` | string | no | IANA timezone for date boundaries. Defaults to UTC. |

**Response:** `GraphDiffResponse`

```json
{
  "nodes": [
    {
      "id": "lkc-abc123",
      "resource_type": "kafka_cluster",
      "display_name": "Production Cluster",
      "parent_id": "env-abc",
      "cost_before": "100.00",
      "cost_after": "150.00",
      "cost_delta": "50.00",
      "pct_change": "50.00",
      "status": "changed"
    }
  ]
}
```

**Node fields:**

| Field | Type | Description |
|---|---|---|
| `id` | string | Entity ID |
| `resource_type` | string | Entity type |
| `display_name` | string\|null | Human-readable name |
| `parent_id` | string\|null | Parent entity ID |
| `cost_before` | decimal string | Total cost in the "before" window. `"0"` for new entities. |
| `cost_after` | decimal string | Total cost in the "after" window. `"0"` for deleted entities. |
| `cost_delta` | decimal string | `cost_after - cost_before`. Negative means cost decreased. |
| `pct_change` | decimal string\|null | Percentage change: `(cost_delta / cost_before) * 100`. `null` when `cost_before == 0`. |
| `status` | string | `new` (only in after), `deleted` (only in before), `changed` (in both, cost differs), `unchanged` (in both, cost equal) |

**Error codes:** 404 (unknown tenant or unknown `focus` entity), 422 (missing date parameters).

### `GET /api/v1/tenants/{tenant_name}/graph/timeline`

Return a daily cost time series for a single entity. Enables sparklines and cost-over-time drill-down in the cost explorer graph frontend without leaving the graph view.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `entity_id` | string | yes | `resource_id` or `identity_id` of the entity. |
| `start` | date | yes | Start date (inclusive). |
| `end` | date | yes | End date (inclusive). |
| `timezone` | string | no | IANA timezone for date boundaries. Defaults to UTC. |

Entity type routing: topics use `topic_attribution_facts`; environments use `chargeback_facts` grouped by `env_id`; all other resources use `chargeback_facts` grouped by `resource_id`; identities use `chargeback_facts` filtered by `identity_id`.

Every calendar day in `[start, end]` is present in the response. Days with no billing data are returned with `cost: "0"` (gap filling).

**Response:** `GraphTimelineResponse`

```json
{
  "entity_id": "lkc-abc123",
  "points": [
    {"date": "2026-04-01", "cost": "12.34"},
    {"date": "2026-04-02", "cost": "0"},
    {"date": "2026-04-03", "cost": "15.00"}
  ]
}
```

**Point fields:**

| Field | Type | Description |
|---|---|---|
| `date` | date string | Calendar date in `YYYY-MM-DD` format |
| `cost` | decimal string | Total cost on that day. `"0"` for days with no billing data. |

**Error codes:** 404 (unknown tenant or `entity_id` not found in resources or identities), 422 (missing required parameters).

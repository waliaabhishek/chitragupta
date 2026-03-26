# API Reference

Base URL: `http://localhost:8080` (configurable via `api.host` and `api.port`)

All endpoints except `/health` are prefixed with `/api/v1`.

## General behavior

- **Authentication:** None built-in. Use a reverse proxy for auth.
- **CORS:** Configurable via `api.enable_cors` and `api.cors_origins`. Allowed methods: GET, POST, PATCH, DELETE.
- **Request timeout:** Requests exceeding `api.request_timeout_seconds` (default 30, max 300) return HTTP 504.
- **Content type:** JSON for all endpoints except export (CSV).

### Pagination

List endpoints accept:

| Parameter | Type | Default | Constraints |
|---|---|---|---|
| `page` | int | 1 | >= 1 |
| `page_size` | int | 100 | 1–1000 |

Response includes: `items`, `total`, `page`, `page_size`, `pages`.

### Date range defaults

When `start_date` / `end_date` are omitted, the API uses the tenant's configured lookback window.

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

**Per-tenant fields:** `tenant_name`, `tables_ready`, `has_data`, `pipeline_running`, `pipeline_stage`, `pipeline_current_date`, `last_run_status`, `last_run_at`, `permanent_failure`.

---

## Tenants

### `GET /api/v1/tenants`

List all configured tenants with summary pipeline state.

**Response fields per tenant:** `tenant_name`, `tenant_id`, `ecosystem`, `dates_pending`, `dates_calculated`, `last_calculated_date`.

### `GET /api/v1/tenants/{tenant_name}/status`

Detailed per-date pipeline state for a tenant.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `start_date` | date | no | Filter from this date |
| `end_date` | date | no | Filter to this date |

**Response:** List of `{tracking_date, billing_gathered, resources_gathered, chargeback_calculated}` per date.

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
| `group_by` | list[string] | `["identity_id"]` | Columns to group by (repeatable) |
| `time_bucket` | string | `day` | `hour`, `day`, `week`, or `month` |
| `start_date` | date | no | Filter start |
| `end_date` | date | no | Filter end |
| `timezone` | string | no | IANA timezone for date boundaries (e.g. `America/Denver`). Defaults to UTC. |
| `identity_id` | string | no | Filter by identity |
| `product_type` | string | no | Filter by product type |
| `resource_id` | string | no | Filter by resource |
| `cost_type` | string | no | Filter by cost type |

**Valid `group_by` columns:** `identity_id`, `resource_id`, `product_type`, `product_category`, `cost_type`, `allocation_method`, `environment_id`.

**Response:**

```json
{
  "buckets": [
    {
      "dimensions": {"identity_id": "sa-12345"},
      "time_bucket": "2026-01-01",
      "total_amount": "150.00",
      "usage_amount": "120.00",
      "shared_amount": "30.00",
      "row_count": 42
    }
  ],
  "total_amount": "150.00",
  "usage_amount": "120.00",
  "shared_amount": "30.00",
  "total_rows": 42
}
```

`usage_amount` is cost attributed by actual usage metrics. `shared_amount` is cost split evenly.

---

## Resources

### `GET /api/v1/tenants/{tenant_name}/resources`

List discovered resources. Paginated. Supports three temporal query modes:

| Parameter | Type | Description |
|---|---|---|
| `resource_type` | string | Filter by type |
| `status` | string | Filter by status |
| `active_at` | datetime | Resources active at this point in time |
| `period_start` + `period_end` | datetime | Resources active during this period |

If no temporal params: returns all resources. Cannot combine `active_at` with `period_start`/`period_end`.

**Response fields per item:** `ecosystem`, `tenant_id`, `resource_id`, `resource_type`, `display_name`, `parent_id`, `owner_id`, `status`, `created_at`, `deleted_at`, `last_seen_at`, `metadata`.

---

## Identities

### `GET /api/v1/tenants/{tenant_name}/identities`

List discovered identities. Paginated. Same temporal query modes as resources.

| Parameter | Type | Description |
|---|---|---|
| `identity_type` | string | Filter by type |
| `active_at` | datetime | Identities active at this point |
| `period_start` + `period_end` | datetime | Identities active during period |

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

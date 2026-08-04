# Troubleshooting

## Operational logging

Chitragupta emits diagnostic events at workflow and resource-owner boundaries.
Use them to follow one request or pipeline run without exposing source records,
credentials, or raw failure messages.

### Event categories and levels

Operational events cover:

- application startup, configuration, storage availability, and cleanup;
- API request lifecycle, timeouts, rejected work, and uncaught failures;
- pipeline, gather, calculation, allocation, and emitter lifecycle;
- provider and metrics requests, retries, and terminal failures;
- FOCUS Mapping Preview requests, revisions, repairs, retention, and recovery.

Log levels have consistent operational meaning:

| Level | Meaning | Operator response |
|---|---|---|
| `DEBUG` | Request or provider-call lifecycle detail useful for temporary tracing. | Enable for the affected module while investigating, then return it to its normal level. |
| `INFO` | Expected lifecycle transitions and bounded completion summaries. | Use for run history and correlation; no action is normally required. |
| `WARNING` | Degraded, deferred, retryable, or fallback behavior where processing can continue. | Check `retryable`, the attempt fields, and the matching completion or fallback event. |
| `ERROR` | Terminal operation failure or failure to record a safe fallback/cleanup result. | Follow the correlation fields to the owner event, restore the affected dependency, and retry only when the event says the operation is retryable. |

Pure calculation and transformation modules generally do not log. The workflow
or resource owner that invokes them logs the lifecycle result. This keeps routine
volume bounded and avoids per-record `INFO` events. Provider calls, emitters, and
other potentially high-cardinality work emit start/completion counts or bounded
failure summaries rather than payloads or one event per record.

### Correlation fields

Context fields are appended as `key=value`. Values are bounded, escaped, and
appear only when they apply to that event. Absence means the identifier or
attribute is not available at that boundary; it must not be treated as an empty
identifier.

| Purpose | Canonical fields |
|---|---|
| Tenant | `tenant_name`, `tenant_id`, `ecosystem` |
| API | `request_id`, `error_id` |
| Workflow | `pipeline_run_id`, `calculation_id`, `stage`, `operation`, `outcome`, `pipeline` |
| Preview | `revision_id`, `repair_id`, `month`, `diagnostic_code` |
| Daily processing | `tracking_date` |
| Retry | `retryable`, `attempt_number`, `max_attempts` |
| Failure | `error_type`, `root_error_type`, `root_error_code`, `traceback_frames` |
| Resource summary | `resource_id`, `product_type`, `service_type`, `emitter_name` |

Use `tracking_date` for a daily billing or calculation date. Retry position is
always `attempt_number` of `max_attempts`. Search with the most specific
identifier available:

1. Start with `request_id` for an API call or `pipeline_run_id` for a pipeline.
2. Add `calculation_id` and `tracking_date` to isolate one calculated day.
3. Add `revision_id`, `repair_id`, or `month` for Preview work.
4. Follow `stage`, `operation`, and `outcome` to find the last successful
   transition and the owner of the failure.

### Exception ownership and redaction

The component that retries, converts, suppresses, or records a terminal failure
owns the detailed exception event. A calling API or scheduler may emit a result
summary, but it does not repeat traceback details. This exactly-once ownership
prevents one failure from appearing to be several independent incidents.

Failure context is sanitized:

- `error_type` is the immediate exception class;
- `root_error_type` is the deepest different cause class, when present;
- `root_error_code` is a safe scalar code or status, when available;
- `traceback_frames` contains at most eight `module:function:line` frames and
  never includes source lines or an exception message.

Logs never intentionally include credentials, tokens, authentication headers,
connection strings, provider or source payloads, database queries or
parameters, response bodies, raw URLs with query strings, or raw exception
messages. Do not add these values to custom log formats or wrapper messages.

### Representative traces

| Scenario | Owner and event | Expected context | Operator action |
|---|---|---|---|
| Routine pipeline run | Workflow owner: `pipeline_run_started`, then `pipeline_run_completed` | Same tenant and `pipeline_run_id`; `operation=pipeline_run`; stages and `outcome=started` / `completed`; bounded gathered/calculated/emitted counts. | No action. Use the run ID and `tracking_date`/`calculation_id` events to investigate an unexpected result. |
| API request fails unexpectedly | Global API error owner | `request_id`, `error_id`, `stage=api_request`, `outcome=failed`, and sanitized cause fields. | Give support the request and error IDs. Search the same request ID for the route decision immediately before the failure. |
| Provider request is retried | Provider or metrics request owner | `stage=provider_request` or metrics stage, `outcome=retry`, `retryable=true`, attempt position, and sanitized cause. | Check reachability, rate limits, and credentials without copying them into logs. Escalate if the final attempt produces a terminal event. |
| Pipeline fails | Workflow owner: `pipeline_run_failed`; API or scheduler emits only a completion summary | Tenant, `pipeline_run_id`, `stage=pipeline_run`, `operation=pipeline_run`, `outcome=failed`, retryability, and sanitized cause fields on the owner event. | Fix the cause, confirm durable pipeline status, then trigger or wait for the next run according to `retryable`. |
| Preview supporting-evidence write falls back safely | `lineage_persistence_failed`, followed by `lineage_fallback_persisted` | Same tenant, `pipeline_run_id`, `calculation_id`, and `tracking_date`; first event has `outcome=mark_unavailable` and safe database cause; second has `outcome=lineage_unavailable`. | Restore database write health and rerun the affected date before relying on Preview output. Generic pipeline results remain governed by their own completion event. |
| Preview revision publication is deferred | Revision owner | `revision_id` when one exists, `month`, publication or retention stage, `outcome=deferred`, retryability, and sanitized cause. | Preserve the current revision, restore artifact/storage availability, and allow the next scheduled cycle to retry. |
| Fallback or cleanup also fails | Resource owner with `ERROR` and `outcome=fallback_failed` or `failed` | Applicable request/run/revision/repair correlation plus sanitized cause fields. | Treat as an operator incident: restore the named dependency, verify readiness/status, and retry only after the dependency is healthy. |

## Config errors

### `Required environment variable 'X' is not set`

**Cause**: `${X}` in YAML but `X` not in environment.
**Fix**: Export the variable or add a default: `${X:-fallback}`.

### `tenants A and B share storage connection_string`

**Cause**: Two tenants configured with same DB path.
**Fix**: Give each tenant a unique database path.

### `lookback_days must be > cutoff_days`

**Cause**: `lookback_days` ≤ `cutoff_days` in tenant config.
**Fix**: Set `lookback_days` higher than `cutoff_days` (default: 200 > 5).

### Preview capacity configuration is rejected

**Cause**: The global and per-tenant queued limits are not both zero or both
positive; the positive per-tenant queued limit is not lower than the global
limit; or `preview.max_running_generations_per_tenant` exceeds
`preview.max_workers`. Historical repair configuration is also rejected when
`preview.max_queued_repairs` is negative or is not an integer.

**Fix**: Use the defaults (`max_workers: 2`, global queued: `8`, per-tenant
running: `1`, per-tenant queued: `2`, repair queued: `8`) or preserve those
relationships. To disable generation waiting, set both generation queue limits
to `0`. To disable repair waiting independently, set
`preview.max_queued_repairs: 0`.

### `username and password required for basic auth`

**Cause**: `auth_type: basic` set but credentials missing.
**Fix**: Add `username` and `password` fields under `metrics:`.

### `bootstrap_servers required when source='admin_api'`

**Cause**: `resource_source.source: admin_api` but no broker address.
**Fix**: Set `resource_source.bootstrap_servers: host:9092`.

### `discovery_query required when source includes 'prometheus'`

**Cause**: `identity_source.source: prometheus` but no `discovery_query`.
**Fix**: Add `discovery_query` pointing to a metric with your identity label.

## Runtime errors

### `No WorkflowRunner available — run in 'both' mode`

**Cause**: Pipeline triggered via API but engine started with `--mode api` only.
**Fix**: Restart with `--mode both` or trigger runs via cron/scheduler externally.

### `Pipeline is already running for tenant X` (HTTP 409)

**Cause**: Concurrent API trigger while pipeline is in progress.
**Fix**: Wait for the current run to complete, check `/pipeline/status`.

### `Execution timed out after Xs`

**Cause**: Tenant run exceeded `tenant_execution_timeout_seconds`.
**Fix**: Increase timeout or reduce `lookback_days`.

### `ALERT: Tenant X has been permanently suspended`

**Cause**: Gather failures exceeded `gather_failure_threshold` (default 5).
**Fix**:
1. Check logs for the root cause (API key expired, Prometheus unreachable, etc.)
2. Fix the underlying issue
3. Restart the engine (resets failure state)

## Prometheus connectivity

### No metrics data returned

**Causes**:
- Wrong URL — check `metrics.url` resolves from engine host
- Auth failure — check `auth_type`, credentials
- Metric name mismatch — verify metric names with `curl prometheus:9090/api/v1/label/__name__/values`
- No data in range — check that metrics exist for the billing period dates

### Wrong identity label

**Cause**: `identity_source.label` doesn't match actual Prometheus label name.
**Fix**: Run `curl "prometheus:9090/api/v1/query?query=<your_metric>"` and check label names.

## CSV emitter

### Empty CSV files

**Cause**: All costs allocated to UNALLOCATED — no identities resolved.
**Fix**: Check identity discovery (verify Prometheus metrics have expected labels).

### Permission denied writing CSV

**Cause**: `output_dir` not writable by engine process.
**Fix**: Create directory and grant write access, or change `output_dir`.

## Database issues

### `sqlite3.OperationalError: database is locked`

**Cause**: Two processes writing to the same SQLite file simultaneously, or a crashed process left a lock.
**Fix**:
1. Ensure only one engine process runs per tenant database.
2. If stale lock: stop the engine, delete the `-wal` and `-shm` sidecar files alongside the `.db` file, restart.
3. For multi-process use, switch to PostgreSQL.

### `alembic.util.exc.CommandError: Can't locate revision`

**Cause**: Database schema is ahead of the codebase (downgraded to older version) or migration history is corrupted.
**Fix**:
1. Check migration state: `uv run alembic -c src/core/storage/migrations/alembic.ini history`
2. If schema is ahead: upgrade codebase to match or run `alembic downgrade` to target revision.
3. If history corrupted: back up data, drop and recreate the database, restart engine (tables auto-created).

### `sqlalchemy.exc.OperationalError: no such table`

**Cause**: Tables not created — engine did not run `bootstrap_storage()` on first start, or database file was replaced.
**Fix**: Tables are created automatically on first `run_once()` or `run_loop()`. Ensure the engine starts with `--mode worker` or `--mode both`. If the database file was manually replaced, restart the engine.

### Chargeback rows missing for some dates

**Cause**: `cutoff_days` window excludes recent dates.
**Fix**:
- Check `lookback_days` and `cutoff_days` — recent dates within `cutoff_days` of today are intentionally skipped.
- Check logs for `gathered=0` — indicates billing API returned no data for those dates.

## Performance issues

### High memory usage

**Cause**: Large `lookback_days` window on first run fetches many billing dates at once.
**Fix**:
- Reduce `metrics_step_seconds` only if finer granularity is actually needed — lower values increase Prometheus query volume.
- For CCloud: lower `billing_api.days_per_query` (default 15) to fetch smaller billing windows.

### Slow pipeline runs

**Cause**: Prometheus queries time out or are slow; many billing dates to catch up; high tenant count.
**Fix**:
- Check Prometheus query duration in logs with `per_module_levels: core.metrics.prometheus: DEBUG`.
- Reduce `lookback_days` once caught up — set to 30–60 days for steady-state operation.
- Increase `features.max_parallel_tenants` if host has spare CPU (default 4, max 64).
- Set `tenant_execution_timeout_seconds: 0` to disable per-tenant timeout during initial backfill.

### Pipeline runs overlap (skipped — already in progress)

**Cause**: `features.refresh_interval` (default 1800s) is shorter than actual run duration.
**Fix**: Increase `features.refresh_interval` to at least 2× your typical run duration. Check `gathered` / `calculated` counts in logs to estimate run time.

## Topic attribution issues

### No topic attribution data appearing

**Cause**: Feature not enabled, metrics source missing, or pipeline hasn't reached the overlay stage yet.
**Fix**:
- Verify `plugin_settings.topic_attribution.enabled: true` in your config.
- Verify `plugin_settings.metrics` is configured — topic attribution requires a Prometheus source.
- Check pipeline status API: `topic_overlay_gathered` flag indicates whether the overlay stage has run for each date.

### All topics showing `even_split` attribution

**Cause**: Prometheus is not returning per-topic metrics for the queried clusters.
**Fix**:
- Verify your Prometheus instance has `received_bytes`, `sent_bytes`, and `retained_bytes` per topic.
- Check the `missing_metrics_behavior` setting — `even_split` (default) distributes costs evenly when metrics are zero or unavailable. Set to `skip` to omit clusters with no metrics instead.

### Sentinel rows with `ATTRIBUTION_FAILED`

**Cause**: Prometheus fetch retries exhausted for a cluster (`topic_attribution_retry_limit` reached).
**Fix**:
- Check Prometheus connectivity — the pipeline retries on each run until the limit is hit.
- Verify `metric_name_overrides` if you use non-standard Prometheus metric names.
- Increase `topic_attribution_retry_limit` (default 3) if outages are transient but longer than your run interval.

### Topic attribution stuck on old dates

**Cause**: Dates processed before topic attribution was enabled don't have overlay data.
**Fix**:
- These dates need a backfill — the pipeline only runs topic attribution for dates that enter the processing queue.
- Trigger recalculation for the affected date range to queue them for overlay processing.

## API issues

### `HTTP 401 Unauthorized` on API requests

**Cause**: The engine's REST API has no built-in auth — a reverse proxy or API gateway is returning 401.
**Fix**: Check your proxy/gateway auth configuration. The engine itself does not issue or validate tokens.

### `HTTP 429 Too Many Requests` from CCloud Billing API

**Cause**: CCloud billing API rate limit hit — too many requests in a short window.
**Fix**:
- Increase `billing_api.days_per_query` to fetch more days per request (max 30).
- Increase `min_refresh_gap_seconds` to reduce pipeline run frequency.
- Check if multiple tenants are querying the same CCloud org simultaneously — they share the rate limit.

### `HTTP 429` with `preview_capacity_exhausted`

**Cause**: The application process has reached its configured global or
per-tenant running/queued Preview generation limit.

**Fix**:

- Wait for existing Preview work to finish, then submit the same request again.
  The rejected attempt created no request ID or artifacts.
- If bursts are expected, increase both queue limits while keeping the
  per-tenant queued limit lower than the global limit.
- If generation is continuously saturated and the host has sufficient disk and
  memory headroom, tune `preview.max_workers` and the per-tenant running limit
  together.
- Remember that limits are per process. Additional replicas add independent
  capacity rather than sharing one global counter.

The remote `chitragupta-preview` CLI exits with code 1 for this response and
does not retry automatically.

### `HTTP 429` with `focus_preview_repair_capacity_exhausted`

**Cause**: The process has reached its configured running and waiting
historical-repair capacity.

**Fix**:

- Wait for current repairs to finish, then submit the repair again. The
  rejected attempt created no repair.
- Increase `preview.max_queued_repairs` if bounded waiting is appropriate.
- Increase `preview.max_workers` only after confirming the process can safely
  run more repair and generation work concurrently.
- Remember that only one repair may be active per tenant and each replica has
  independent repair limits.

Repair submission does not retry automatically.

### FOCUS Mapping Preview is `upgrading`, `degraded`, or `unavailable`

**Cause**:

- `upgrading`: a historical repair is queued or running.
- `degraded`: repair failed, completed with failed dates, was interrupted
  during restart, or the latest recorded ordinary or Preview evidence retention
  attempt failed. Repair and retention causes can exist together.
- `unavailable`: Preview readiness or storage cannot be read safely, or startup
  recovery for the tenant did not complete.

**Fix**:

- For `upgrading`, monitor **Date progress** in `GET /api/v1/readiness` and wait
  for a terminal state. Existing valid packages and unrelated application
  features remain available.
- For `degraded`, inspect repair progress plus
  `focus_preview_ordinary_retention` and
  `focus_preview_evidence_retention` in `GET /api/v1/readiness`.
  - If repair needs attention, submit a new bounded repair for failed dates.
    Failed dates count as completed lifecycle progress.
  - If a retention outcome failed, follow its operator-facing diagnostic,
    inspect worker logs, restore the affected storage or cleanup dependency, and
    allow the next scheduled cycle to retry.
  - A later successful cleanup replaces only its matching failure. Preview
    remains degraded while the other retention outcome or repair still needs
    attention.
- For `unavailable`, restore Preview storage and retry. The next repair
  submission retries interrupted-work recovery for that tenant first. If it
  returns `FOCUS Mapping Preview repair worker is unavailable`, recovery still
  failed and no new repair was created.

Existing valid Preview packages and revisions remain available while repair or
retention is degraded. Billing, chargeback, inventory, and ordinary pipeline
operations remain independent.

### `preview_generation_spool_limit_exceeded`

**Cause**: One requested or scheduled package needed more temporary disk than
`preview.max_generation_spool_bytes` (default 2 GiB).

**Fix**:

- Confirm the artifact filesystem has enough free space for
  `preview.max_workers × preview.max_generation_spool_bytes`, retained
  packages, and safety margin.
- Increase the per-generation limit only after sizing every process or replica
  that shares the artifact root.
- Reducing `preview.max_csv_file_bytes` changes part boundaries but does not
  reduce the aggregate generation spool requirement.

The request diagnostic is non-retryable with the same limit. Scheduled
publication creates no revision and leaves the current pointer unchanged.

### `HTTP 409 Conflict` on `POST /api/v1/tenants/{name}/pipeline/run`

**Cause**: Pipeline is already running for that tenant.
**Fix**: Wait for the current run to complete. Check `GET /api/v1/tenants/{name}/pipeline/status`.

### API returns stale data

**Cause**: `--mode api` only — no pipeline running to update data.
**Fix**: Run with `--mode both` or trigger pipeline runs via `POST /api/v1/tenants/{name}/pipeline/run`.

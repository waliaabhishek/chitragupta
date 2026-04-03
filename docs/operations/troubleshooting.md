# Troubleshooting

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

### `HTTP 409 Conflict` on `POST /api/v1/tenants/{name}/pipeline/run`

**Cause**: Pipeline is already running for that tenant.
**Fix**: Wait for the current run to complete. Check `GET /api/v1/tenants/{name}/pipeline/status`.

### API returns stale data

**Cause**: `--mode api` only — no pipeline running to update data.
**Fix**: Run with `--mode both` or trigger pipeline runs via `POST /api/v1/tenants/{name}/pipeline/run`.

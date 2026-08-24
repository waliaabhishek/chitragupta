# Monitoring

## Log levels

Set `logging.level: INFO` for production. Use `DEBUG` for plugin-specific tracing:

```yaml
logging:
  per_module_levels:
    core.metrics.prometheus: DEBUG
```

## Log format

The log format is configurable via `logging.format` using standard Python
[LogRecord attributes](https://docs.python.org/3/library/logging.html#logrecord-attributes):

```yaml
logging:
  format: "%(asctime)s - %(name)s - %(levelname)s - %(message)s"  # default
```

For JSON-style structured logging (useful with log aggregators like Loki or ELK),
use a format string like:

```yaml
logging:
  format: '{"time":"%(asctime)s","logger":"%(name)s","level":"%(levelname)s","msg":"%(message)s"}'
```

## Key log messages

| Message | Meaning |
|---|---|
| `Tenant X: gathered=N, pending=P, calculated=M, rows=R` | Successful pipeline run |
| `Tenant X completed with errors: [...]` | Partial run — some dates failed |
| `ALERT: Tenant X has been permanently suspended` | Gather failure threshold breached |
| `ALERT: All N tenant(s) have been permanently suspended` | All tenants failed — engine is idle |
| `FOCUS Mapping Preview revision generation skipped tenant=... month=... diagnostic_code=preview_generation_spool_limit_exceeded` | A scheduled revision exceeded its per-generation spool ceiling; no revision was published. |
| `FOCUS Mapping Preview revision publication failed ...` | Scheduled publication failed and left the current revision unchanged. |

## API health check

```
GET /health
→ {"status": "ok", "version": "<version>"}
```

`version` is the installed package version, or `"0.0.0-dev"` when running from source.

## Readiness check

```http
GET /api/v1/readiness
```

The response is cached for two seconds. See the
[API reference](../api-reference.md#get-apiv1readiness) for the complete
response schema.


FOCUS Preview readiness is tenant-scoped:

| State | Operational meaning |
|---|---|
| `disabled` | Preview is not configured for the tenant. |
| `ready` | Preview is available and no repair or retention cause needs attention. |
| `upgrading` | A historical repair is queued or running. Existing valid Preview data remains available. |
| `degraded` | Historical repair or either retention cleanup needs attention. Use repair progress and the two structured retention outcomes to identify each cause. |
| `unavailable` | Preview readiness or storage cannot be determined safely. Restore Preview availability before retrying. |

The completed and total fields are **Date progress**. Succeeded and failed
dates both count once terminal, so the ratio is not data-volume or success
progress.

Monitor `focus_preview_ordinary_retention` and
`focus_preview_evidence_retention` separately. Each is null until an outcome is
available; otherwise `attempted_at`, `status`, and `diagnostic` describe the
latest recorded attempt. On failure, alert on the diagnostic code and use its
operator-facing message with worker logs. A later success clears the diagnostic
only for that cleanup kind. The other retention outcome and repair progress
remain visible, so multiple causes are not collapsed into one status.

An `upgrading`, `degraded`, or `unavailable` Preview state does not change
generic application readiness or unrelated tenant features. Existing valid
Preview packages and revisions remain available during retention degradation.
The web client polls readiness every five seconds while any tenant is
`upgrading` and returns to the normal fifteen-second cadence after terminal
state.

## Pipeline status

```
GET /api/v1/tenants/{tenant_name}/pipeline/status
→ {
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

`last_result` is `null` if no completed or failed runs exist yet.

## Failure detection

A tenant enters permanently-failed state after `gather_failure_threshold` (default 5)
consecutive gather failures. The engine logs a `CRITICAL` alert and stops processing
that tenant. Manual operator intervention (fix config + restart) is required.

## Monitoring topic attribution

If topic attribution is enabled, monitor these additional indicators:

**Pipeline status flags** (via `GET /api/v1/tenants/{name}/pipeline/status`):

- `topic_overlay_gathered` — topic discovery and metrics fetch completed for a date
- `topic_attribution_calculated` — attribution rows written for a date

**Log messages:**

- `Topic discovery` — topic resources being gathered from Prometheus
- `Topic attribution backfill` — overlay processing queued dates

**Sentinel row detection:**

Rows with `attribution_method = 'ATTRIBUTION_FAILED'` indicate a cluster where Prometheus retries were exhausted. Query the API or database:

```sql
SELECT * FROM topic_attribution_facts
JOIN topic_attribution_dimensions USING (dimension_id)
WHERE attribution_method = 'ATTRIBUTION_FAILED';
```

**Per-date processing:** Use the pipeline status API to check which dates have completed topic attribution. Dates where `topic_overlay_gathered = true` but `topic_attribution_calculated = false` are still pending calculation.

## Metrics to collect from logs

- `gathered` count per run — drop indicates billing API issues
- `errors` list per run — content identifies root cause
- Pipeline run duration — set alerts if > `tenant_execution_timeout_seconds`

## Monitoring FOCUS Mapping Preview capacity

Track HTTP 429 responses with
`detail.code=preview_capacity_exhausted` on request submission. Sustained 429s
mean the process-local running or queued generation limit is full. No request is
created, so clients must submit again later.

Also track failed request diagnostics by code, especially
`preview_generation_spool_limit_exceeded`. That diagnostic is non-retryable for
the same configured spool limit. Compare artifact-root free space with
`preview.max_workers × preview.max_generation_spool_bytes`, then include
retained package size and filesystem safety margin.

Scheduled capacity deferral intentionally creates no revision or success event.
Monitor settlement-ready months that remain without a current revision across
later periodic cycles, together with worker-cycle completion and the revision
generation/publication log messages above. Repeated absence while ad-hoc
requests saturate capacity indicates that process limits or replica sizing need
tuning.

Track repair-submission HTTP 429 responses separately by
`detail.code=focus_preview_repair_capacity_exhausted`. The rejected attempt
created no repair. Sustained responses mean the per-process running limit
(`preview.max_workers`) and waiting limit (`preview.max_queued_repairs`) are
full. Wait for current work to finish or adjust process sizing and limits;
replicas have independent repair capacity.

After restart, interrupted repair work becomes `degraded` and is not resumed
automatically. Monitor a tenant that remains `unavailable`: its next repair
submission retries recovery before admission. A repeated
`FOCUS Mapping Preview repair worker is unavailable` response means recovery
still cannot complete, and no new repair was created.

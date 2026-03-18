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

## API health check

```
GET /health
→ {"status": "ok", "version": "<version>"}
```

`version` is the installed package version, or `"0.0.0-dev"` when running from source.

## Readiness check

```
GET /api/v1/readiness
→ {
    "status": "ready",           // ready | initializing | no_data | error
    "version": "<version>",
    "mode": "both",
    "tenants": [
      {
        "tenant_name": "my-org",
        "tables_ready": true,
        "has_data": true,
        "pipeline_running": false,
        "pipeline_stage": null,
        "pipeline_current_date": null,
        "last_run_status": "completed",
        "last_run_at": "2026-03-17T12:00:00Z",
        "permanent_failure": null
      }
    ]
  }
```

Response is TTL-cached for 2 seconds.

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

## Metrics to collect from logs

- `gathered` count per run — drop indicates billing API issues
- `errors` list per run — content identifies root cause
- Pipeline run duration — set alerts if > `tenant_execution_timeout_seconds`

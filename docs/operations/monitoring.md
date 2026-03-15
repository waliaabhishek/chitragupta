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
→ {"status": "ok", "version": "1.0.0"}
```

## Pipeline status

```
GET /api/v1/tenants/{tenant_name}/pipeline/status
→ {"tenant_name": "...", "is_running": false, "last_run": "...", "last_result": {...}}
```

## Failure detection

A tenant enters permanently-failed state after `gather_failure_threshold` (default 5)
consecutive gather failures. The engine logs a `CRITICAL` alert and stops processing
that tenant. Manual operator intervention (fix config + restart) is required.

## Metrics to collect from logs

- `gathered` count per run — drop indicates billing API issues
- `errors` list per run — content identifies root cause
- Pipeline run duration — set alerts if > `tenant_execution_timeout_seconds`

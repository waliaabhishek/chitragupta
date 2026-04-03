# Data Retention

## How it works

After each pipeline cycle, the engine deletes records older than `retention_days`
from all tables for each tenant.

File: `src/workflow_runner.py`, `_cleanup_retention()` method.

Tables affected:
- `billing_line_items`
- `resources`
- `identities`
- `chargeback_rows`
- `topic_attribution_facts` and `topic_attribution_dimensions` (when topic attribution is enabled)

## Configuration

```yaml
tenants:
  my-tenant:
    retention_days: 250    # delete records older than 250 days (default)
```

| Field | Default | Range | Notes |
|---|---|---|---|
| `retention_days` | 250 | 1–730 | Set 0 to disable (not recommended) |
| `lookback_days` | 200 | 1–364 | Must be < retention_days for data continuity |

**Topic attribution retention** is independent of tenant-level `retention_days`:

| Setting | Scope | Default | Range | What it purges |
|---|---|---|---|---|
| `tenants.*.retention_days` | Tenant | 250 | 1–730 | `billing_line_items`, `resources`, `identities`, `chargeback_rows` |
| `plugin_settings.topic_attribution.retention_days` | Topic attribution | 90 | 1–365 | `topic_attribution_facts`, `topic_attribution_dimensions` |

The two settings are evaluated independently — tenant retention does not affect topic attribution tables and vice versa. Set each according to the relevant reporting needs.

## Recommendation

Set `retention_days` > `lookback_days`. If `retention_days` < `lookback_days`, the
engine may attempt to re-fetch data that has already been deleted.

## Manual cleanup

There is no CLI tool for manual cleanup. To delete all data for a tenant,
drop and recreate the database file (`storage.connection_string` path).

## Audit trail

Chargeback rows are the primary audit trail. Set `retention_days` to match your
organization's cost accounting retention policy (typically 365–730 days).

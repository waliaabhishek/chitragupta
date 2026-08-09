# Configuration Concepts

## File format

YAML. Loaded via `src/core/config/loader.py:load_config()`.

## Environment variable substitution

```yaml
# Required variable — startup fails if not set:
secret: ${MY_SECRET}

# With default — uses fallback if not set:
host: ${DB_HOST:-localhost}
```

**Limitation:** default values cannot contain a literal `}` character. The substitution
pattern uses a non-greedy match, so `${VAR:-a}b}` resolves to default `a` followed by
literal text `b}`.

## .env file discovery

If `--env-file` is not passed, the engine looks for `.env` in the same directory
as the config file. Variables already in the environment take precedence.

## Top-level structure

```yaml
logging:       # Optional — log level and format
features:      # Optional — periodic refresh, parallelism
api:           # Optional — HTTP server settings
preview:       # Optional — FOCUS Mapping Preview storage, workers, and CSV part size
tenants:       # Required — one entry per managed tenant
  <name>:
    ecosystem: ...
    tenant_id: ...
    focus_preview: ...  # Optional Confluent Cloud Preview eligibility contract
    storage: ...
    plugin_settings: ...
```

FOCUS Mapping Preview needs a durable process-wide artifact root and an
optional Confluent Cloud tenant eligibility block:

```yaml
preview:
  artifact_root: /var/lib/chitragupta/focus-preview

tenants:
  production:
    focus_preview:
      commercial_profile: direct_payg
      effective_start_date: 2026-01-01
```

Omit `focus_preview` to leave the feature disabled for that tenant. The artifact
root must be writable by the API and worker; separate processes must share the
same mounted path. See the
[configuration reference](../configuration/index.md#focus-mapping-preview) for
all process settings, the
[Confluent Cloud reference](../configuration/ccloud-reference.md#focus-mapping-preview-eligibility)
for tenant fields, and [FOCUS Mapping Preview](../focus-mapping-preview.md) for
the complete workflow.

Tenant `lookback_days` is capped at 364 and controls acquisition/recalculation,
not retention or guaranteed historical reconstruction.

## Tenant isolation

Each tenant must use a **separate** `storage.connection_string`. Sharing databases
between tenants is rejected at startup.

## Config validation

All config models use Pydantic v2. Invalid config raises `ValueError` with a field path
and human-readable message before any network calls are made.

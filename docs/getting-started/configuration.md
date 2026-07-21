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

For Confluent Cloud FOCUS Mapping Preview, the optional tenant block is:

```yaml
focus_preview:
  commercial_profile: direct_payg
  billing_currency: USD       # optional; defaults to normalized USD
  effective_start_date: 2026-01-01
  effective_end_date: 2027-01-01
```

Omitting the block leaves configuration valid but makes Preview fail closed.
Only `direct_payg` with a request contained in the half-open effective interval
is supported. Non-USD values are not converted. Confluent's Costs API does not
provide per-record ISO currency, so generated FOCUS `BillingCurrency` remains
null even when configured/default USD makes the request eligible. See the
[Confluent Cloud reference](../configuration/ccloud-reference.md#focus-mapping-preview-eligibility).

Preview's process-wide package settings are separate from the tenant block:

```yaml
preview:
  artifact_root: /var/lib/chitragupta/focus-preview
  max_workers: 2
  max_csv_file_bytes: null
```

The artifact root must be durable and writable by the API process.
`max_csv_file_bytes` is either null for one CSV or a positive byte ceiling for
deterministic row-boundary parts. See
[FOCUS Mapping Preview](../focus-mapping-preview.md) for the complete user
workflow and supported customization boundary.

Tenant `lookback_days` is capped at 364 and controls acquisition/recalculation,
not retention or guaranteed historical reconstruction.

## Tenant isolation

Each tenant must use a **separate** `storage.connection_string`. Sharing databases
between tenants is rejected at startup.

## Config validation

All config models use Pydantic v2. Invalid config raises `ValueError` with a field path
and human-readable message before any network calls are made.

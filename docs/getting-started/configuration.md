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
tenants:       # Required — one entry per managed tenant
  <name>:
    ecosystem: ...
    tenant_id: ...
    storage: ...
    plugin_settings: ...
```

## Tenant isolation

Each tenant must use a **separate** `storage.connection_string`. Sharing databases
between tenants is rejected at startup.

## Config validation

All config models use Pydantic v2. Invalid config raises `ValueError` with a field path
and human-readable message before any network calls are made.

# First Run

## Run modes

See [Run Modes](../operations/deployment.md#run-modes) for full details.

## Example invocations

```bash
# One-shot run, worker only:
uv run python src/main.py --config-file config.yaml --run-once

# Continuous loop:
uv run python src/main.py --config-file config.yaml

# API + worker:
uv run python src/main.py --config-file config.yaml --mode both
```

## What happens on first run

1. Storage backend created (tables migrated via Alembic)
2. Plugin initialized with ecosystem-specific credentials
3. Billing data fetched for `lookback_days` window (default 200)
4. Resources and identities discovered per handler
5. Costs allocated per billing line + product type
6. Results written to configured emitters
7. Loop sleeps for `features.refresh_interval` seconds (default 1800)

## Reading logs

```
INFO  Tenant my-org: gathered=14, calculated=14, rows=320
```

`gathered` = billing dates fetched from source
`calculated` = dates where allocation was computed
`rows` = chargeback rows written to storage + emitters

## Common first-run issues

See [Troubleshooting](../operations/troubleshooting.md).

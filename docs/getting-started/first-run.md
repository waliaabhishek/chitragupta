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

# Re-run emitters only (no pipeline, no data collection):
uv run python src/main.py --config-file config.yaml --emit-once
```

## What happens on first run

1. Storage backend created (tables migrated via Alembic)
2. Plugin initialized with ecosystem-specific credentials
3. Billing data fetched for `lookback_days` window (default 200)
4. Resources and identities discovered per handler
5. Costs allocated per billing line + product type
6. Chargeback rows written to storage
7. `EmitterRunner` dispatches pending dates to each configured emitter; outcomes persisted
8. Loop sleeps for `features.refresh_interval` seconds (default 1800)

## `--emit-once` flag

Runs `EmitterRunner` for all configured tenants without triggering a pipeline
run, then exits. Use this to replay emitters after a destination outage:

```bash
uv run python src/main.py --config-file config.yaml --emit-once
```

The engine reads pending (not yet emitted or previously failed) dates from
storage and dispatches them to each emitter. No billing data is fetched and no
chargeback recalculation occurs.

## Reading logs

```
INFO  Tenant my-org: gathered=14, pending=0, calculated=14, rows=320
```

`gathered` = billing dates fetched from source
`calculated` = dates where allocation was computed
`rows` = chargeback rows written to storage + emitters

## Common first-run issues

See [Troubleshooting](../operations/troubleshooting.md).

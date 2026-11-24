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
7. The engine dispatches configured emitters and saves their outcomes. An
   enabled Iceberg emitter publishes completed dates in a separate worker pass.
8. Loop sleeps for `features.refresh_interval` seconds (default 1800)

If topic attribution is enabled, an additional **Topic Overlay** stage runs between steps 6 and 7: topic resources are discovered via Prometheus, and Kafka cluster costs are attributed to individual topics.

## `--emit-once` flag

Publishes pending results for the configured tenants from storage, then exits.
Use this command after a destination outage.

```bash
uv run python src/main.py --config-file config.yaml --emit-once
```

The engine reads pending dates from storage and dispatches them to each
emitter. With Iceberg enabled, it also scans for completed dates that are
unpublished or have a newer calculation. Use `uv run --extra iceberg` to run
this command from source. Before a standalone Iceberg write, stop scheduled
publication and pause calculation, repair, and retention for the affected
tenants. For an unscoped `--emit-once`, this applies to every tenant with an
Iceberg emitter. Resume those jobs after the command exits. Follow the
[Iceberg recovery steps](../operations/troubleshooting.md#iceberg-export) to
replay a selected date range.

The next scheduled Iceberg pass picks up completed API-triggered calculations.
For a manual-only deployment, use `--emit-once` after the calculation completes.

## Reading logs

```
INFO  Tenant my-org: gathered=14, pending=0, calculated=14, rows=320
```

`gathered` = billing dates fetched from source
`calculated` = dates where allocation was computed
`rows` = chargeback rows written to storage + emitters

## Common first-run issues

See [Troubleshooting](../operations/troubleshooting.md).

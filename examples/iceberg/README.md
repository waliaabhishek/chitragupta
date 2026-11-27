# Iceberg chargeback export

[config.yaml](config.yaml) shows Confluent Cloud, self-managed Kafka, and
Generic Metrics tenants using one HTTPS REST catalog. Each tenant writes to
its own Iceberg v2 table. The `&finance_catalog` anchor names the connection
settings. `*finance_catalog` reuses those settings within the same YAML file.

## Add Iceberg to an existing tenant

Copy the Iceberg entry into the tenant's existing `plugin_settings.emitters`
list and set `params.table` to the destination. Keep your source settings,
database, and any emitters you already use. For a single tenant, use the full
`catalog` mapping. When sharing a catalog, define it in the first tenant that
uses it and refer to it from the other tenants.

Create the destination namespace and tables with the schema and identity
partitions in the [deployment guide](../../docs/operations/deployment.md#iceberg-export).
The worker needs catalog and S3 write access. This example uses credentials
provided by the catalog. The commented `s3` block shows how to supply your
own endpoint and credentials in the same YAML.

## Run the example

From the repository root, copy the environment template and fill in the
catalog and source values for the tenants you keep. Update the Kafka cluster
label, broker count, and the example rates to match your sources.

```bash
cp examples/iceberg/.env.example examples/iceberg/.env
mkdir -p data
```

To run the released worker in Docker, give it Prometheus URLs reachable
from the container. Mount the configuration and a volume for the SQLite
databases after you create the destination tables.

```bash
docker run --rm --env-file examples/iceberg/.env \
  -v "${PWD}/examples/iceberg/config.yaml:/app/config.yaml:ro" \
  -v chitragupta-iceberg-data:/app/data \
  ghcr.io/waliaabhishek/chitragupta:latest-iceberg \
  --config-file /app/config.yaml --mode worker
```

### Run from source

Validate the configuration, then start the worker once your v2 tables exist.

```bash
uv run --python 3.14 --extra iceberg python src/main.py \
  --config-file examples/iceberg/config.yaml \
  --env-file examples/iceberg/.env --validate

uv run --python 3.14 --extra iceberg python src/main.py \
  --config-file examples/iceberg/config.yaml \
  --env-file examples/iceberg/.env --mode worker
```

The example exports up to 30 days of completed chargebacks retained in SQL.
For stored chargebacks, see the [manual export steps](../../docs/getting-started/first-run.md#--emit-once-flag).
Use [Iceberg status](../../docs/operations/troubleshooting.md#iceberg-export)
to check which days the worker has published.

## Test the example in Docker

Run this test from the repository root. It publishes a 100,001-row day for each
tenant, then replaces each day with 150,000 rows. The test uses a local REST
catalog, MinIO, and generated chargebacks. It removes its containers and
volumes on exit.

```bash
bash tests/integration/iceberg/run.sh --example
```

To test public PyIceberg microbatch publication and recovery, run:

```bash
bash tests/integration/iceberg/run.sh --microbatch
```

All three SQLite plug-ins publish 100,001 rows, then replace the day with
150,000 rows. DuckDB and Trino read the previous complete version while the
writer holds two staged batches. Both readers check the complete day after
one catalog commit.

The test also interrupts the writer and simulates lost commit responses for
a 150,000-row day. It verifies that a fresh export replays the complete day.
An early end to the batch iterator must fail before commit and leave the
previous version unchanged. One commit can publish several Iceberg snapshots.
This command tests a candidate writer separately from the worker.

To run the wider public API diagnostics, use:

```bash
bash tests/integration/iceberg/run.sh --public-api
```

This test uses the example's SQLite plug-ins with 100,001 and 150,000 rows per
tenant-day. It prints successful data operations and failed or unproved
qualification checks separately. A failed or unproved check gives a nonzero
exit code. This test does not change the worker's current export limits.

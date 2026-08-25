# Upgrading

## Before you upgrade

Back up your data before every upgrade. The engine runs database migrations automatically on startup — there is no undo once the process starts.

### SQLite

Copy the database file and its sidecar files:

```bash
cp data/chargeback.db data/chargeback.db.bak
cp data/chargeback.db-wal data/chargeback.db-wal.bak 2>/dev/null
cp data/chargeback.db-shm data/chargeback.db-shm.bak 2>/dev/null
```

If you have multiple tenants, back up each tenant's database.

If the deployment has generated FOCUS Mapping Preview packages, also back up
the configured `preview.artifact_root`. The database contains request/package
metadata, while immutable manifest and CSV bytes live under that filesystem
root. A usable restore requires the matching database and artifact-root backup.

### PostgreSQL

```bash
pg_dump -Fc -f chargeback_backup_$(date +%Y%m%d).dump dbname
```

## Check current schema version

Before upgrading, note your current migration state so you can diagnose issues if the upgrade fails:

```bash
uv run alembic -c src/core/storage/migrations/alembic.ini current
uv run alembic -c src/core/storage/migrations/alembic.ini history
```

For Docker deployments, run inside the container:

```bash
docker exec chitragupta python -m alembic -c src/core/storage/migrations/alembic.ini current
```

## Upgrade procedure

### Docker (docker compose)

```bash
# Stop the running stack
docker compose down

# Pull or build the new image
docker compose pull        # if using a registry
# OR
docker compose build       # if building locally

# Start with new version — migrations run automatically
docker compose up -d

# Verify
docker compose logs -f chitragupta | head -50
curl http://localhost:8080/health
```

### Docker (standalone)

```bash
docker stop chitragupta
docker rm chitragupta
docker pull your-registry/chitragupta:new-version
docker run -d --name chitragupta \
  -v ./config:/app/config:ro \
  -v ./data:/app/data:rw \
  -e CCLOUD_API_KEY=... \
  -e CCLOUD_API_SECRET=... \
  your-registry/chitragupta:new-version \
  --config-file /app/config/config.yaml --mode both
```

### Source-based (systemd)

```bash
# Stop the service
sudo systemctl stop chitragupta

# Update the code
cd /opt/chitragupta
git pull origin main   # or checkout a specific tag

# Update dependencies
uv sync

# Start — migrations run automatically
sudo systemctl start chitragupta

# Verify
sudo journalctl -u chitragupta -f | head -50
curl http://localhost:8080/health
```

## Database migrations

Migrations run automatically on startup. When the engine calls `bootstrap_storage()`, it executes `alembic upgrade head` against each tenant's database. No manual migration step is needed.

Preview evidence migrations are selected per tenant. A tenant without
`focus_preview` runs the ordinary migration chain without creating or repairing
the optional Confluent Preview evidence schema. An enabled Confluent Cloud
tenant prepares that schema online during startup. In a mixed deployment, this
selection is independent for each tenant database.

If you want to run migrations manually (for example, to test before starting
the engine), set `CHITRAGUPTA_DATABASE_URL` to the target connection URL and
supply it explicitly:

```bash
uv run alembic -c src/core/storage/migrations/alembic.ini \
  -x sqlalchemy.url="${CHITRAGUPTA_DATABASE_URL}" \
  upgrade head
```

Upgrades or downgrades that cross persisted timestamp canonicalization require
an online connection. Offline `--sql` execution stops before changing the
database because existing values must be inspected first. The preflight also
stops without advancing the schema when invalid timestamps, conflicting rows
that collapse to one canonical key, or unsafe allocation-lineage parent state
is found. Follow the emitted table and key diagnostic, reconcile that data, and
retry the online migration.

### Self-managed Kafka storage migrations

Startup selects storage preparation independently for each tenant. A
self-managed Kafka tenant prepares and verifies its plugin-owned storage before
the tenant backend is published. This also applies when the database is already
at the current core head, so selecting self-managed Kafka after a prior generic
upgrade prepares the missing plugin tables before they are used.

All tenants use the same migration history. To prepare a self-managed Kafka
database manually, use an online connection and select the ecosystem:

```bash
uv run alembic -c src/core/storage/migrations/alembic.ini \
  -x sqlalchemy.url="${CHITRAGUPTA_DATABASE_URL}" \
  -x plugin_storage=self_managed_kafka \
  upgrade head
```

The command is safe to rerun when the plugin tables already exist. To downgrade
a self-managed database across the plugin storage revision, select the same
ecosystem so its tables are removed before the shared migration state changes:

```bash
uv run alembic -c src/core/storage/migrations/alembic.ini \
  -x sqlalchemy.url="${CHITRAGUPTA_DATABASE_URL}" \
  -x plugin_storage=self_managed_kafka \
  downgrade 032
```

For a core, generic-metrics, or Confluent Cloud database, make the no-plugin
choice explicit when downgrading:

```bash
uv run alembic -c src/core/storage/migrations/alembic.ini \
  -x sqlalchemy.url="${CHITRAGUPTA_DATABASE_URL}" \
  -x plugin_storage=disabled \
  downgrade 032
```

When a downgrade crosses the plugin storage revision, omitting
`plugin_storage` stops before any schema or migration-state change. Retry with
the self-managed ecosystem selector or with `plugin_storage=disabled`, as
appropriate for the tenant.

The override uses normal SQLAlchemy URL syntax. Percent-encode reserved
characters in credentials and query values once, using standard single-percent
URL encoding; do not double percent signs. A blank or invalid override stops
before Alembic can use the configured default database. The diagnostic
identifies the invalid `sqlalchemy.url` override without including the supplied
URL or credentials. Correct the URL and rerun the command.

### PostgreSQL migration compatibility

PostgreSQL 17 is verified through the supported migration chain to head from
these starting states:

- A fresh database with no Alembic revision.
- Revision 004, before the billing primary-key change.
- Revision 005, after the billing primary-key change.
- Revision 008, before `chargeback_dimensions.env_id` is added.

This verification does not claim compatibility for other PostgreSQL versions
or for schemas that were altered manually.

### FOCUS Mapping Preview upgrades

Before upgrading a Preview-enabled deployment:

1. Stop API and worker processes that write tenant data or Preview artifacts.
2. Back up each tenant database and its matching `preview.artifact_root` from
   the same point in time.
3. Keep the current configuration with the backup.

Preview schema migrations run automatically during normal application startup.
They preserve existing configuration and package metadata, but do not invent
missing historical calculation metadata, provider source evidence, or
allocation lineage. Packages that satisfy the current public manifest and
mapping-profile contracts remain downloadable. Stored artifacts that do not
satisfy those contracts fail closed; physical preservation alone does not
guarantee download compatibility.

Direct Alembic commands treat Preview as disabled unless the enabled ecosystem
is selected explicitly. To prepare an enabled Confluent Cloud database manually,
use an online connection:

```bash
uv run alembic -c src/core/storage/migrations/alembic.ini \
  -x sqlalchemy.url="${CHITRAGUPTA_DATABASE_URL}" \
  -x focus_preview=confluent_cloud \
  upgrade head
```

Enabled Preview schema preparation also requires the online connection because
it inspects the live database. Disabled tenants do not require Preview evidence
tables or a writable artifact root.

Older retained dates may still lack the evidence required for Preview. If a
request fails with `calculation_metadata_unavailable`, use
[historical repair](../focus-mapping-preview.md#repair-retained-dates) while the
provider and metrics history remains available. Increasing `lookback_days`
does not restore deleted or unavailable history.

A Preview-specific schema, evidence, or repair failure leaves generic billing
and chargeback storage available, but Preview generation fails closed until the
problem is corrected. Review
[Preview troubleshooting](troubleshooting.md#focus-mapping-preview-is-upgrading-degraded-or-unavailable)
before retrying.

The capacity and spool settings are additive and use documented defaults when
omitted. Review [deployment sizing](deployment.md#preview-capacity-and-storage-sizing)
before increasing worker capacity.

## Rollback

If an upgrade fails or the new version misbehaves:

1. **Stop the engine** immediately to prevent further data changes.

2. **Restore your backup:**

   SQLite:
   ```bash
   cp data/chargeback.db.bak data/chargeback.db
   cp data/chargeback.db-wal.bak data/chargeback.db-wal 2>/dev/null
   cp data/chargeback.db-shm.bak data/chargeback.db-shm 2>/dev/null
   ```

   PostgreSQL:
   ```bash
   pg_restore -c -d dbname chargeback_backup_YYYYMMDD.dump
   ```

3. **Revert to the previous version** of the engine (previous Docker image tag or git checkout).

4. **Start the old version.** It will work with the restored database since the schema matches.

Restoring from backup remains the preferred rollback path. PostgreSQL 17
downgrade without identity conflicts is tested through revision 004. A
downgrade stops before destructive changes if distinct billing rows depend on
`product_category`, distinct Confluent Cloud billing rows depend on `env_id`,
or distinct `chargeback_dimensions` rows depend on `env_id`. Restore the
backup, or reconcile and merge the conflicting rows before retrying the
downgrade.

## Configuration compatibility

Review release notes before upgrading. Configuration changes fall into two categories:

- **Additive** — new optional fields with defaults. No action needed.
- **Breaking** — renamed or removed fields. The engine validates configuration on startup and will fail fast with a clear error message if required fields are missing or invalid.

Keep a copy of your `config.yaml` alongside your database backup so you can revert both together if needed.

## Breaking changes policy

Breaking changes (configuration format, API contracts, database schema) will be documented in the [CHANGELOG](https://github.com/waliaabhishek/chitragupta/blob/main/CHANGELOG.md). Releases that contain breaking changes will be called out explicitly in release notes.

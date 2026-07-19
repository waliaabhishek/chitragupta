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
metadata, while the immutable manifest and CSV bytes live under that filesystem
root.

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

If you want to run migrations manually (e.g., to test before starting the engine):

```bash
uv run alembic -c src/core/storage/migrations/alembic.ini upgrade head
```

Set the database URL first if it differs from the default:

```bash
uv run alembic -c src/core/storage/migrations/alembic.ini \
  -x sqlalchemy.url="postgresql+psycopg2://user:pass@host/dbname" \
  upgrade head
```

### Migration 019: FOCUS Mapping Preview

Migration 019 adds the `preview_requests` table and nullable per-date
`calculation_id`, `calculation_completed_at`, and `calculation_run_id` fields to
`pipeline_state`, plus their indexes and optional run foreign key.

The migration is additive and performs no data-repair update or backfill.
Existing calculated dates therefore retain null correlation metadata and remain
unchanged. A Preview request covering such a date fails with
`calculation_metadata_unavailable` and `retryable=false`; Preview does not expose
an edit, approval, backfill, or repair operation. The ordinary collector and
calculation lifecycle remains the only producer of new calculation metadata.

### Migration 020: Preview eligibility diagnostics

Migration 020 adds nullable
`preview_requests.diagnostic_source_correlation_ids_json`. Existing Preview
requests and per-date calculation metadata are preserved. A legacy null value is
read as an empty public correlation list, and downgrading removes only the new
column.

The related tenant `focus_preview` configuration is additive and optional. An
existing configuration still loads without it, but new Preview requests fail
closed with `preview_commercial_profile_unavailable` until the operator declares
`commercial_profile: direct_payg` and a containing effective interval.
`billing_currency` defaults to normalized `USD`; non-USD fails Preview with no
currency conversion. Confluent's Costs API does not provide per-record ISO
currency, so `BillingCurrency` remains null in generated output.

Do not increase `lookback_days` in an attempt to recover absent Preview history.
Its maximum remains 364 and it defines acquisition/recalculation eligibility,
not retention, archival history, or guaranteed reconstruction from billing and
Metrics APIs. TASK-256 owns a future independent archive/retention design.

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

Alembic supports `downgrade` but migration scripts may not always have complete downgrade logic. Restoring from backup is the safer path.

## Configuration compatibility

Review release notes before upgrading. Configuration changes fall into two categories:

- **Additive** — new optional fields with defaults. No action needed.
- **Breaking** — renamed or removed fields. The engine validates configuration on startup and will fail fast with a clear error message if required fields are missing or invalid.

Keep a copy of your `config.yaml` alongside your database backup so you can revert both together if needed.

## Breaking changes policy

Breaking changes (configuration format, API contracts, database schema) will be documented in the [CHANGELOG](https://github.com/waliaabhishek/chitragupta/blob/main/CHANGELOG.md). Releases that contain breaking changes will be called out explicitly in release notes.

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
`calculation_metadata_unavailable` and `retryable=false`. Migration 027 adds an
explicit repair operation for dates that are still inside the complete
eligibility and retained-data interval. The migration itself never creates
correlation metadata, source evidence, or allocation lineage from legacy rows.

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
Metrics APIs.

### Migrations 021–022: allocation lineage and report profiles

Migration 021 associates retained Confluent Cost source rows with their billing
origins and adds persisted calculation-lineage runs and portions. Existing rows
are not guessed or financially rewritten. When Preview is enabled, valid legacy
source rows are assigned local evidence authority from their retained values;
this bootstrap does not call Confluent Cloud. Unreadable or inconsistent legacy
evidence makes Preview unavailable or fail closed without preventing generic
chargeback access. A later ordinary gather/calculation can establish new
current evidence.

Migration 022 adds effective-column and evidence-coverage fields used by Daily
and Monthly Full/Summary/Custom requests. Existing Daily/Full requests retain
their original immutable package behavior.

### Migration 023: package expiry and worker leases

Migration 023 adds `expires_at`, `worker_id`, and `lease_expires_at` to Preview
requests plus owner-scoped expiry, recovery, and lease indexes. Existing ready
and expired requests with a completion timestamp are backfilled to expire seven
days after completion; queued, running, and failed requests keep null expiry.

On startup, the API cleans interrupted staging directories and reconciles
interrupted requests through persisted worker leases. Live leases remain
protected. Ready packages at or beyond their expiry become unavailable before
filesystem cleanup.

The new process setting `preview.max_csv_file_bytes` is optional and defaults to
null, so existing configuration remains valid. Set it only when deterministic
multi-part CSV output is required. Back up the artifact root before upgrade and
verify that it remains mounted at the same configured path after restart.

### Migration 024: published monthly revisions

Migration 024 adds storage for immutable published Monthly Full revisions and
enforces one current revision per configured storage owner and UTC month. It
does not convert requested Preview packages or backfill revision rows. The first
successful periodic cycle after upgrade evaluates every eligible month in the
current acquisition/effective window. It publishes only settlement-ready months
whose source-arrival threshold and configured acquisition cutoff have passed and
whose complete full-month calculation, source coverage, reconciliation, and
mapping validation produce a Settled result.

Existing persisted Provisional revisions are not deleted or rewritten by the
upgrade. They remain available under the existing supersession and retention
rules. The first valid Settled revision supersedes a current Provisional revision
through the ordinary replacement transaction, even when logical report content
is unchanged.

Before upgrading, back up each tenant database and the matching
`preview.artifact_root` together. Restoring only one side can leave revision
metadata without its immutable manifest/CSV bytes, or bytes without their
current metadata. Automatic publication requires periodic refresh; existing
run-once and ad-hoc request behavior is unchanged, including on-demand
Provisional packages for active or otherwise incomplete months.

### Migration 025: revision history retention

Migration 025 adds pending-cleanup state and indexes for visible revision
history, newly due revisions, and cleanup retries. It preserves the existing
one-current-revision constraint. The migration does not backfill, hide, or
delete any revision or package by itself.

After the upgrade, scheduled periodic cycles apply the tenant billing-data
`retention_days` cutoff to published revisions. Revisions for an out-of-policy
month become unavailable before package deletion; failed deletions remain
pending and retry after later cycles or restarts. Requested ad-hoc Preview
packages retain their independent seven-day expiry.

Before this upgrade, stop writers and take a coordinated backup of every tenant
database and its matching Preview packages. Restore both sides from the same
backup if rollback is required. Restoring only the database or only the packages
can leave retained revision metadata and immutable package bytes inconsistent.

### Migration 026: opt-in Preview evidence

Migration 026 adds the source-attempt/readiness and organization-authority
metadata used to prove that package generation is reading the newest successful
provider evidence. Automatic startup enables this migration work only for an
enabled Confluent Cloud tenant. Disabled tenants do not require the optional
tables, indexes, repositories, or writable Preview artifact root.

Direct Alembic commands default to Preview disabled. To prepare an enabled
Confluent Cloud database manually, use an online connection and the explicit
selection:

```bash
uv run alembic -c src/core/storage/migrations/alembic.ini \
  -x focus_preview=confluent_cloud upgrade head
```

Offline SQL generation is not supported for enabled Preview evidence because
the migration requires a live connection to inspect, prepare, and repair the
optional schema. Run the command above against the database instead. Retained
legacy-row bootstrap is separate: it runs when the enabled tenant's backend is
initialized during application startup, not inside the migration hook. A
Preview-only schema or bootstrap failure leaves generic billing and chargeback
storage usable, but new Preview generation—including header-only output—fails
closed until the evidence problem is repaired and the pipeline runs
successfully.

### Migration 027: retained historical repair

Migration 027 adds durable repair operations, per-date results, and source
readiness history for Preview-enabled Confluent Cloud tenants. It does not
modify existing billing, chargebacks, pipeline state, source evidence, or
lineage during upgrade.

Use repair when retained dates upgraded from an earlier release still have
`calculation_metadata_unavailable` or lack native source/allocation evidence.
Repair is an explicit asynchronous REST operation available for submission only
in `both` mode. Submit an inclusive-start/exclusive-end UTC range contained in
the intersection of the tenant's `focus_preview` effective interval,
`lookback_days`, `cutoff_days`, and complete `retention_days` interval. The
operator must still have valid Confluent Cloud billing credentials, provider
history for every selected date, and historical metrics required by the
configured allocators. Retention and lookback configuration do not guarantee
that those external inputs remain available.

For every selected date, repair replaces that tenant/date's billing from the
authoritative provider response, including an authoritative empty response, and
runs the canonical calculation and evidence path. It never copies or infers
calculation identifiers, timestamps, source records, or lineage from legacy
aggregates. Expected date failures are stored with a stage and diagnostic while
later dates continue. `daily_validated` means Daily validation passed and the
date is waiting for validation of a wholly selected UTC month.

Repair changes billing, chargebacks, pipeline state, and therefore generic
exports only inside the selected tenant/date range. Dates and tenants outside
that range are preserved. The operation creates no requested package or
published revision. After every needed date succeeds, submit the normal Daily
or Monthly Preview request.

Interrupted operations are durably marked failed and are not resumed
automatically. Retrying submits a new repair operation; exact-date replacement
makes the same bounded retry deterministic without duplicate current lineage.
API-only deployments can read retained repair status but return 503 for new
submissions. Disabled tenants cannot submit or read repair operations.

### Migration 029: canonical UTC-second timestamps

Migration 029 establishes one persistence contract for financial-period keys
and Preview lifecycle state on SQLite and PostgreSQL: values are UTC and stored
at whole-second precision. It covers the in-scope billing, chargeback,
topic-allocation, source-evidence, calculation-lineage, Preview
request/revision/repair scalar timestamps, plus the named timestamps in
persisted request coverage and revision source-snapshot JSON.

The migration requires an online database connection because it preflights all
supported tables before changing data. Offline `--sql` upgrade or downgrade
fails with an instruction to run against the database. During preflight,
logically identical records that collapse to the same canonical natural key are
converged deterministically. Conflicting payloads, invalid timestamps, naive
JSON timestamps, or unsafe allocation-lineage parent state abort the transaction
with the affected table/key and repair guidance; the database remains at
revision 028. When lineage portions converge, their complete parent run's
`portion_count` is recalculated from the canonical survivors.

SQLite downgrade from 029 restores the revision-028 zero-fraction text form
ending in `.000000` before removing the new retention retry column. This is
only compatibility with revision-028 upsert identity, not a sub-second
precision contract. PostgreSQL migration behavior and rollback are verified by
the dedicated real-PostgreSQL CI job.

Existing API field names, shapes, and semantics remain compatible; affected
lifecycle timestamp values follow the new UTC whole-second contract. Production
manifest timestamps already emitted at whole-second precision keep their
formatting. Immutable artifact bytes, storage keys, financial values,
correlations, and totals are unchanged. Canonical duplicate convergence
prevents one logical financial origin from being processed twice.

## FOCUS Mapping Preview compatibility

Existing Preview configuration and previously published requested packages and
monthly revisions remain compatible after this upgrade. The new capacity and
generation-spool settings are additive and use their documented defaults when
omitted.

Requested and scheduled generation now share process-local running and queued
limits. The per-generation spool ceiling defaults to 2 GiB. Review disk sizing
before increasing `preview.max_workers`, because each running generation can
use its complete configured ceiling.

Keep each tenant database and its matching `preview.artifact_root` together
during backup, restore, or storage moves. Restore both from the same point in
time so package metadata and immutable package bytes remain consistent.

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

# Data Retention

Chitragupta has separate lifecycles for tenant pipeline data, topic attribution
data, requested FOCUS Preview packages, and published FOCUS revisions. Published
revision retention follows the tenant billing-data cutoff; requested packages
keep a fixed seven-day lifetime.

## Tenant pipeline data

After each pipeline cycle, the engine applies
`tenants.*.retention_days` to the cached tenants that ran in that process.
Resource and identity records use the exact UTC cleanup timestamp minus the
configured duration. Calculation-owned data uses the start of the UTC calendar
day containing that cutoff: billing, chargebacks, pipeline state, Preview Cost
source records, source readiness, and allocation lineage therefore retain that
whole calculation day rather than deleting part of it. Organization-authority
attempts use the exact tenant cutoff, and topic attribution keeps its separate
exact timestamp cutoff described below.

```yaml
tenants:
  my-tenant:
    lookback_days: 200
    retention_days: 250
```

| Field | Default | Range | Purpose |
|---|---:|---:|---|
| `retention_days` | 250 | 1–730 | Age at which tenant pipeline data is deleted. |
| `lookback_days` | 200 | 1–364 | Provider acquisition/recalculation window; must be greater than `cutoff_days`. |

Set `retention_days` greater than `lookback_days` when you want recalculation to
retain all data inside the acquisition window. `lookback_days` is not an
archive or a guarantee that old provider inputs can be reconstructed.

There is no CLI for selective manual retention cleanup. To remove all data for
a tenant, stop the service and remove or recreate that tenant's database only
after taking any required backup.

## Topic attribution data

Topic attribution uses its own setting:

```yaml
tenants:
  my-tenant:
    plugin_settings:
      topic_attribution:
        retention_days: 90
```

| Setting | Default | Range | Data affected |
|---|---:|---:|---|
| `tenants.*.retention_days` | 250 | 1–730 | Tenant billing, resource, identity, and chargeback data. |
| `plugin_settings.topic_attribution.retention_days` | 90 | 1–365 | Topic attribution facts and dimensions. |

The settings are evaluated independently.

## FOCUS Preview evidence

Preview evidence retention runs only for enabled tenants. It removes expired raw
Cost evidence, source-readiness history, allocation lineage, and superseded
organization authority using the tenant's `retention_days` cutoff.

Ordinary tenant cleanup and Preview evidence cleanup have separate readiness
outcomes. A failed cleanup makes Preview `degraded` without removing existing
valid packages or blocking unrelated billing, chargeback, inventory, and
pipeline operations. Follow the readiness diagnostic, restore the affected
storage or cleanup dependency, and allow a later scheduled cycle to retry.

Disabling Preview stops new evidence and cleanup work but does not delete
existing evidence. Re-enabling cannot reconstruct data already outside the
acquisition or retention windows.

## Historical repair and retention

Historical repair is limited to dates inside the tenant's commercial,
acquisition, cutoff, and retention intervals. It does not extend retention or
restore provider billing and metrics history that is no longer available.

Repair replaces billing and recalculates only the selected tenant/date scope.
The matching chargebacks, pipeline state, source evidence, and allocation
lineage follow the ordinary retention policy. Repair creates no requested
package or published revision.

Repair status is durable. Expected failures do not stop later dates. Interrupted
work is not resumed automatically; submit a new bounded repair after correcting
the cause. See [Repair retained dates](../focus-mapping-preview.md#repair-retained-dates)
for the workflow.

## Preview artifact recovery

Requested packages and published revisions share the configured artifact root
but keep separate lifecycles. Chitragupta automatically cleans interrupted
writes and preserves any package that is still referenced or cannot be verified
safely.

Do not edit or delete files inside `preview.artifact_root` directly. If recovery
cannot verify the database and artifact root together, affected requested-package
operations return `FOCUS Mapping Preview recovery is unavailable`. Restore both
from the same backup or correct storage access, then retry. See
[Preview troubleshooting](troubleshooting.md#focus-mapping-preview-is-upgrading-degraded-or-unavailable).

## Requested FOCUS Preview packages

A requested Preview package has a fixed seven-day availability window measured
from durable ready publication:

```text
ready_at <= downloadable time < expires_at
expires_at = ready_at + 7 days
```

At the exact `expires_at` instant, the request transitions to `expired` and
manifest, individual-file, and archive downloads return 410. Download access is
blocked before artifact cleanup, so a cleanup failure cannot extend the
availability window. The request lifecycle and source snapshot remain visible
in request history, but expired status responses contain `package: null`.
Manifest/file metadata and all download URLs are therefore no longer exposed.

The seven-day lifetime is fixed and is not configured by
`tenants.*.retention_days`, topic-attribution retention,
`preview.max_csv_file_bytes`, or `lookback_days`. Re-requesting after expiry
creates a new package from the then-current persisted source snapshot; it does
not recreate the expired bytes.

Back up both each tenant database and `preview.artifact_root` when preserving
currently downloadable packages during an upgrade or restore. See
[FOCUS Mapping Preview](../focus-mapping-preview.md#package-contents-and-lifecycle)
for package behavior.

## Published monthly revisions

Published monthly revisions are separate from seven-day requested packages.
Current and superseded revisions remain available through the revision-history
UI, API, and CLI while their billing month is inside the tenant's
`retention_days` window. Each revision is a complete replacement for its month;
consumers must select the current revision and must not aggregate revisions.

Revision retention uses calendar-month boundaries. At a cleanup time `now`, the
cutoff is:

```text
cutoff_date = (now in UTC - retention_days).date()
```

A month is eligible for removal when its exclusive month end is on or before
`cutoff_date`. This includes the exact boundary. All revisions for such a month,
including its current revision, are eligible; newer current revisions remain
protected.

Cleanup runs only as part of scheduled periodic processing. It first makes each
eligible revision unavailable to public history and direct downloads, then
removes its package, and finally removes its metadata. Failed removals remain
unavailable and are retried by later periodic runs, including after restart.

Scheduled publication does not seed months that are already outside this
retention window. `lookback_days` still controls acquisition and recalculation;
it does not extend revision retention. Requested ad-hoc packages keep their
fixed seven-day availability independently of `retention_days`.

Back up the tenant database and Preview packages together when retained
published revisions must survive an upgrade or restore.

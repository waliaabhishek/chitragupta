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

Preview evidence retention runs only for tenants with `focus_preview` enabled.
During scheduled retention, Chitragupta removes expired raw Cost evidence,
source-readiness history, allocation-lineage records, and superseded
organization-authority attempts using the tenant's `retention_days` cutoff.
Lineage cleanup also reconciles the retained window: a run is removed when its
calculation identity no longer matches an authoritative calculated pipeline
state, or when any of its portions no longer has its complete billing-origin
key. A retained zero-portion or unavailable run remains valid when its
calculation identity still matches pipeline state. Repeating cleanup after
success makes no additional changes.

This cleanup is separate from generic billing and chargeback cleanup. If
Preview evidence cleanup fails, its transaction is rolled back, the failure is
logged for operators, and a later scheduled cycle retries it. Generic cleanup
and chargeback operation remain independent.

While disabled, tenants create or access no new Preview evidence and run no
Preview retention work. Disabling Preview does not delete evidence that was
already persisted. Re-enabling may reuse or revalidate retained evidence, but
does not reconstruct evidence already outside the acquisition or retention
windows.

## Historical repair and retention

Historical repair is limited to dates wholly inside the tenant's current
effective, acquisition, cutoff, and complete retention intervals. It does not
extend retention or restore data already deleted by retention. Provider billing
history and any historical metrics needed by allocation must also remain
available outside Chitragupta.

For each selected date, the authoritative provider result replaces only that
tenant/date's billing rows. An authoritative empty result removes stale billing
for that date and still runs the canonical zero-row calculation and validation
path. Matching chargebacks, pipeline state, source evidence, and allocation
lineage are updated consistently; data outside the selected tenant/date range
is preserved. New evidence and lineage then follow the ordinary tenant
retention policy.

Repair operation and per-date statuses are durable. Expected date failures do
not stop later dates. Interrupted work is marked failed and is not automatically
resumed; retrying creates a new operation over the requested range. Repair
creates no requested package or published revision, so their separate retention
lifecycles remain unchanged.

## Preview artifact recovery

Requested packages and published revisions share one durable artifact root but
keep separate metadata lifecycles. New packages use an opaque versioned
namespace derived from ecosystem, provider tenant ID, and the configured
storage backend. The display tenant name is not part of that storage identity,
so renaming a configured tenant does not move its packages; two databases with
the same provider tenant ID remain isolated.

Publication writes and synchronizes a staging package, atomically finalizes it,
and holds one package lock until the request or revision metadata commit
finishes. Same-process retries and restart recovery remove interrupted staging
work. Finalized packages are removed only after an owner-scoped metadata
snapshot misses the package and a fresh authoritative reference check under the
same stable package lock also reports it unreferenced. A live publisher,
reference-query failure, deletion failure, or synchronization failure preserves
or defers work for a later recovery attempt rather than treating cleanup as
successful.

Recovery preserves every package referenced by request or revision metadata,
including referenced packages created by an older release. Legacy or otherwise
unverifiable finalized paths are not automatically deleted. Request expiry
still blocks downloads before deleting bytes, and revision retention still
hides eligible revisions before package deletion and retries pending cleanup
after later cycles or restarts.

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
removes its package, and finally removes its metadata. If package removal fails,
the revision stays unavailable and pending cleanup. Later periodic runs retry
it, including after a service restart. An already-absent package is treated as
success so cleanup can finish.

Scheduled publication does not seed months that are already outside this
retention window. `lookback_days` still controls acquisition and recalculation;
it does not extend revision retention. Requested ad-hoc packages keep their
fixed seven-day availability independently of `retention_days`.

Back up the tenant database and Preview packages together when retained
published revisions must survive an upgrade or restore.

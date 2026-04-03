# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed
- **Fix:** Retry counters (`allocation_attempts`, `topic_attribution_attempts`) now reset to 0 when a date is re-queued via the recalculation window, preventing permanently stuck sentinel rows after transient infrastructure failures. (TASK-184)

- **Fix:** Topic discovery returning zero topics no longer leaves pipeline dates permanently stuck. `mark_topic_overlay_gathered` is now called unconditionally after a successful gather, regardless of whether any topic resources were returned. Previously, an empty topic list silently skipped the mark, causing the date to remain pending and retry forever. (TASK-183)

- **TASK-182**: Resource type filtering is now mandatory on all resource repository read methods. This fixes three bugs: (1) topic resources being falsely marked as deleted during pipeline runs, (2) topic resources inflating Prometheus `chitragupta_resource_active` metric cardinality, (3) topic resources unnecessarily loaded into the chargeback resource cache.

- **Fix:** Poison-pill dates with permanently unavailable Prometheus metrics no longer retry forever after TASK-177. Topic attribution now tracks per-billing-line attempt counts (`topic_attribution_attempts` column, migration 016) and caps retries at `topic_attribution_retry_limit` (default 3, range 1–10). When all billing lines for a cluster exhaust the limit, sentinel `TopicAttributionRow` entries are produced (`topic_name=__UNATTRIBUTED__`, `attribution_method=ATTRIBUTION_FAILED`) preserving full cost. The date is marked calculated only when every cluster is resolved (success, empty, or sentinel). Clusters still below the limit return `None`, leaving the date pending for the next run. (TASK-181)

- **Fix:** Topic attribution no longer filters Prometheus metric data through the resources table. `_attribute_cluster` now computes the union of resources-table topics and metrics-discovered topics before constructing `TopicAttributionContext`. Topics that Prometheus reports as active during the billing window but that are absent from the resources table (never discovered by gather, or deleted before gather ran) are now correctly included in cost attribution instead of being silently discarded. (TASK-180)

- **Fix:** Topic attribution now uses point-in-time topic membership for historical billing periods instead of current topic inventory. `_get_cluster_topics` queries `find_by_period` with the billing window `[b_start, b_end)` and `parent_id` filtering, so deleted topics that had traffic during the billing period are correctly included, and topics created after the billing period are excluded. `ResourceRepository.find_by_period` gains an optional `parent_id` parameter. (TASK-179)

- **Fix:** Recalculation window now deletes stale topic attribution facts before recompute. Previously, only chargeback rows were deleted, so topics removed between runs could cause silent double-accounting on the next calculation. (TASK-178)

- **Fix:** Transient Prometheus outages during topic attribution no longer permanently mark affected dates as calculated. `_attribute_cluster` now returns `None` (instead of `[]`) on infrastructure failure, and `run()` skips `mark_topic_attribution_calculated` when any cluster experienced a metrics fetch failure — leaving the date pending for retry on the next pipeline run. (TASK-177)

- **Chore:** Unified emitter system — one generic `CsvEmitter` and one generic `PrometheusEmitter` now serve all pipelines. Row types (`ChargebackRow`, `TopicAttributionRow`, `BillingEmitRow`, `ResourceEmitRow`, `IdentityEmitRow`) declare `__csv_fields__` and `__prometheus_metrics__` ClassVars; emitters read these at emit time. `PrometheusEmitter` no longer takes `storage_backend` — billing, resource, and identity Prometheus metrics are now emitted via separate `EmitterRunner` instances with dedicated row fetchers (`BillingRowFetcher`, `ResourceRowFetcher`, `IdentityRowFetcher`). `TopicAttributionConfig` now auto-injects `output_dir="/tmp/topic_attribution"` and `filename_template="topic_attr_{tenant_id}_{date}.csv"` for CSV emitter specs under `topic_attribution.emitters` (overridable per-spec). `TopicAttributionEmitterBuilder` and `TopicAttributionCsvEmitter` removed. (TASK-176)

- **Chore:** Replaced runtime `getattr` config probing for topic attribution with typed `OverlayConfig`/`OverlayPlugin` protocols in `src/core/plugin/protocols.py`. Core code (`orchestrator.py`, `workflow_runner.py`) now accesses overlay config via `plugin.get_overlay_config("topic_attribution")` instead of `getattr(getattr(config, "plugin_settings", None), "topic_attribution", None)`. No behavioral change — same config values reached via typed protocol instead of runtime attribute probing. (TASK-175)
- **Chore:** Removed dead `metrics_available` field from `TopicAttributionContext` and unreachable guard in `TopicUsageRatioModel.attribute()`. The field was always `True` — infra failure is handled at the phase level before context construction. No behavioral change. (TASK-174)
- **Fix:** Topic attribution CSV export (`POST /topic-attributions/export`) no longer silently truncates results at 100,000 rows or materializes the full result set into memory. The endpoint now uses `iter_by_filters` for true streaming with bounded memory, matching the chargeback export pattern fixed in TASK-026. (TASK-173)
- **Fix:** Added missing `attribution_method` label to the `chitragupta_topic_attribution_amount` Prometheus gauge. Without this label, topic attribution rows with the same topic but different attribution methods (e.g. `bytes_ratio` vs `even_split`) silently overwrote each other. The Prometheus emitter now matches the CSV emitter's output dimensions. Also fixed test fixture cleanup for `_topic_attribution_col`. (TASK-172)
- **Fix:** Removed `server_default=""` from `topic_attribution_facts.amount` column — the database now rejects inserts with missing amount values instead of silently storing empty strings, matching the `chargeback_facts` schema pattern. New migration 015 removes the default for existing installs. (TASK-171)

### Added
- Feat: TASK-168 — Generalize `EmitterRunner` for pipeline-agnostic emission tracking

  `EmitterRunner` is now pipeline-agnostic via constructor-injected protocols:
  `PipelineDateSource`, `PipelineRowFetcher` / `PipelineAggregatedRowFetcher`,
  and `PipelineEmitterBuilder`. The `pipeline` parameter acts as a discriminator
  so chargeback and topic attribution emission records coexist without collision.

  Changes:
  - `EmitterRunner.__init__` now accepts `date_source`, `row_fetcher`,
    `emitter_builder`, and `pipeline` in place of direct storage coupling
  - `EmissionRecord` gains a `pipeline` field; `emission_records` table gains
    a `pipeline` column (migration 014, default `"chargeback"`)
  - `get_emitted_dates` / `get_failed_dates` are now pipeline-scoped
  - `ChargebackDateSource`, `ChargebackRowFetcher`, and `RegistryEmitterBuilder`
    in `core/emitters/sources.py` provide the concrete chargeback implementations
  - Topic attribution emission now uses the same idempotency loop: skips
    already-emitted dates, retries failed dates, and respects `lookback_days`
  - `TopicAttributionEmitterRunner` deleted — superseded by the generalized runner

- Feat: TASK-165 — Topic attribution overlay for Confluent Cloud

  Adds a new optional pipeline stage (`topic_overlay`) that attributes Kafka
  cluster billing costs down to individual topics using Prometheus metrics.
  Enabled via `plugin_settings.topic_attribution.enabled: true` in CCloud
  tenant config.

  New features:
  - `TopicAttributionPhase`: computes per-topic cost rows for each Kafka
    billing line using a configurable chain model (bytes_ratio →
    even_split fallback)
  - Default cost mappings for `KAFKA_NETWORK_WRITE`, `KAFKA_NETWORK_READ`,
    `KAFKA_STORAGE`, `KAFKA_PARTITION`, `KAFKA_BASE`, `KAFKA_NUM_CKU/CKUS`
  - `topic_attribution` config block with `exclude_topic_patterns`,
    `missing_metrics_behavior`, `cost_mapping_overrides`,
    `metric_name_overrides`, `retention_days`, and `emitters`
  - 4 new API endpoints under `/api/v1/tenants/{name}/topic-attributions`:
    list, aggregate, dates, and CSV export
  - Database migration 012 adds `topic_attribution_dimensions` and
    `topic_attribution_facts` star-schema tables
  - `TopicAttributionConfigProtocol` in `core/` keeps plugin config decoupled
    from core attribution models (DIP-compliant)

### Fixed
- Fix: TASK-170 — Topic discovery queries now respect `metric_name_overrides`

  Topic discovery (`gather_topic_resources`) was querying hardcoded Confluent metric names,
  silently ignoring any `topic_attribution.metric_name_overrides` configured by the user.
  Discovery and attribution were resolving different metric names, so overrides only took
  effect for attribution — not for discovering which topics exist.

  `build_discovery_queries` now delegates to `build_metric_queries` (the single source of
  truth for `_DEFAULT_METRIC_NAMES` and `_QUERY_TEMPLATES`) and renames keys with a `disc_`
  prefix. Both phases now use the same resolved metric names.

- Fix: TASK-169 — Config validation rejects `topic_attribution.enabled: true` without a `metrics` source

  Previously, enabling topic attribution without a configured Prometheus metrics source was silently
  accepted. `TopicAttributionPhase._fetch_topic_metrics()` returned `{}` (empty), which the caller
  treated as "healthy but no data" and proceeded to produce even-split attribution rows based on zero
  metric data — indistinguishable from a legitimate empty cluster.

  A new `@model_validator` on `CCloudPluginConfig` now raises `ValidationError` at load time when
  `topic_attribution.enabled=True` and `metrics` is `None`. `_fetch_topic_metrics()` also raises
  `RuntimeError` if somehow called without a metrics source, making the invariant violation loud
  rather than silent.

- Fix: TASK-167 — Remove duplicated `_distribute_remainder` from `topic_attribution_models.py`

  The local copy used an unbounded `while diff != 0` loop, which could hang forever on
  sub-cent precision edge cases. Now imports the bounded version from `helpers.py`
  (fixed in TASK-120), which raises `RuntimeError` after `len(amounts) * 2` iterations.

- Fix: TASK-166 — Fix `attribution_method` nullable constraint in topic attribution schema

  The `attribution_method` column in `topic_attribution_dimensions` was
  `nullable=True`, making the unique constraint `uq_topic_attribution_dimensions`
  ineffective on PostgreSQL (NULL != NULL). Changed to `NOT NULL DEFAULT ''`
  across ORM, baseline migration 012, and new migration 013. Removes the
  defensive `or ""` guard in the repository read path.

---

## [2.0.0] - 2026-03-27

### Added
- Feat: TASK-163 — Pipeline Status page with workflow stepper visualization

Replace placeholder with full pipeline status page: horizontal Ant Design
Steps stepper showing gathering/calculating/emitting stages, Run Pipeline
button with POST trigger and feedback alerts, Last Run Summary card with
Descriptions component, and Per-Date Processing Status table with boolean
icon columns. Uses existing useReadiness() context for live pipeline state
and @tanstack/react-query useQuery for API fetches with adaptive refetch
intervals. 21 tests, zero regressions. ([730133c](https://github.com/waliaabhishek/chitragupta/commit/730133c6aaaaf44c85a7702fe0b49c64d8804f9c))


### Changed
- Refactor: Standardize all tables on AG Grid, remove Ant Design Table usage

Migrate AllocationIssuesTable to AG Grid infinite scroll with server-side
pagination and PipelineStatus table to AG Grid client-side row model.
Delete unused useAllocationIssues hook. Update tests for both components. ([f29cbce](https://github.com/waliaabhishek/chitragupta/commit/f29cbce45807fa890be496cc80fb5db7ef18bc58))


### Documentation
- Docs: Update README.md

Updated performance metrics and added custom tags support. ([6ccfe23](https://github.com/waliaabhishek/chitragupta/commit/6ccfe235da3d014c3466bdb4b7ed997a03627772))


### Fixed
- Fix: Migrate AG Grid from legacy CSS to v35 JS Theme API

AG Grid v35 defaults to themeQuartz (JS-based). Without setting the theme
prop, the grid injected Quartz styles that conflicted with our legacy CSS
imports, causing themes to break on navigation between pages.

Migrated to the proper v35 approach: themeAlpine.withParams() for light
mode, .withParams({}, "dark") for dark mode, with data-ag-theme-mode
attribute on body for mode switching. Removed all legacy CSS imports and
the ag-grid-theme.css override file. Added defaultColDef with sortable
and resizable to all grids. ([95a0f94](https://github.com/waliaabhishek/chitragupta/commit/95a0f9445055c3e3caca44dd125c654446e32e49))
- Fix: AG Grid theme not applying — add Alpine base class, sorting, resizing, and alternating rows

The grid wrapper divs used className="ag-theme-chitragupta" but ag-theme-alpine.css
only targets .ag-theme-alpine, so Alpine's visual styles never applied. Now uses both
classes. Also adds shared defaultColDef with sortable/resizable and subtle alternating
row backgrounds for both light and dark modes. ([4ecc39c](https://github.com/waliaabhishek/chitragupta/commit/4ecc39c00df416314423a7145ea6ac0c3383bcdb))
- Fix: Auto-detect docker compose v1/v2 in dev Makefile ([6094965](https://github.com/waliaabhishek/chitragupta/commit/6094965b2578738defc1386b37d290180cf933b7))


## [2.0.0-rc3] - 2026-03-27

### Added
- Feat: TASK-161 — Unified AG Grid table system with Ant Design theme sync and server-side filtering

Standardize all table views on AG Grid with custom ag-theme-chitragupta theme
matching Ant Design's visual style. Theme syncs with dark/light mode toggle via
data-theme attribute on document.body. Migrate Identities, Resources, and Tags
pages from Ant Table to AG Grid with infinite scroll, filter bars, and URL param
persistence. Extend identity and resource list API endpoints with search (ILIKE),
sort_by, sort_order, tag_key, and tag_value query params. Keep AllocationIssuesTable
as Ant Table (bounded volume, dashboard context). ([7ed0fc0](https://github.com/waliaabhishek/chitragupta/commit/7ed0fc00c6a9db3a72ed05deef6f8fc53a830c3a))
- Feat: TASK-160.02 — Frontend entity tag editor, inherited tag display, and tag-based filtering

Replace dimension-level tag UI with entity-level tag system: new EntityTagEditor
component for resources/identities, read-only inherited tags on chargeback views,
tag key/value filter inputs, and removal of old TagEditor/BulkTagModal/SelectionToolbar. ([f1dfdda](https://github.com/waliaabhishek/chitragupta/commit/f1dfdda5c8d8473d354d77bb99bcec1d0a536101))
- Feat: TASK-160.01 — Entity-level tag system replacing dimension-level tags

Tags now attach to core entities (resources, identities) instead of
ephemeral chargeback dimensions. Chargebacks inherit tags at query time
via resource_id/identity_id joins with resource tags overriding identity
tags on key collision. Old custom_tags table dropped, new tags table
created. ChargebackResponse.tags changed from list[str] to dict[str,str]. ([0211241](https://github.com/waliaabhishek/chitragupta/commit/02112416a87f25260481b4af38f744e533503f65))
- Feat: Add help target to dev stack Makefile ([7c5f215](https://github.com/waliaabhishek/chitragupta/commit/7c5f215162d860c24b8261c40d50d73489290a92))
- Feat: Development environment for Docker based app ([197b418](https://github.com/waliaabhishek/chitragupta/commit/197b4189795bfbd7395b1c43a35ea3f7044b9eca))
- Feat: TASK-156 — Add active/deleted status breakdown to Inventory UI

Full-stack change: inventory summary now returns per-type active/deleted
counts. ResourceRepository groups by status column; IdentityRepository
derives status from deleted_at via SQL CASE. New TypeStatusCounts frozen
dataclass in core.models.counts. Frontend displays total prominently with
"Active: N / Deleted: N" secondary text. All existing tests updated;
new tests for mixed-status resources and identity deleted_at derivation. ([cc1def8](https://github.com/waliaabhishek/chitragupta/commit/cc1def8e912806a52baf7c48f705527cc85be945))


### Changed
- Refactor: Move docker commands to examples/dev/Makefile

Automated builds handle production images now. Removed all docker
targets from root Makefile and added a simple Makefile in examples/dev/
with up, down, logs, and clean targets. ([7e57bbf](https://github.com/waliaabhishek/chitragupta/commit/7e57bbf7d8f79ecdf5a031b59a4c979c75a81a83))


### Fixed
- Fix: AG Grid infinite row model stuck after aborted fetch in all grid components

When a fetch was aborted via AbortController, the getRows error handler
silently returned without calling successCallback or failCallback. This
left AG Grid's block in a permanent "in-flight" state, preventing any
subsequent purgeInfiniteCache from triggering new data fetches. The grid
appeared permanently empty when typing in text-based filter inputs (tag
key/value) since each keystroke aborted the previous request.

Always call failCallback() on any fetch error so AG Grid properly resets
the block state and allows cache purges to trigger fresh getRows calls. ([8ccff0f](https://github.com/waliaabhishek/chitragupta/commit/8ccff0f0ecc26f6cba8046b7b7f08f6c5d2f847b))
- Fix: TASK-162 — AG Grid theme with alpine base CSS, Ant Design visual parity, and test rewrites

Add ag-theme-alpine.css as base theme import so ag-theme-chitragupta CSS
variable overrides actually apply. Enhanced theme CSS with structural
overrides for Ant Design Table parity: horizontal-only borders, no grid
border, font-weight 500 headers, 48px row height, no cell focus outline,
no zebra striping. Centralized all AG Grid CSS imports in main.tsx and
removed per-component imports. Rewrote identity/resource/tag list page
tests to mock new AG Grid components instead of Ant Design Table. Added
TagsGrid.test.tsx for edit/delete/error/readonly behavior coverage.

42 test files, 316 tests passing. tsc + vite build clean. ([36480ad](https://github.com/waliaabhishek/chitragupta/commit/36480ad9b4927cc45824d88d2a6cce2ed3f5bf0d))
- Fix: Resolve TypeScript errors in frontend tests and add tsc pre-commit hook

Test files were missing tag_key/tag_value fields on ChargebackFilters,
EntityTagEditor tests used wrong types, and the Docker build failed on
tsc. Added a pre-commit hook to run frontend typecheck on .ts/.tsx changes. ([5de1193](https://github.com/waliaabhishek/chitragupta/commit/5de1193b34b9b52bb65bf3fe11716a21169bbc78))
- Fix: Bump requests 2.32.5→2.33.0 and picomatch 4.0.3→4.0.4 for Dependabot alerts

Resolves 2 moderate GitHub Dependabot vulnerabilities:
- requests: insecure temp file reuse in extract_zipped_paths()
- picomatch: method injection in POSIX character classes ([416db5e](https://github.com/waliaabhishek/chitragupta/commit/416db5eba6b9b4bcc8bd04b6153d3cf3c9e45acf))
- Fix: TASK-159 — Add timezone picker to filter panels and wire to API

Frontend timezone Select added to Chargebacks, Billing, and Dashboard
filter panels. Defaults to browser timezone, persists to localStorage
(user_timezone), syncs via URL search param. All date-filtered API
calls (grid, aggregate, allocation issues, CSV export) now include the
timezone parameter, resolving the data discrepancy where UI totals
diverged from Grafana for non-UTC users. ([dee6e39](https://github.com/waliaabhishek/chitragupta/commit/dee6e39deaa2578cf9c6362937259293f9d8529a))
- Fix: TASK-158 — Add timezone query param to all date-filtered API endpoints

Wire the timezone parameter (added to resolve_date_range in TASK-157) through
all 6 call sites: billing, chargebacks (list + allocation-issues), aggregation,
export, and tags bulk-by-filter. GET endpoints accept timezone as a query param;
POST endpoints accept it in the request body via ExportRequest and
BulkTagByFilterRequest schemas. Backward compatible — omitting timezone
preserves UTC behavior. ([050e7b5](https://github.com/waliaabhishek/chitragupta/commit/050e7b55c7652fee1cf3a847493ce40cefd7b1a7))
- Fix: TASK-157 — Add timezone parameter to resolve_date_range

resolve_date_range now accepts an optional IANA timezone string so date
boundaries are computed in the user's local timezone before converting
to UTC. Fixes data discrepancy where non-UTC users missed records near
midnight boundaries (e.g. America/Denver end_date=Dec 31 now correctly
produces end_dt=2026-01-01T07:00:00 UTC instead of 00:00:00 UTC). ([35800b0](https://github.com/waliaabhishek/chitragupta/commit/35800b0bd1f17e11b4480b3ab3502495842d5709))
- Fix: Remove 10k row limit on aggregate endpoint that silently truncated totals

The hardcoded limit=10000 on the aggregate SQL query caused the dashboard
to show $8k instead of $16.6k when identity_id × daily buckets exceeded
10k rows. Grafana was correct because it queries SQLite directly. ([3353312](https://github.com/waliaabhishek/chitragupta/commit/3353312207e4222bd9c391a675a081338bef8328))


## [2.0.0-rc2] - 2026-03-25

### Added
- Feat: TASK-155 — Add TanStack Query v5 for client-side data caching

Convert all five data-fetching hooks (useAggregation, useDataAvailability,
useInventorySummary, useAllocationIssues, useFilterOptions) from
useState+useEffect+fetch to useQuery with shared QueryClient cache.
Page navigation now returns cached data instantly without re-fetching.

- QueryClientProvider between AntApp and TenantProvider (staleTime 5min,
  gcTime 10min, refetchOnWindowFocus false, retry 1)
- ReactQueryDevtools in dev mode
- All hook return types preserved — zero consumer changes
- All 258 tests pass with QueryClientProvider test wrapper ([82c74d4](https://github.com/waliaabhishek/chitragupta/commit/82c74d40acf8319035fae2c4a54252248096815c))
- Feat: TASK-154 — Implement Billing List Page with shared utility extraction

Replace the "Coming soon" placeholder at /billing with a functional billing
data viewer: AG Grid infinite scroll, date/product/resource filters with URL
sync and localStorage persistence, reset and refresh controls.

Extract shared utilities (dateFilterStorage, gridFormatters, filterHelpers)
from chargebacks code to eliminate duplication across both features. ([1cf96e6](https://github.com/waliaabhishek/chitragupta/commit/1cf96e67083837ecb8747445b05a933d229d0766))


### Changed
- Tests: Suppress 20 harmless pytest warnings via filterwarnings

Filter SAWarning from Alembic batch-mode PK mismatch on SQLite temp
tables and ResourceWarning from GC'd in-memory SQLite connections. ([de4a0b0](https://github.com/waliaabhishek/chitragupta/commit/de4a0b062f23a4b92856c58141dc46fba9229880))
- Docker: Add version pinning via CHITRAGUPTA_VERSION and explicit container names

- Example compose files now use ${CHITRAGUPTA_VERSION:-latest} for image tags
- Added CHITRAGUPTA_VERSION=latest to all .env.example files
- Added container_name to all services to avoid -1 suffix in logs
- Release notes now include Docker pull commands and .env pin instructions ([e20ae43](https://github.com/waliaabhishek/chitragupta/commit/e20ae4340a8d221db2495f2f3abb84fd77f44618))


### Fixed
- Fix: Resolve pre-existing lint errors across frontend and backend

- FilterPanel.test.tsx: remove unused `_` params in format callbacks
- chargebacks/list.tsx: move eslint-disable to correct line
- test_cost_input.py: fix import sorting (I001) and add strict=True to zip (B905) ([cea15b8](https://github.com/waliaabhishek/chitragupta/commit/cea15b858f65b6d74631307499b1fc844537ce9b))
- Fix: Register AG Grid modules to fix empty Chargebacks page

AG Grid v35 requires explicit module registration. Without it, the
infinite row model silently fails (error #272) and no API calls are
made, leaving the Chargebacks grid permanently empty.

Also add **/data/ to .gitignore. ([c6df56f](https://github.com/waliaabhishek/chitragupta/commit/c6df56f5e74cfa8eb68f6e9543179e3d5184f020))
- Fix: TASK-153 — Aggregate tiered billing rows in CCloud gather()

Confluent Cloud billing API returns multiple rows per resource/date/product
when tiered pricing applies (different price points for the same line item).
These rows share the same 7-field billing PK, causing the second tier to
silently overwrite the first via session.merge(), losing cost data.

Adds tier aggregation in _fetch_window() before yielding: rows sharing the
same billing key are merged into a single CCloudBillingLineItem with summed
costs. Per-tier breakdown preserved in metadata["tiers"] for auditability.
Single-row keys pass through unchanged. Plugin-scoped fix — no schema
migration, no core engine changes. ([6a99169](https://github.com/waliaabhishek/chitragupta/commit/6a991691e8c360f3613bb02f4682854afa340cd3))
- Fix: Pre-create /app/data dir in Dockerfile for named-volume compatibility

Docker named volumes mount as root:root when the target directory doesn't
exist in the image. appuser (uid 1000) cannot write, causing Alembic
migrations to fail with "unable to open database file" on fresh volumes. ([538abdb](https://github.com/waliaabhishek/chitragupta/commit/538abdbba1f75473431ca0abcdc237ee0c4582e0))
- Fix: TASK-124 — Eliminate resource cache multi-window collision in orchestrator

_build_resource_cache now keys by (b_start, b_end) billing window tuple,
matching the existing _build_tenant_period_cache pattern. This eliminates
the setdefault() collision where non-deterministic set iteration could
shadow a resource from one window with the same resource_id from another.
_collect_billing_line_rows uses window-scoped sub-dict for both
active_fraction lookup and ResolveContext, preserving the dict[str, Resource]
contract for downstream handlers. ([a03d63e](https://github.com/waliaabhishek/chitragupta/commit/a03d63e83c30c9028effed38f6ced11333ff9216))
- Fix: TASK-122 — Add missing context parameter to IdentityResolver protocol

IdentityResolver.__call__ had 6 params while ServiceHandler.resolve_identities
had 7 (including context: ResolveContext | None). This caused _validate_signature
to reject valid 7-param custom identity resolvers with a false TypeError. ([7511bbe](https://github.com/waliaabhishek/chitragupta/commit/7511bbe0d00856d7cbfbfffb3a13c32f4c421fa4))


### Security
- Security: Bump flatted 3.4.1 → 3.4.2 to fix CVE-2026-33228

Resolves Dependabot alert #5 — high-severity prototype pollution
via parse() in flatted. Dev dependency only (eslint → flat-cache). ([cdfacb1](https://github.com/waliaabhishek/chitragupta/commit/cdfacb1e36a923be660ee1eece4a69d9ff9a77c9))


## [2.0.0.rc1] - 2026-03-24

### Added
- Feat: Treemap panel for principal costs, better line items panel

- Replace gauge panel with treemap for Cost breakdown by Principal (continuous color gradient, single consolidated query, no row limit)
- Rename Data Availability → Chargeback Line Items (Daily), legend count → line_items
- Add filled area with opacity gradient for better trend visibility
- Add marcusolsson-treemap-panel plugin to all docker-compose files ([83d3ecc](https://github.com/waliaabhishek/chitragupta/commit/83d3ecc13b0d2c4b5c24aaf4d73bcf1a2e998fd7))
- Feat(ccloud): complete plugin with OrgWide and Default handlers (chunk 2.6)

- Add OrgWideCostHandler for AUDIT_LOG_READ, SUPPORT (tenant_period split)
- Add DefaultHandler for TABLEFLOW_*, CLUSTER_LINKING_* (UNALLOCATED)
- Add org_wide_allocator (SHARED), default_allocator, cluster_linking_allocator (USAGE)
- Wire all 7 handlers into plugin with documented ordering
- Rename _make_row → make_row (public API for plugins)
- Add full pipeline e2e integration tests

Phase 2 complete. CCloud plugin fully operational. ([7e4b051](https://github.com/waliaabhishek/chitragupta/commit/7e4b05164b547e406a4176077ed2051c08170fca))
- Feat(ccloud): add FlinkHandler with metrics-driven identity resolution (chunk 2.5)

- FlinkHandler: service handler for FLINK_NUM_CFU/FLINK_NUM_CFUS product types
- flink_identity.py: two-step identity resolution (metrics → statement owner lookup)
- flink_allocators.py: CFU usage-ratio allocation via stmt_owner_cfu context
- create_flink_sentinel() in _identity_helpers.py for DRY compliance
- Handler wired as 5th in plugin chain
- 37 new tests, 97% coverage ([ee0c8c6](https://github.com/waliaabhishek/chitragupta/commit/ee0c8c60f7a4cd1eb1187cab6deee94db501cb27))
- Feat(ccloud): add ConnectorHandler and KsqldbHandler (chunk 2.4)

Implements ServiceHandler protocol for Connector and ksqlDB services:

ConnectorHandler:
- service_type: "connector"
- Handles: CONNECT_CAPACITY, CONNECT_NUM_TASKS, CONNECT_THROUGHPUT, CUSTOM_CONNECT_PLUGIN
- Identity resolution via connector auth mode (SERVICE_ACCOUNT, KAFKA_API_KEY, UNKNOWN)
- Allocators: capacity (SHARED), tasks/throughput (USAGE)

KsqldbHandler:
- service_type: "ksqldb"
- Handles: KSQL_NUM_CSU, KSQL_NUM_CSUS
- Identity resolution via direct owner_id
- Allocator: ksqldb_csu_allocator (USAGE)

Plugin wiring:
- Handler order: kafka, schema_registry, connector, ksqldb
- All handlers tested and wired into get_service_handlers()

32 new tests, 680 total passing, 99% coverage on CCloud plugin. ([ffad716](https://github.com/waliaabhishek/chitragupta/commit/ffad716a3a98a9d246ca7a0b16e984e5ebc26b73))
- Feat(ccloud): add ksqlDB identity resolution helper ([ebeeb91](https://github.com/waliaabhishek/chitragupta/commit/ebeeb91a764994977991f06ebb977951c340430f))
- Feat(ccloud): add connector identity resolution helper (chunk 2.4)

Implements resolve_connector_identity() for resolving connector owners
based on authentication mode (SERVICE_ACCOUNT, KAFKA_API_KEY, UNKNOWN).
Uses sentinel identities for unknown/masked credential cases. ([6fcaa95](https://github.com/waliaabhishek/chitragupta/commit/6fcaa95d2b3b705333bb6e313595199f770494db))
- Feat(ccloud): implement ksqlDB CSU allocator (chunk 2.4)

Add ksqldb_csu_allocator for even split across active identities
with USAGE cost type. Follows same pattern as connector_allocators.

Fallback chain: merged_active -> tenant_period -> UNALLOCATED ([f2016d4](https://github.com/waliaabhishek/chitragupta/commit/f2016d462d683b39415ef5636e4edaeb4806613a))
- Feat(ccloud): add Connect allocators for chunk 2.4

Implement three connector cost allocators:
- connect_capacity_allocator: even split, SHARED cost type
- connect_tasks_allocator: even split, USAGE cost type
- connect_throughput_allocator: delegates to tasks allocator

All use merged_active -> tenant_period -> UNALLOCATED fallback chain.
100% test coverage with 9 unit tests. ([340015f](https://github.com/waliaabhishek/chitragupta/commit/340015f4c400fdf28605bd26ffca1a4dd550c944))
- Feat(ccloud): implement Kafka + Schema Registry handlers (chunk 2.3)

- KafkaHandler: 7 product types, Prometheus bytes_in/bytes_out queries
- SchemaRegistryHandler: 3 product types, no metrics needed
- Identity resolution with temporal filtering (billing window)
- Allocators: hybrid 70/30, pure usage-based, even split
- Plugin wiring: get_service_handlers(), get_metrics_source()
- 74 new tests, 97% coverage on handlers/allocators ([e8976d5](https://github.com/waliaabhishek/chitragupta/commit/e8976d584b1826be9b2df95255c266fa2706d0ff))
- Feat(ccloud): implement resource + identity gathering (chunk 2.2)

- Add CRN parser for Confluent Resource Names
- Add CCloudConnection.get_raw() for non-envelope APIs
- Add proactive throttling with request_interval_seconds
- Implement CCloudBillingCostInput with date windowing
- Implement all resource gatherers: environments, kafka clusters,
  connectors, schema registries, ksqlDB, flink pools/statements
- Implement identity gatherers: service accounts, users, API keys,
  identity providers/pools
- Add connection caching for Flink regional connections
- Fix TD-020: ensure plugin.initialize() before get_metrics_source()
- Fix TD-017: wire get_cost_input() to return CCloudBillingCostInput

520 tests passing, 97% coverage ([9848a72](https://github.com/waliaabhishek/chitragupta/commit/9848a72cb05395c1b070846d33fbea1e45ad763b))
- Feat: GAP-002+003+005+010+015+017 workflow runner + plugin metrics

- GAP-015+017: EcosystemPlugin protocol now owns get_metrics_source();
  CCloud plugin returns None; workflow_runner uses plugin.get_metrics_source()
  instead of standalone _create_metrics_source()
- GAP-002: wait()-based global timeout replaces as_completed per-future timeout
- GAP-003: storage.create_tables() called before orchestrator runs
- GAP-005: enable_periodic_refresh=False runs single cycle then returns
- GAP-010: max_parallel_tenants (default=4) bounds ThreadPoolExecutor size
- FeaturesConfig gains max_parallel_tenants field with validation ([f14a1f9](https://github.com/waliaabhishek/chitragupta/commit/f14a1f95ec8b7f201f30b50c0fbae85704611456))
- Feat(ccloud): add ConfluentCloudPlugin stub implementing EcosystemPlugin ([d92605e](https://github.com/waliaabhishek/chitragupta/commit/d92605e2d0ae8dbf8e11a18a39219972ddd9baef))
- Feat(ccloud): add typed view models for Flink and Connectors ([633d9f4](https://github.com/waliaabhishek/chitragupta/commit/633d9f46a949919ed4d1720439725e8d94f0f03b))
- Feat(ccloud): add CCloudPluginConfig with validation ([d3cef84](https://github.com/waliaabhishek/chitragupta/commit/d3cef84fbbfdc91f534529bd905865d923742b48))
- Feat(ccloud): add CCloudConnection.post() method ([76dc209](https://github.com/waliaabhishek/chitragupta/commit/76dc209b3224d9cead5bbbe442aeca30c45ba702))
- Feat(ccloud): add retry logic with rate limit handling ([65e420a](https://github.com/waliaabhishek/chitragupta/commit/65e420addfa4ddec6fab6aabaf6220fbbb13e4be))
- Feat(ccloud): implement CCloudConnection.get() with pagination ([d556b96](https://github.com/waliaabhishek/chitragupta/commit/d556b966a69eed5a1cde56e25941ef0a9e7f259d))
- Feat(ccloud): add CCloudConnection dataclass structure ([1516302](https://github.com/waliaabhishek/chitragupta/commit/151630201fe852c850da02c628c1c883b8feb861))
- Feat(ccloud): add CCloudApiError and CCloudConnectionError exceptions ([9157bd4](https://github.com/waliaabhishek/chitragupta/commit/9157bd455fd892f915ecf8fcc933364e216c16b1))


### Changed
- Rename: chitragupt → chitragupta across entire codebase

Rename all references from the old spelling "chitragupt" to the correct
"chitragupta" before v2 release. 40 files, ~130+ occurrences including
package name, CLI entrypoint, Prometheus metric names, Docker image
names, collector env vars, plugin prefix, and all documentation.

BREAKING CHANGE: Prometheus metrics renamed (chitragupta_*), Docker
images renamed, CLI command is now `chitragupta`, env vars are now
CHITRAGUPTA_*, plugin prefix is chitragupta_plugin_*. ([37809de](https://github.com/waliaabhishek/chitragupta/commit/37809de4802dcc33c1644b4440a6026dc094cc4b))
- Style: Move Cost Over Time panel above Cost breakdown summary section ([ffff570](https://github.com/waliaabhishek/chitragupta/commit/ffff5701c6edaf85f8c8d975010c65986004e422))
- Fix Grafana billing panels: query both billing and ccloud_billing tables

CCloud billing data lives in ccloud_billing (migrated by ddebea2fe0a8),
not the core billing table. The three billing stat panels were querying
an empty table. Use UNION ALL to cover both tables so the dashboard
works for any ecosystem. ([d768a78](https://github.com/waliaabhishek/chitragupta/commit/d768a78d7c08154f5de8ade5c08c87026cf8b040))
- Fix Grafana dashboards: broken pie charts, missing panels, wrong column names

Overview dashboard (full rewrite):
- Fix pie charts showing 100% single-color by adding reduceOptions.values
- Add unit:currencyUSD, displayLabels:name, legend placement, sort:desc
- Reorganize to 4 pie charts: Environment, Resource, Product Category, Product Type
- Replace identity pie chart with gauge panel (arc viz, top 30 principals)
- Add collapsible billing detail rows (per-environment, per-resource, per-product-type)
- Add Object Roster row with resource/identity count stats
- Add "Cost breakdown summary" row separator

Details dashboard (column fixes):
- billing panel: b.total -> b.total_cost (correct column name)
- resources panel: r.updated_at -> r.last_seen_at (column doesn't exist)
- identities panel: i.updated_at -> i.last_seen_at (column doesn't exist) ([4337fa5](https://github.com/waliaabhishek/chitragupta/commit/4337fa514b32ed057ba1882ce3d34776618ef045))
- Build multi-arch Docker images (amd64 + arm64) via QEMU ([22dda6f](https://github.com/waliaabhishek/chitragupta/commit/22dda6f879f778dde24eff7d495e48b8b8349dc3))
- Remove local build sections from example Docker Compose files

Images are published to GHCR — no need for local build context.
Updated stale comments accordingly. ([a9d4651](https://github.com/waliaabhishek/chitragupta/commit/a9d465143027d7a93846d98adf6429ee92e11bd4))
- Cleanup: Remove stale configs/ and deployables/, consolidate into examples/

- Delete configs/examples/ (3 stale config files from chunk 1.3, unreferenced)
- Delete deployables/README.md and QUICKSTART.md (redirect stubs, docs already
  point to examples/)
- Move collector.sh from deployables/assets/ to examples/shared/scripts/
- Update all references to collector.sh path in docs, tests, and examples
- Update .dockerignore to exclude examples/ instead of deployables/ and configs/
- deployables/ directory is now fully removed from the repository ([f9136cf](https://github.com/waliaabhishek/chitragupta/commit/f9136cf89c8a7eab1f8a5920c6961cdc11673c3c))
- Maintenance: TASK-118 — Upgrade React to v19 with dependency cascade

Major version upgrades: react/react-dom 18→19, @refinedev/core 4→5,
@refinedev/antd 5→6, react-router-dom 6 → react-router 7,
@types/react 18→19. Removes forwardRef from ChargebackGrid (React 19
deprecation), migrates JSX.Element → React.JSX.Element (global JSX
namespace removed), updates Refine pagination API (current → currentPage),
removes react-router v7 future flags from test MemoryRouter usage.

All 258 tests pass, 92.77% coverage, typecheck and build clean. ([69c5969](https://github.com/waliaabhishek/chitragupta/commit/69c5969811f992e749032c0fa7b4b70ffdccf707))
- Maintenance: TASK-117 — Upgrade jsdom to v28

Upgrade jsdom 25→28 (3 major versions). All 258 tests pass,
coverage thresholds met, build and typecheck clean.

eslint 10 upgrade deferred — eslint-plugin-react-hooks has no
stable release declaring eslint 10 peer dep support. ([4206b80](https://github.com/waliaabhishek/chitragupta/commit/4206b8076b3ff7a2e0fb6996c6af7520bb5e403f))
- Maintenance: TASK-116 — Upgrade vite/vitest/plugin-react to latest majors

Upgrade vite 5→8 (Rolldown), vitest 2→4, @vitejs/plugin-react 4→6,
@vitest/coverage-v8 2→4. Migrate custom vitest environment to v4 API
(vitest/runtime imports, viteEnvironment). Fix pre-existing TS errors
in dashboard and TenantContext test mocks. ([5200739](https://github.com/waliaabhishek/chitragupta/commit/520073915c350ecf8330c1bc4ace8fa17e8e1328))
- Maintenance: TASK-113 — Upgrade backend Python dependencies

fastapi 0.133.0→0.135.1, cachetools 7.0.3→7.0.5, ruff 0.15.2→0.15.6,
sqlalchemy 2.0.46→2.0.48, plus transitive dependency updates.
All 2364 tests pass, ruff and mypy clean. ([910d535](https://github.com/waliaabhishek/chitragupta/commit/910d53523ed9d48f63d38f41edd04c77fd184a56))
- Cleanup: TASK-112 — Remove duplicate LOGGER definitions and dead helper functions

Remove uppercase LOGGER duplicates from 3 files (crn.py, connector_identity.py,
connections.py), replacing call sites with the standard lowercase logger convention.
Delete unused allocate_to_owner and allocate_to_resource from helpers.py (superseded
by DirectOwnerModel/TerminalModel). Update test fixtures accordingly. ([0968c34](https://github.com/waliaabhishek/chitragupta/commit/0968c34ea0e28fccd2548dda67d87d66f2694960))
- Add details about Log Format changing capability ([0531fbd](https://github.com/waliaabhishek/chitragupta/commit/0531fbdb73a318ac6856c04f35dcde45f94aae31))
- Other stuff ([b674808](https://github.com/waliaabhishek/chitragupta/commit/b674808f6d208b8cfed679901f94a4d7f4d97665))
- Fix slow shutdown: drain() now signals shutdown event before waiting

In 'both' mode, uvicorn's lifespan teardown called drain() which
passively waited for running tenants to finish. But shutdown_event
was only set after run_api() returned — creating a circular dependency
where drain waited for pipelines that didn't know they should stop. ([93001be](https://github.com/waliaabhishek/chitragupta/commit/93001be37d99ba03f91a631a0069772a6b2e0708))
- Fix pipeline run persistence and test expectations

_run_pipeline now always persists workflow_runner results to the
PipelineRun DB record (status, dates_gathered, dates_calculated,
rows_written, error_message) and sets status=skipped when
already_running. trigger_pipeline always creates a PipelineRun
regardless of whether a workflow_runner is present, so every
trigger has a trackable record. Corrected test expectations:
no-runner case is status=failed (not completed), and the
capture_run_api mock now accepts the mode kwarg. ([4a8e691](https://github.com/waliaabhishek/chitragupta/commit/4a8e69130e4370936831b478791f8bcd9fa1da8c))
- Add application lifecycle layer with pipeline status tracking and readiness endpoint

The backend was built as a headless batch processor with API and frontend
bolted on afterward. Neither side understood the application's lifecycle
state — on first startup the UI showed empty charts identical to a broken
system, and during pipeline runs the UI allowed writes that could conflict.

This adds:
- Explicit pipeline stage tracking (stage + current_date fields on PipelineRun)
- PipelineRunTracker class managing run lifecycle (create, progress, finalize, fail)
- Orphaned run cleanup on startup (handles process crash mid-pipeline)
- GET /api/v1/readiness endpoint with per-tenant status
- Frontend readiness-first initialization with polling
- Persistent pipeline status banner showing live stage and date
- Read-only mode during pipeline activity (disables write operations)
- Alembic migration 007 for new columns ([bc22c01](https://github.com/waliaabhishek/chitragupta/commit/bc22c019e367488373ddbfa4ff1470046d6fee89))
- Style: apply ruff formatting and remove unused import ([e9ae42a](https://github.com/waliaabhishek/chitragupta/commit/e9ae42a17822ae24125c5ca0c3d8027bc9e0a0a6))
- Set explicit image names in docker-compose

Removes 'deployables-' prefix from built images ([58aec77](https://github.com/waliaabhishek/chitragupta/commit/58aec77cea7589d07cf81ad97c056e99c7185c88))
- Add docker-push target for multi-arch builds

- Builds linux/amd64 + linux/arm64 images
- Pushes to registry (default docker.io, override with REGISTRY=)
- Local docker-build unchanged (single arch, no push) ([93d44eb](https://github.com/waliaabhishek/chitragupta/commit/93d44ebfa7e545afc858f204de139729751a2f1a))
- Fix frontend TypeScript errors

- Remove invalid style prop from AgGridReact (goes on wrapper div)
- Fix htmlType type in test mock to match button type union
- Remove unused setSearchParams destructure
- Add missing resource_id to TagWithDimensionResponse mocks ([b9ffd03](https://github.com/waliaabhishek/chitragupta/commit/b9ffd03d91bfb38c8a217f6e5f27309fc4c2b62b))
- Fix Dockerfile: copy uv from official image, fix PYTHONPATH

- Use ghcr.io/astral-sh/uv image instead of install script (slim has no curl)
- Fix PYTHONPATH to /app/src where modules actually live
- Fix entrypoint to 'python -m main' (not src.main) ([d5223a8](https://github.com/waliaabhishek/chitragupta/commit/d5223a8e2fa3db82d4efd53f9739e4ead715750c))
- Add Docker make commands for local development

- docker-build: Force rebuild all images (--no-cache)
- docker-up: Start backend + grafana
- docker-dev: Start all services including frontend
- docker-dev-ui: Backend + frontend only (skip grafana)
- docker-down: Stop all services
- docker-logs: Tail logs ([203a73b](https://github.com/waliaabhishek/chitragupta/commit/203a73b14102b307ed776b506750e292bde74c52))
- Perf: Cut test suite runtime from 235s to 18s

SMK plugin tests called plugin.initialize() with identity_source
defaulting to "prometheus", triggering _validate_principal_label()
which hit a real Prometheus endpoint with 4-retry exponential backoff
(~15s per test, ~140s total). Fixed by setting identity_source=static
in base_settings fixtures — tests that specifically exercise the
prometheus validation path already override this explicitly.

CCloud connection retry tests slept through real backoff due to a
1.0s floor guard in _get_rate_limit_wait plus additive random jitter
in _calculate_backoff (~6-7s for max-retries test, ~1.2s for
rate-limit header tests). Added autouse fixture in conftest patching
time.sleep in the connections module. Replaced wall-clock elapsed
assertions with mock call_count checks for the throttling tests. ([05291b8](https://github.com/waliaabhishek/chitragupta/commit/05291b83b9fdf578659df0ca3a39603bf63e9f4f))
- Fix dev targets: add PYTHONPATH and use npx vite ([9cc5d41](https://github.com/waliaabhishek/chitragupta/commit/9cc5d413064d9b538a19ad3d285d126104285950))
- Add dev targets for running backend and frontend together

- make dev: API + worker + frontend
- make dev-api: API + worker only
- make dev-ui: API only + frontend (for UI development) ([701cd22](https://github.com/waliaabhishek/chitragupta/commit/701cd22378be49ee513b1acfb235816deba7695f))
- Add per-date progress logging during chargeback calculation

Logs start/end of each billing date with row count and elapsed time,
matching the reference codebase behavior for tracking calculation progress. ([c237b1a](https://github.com/waliaabhishek/chitragupta/commit/c237b1a3f7d5096e88eaf9119d1b93f6a71fc6db))
- Fix default plugins path to point to src/plugins

_DEFAULT_PLUGINS_PATH was pointing to <repo>/plugins/ which doesn't
exist. Plugins are in src/plugins/. Removes need for explicit
plugins_path config in every YAML file. ([20bf1fc](https://github.com/waliaabhishek/chitragupta/commit/20bf1fcffd2d0e23c8a678d1b91d5b0c436eeed1))
- Fix logging disabled by alembic and CCloud API page_size error

- env.py: Add disable_existing_loggers=False to fileConfig() call
  Alembic's logging config was silently disabling all existing loggers,
  causing tenant errors to not be displayed after migrations ran.

- connections.py: Reduce DEFAULT_PAGE_SIZE from 500 to 99
  CCloud API requires page_size < 100. Endpoints without explicit
  page_size override were failing with 400 error. ([35a6dd9](https://github.com/waliaabhishek/chitragupta/commit/35a6dd95998e9ff52086a501c86ad2a4784d4ffe))
- Fix broken example configs to match actual plugin schemas

README quickstart referenced non-existent config.yaml. Example configs
used wrong field names (flat api_key vs nested ccloud_api.key) and
non-existent fields for self-managed plugin. ([ace66f3](https://github.com/waliaabhishek/chitragupta/commit/ace66f300c994084dfa84f32fac3c16948ab5b72))
- Rename project to Chitragupt, optimize Dockerfile, fix lint issues

- Rename package from chargeback-engine to chitragupt
- Update all references in docs, configs, and code
- Optimize Dockerfile: use uv standalone installer, remove uv from runtime
- Fix 45 ruff lint issues in test files (unused imports, long lines, import order) ([2c565ac](https://github.com/waliaabhishek/chitragupta/commit/2c565ac86a17371651ab5368cbc7c2006d7bb406))
- Add Makefile with common dev commands ([f5e6295](https://github.com/waliaabhishek/chitragupta/commit/f5e629545dcd1b5f060c8a8d49553e04c6aa950a))
- Ignore MkDocs build output (site/) ([aa1d1f2](https://github.com/waliaabhishek/chitragupta/commit/aa1d1f2af473c881180720096a2ab7454a6d7fad))
- 5.1: Tech Debt Phase 2

Resolves TD-019, TD-021, TD-031, TD-034, TD-035, TD-037.

- TD-031/034/035: Add AllocationDetail StrEnum with standardized reason codes
  for allocation decisions. Update helpers and all allocators to use enum.

- TD-019: Migrate from requests to httpx for thread-safe HTTP clients.
  Update CCloudConnection and PrometheusMetricsSource.

- TD-037: Add SQL-level pagination to find_active_at/find_by_period.
  Return (list, total_count) tuples with filter params and LIMIT/OFFSET.

- TD-021: Add TenantRuntime caching in WorkflowRunner with health checks,
  config change detection, and proper lifecycle management.

1123 tests, 98.17% coverage. ([f92e1e7](https://github.com/waliaabhishek/chitragupta/commit/f92e1e7d4f49fb2d09784a43c8f20553b3c04b76))
- 5.0: Tech Debt Cleanup

Resolves 12 tech debt items:
- TD-008: Document step_seconds fallback
- TD-010: HTTP connection pooling in Prometheus
- TD-016: Data retention cleanup in WorkflowRunner
- TD-018/TD-024: Session lifecycle cleanup (plugin.close())
- TD-023: Orchestrator test invariants
- TD-029: Handler gather_identities tests
- TD-032: FlinkContextDict TypedDict
- TD-033: Flink statement name collision fix
- TD-036: Flink region skip logging
- TD-038: Pipeline run state persistence (PipelineRunTable)
- TD-039: Single-tenant pipeline trigger (run_tenant)
- TD-040: OpenAPI TypeScript generation setup
- TD-041: .nvmrc for Node 22 ([493d5b6](https://github.com/waliaabhishek/chitragupta/commit/493d5b6e09e2725aac356ce116cad65c14d9c77c))
- 4.4: Tag Management + Export

Backend:
- GET/PATCH/DELETE /tags endpoints with search, pagination
- POST /tags/bulk for bulk tagging by dimension IDs
- POST /tags/bulk-by-filter for bulk tagging by filter criteria
- Migration 003: add display_name column + UNIQUE(dimension_id, tag_key)
- Tag model: tag_key (immutable), tag_value (auto-UUID), display_name (mutable)
- find_by_filters() now overlays custom tag display_names onto ChargebackRow.tags

Frontend:
- TagManagementPage (/tags) with search, inline edit, delete w/ Popconfirm
- BulkTagModal for bulk tagging (by IDs or by filter)
- ExportButton for CSV export
- SelectionToolbar shows when rows selected
- ChargebackGrid with row selection checkboxes
- TagEditor simplified to 2-field form (Key + Display Name)

Tests: 1077 backend (97.9% cov), 135 frontend (86.9% func cov)
QA Rounds: 3 ([541eee3](https://github.com/waliaabhishek/chitragupta/commit/541eee3c99b47f5dea868d47e33c5eb0e870c01f))
- 4.3: Cost Dashboards with polish (error/retry + chart toggle)

Backend:
- Aggregation endpoint filter params (identity_id, product_type, resource_id, cost_type)
- Repository aggregate() with optional filter WHERE clauses

Frontend:
- 4 ECharts components (CostTrendChart, CostByIdentityChart, CostByProductChart, CostByResourceChart)
- useAggregation hook with refetch
- ChartCard with error/onRetry props
- ProductChartTypeToggle (Segmented pie/treemap)
- Dashboard page with 2x2 grid, filter sync, time bucket selector
- DRY optimization: trendData reused for identity chart ([2b6671f](https://github.com/waliaabhishek/chitragupta/commit/2b6671fdcd3c454e815c805d4b59e57d1adb094c))
- 4.2: Chargeback Explorer with AG Grid + tag editing

Backend:
- Add dimension_id to ChargebackRow and ChargebackResponse
- Add GET /chargebacks/{dimension_id} endpoint
- Fix route order (aggregate before dynamic path)
- Fix date.today() -> datetime.now(UTC).date()

Frontend:
- AG Grid with infinite scroll (100k+ rows)
- FilterPanel with URL-synced state
- ChargebackDetailDrawer with tag editing
- TagEditor component
- 59 tests, 95% coverage ([6876280](https://github.com/waliaabhishek/chitragupta/commit/6876280b4ff8fe9b3bfd599747fe3cd44cdad083))
- 4.1: Frontend scaffold with Refine.dev + Ant Design

- Custom tenant-scoped data provider for FastAPI backend
- TenantContext with localStorage persistence and retry
- Ant Design Layout with Sider/Header/Content
- Disabled menu items when no tenant selected
- 5 placeholder pages with tenant checks
- MSW test infrastructure (20 tests, 98% coverage)
- Vite dev proxy to backend ([ff7d794](https://github.com/waliaabhishek/chitragupta/commit/ff7d7946f0e9e99cbfe34a5fdf9a3260eca63a6f))
- 3.5: Docker deployment + CLI polish

- Dockerfile: Python 3.14, uv 0.10.6, multi-stage, non-root user
- docker-compose.yml: engine + grafana with healthcheck + depends_on
- Docker-ready configs: config-ccloud.yaml, config-self-managed.yaml
- .env.example with credential templates
- Updated datasource.yml for directory mount
- Comprehensive README.md deployment guide

Phase 3 complete. ([e128108](https://github.com/waliaabhishek/chitragupta/commit/e128108c34e68b16f67bbcecb6263d332dd97759))
- 3.4: Self-Managed Kafka Plugin

Implements metrics-only chargeback paradigm where costs are
constructed from YAML pricing model × Prometheus usage metrics
rather than fetched from a billing API.

- SelfManagedKafkaPlugin with dependency injection pattern
- ConstructedCostInput generates BillingLineItems from infra costs
- SelfManagedKafkaHandler handles all 4 product types
- Per-product-type allocators (COMPUTE/STORAGE even, NETWORK usage-ratio)
- Prometheus + Admin API resource/identity discovery
- kafka-python as optional dependency
- 132 tests, 100% coverage on new module ([7ed3030](https://github.com/waliaabhishek/chitragupta/commit/7ed303072544fb27312a93d937e7243414f0cac6))
- 3.3: Grafana dashboards + Docker Compose

- Docker Compose with Grafana + SQLite datasource plugin
- chargeback_overview.json: 10 panels (stats, pie, time series)
- chargeback_details.json: 6 table panels with pagination
- Template variables with cascade queries (:sqlstring for IN clauses)
- README.md with setup instructions ([713bc4f](https://github.com/waliaabhishek/chitragupta/commit/713bc4fe5cb73c2b84b10a5acdb07dde1907df68))
- 3.2: FastAPI write endpoints + aggregation

- PATCH /chargebacks/{dimension_id} for tag management (replace/add/remove)
- Tags CRUD routes (GET/POST/DELETE)
- Pipeline trigger/status with WorkflowRunner.run_once() integration
- Server-side aggregation (multi-dimension GROUP BY, time bucketing)
- CSV export with filters and streaming
- Repository extensions: get_dimension, aggregate, get_tag, find_tags_for_tenant
- Schemas: Tag*, Pipeline*, Aggregation*, Export* models
- 91 new tests (895 total), 98% coverage ([1c7d417](https://github.com/waliaabhishek/chitragupta/commit/1c7d417404c9f6cd54af0b4203851f0b4cb40b82))
- 3.1: FastAPI core + read endpoints

- FastAPI factory with lifespan (shared backend caching, disposal)
- 6 read endpoints: tenants, billing, chargebacks, resources, identities, health
- Temporal query support (active_at vs period_start/period_end)
- Database-level pagination with LIMIT/OFFSET
- Datetime validation (reject naive datetimes)
- ApiConfig extensions (enable_cors, cors_origins, request_timeout_seconds)
- main.py --mode api|worker|both
- 63 API tests, 98% coverage ([0b803d6](https://github.com/waliaabhishek/chitragupta/commit/0b803d63d78e960534184d935e92f34efd2f8775))
- Style: ruff format ([2232aaa](https://github.com/waliaabhishek/chitragupta/commit/2232aaa17579d41a5e6cce56e69e85c0468d7379))
- 2.1: post-implementation hardening

- Add connection pooling via requests.Session
- Add close() method for session cleanup
- Fix rate limit headers per Confluent Cloud API docs:
  use rateLimit-reset (relative seconds) not X-RateLimit-Reset
- Add test coverage for RateLimit-Reset header variant
- Module-level imports in test_connections.py
- Remove dead code (unreachable raise)
- Improve test_connection_close with mock assertion ([d092874](https://github.com/waliaabhishek/chitragupta/commit/d09287493f19a204bde570937dea140effdd8697))
- Added memory folder to ignored ([abe6bcc](https://github.com/waliaabhishek/chitragupta/commit/abe6bccccd2f164ff7a544cd869a9faa5f7d7239))
- 1.7: Pipeline orchestrator + workflow runner

- ChargebackOrchestrator: gather→calculate pipeline with UTC validation,
  UNALLOCATED identity fallback, zero-gather protection, allocation retry
- WorkflowRunner: concurrent tenant execution with per-tenant timeout
- main.py: CLI entry point with --config-file, --env-file, --run-once
- Storage: mark_resources_gathered, mark_needs_recalculation,
  increment_allocation_attempts, allocation_attempts column
- Config: TenantConfig fields (allocation_retry_limit, max_dates_per_run,
  zero_gather_deletion_threshold, tenant_execution_timeout_seconds),
  LoggingConfig.per_module_levels
- 381 tests, 96% coverage (62 new tests)

Phase 1 Core Framework complete. ([d0df416](https://github.com/waliaabhishek/chitragupta/commit/d0df4164b610a615fc6e094daf0097f0b210133f))
- 1.6: Metrics layer — MetricsSource protocol, PrometheusMetricsSource

Thread-safe TTL cache, retry with backoff + jitter, parallel query
execution via ThreadPoolExecutor, basic/digest/bearer auth, bounded
cache eviction. 44 tests, 100% coverage on metrics modules. ([bd0ab91](https://github.com/waliaabhishek/chitragupta/commit/bd0ab91aec0db9bb3ef6b535ef89ff8351529150))
- 1.5: Allocation engine — helpers, registry with overrides, dynamic loader

AllocationContext/AllocationResult dataclasses, AllocatorRegistry with
two-tier override support, 6 allocation helpers (usage_ratio, evenly,
hybrid, to_owner, to_resource, active_fraction), split_amount_evenly,
load_protocol_callable with signature validation for customer
extensibility. Resolves TD-001/TD-002. ([1d71fd8](https://github.com/waliaabhishek/chitragupta/commit/1d71fd832f66a1f09db06b59658c71a5dee2b9d1))
- 1.4: Storage layer — schema, repositories, mappers, migrations

SQLModel tables (7), per-entity repository protocols (6), stateless
domain↔ORM mappers, UnitOfWork protocol, SQLModelBackend with engine
cache, Alembic baseline migration. Temporal queries (find_active_at,
find_by_period), star-schema chargebacks, Decimal-as-string for SQLite.

PipelineState + CustomTag domain models. TD-003 resolved (UnitOfWork
import). TD-004/005 resolved (Alembic warning, ResourceWarning).

89 storage tests, 210 total, 95% coverage, 3 QA rounds. ([9ce3382](https://github.com/waliaabhishek/chitragupta/commit/9ce3382cd521d67344b956f5b6ae0a86eea2c510))
- 1.3: Configuration system — YAML loader, Pydantic models, env substitution ([838f121](https://github.com/waliaabhishek/chitragupta/commit/838f1211b4ac6aaecad6dd9123d8b1a6f8787dcb))
- 1.2: Plugin protocols, registry, and loader

4 runtime_checkable protocols (CostAllocator, CostInput, ServiceHandler,
EcosystemPlugin), factory-based PluginRegistry, EcosystemBundle with
post-init product_type indexing, and discover_plugins() loader with
structural validation. 26 tests, 100% coverage. ([4bc2e66](https://github.com/waliaabhishek/chitragupta/commit/4bc2e667b8f57f5b5f3715c4e79c9f01da1a21d7))
- Flatten project structure to repo root

Move pyproject.toml, src/, tests/ from chargeback-engine/ subdirectory
to repo root. Standard Python project layout. Update .gitignore for
Python artifacts. Add uv.lock. ([2a0b1c4](https://github.com/waliaabhishek/chitragupta/commit/2a0b1c4206b5e3c77f00f0a87a2578423617b3d6))
- 1.1: Project scaffold + domain models

Core domain models: Resource, ResourceStatus, Identity, IdentitySet,
IdentityResolution, BillingLineItem, ChargebackRow, CostType,
MetricQuery, MetricRow. Pure dataclasses, frozen where immutability
matters. 49 tests, 100% coverage. ([8beaf76](https://github.com/waliaabhishek/chitragupta/commit/8beaf7610c0a058b0251c9ad427e74a026ae1742))
- Initial project setup: .gitignore

Git exclusion rules for chargeback-engine.
Excludes backlog/, .claude/, ccloud-chargeback-helper-reference/, CLAUDE.md from git. ([1d145b7](https://github.com/waliaabhishek/chitragupta/commit/1d145b79dd7eb5a27d165b8bec3d40072d6eb56a))


### Documentation
- Docs: Add v2 migration notice and former project name to README

Updated README.md to reflect v2 as a complete rewrite with plugin
architecture, multi-tenancy, FastAPI, and proper storage layer.
Added subtitle showing former name (ccloud-chargeback-helper) for
discoverability. Reorganized features into "New Features" and
"Breaking Changes from V1" sections. Simplified docs section to
point to website instead of listing individual doc pages. ([eb1a316](https://github.com/waliaabhishek/chitragupta/commit/eb1a316842d8f693e877276f3e7b3f9e1fc8533f))
- Docs: Wipe CHANGELOG.md for v2 clean slate

Old changelog contained ~282 commit URLs pointing to the pre-rename
repo name. Since v2 is a ground-up rewrite, a clean starting point
is more appropriate than rewriting history. cliff.toml already
generates correct URLs for new entries.

Closes TASK-152. ([e6526d4](https://github.com/waliaabhishek/chitragupta/commit/e6526d45e003daae06195c3d6f9ed2ad3e29d52f))
- Docs: Make Docker Compose the primary quickstart path

Rewrite quickstart to lead with Docker Compose instead of Python/uv. The full
walkthrough now goes: create CCloud service account → get API key → docker
compose up. Python/uv is documented as an alternative at the bottom.

- Remove all GitHub URL links from docs — use relative links to stay within
  the documentation site
- Inline Docker Compose instructions in deployment.md instead of linking out
- Update root README quick start to show docker compose first
- Update prerequisites to list Docker as primary runtime
- Update getting-started index description ([356a867](https://github.com/waliaabhishek/chitragupta/commit/356a867f31b03b12f389a6b788e1b1372a927620))
- Docs: Add configuration guide, cost model explainer, and fix doc build

New pages:
- Configuration Guide: narrative walkthrough of building a config with
  decision points, tradeoffs, and worked examples for all 3 ecosystems
- How Costs Work: complete cost lifecycle from billing data through
  allocation with exact math, rounding guarantees, and the fallback chain

Enhanced existing references:
- Added "why" columns to product type and allocation strategy tables
- Added decision guidance callouts to all config reference pages
- Expanded identity discovery and fallback behavior documentation
- Added "when to change" guidance to tuning parameters

Fixes:
- Converted 4 broken relative links (deployables/, CHANGELOG.md) to
  absolute GitHub URLs so mkdocs strict build passes
- Added api-reference.md and operations/upgrading.md to nav (were orphaned)
- Replaced deprecated pymdownx.slugs.uslugify with configurable slugify ([32a6098](https://github.com/waliaabhishek/chitragupta/commit/32a6098fd5089162b983f864ebece6a453c3185d))
- Docs: Add status badges to README and Codecov integration to CI

Add CI, coverage, Python, Ruff, mypy, and uv badges to README.
Add Codecov upload step to CI workflow for coverage reporting. ([f9aabac](https://github.com/waliaabhishek/chitragupta/commit/f9aabac4029cf983ed0c520b0baa866b2e157eb7))
- Docs: Comprehensive documentation audit and fixes across all doc areas

- Create API reference (docs/api-reference.md) covering all 25 REST endpoints
- Fix architecture docs: correct storage table names, add phase objects,
  deletion detection step, pipeline_state tracking, StorageModule protocol
- Fix config docs: correct Flink allocation strategy, add missing connector
  product types, self-managed product types, SASL credentials, undocumented
  fields, cross-field validation constraints, tuning parameters
- Fix ops docs: remove non-functional LOG_LEVEL env var, add Grafana
  multi-tenant warning, fix health/pipeline response schemas, add readiness
  endpoint, fix Dockerfile example
- Fix getting-started: add .env auto-discovery, env var limitation warning,
  clarify emitter availability ([a66fe1d](https://github.com/waliaabhishek/chitragupta/commit/a66fe1d956eebacf88783a60c5e9401dc77be078))
- Docs: Merge CCloud prerequisites into quickstart for single-page setup guide

Fold service account creation, permissions, and API key setup into
the quickstart so users don't need to hop between pages. Add
architecture overview section to README. ([01f300f](https://github.com/waliaabhishek/chitragupta/commit/01f300f6e5175078f2badd0d0456761e3d270d27))
- Docs: TASK-109 — Add PostgreSQL connection string examples to config and docs

Add commented-out PostgreSQL examples to ccloud-complete.yaml and
self-managed-complete.yaml. Expand deployment.md storage section with
driver requirements, connection string format, one-database-per-tenant
rule, env var usage, and SQLite-vs-PostgreSQL comparison table. ([15c9c88](https://github.com/waliaabhishek/chitragupta/commit/15c9c88f695233ee74f6a50f3091b1e93691690f))
- Docs: Add CCloud RBAC permissions and service account setup to prerequisites ([cb5c371](https://github.com/waliaabhishek/chitragupta/commit/cb5c3711ec012743966f7da2089c1680835d7971))
- Docs: TASK-108 — Add orchestration override examples to config examples

Add commented-out examples of allocator_overrides, identity_resolution_overrides,
and allocator_params to self-managed-complete.yaml, generic-postgres.yaml, and
generic-redis.yaml so users can discover these customization hooks without reading
source code. ([af63bee](https://github.com/waliaabhishek/chitragupta/commit/af63bee25c37afaac226de38d5dc78a1ae06fe31))
- Docs: TASK-105 — Add upgrade and migration guide to operations documentation

Covers backup procedures (SQLite/PostgreSQL), upgrade steps for Docker
and source-based deployments, auto-migration behavior, rollback, and
breaking changes policy. Linked from deployment doc and operations index. ([5b5b7a7](https://github.com/waliaabhishek/chitragupta/commit/5b5b7a7dfb899a215582753f26ab7fd63ed23798))
- Docs: TASK-103 — Add CHANGELOG and release notes mechanism

Add git-cliff-powered changelog generation with Keep a Changelog format.
Combined release + docs workflow replaces docs.yml: tag push triggers
changelog-based GitHub Release creation then versioned MkDocs deployment.
Includes CONTRIBUTING.md with commit conventions and release process. ([e1af479](https://github.com/waliaabhishek/chitragupta/commit/e1af4792f01d8ed6d066127280b08ebe4b375fd6))
- Docs: Add project name origin and extended description to README ([1ba7ec1](https://github.com/waliaabhishek/chitragupta/commit/1ba7ec1fc53b956ac2fb8f876d262223e812e20d))
- Docs: TASK-102 — Add Docker-based quickstart guide to deployables/

Step-by-step guide for running the full stack with Docker Compose,
covering config selection, credentials, UI profile, smoke tests,
and teardown. Linked from root README. ([554bf3b](https://github.com/waliaabhishek/chitragupta/commit/554bf3b258ad65b8c824be8ee354776ea696e1c6))
- Docs: Open external links in new tab ([00e64da](https://github.com/waliaabhishek/chitragupta/commit/00e64da4bca5c8570e3518c41edf3f9bb23f3cc1))
- Docs: Add pipeline flow diagram to data-flow.md ([afacb06](https://github.com/waliaabhishek/chitragupta/commit/afacb06e7e6854c8ba8537428a6bd28f9efd0cde))
- Docs: Fix ccloud-reference.md to match actual implementation ([fcfc7e7](https://github.com/waliaabhishek/chitragupta/commit/fcfc7e7e1a2016e7dcf605146160336fd1d4850a))
- Docs: Add README.md and fix mkdocs anchor slugify

- Add root README with quick start, features, and doc links
- Add slugify setting to mkdocs.yml for consistent anchor generation ([9dfe76e](https://github.com/waliaabhishek/chitragupta/commit/9dfe76ed96baec8af6147899ac2eded59265cdb1))
- Docs: TASK-029 — Comprehensive user documentation infrastructure

Add complete MkDocs-based documentation:
- mkdocs.yml with Material theme, mermaid, versioning via mike
- .github/workflows/docs.yml for tag-triggered deployment
- 20 markdown files covering getting-started, configuration,
  architecture, and operations
- pyproject.toml docs dependency group (mkdocs-material, mike) ([da4809f](https://github.com/waliaabhishek/chitragupta/commit/da4809f6f58f1c01e73850e43e62c3cb8d017281))


### Fixed
- Fix: Scope billing pipeline_state filter to billing panels only

Previous commit applied filter too broadly — chargeback panels broke
because replace_all matched their timestamp pattern too. Reverted and
re-applied targeting only billing UNION ALL queries via b.tenant_id. ([d950f5c](https://github.com/waliaabhishek/chitragupta/commit/d950f5cbc08bba0719d7345f5c553b0314ec58c4))
- Fix: Billing panels show only dates with chargeback data

Filter all billing queries against pipeline_state.chargeback_calculated
so billing totals don't exceed chargeback totals when processing is
still catching up. Prevents misleading cost gaps in dashboards. ([e9042d3](https://github.com/waliaabhishek/chitragupta/commit/e9042d36a324b773158ddd3a172255c6fbc7bdcc))
- Fix: Cost per Resource panel — use resource_id instead of display_name

display_name is not unique and was mixing IDs with names due to COALESCE
fallback. Use resource_id directly and drop unnecessary LEFT JOIN. ([cbff0e9](https://github.com/waliaabhishek/chitragupta/commit/cbff0e9ff2f9f94a70a7709cbdbbe07c91eac9f6))
- Fix: CCloudBillingRepository.upsert() — idempotent replace instead of accumulate

upsert() incorrectly summed total_cost and quantity on re-ingestion,
inflating costs Nx per pipeline restart. Replaced accumulation branch
with unconditional session.merge() and added billing revision detection
warning log, matching the base BillingRepository pattern. ([6b6a7bd](https://github.com/waliaabhishek/chitragupta/commit/6b6a7bdfc795a35d6a626cbc1eac4a6e07b77e19))
- Fix: Cost Over Time — pivoted barchart with dates on X-axis

Timeseries drawStyle=bars produced thin illegible bars. Switch back
to barchart with pivoted query (CASE WHEN per product_category) so
each category is a named column. Dates on X-axis, stacked bars show
daily cost breakdown with readable legend names. ([39cf2f9](https://github.com/waliaabhishek/chitragupta/commit/39cf2f96f1c1e922a943c01f31f4609dab744383))
- Fix: Cost Over Time panel — switch to stacked bar timeseries

barchart panel was putting product_category on X-axis instead of
dates. Switch to timeseries panel with drawStyle=bars and stacking
to show daily cost bars stacked by product category. ([1d97a3b](https://github.com/waliaabhishek/chitragupta/commit/1d97a3bc6455f4ebfa36584fbcd3094a15388b14))
- Fix: Billing environment panel — use env_id instead of r.parent_id

Cost Split per Environment via Billing was joining through resources
table parent_id, showing lkc-* clusters and "No Environment". Now
uses env_id directly from ccloud_billing ('' AS env_id for base
billing table), matching the chargeback panel fix. ([6cd83a7](https://github.com/waliaabhishek/chitragupta/commit/6cd83a78af8f0bf3b3881d94007a7b1f8cbf2723))
- Fix: Resource pie chart — consolidate by display_name, use d.env_id for env filter

GROUP BY display_name instead of resource_id so same-named resources
(e.g. Stream Governance across environments) merge into one slice.
Also switched environment filter from r.parent_id to d.env_id. ([39a5872](https://github.com/waliaabhishek/chitragupta/commit/39a5872e9e74e66c2dc67902b0f09861e2f98337))
- Fix: Environment pie chart — use d.env_id instead of r.parent_id

parent_id traversal was incorrect — connectors have parent_id=lkc-*
(cluster), not env-*, causing clusters to appear as environments.
Use the denormalized env_id column added in migration 009 for this purpose.
Empty env_id now labeled "Org-level" instead of "No Environment". ([8bb567e](https://github.com/waliaabhishek/chitragupta/commit/8bb567e03d525d9e710d237279f30483881756a6))
- Fix: Correct treemap plugin ID, dynamic cost-type stat panel

- Fix treemap type from "treemap" to "marcusolsson-treemap-panel" (was breaking)
- Remove invalid treemap options (showValue, custom.hideFrom)
- Replace hardcoded Usage/Shared cost panels with single dynamic "Cost by Type" panel
  that auto-discovers cost types via GROUP BY
- Fix calcs on stat panel (unused when values: true) ([6cd9d6b](https://github.com/waliaabhishek/chitragupta/commit/6cd9d6be720059f86ec0e74d6bb5f86a6165a924))
- Fix: task-147 — Fix plugin loader to actually use plugins_path for external plugin discovery

discover_plugins() hardcoded f"plugins.{entry.name}" for imports, making external
plugin directories (via plugins_path config) scan correctly but fail on import.
Extract _import_plugin_module() helper that routes built-in plugins through
importlib.import_module (when parent is on sys.path) and external plugins through
spec_from_file_location (file-based import, no sys.path mutation). Missing
__init__.py now raises ImportError with actionable message. ([c5fd57d](https://github.com/waliaabhishek/chitragupta/commit/c5fd57d63b3a78f1f16df95b12e9618149ea335e))
- Fix: task-148 — Decouple emitters from pipeline loop, make them independent DB readers

Remove EmitPhase, _EmitterEntry, EmitResult, _load_emitters, _aggregate_rows,
_Bucket, and _GRANULARITY_ORDER from orchestrator. Pipeline loop is now
gather → calculate → commit only.

Add EmitterRunner as independent post-pipeline component that reads chargebacks
from DB, drives configured emitters via PerDateDriver/LifecycleDriver, and
persists per-tenant/emitter/date emission state (EmissionRecord table).

Add --emit-once CLI flag for standalone re-emission without pipeline run.
Add EmitterSpec.name and lookback_days config fields. Add Alembic migration 010.
Wire EmitterRunner in WorkflowRunner._run_tenant as post-pipeline hook. ([841963c](https://github.com/waliaabhishek/chitragupta/commit/841963c9f33bdde9b6404568682ab76be2e34b87))
- Fix: task-146 — Remove ecosystem_name from generic_metrics_only plugin, hardcode ecosystem like CCloud/SMK

The generic_metrics_only plugin exposed a configurable ecosystem_name field
used as the data partition key in billing lines. This broke the orchestrator
contract: ecosystem should be the hardcoded plugin selector, not user-configured.
The mismatch caused find_by_date() to return zero rows — silent data loss.

Removed ecosystem_name from GenericMetricsOnlyConfig and hardcoded
"generic_metrics_only" in all 5 emission sites (plugin property, build_shared_context,
CoreBillingLineItem, handler identity construction, log message). Added ECOSYSTEM
module constant in cost_input.py matching peer plugin pattern. Updated docs and
changelog. 95% test coverage with full data flow integration test. ([1b32756](https://github.com/waliaabhishek/chitragupta/commit/1b32756f80c0e9b4de33f1bd4d1c93fc9d5e3563))
- Fix: task-145 — Reject pipeline trigger in API-only mode, prevent double PipelineRun records

In API-only mode (workflow_runner=None), trigger_pipeline now returns 400
immediately without creating a PipelineRun record. In both mode, the
endpoint no longer creates its own PipelineRun — lifecycle is fully owned
by WorkflowRunner._run_tenant() via PipelineRunTracker, eliminating
duplicate records. _run_pipeline simplified to thin async wrapper with
logging only. ([e819e59](https://github.com/waliaabhishek/chitragupta/commit/e819e59bef87ecbb729bb71166f31f528a3b8cf1))
- Fix: task-144 — Convert StorageConfig.connection_string to SecretStr and mask secrets in --show-config

Prevent two secret leak paths: (1) connection_string with embedded DB passwords
now uses Pydantic SecretStr, masked as ********** in all serialization; (2) --show-config
excludes plugin_settings to prevent raw API key dumps. Engine log output stripped of
credentials via urlparse masking. ([b70a9e5](https://github.com/waliaabhishek/chitragupta/commit/b70a9e5c1608091642ea3e2be4b44b644ac89ff1))
- Fix: task-142 — Fix env_id in API response schema and allocation issue reporting

Add env_id field to ChargebackDimensionResponse and AllocationIssueResponse
API schemas, pass env_id through _build_dimension_response and
list_allocation_issues route, and include env_id in find_allocation_issues
GROUP BY to prevent cross-environment aggregation conflation. ([9c4f11e](https://github.com/waliaabhishek/chitragupta/commit/9c4f11ec1c70040944d0db1aad60da1a31ff13e0))
- Fix: task-141 — Fix env_id propagation gaps in 3 code paths

Three env_id propagation gaps discovered post task-140:

- _allocate_to_unallocated() now accepts metadata param and passes
  dimension_metadata so UNALLOCATED rows carry env_id
- chargeback_to_domain() reconstitutes env_id from dimension table
  into ChargebackRow.metadata on read-back
- ChargebackDimensionInfo gains env_id field, populated at both
  get_dimension() and get_dimensions_batch() construction sites ([2ec86b2](https://github.com/waliaabhishek/chitragupta/commit/2ec86b26b39ec05720256b02fa8b79d9207267bd))
- Fix: task-140 — Add env_id to chargeback dimensions via plugin-extensible chargeback repository

env_id from CCloud billing API was dropped during chargeback calculation,
causing environment_id aggregation to fail for 94% of resources (broken
resource table join). Now stored directly on chargeback_dimensions via
plugin-extensible ChargebackRepository pattern.

- StorageModule protocol gains create_chargeback_repository()
- UnitOfWork delegates chargebacks repo to plugin StorageModule
- AllocationContext.dimension_metadata propagates env_id from billing line
- CCloudChargebackRepository writes env_id to dimension table
- aggregate() uses native env_id column (resource join removed)
- Migration 009 adds env_id column and backfills from ccloud_billing ([006e8b5](https://github.com/waliaabhishek/chitragupta/commit/006e8b5d92c9d1e3cc2825b51a76d76be5b42831))
- Fix: Add latest tag to chitragupt-ui Docker image on release ([2af79bd](https://github.com/waliaabhishek/chitragupta/commit/2af79bd4126e4299d8d6db7c62b84bca23c49205))
- Fix: Update changelog test to allow git-cliff in docs workflow

The docs.yml workflow now legitimately uses git-cliff to generate
CHANGELOG.md before building docs. The test should only assert that
GitHub Release creation stays out of docs.yml. ([592ff12](https://github.com/waliaabhishek/chitragupta/commit/592ff12d332ae5c46b3d13497fde357b5cae53ba))
- Fix: Use GHCR image names in example Docker Compose files

Point all example compose files to ghcr.io/waliaabhishek/chitragupt
and ghcr.io/waliaabhishek/chitragupt-ui so users pull the published
images by default. ([13ca2ee](https://github.com/waliaabhishek/chitragupta/commit/13ca2eed839c8e9ada0092908b02f52fbbcf2c67))
- Fix: Generate changelog for both dev and versioned doc deploys

Tag deploys get full changelog (all releases). Main deploys get only
unreleased changes since last tag. ([9dadb03](https://github.com/waliaabhishek/chitragupta/commit/9dadb03b66d0761b7f817c0feeacb325a1dcbed0))
- Fix: Generate CHANGELOG.md in CI so docs changelog page has release entries

docs.yml now runs git-cliff before versioned deploys so the changelog
page includes all releases. release.yml pushes the generated CHANGELOG.md
back to main so the repo stays in sync. ([25a8fb3](https://github.com/waliaabhishek/chitragupta/commit/25a8fb3831e0106ecf32878f7ae8ef9ed840aa30))
- Fix: Include all config parameters in example configs

Example config.yaml files were missing most optional parameters — only showing
the bare minimum. Now every parameter from the config schema is documented
(commented out for optional ones) with explanations of what it does, valid
ranges, and defaults. Covers: logging (format, per_module_levels), features
(max_parallel_tenants), API (request_timeout_seconds), tenant tuning
(retention_days, allocation_retry_limit, zero_gather_deletion_threshold,
gather_failure_threshold, tenant_execution_timeout_seconds,
metrics_prefetch_workers), and plugin settings (billing_api, Prometheus auth
options, flink multi-region, allocator_params, allocator_overrides,
identity_resolution_overrides, metrics_step_seconds, chargeback_granularity,
min_refresh_gap_seconds, granularity_durations, emitters). ([b7ecb90](https://github.com/waliaabhishek/chitragupta/commit/b7ecb907c5931ab4e267f63389298930d0028372))
- Fix: Update docs to reference examples/ instead of deployables/QUICKSTART.md

Three documentation files still pointed to deployables/QUICKSTART.md after
the TASK-139 restructuring. Updated to link directly to examples/ directory. ([0240f48](https://github.com/waliaabhishek/chitragupta/commit/0240f48d92c27e82850b498ee7fb3a682699207d))
- Fix: TASK-139 — restructure deployables into self-contained example directories

Replace the monolithic deployables/ layout with three self-contained examples
under examples/, each runnable with a single `docker compose up`:

- ccloud-grafana: CCloud worker mode + Grafana (no API, no frontend)
- ccloud-full: CCloud full stack (API + worker + Grafana + UI)
- self-managed-full: Self-managed Kafka full stack

Shared Grafana provisioning assets moved to examples/shared/. Stale example
configs in deployables/config/examples/ removed (18 files). Makefile updated
with per-example targets and legacy aliases pointing to ccloud-full. ([5163bb1](https://github.com/waliaabhishek/chitragupta/commit/5163bb1beb2162501ffaa15094b7e27329e62c2f))
- Fix: TASK-138 — fail hard on missing CRN organization segment in Flink gathering

Replace silent tenant_id fallback with explicit ValueError when
parse_ccloud_crn() yields no organization key, preventing wrong
org_id from reaching the Flink Statements API. ([d78cf11](https://github.com/waliaabhishek/chitragupta/commit/d78cf11b9700e8cce44a071c5068e7c46ec53ad7))
- Fix: Clarify tenant_id is an internal partition key, not a CCloud org ID

tenant_id was misleadingly documented as the Confluent Cloud Organization ID
across configs, examples, .env files, and docs. It is actually an arbitrary
string used solely as a DB partition key — CCloud APIs are scoped by
credentials, not by org ID. Renamed env vars from CCLOUD_ORG_ID to
CCLOUD_TENANT_ID and updated all documentation and examples to prevent
user confusion. ([3827afd](https://github.com/waliaabhishek/chitragupta/commit/3827afddfda6adc7a7efae5ff0ce4ff1818ec783))
- Fix: Relax changelog test to allow CI-only releases with no section headers ([139b9b0](https://github.com/waliaabhishek/chitragupta/commit/139b9b07684c218efd4fddc109c700cd3425d9f4))
- Fix: TASK-132 — --validate flag now validates plugin-specific configs

--validate previously only checked top-level AppSettings, missing plugin-specific
validators (e.g. CKU ratio sum, required API credentials). Now discovers plugins
via existing registry, calls validate_plugin_settings() on each tenant's plugin
to exercise Pydantic validators without creating live connections. Extracts
_build_registry() helper shared by _create_runner() and _validate_plugin_configs(). ([52bc6c3](https://github.com/waliaabhishek/chitragupta/commit/52bc6c3caa32ef114a62fe48c8070e2f5965cd93))
- Fix: TASK-135 — Distinguish metrics prefetch failure from empty data in allocation pipeline

Metrics prefetch failures (Prometheus unreachable/timeout/error) now produce
a distinct METRICS_FETCH_FAILED allocation detail instead of being silently
conflated with empty data (NO_USAGE_FOR_ACTIVE_IDENTITIES). Chargeback rows
produced during Prometheus outages are identifiable and filterable in the DB.

Changes: _prefetch_metrics returns failed_keys set alongside prefetched data;
AllocationContext gains metrics_fetch_failed bool; UsageRatioModel,
_kafka_usage_allocation, and allocate_by_usage_ratio guard on the flag;
new AllocationDetail.METRICS_FETCH_FAILED enum value persisted to DB. ([74f39b5](https://github.com/waliaabhishek/chitragupta/commit/74f39b5d7b49573006aca9f7113a7696720f5ba5))
- Fix: Add CLI entry point and git-tag-based dynamic versioning

- Add [project.scripts] entry so `uv run chitragupt` works without PYTHONPATH hacks
- Replace hardcoded version with hatch-vcs (derives from git tags)
- Fix hatch build config: packages=["src"] → explicit package list so editable
  install puts src/ on sys.path
- Dockerfile: accept APP_VERSION build arg for release builds, fallback 0.0.0.dev0
- Release workflow: pass git tag as APP_VERSION to Docker build ([15d9241](https://github.com/waliaabhishek/chitragupta/commit/15d92411754ea1660ef097424a5fd731bfe7bd8b))
- Fix: TASK-134 — Implement Prometheus/OpenMetrics emitter for chargeback and resource presence metrics

Add PrometheusEmitter that exposes chargeback, billing, resource presence,
and identity presence as timestamped Prometheus gauge metrics. Includes
storage injection via needs_storage_backend factory attribute, collector
script for promtool TSDB backfill, and example config. ([b1c7ff8](https://github.com/waliaabhishek/chitragupta/commit/b1c7ff88a0b69251d279a125c1c48176a85557e1))
- Fix: TASK-133 — Patch double table creation in API test fixtures and use rolling timestamps

Suppress cleanup_orphaned_runs_for_all_tenants during TestClient lifespan to prevent
second create_tables() call on shared temp DB. Replace hardcoded 2026-02 timestamps
with rolling dates so tests don't age outside the default 30-day query window. ([bea1a31](https://github.com/waliaabhishek/chitragupta/commit/bea1a312e2c6b8c355c502ba112fae6543f16039))
- Fix: TASK-130 — Replace hardcoded system identity exclusion with SENTINEL_IDENTITY_TYPES constant

Define SENTINEL_IDENTITY_TYPES in identity.py to canonically identify
synthetic fallback identities (e.g. UNALLOCATED). Replace the hardcoded
!= "system" check in orchestrator._build_tenant_period_cache with
not-in-constant, matching the OWNER_IDENTITY_TYPES pattern. ([8f8c090](https://github.com/waliaabhishek/chitragupta/commit/8f8c090e03c5dabc6fd7af5c61f6e023e3599d6b))
- Fix: TASK-125 — Validate CKU ratio sum at config parse time

Move kafka_cku_usage_ratio + kafka_cku_shared_ratio sum check from
allocation time to CCloudPluginConfig.validate_allocator_params so bad
config is caught at startup before expensive gather operations. ([24dd9b2](https://github.com/waliaabhishek/chitragupta/commit/24dd9b204ef5e1c86637b394d553d4222f7bb457))
- Fix: TASK-123 — Guard masked API key check against empty strings

all() on an empty iterable returns True (vacuous truth), causing empty
API key IDs to be incorrectly classified as masked. Replace `is not None`
with truthiness check so empty strings fall through to CREDENTIALS_UNKNOWN. ([e115328](https://github.com/waliaabhishek/chitragupta/commit/e1153287293ad73e7e141ca727321073cce4659c))
- Fix: TASK-121 — Stamp last_seen_at on all CCloud gathered entities

All 12 gather functions in confluent_cloud/gathering.py omitted
last_seen_at, leaving it permanently None. Add datetime.now(UTC) to
every CoreResource and CoreIdentity constructor, matching the pattern
used by self_managed_kafka and generic_metrics_only plugins. ([e8e46c8](https://github.com/waliaabhishek/chitragupta/commit/e8e46c8b85ce9d8fb1e55178fd4201c779836de4))
- Fix: TASK-120 — Add safety guard to remainder distribution loops

Extract _distribute_remainder() helper to DRY up identical loops in
split_amount_evenly() and allocate_by_usage_ratio(). Replace unbounded
while loop with for/else bounded by len(amounts)*2 iterations, raising
RuntimeError on non-convergence. ([eff6621](https://github.com/waliaabhishek/chitragupta/commit/eff6621e332efcc17b0e25016c9861a549addf51))
- Fix: TASK-115 — Upgrade ag-grid to v35

Upgrade ag-grid-community and ag-grid-react from v33.3.2 to v35.1.0.
Migrate deprecated string-based rowSelection to object-based API,
remove deprecated checkboxSelection/headerCheckboxSelection from
column defs, remove suppressRowClickSelection. Clean up pre-existing
quality issues (empty catch, dead code, invalid lint comment,
unmemoized handlers). All 258 frontend tests pass. ([9afb3d9](https://github.com/waliaabhishek/chitragupta/commit/9afb3d9885dc13d50db0e52a1c7c10406518d2f3))
- Fix: Sync package-lock.json with package.json for CI docker build

npm ci was failing because openapi-typescript and related dependencies
were in package.json but missing from the lock file. ([a0121e1](https://github.com/waliaabhishek/chitragupta/commit/a0121e10133b49b3e3349fe26018d27e4172b2a2))
- Fix: Use --latest instead of --unreleased in git-cliff integration test

When CI runs on a tagged commit, --unreleased produces no commits
since the tag points at HEAD. Using --latest validates against the
most recent tagged release which always has content. ([c50c02b](https://github.com/waliaabhishek/chitragupta/commit/c50c02b1b6559986b5a7e8d1fdd7d1feb8475669))
- Fix: Set mike default version in docs workflow

Ensures root GitHub Pages URL redirects to latest version
instead of returning 404. ([ab0af71](https://github.com/waliaabhishek/chitragupta/commit/ab0af71ddb227e7bb09edb5ac4cab77aeed33d17))
- Fix: Use uv run for mike in docs workflow

mike is installed in the docs dependency group via uv, not globally.
Must invoke through uv run in CI where the venv isn't on PATH. ([5498d35](https://github.com/waliaabhishek/chitragupta/commit/5498d3521d8f2fe48a3704d68ac642df5d05e6dc))
- Fix: TASK-106 — Add CLI experience flags: --version, --validate, --show-config

Replace hardcoded API_VERSION with dynamic get_version() via importlib.metadata.
Add --version (argparse built-in), --validate (config pre-flight check), and
--show-config (resolved config with SecretStr masking) flags to CLI entry point.
All three exit immediately without starting the engine or API server.

Also fix all pre-existing mypy strict errors (38 across 14 files) and ruff lint
errors (51 across ~30 files) to pass newly added pre-commit hooks. ([a359136](https://github.com/waliaabhishek/chitragupta/commit/a3591366e83557b2b1d354030b52dc5bdae96532))
- Fix: TASK-101 — Fix UI auto-refresh cascade, filters instability, and missing product_category dimension

Split TenantContext into stable (tenant selection) and volatile (readiness polling)
contexts to prevent 11 of 12 consumers from re-rendering every 5s during pipeline
runs. Memoize filters object and add queryParams value in useChargebackFilters to
eliminate ChargebackGrid and AllocationIssues cascade re-fetches. Fix product_sub_type
→ product_category in dashboard aggregation. ([b4cdaa2](https://github.com/waliaabhishek/chitragupta/commit/b4cdaa21cb79ef228c29bd08a7891c0c856aa9b7))
- Fix: TASK-100 — Fix date persistence, stop auto-refresh cascade, and add Refresh Data button

Three UX bugs fixed: (1) date range now persists across page navigation and
reload via localStorage fallback (URL > localStorage > defaults), (2) readiness
poll no longer cascades into data re-fetches — tenantsLoaded converted to useRef,
setReadiness guarded by JSON fingerprint, context value memoized with useMemo,
restartKey counter for error recovery, (3) Refresh Data button added to FilterPanel
with dashboard key-remount and AG Grid cache refresh wiring.

Also fixes pre-existing jsdom/AbortController incompatibility that caused 45 test
failures by adding a custom Vitest environment that restores native AbortController. ([bcc4006](https://github.com/waliaabhishek/chitragupta/commit/bcc4006433cdeb0b3d8dc6be32b6075846eae26b))
- Fix: TASK-099 — Add missing database indexes to eliminate full table scans on UI/Grafana polls

Add Alembic migration 008 with two composite indexes:
- ix_chargeback_facts_dimension_timestamp(dimension_id, timestamp) on chargeback_facts
- ix_chargeback_dimensions_eco_tenant(ecosystem, tenant_id) on chargeback_dimensions

Update table model __table_args__ to match migration. Fix ruff formatting in readiness tests. ([73ad19b](https://github.com/waliaabhishek/chitragupta/commit/73ad19b46b08c54b816d6d4b2df430b7ef13342e))
- Fix: TASK-098 — Add AbortController to all fetch hooks and backend backpressure to prevent UI Connecting state

Frontend: Replace cancelled-flag pattern with AbortController in all data hooks
(useInventorySummary, useDataAvailability, useAllocationIssues, useAggregation,
useFilterOptions, TenantContext, ChargebackGrid, ChargebackDetailDrawer,
TagManagementPage). In-flight requests now abort on unmount/dep change, preventing
request stampede from overwhelming the backend.

Backend: Add uvicorn concurrency config (limit_concurrency=100, timeout_keep_alive=10),
readiness endpoint TTL cache (2s via time.monotonic), and RequestTimeoutMiddleware
(504 after request_timeout_seconds) to provide backpressure under load. ([f0edc91](https://github.com/waliaabhishek/chitragupta/commit/f0edc916401e9f73354f2855e9be5639308de72d))
- Fix: TASK-097 — Add dates_pending_calculation to PipelineRunResult for log disambiguation

Add pending count from find_needing_calculation() to pipeline run summary logs
so operators can distinguish caught-up (pending=0, calculated=0) from partial
failure (pending=3, calculated=0) without cross-referencing error lines. ([26c5c57](https://github.com/waliaabhishek/chitragupta/commit/26c5c57817948b65c84bcd94c15eda5c4628b130))
- Fix: TASK-096 — Implement read/write connection pool separation to fix frontend disconnects during pipeline execution

Separate read-only and read-write SQLite connection pools so API read endpoints
never contend with the pipeline writer. Read-only engine uses PRAGMA query_only=1
to prevent lock escalation, eliminating SQLITE_BUSY errors and threadpool exhaustion
during pipeline runs in --mode both.

Key changes:
- Add ReadOnlyUnitOfWork protocol (ISP-compliant: no commit/rollback)
- Add get_or_create_read_only_engine() with shared _create_cached_engine helper (DRY)
- Add ReadOnlySQLModelUnitOfWork subclass with commit() guard
- Split API dependencies: get_unit_of_work (read-only) / get_write_unit_of_work
- Fix session leak: dependencies now own UoW context manager
- Update all 15 route files: read routes use ReadOnlyUnitOfWork, write routes use UnitOfWork
- Readiness/pipeline-status/tenants-list use read-only pool directly ([eed262a](https://github.com/waliaabhishek/chitragupta/commit/eed262a41535b372cecd6d6fdcdc644931b88fe8))
- Fix: TASK-095 — Fix SQL parameter explosion and N+1 query patterns in repository layer

Replace materialized dimension ID list in delete_before() with scalar subquery
to avoid SQLite's 32K parameter limit. Rewrite _run_bulk_tag() to batch-fetch
dimensions and tags (2 queries per 500-item chunk instead of 2N individual queries).
Add chunking guards to _overlay_tags() and get_dimensions_batch(). Add
find_tags_by_dimensions_and_key() batch method to TagRepository protocol. ([5580802](https://github.com/waliaabhishek/chitragupta/commit/5580802afbe4575ee8cea9ffe1f4e13651c51b1a))
- Fix: task-094 — Fix readiness endpoint and UI pipeline status across all startup modes

Six bugs fixed:
1. API-only mode no longer reports orphaned DB "running" records as active pipeline
2. Frontend shows mode-appropriate message in no_data state (API-only vs both)
3. Orphan cleanup extracted to shared function, called at API-only startup too
4. Frontend polls at 5s during active pipeline, 15s when idle
5. Dead app.state.pipeline_runs dict removed
6. Per-tenant permanent failure now visible in UI even when other tenants healthy ([fd7932a](https://github.com/waliaabhishek/chitragupta/commit/fd7932a4d92fd89ecf32f8dc89b72bae6b8edc70))
- Fix: task-093 — Add allocation issues diagnostic table

Add dedicated endpoint, repository method, and dashboard table for
surfacing failed cost allocations grouped by dimension + error code,
ordered by total_cost DESC. Filters exclude success codes
(usage_ratio_allocation, even_split_allocation) and NULL allocation_detail. ([28405d4](https://github.com/waliaabhishek/chitragupta/commit/28405d47eb128d8f9b8f1f4cea1fc99bff7c7f34))
- Fix: task-092 — Add object inventory counters panel to dashboard

Add collapsible InventoryCounters panel showing resource and identity counts.
New useInventorySummary hook fetches from /inventory/summary endpoint.
Integration test verifies full wiring from page to component. ([7d8a4c2](https://github.com/waliaabhishek/chitragupta/commit/7d8a4c2d9770d281ca7d76ebabd66152fcf2c7cf))
- Fix: task-091 — Add data availability timeline panel to dashboard

Add visual timeline panel showing dots for each date with chargeback data.
Users can now immediately see data freshness and gaps in the dashboard.

- New useDataAvailability hook fetches from /chargebacks/dates endpoint
- New DataAvailabilityTimeline ECharts scatter chart with date filtering
- Integrated into dashboard between stat cards and cost trend chart
- 16 new tests covering hook and component behavior ([2c22d15](https://github.com/waliaabhishek/chitragupta/commit/2c22d1555f6931f62ac37d26a3a932269efb6064))
- Fix: task-090 — Convert filter inputs to dynamic dropdowns

Replace free-text Input components with Select dropdowns in FilterPanel.
New useFilterOptions hook fetches identities, resources, and product types
from backend APIs with Promise.all, deduplication, and error handling.
Split into two effects to avoid refetching identities/resources on date change.
Both call sites updated to pass tenantName prop.

176 tests, 96% coverage. ([9521a4a](https://github.com/waliaabhishek/chitragupta/commit/9521a4aa982fde6feaca0f4cdf6eb296f4f696a0))
- Fix: task-089 — Add pie charts for environment and product sub-type

Add 4 pie charts to dashboard in responsive row: Environment, Resource,
Product Type, Product Sub-Type. Create DimensionPieChart component with
topNWithOther utility for top-10 + "Other" bucketing. Refactor
CostByProductChart and CostByResourceChart to delegate to new component.
Add environment_id and product_sub_type aggregation hooks.

- Add topNWithOther() to aggregation.ts
- Create DimensionPieChart component
- Refactor CostByProductChart pie mode to use DimensionPieChart
- Convert CostByResourceChart from table to pie
- Add 2 new useAggregation hooks (environment_id, product_sub_type)
- Update dashboard layout: 4 pies at xs=24 sm=12 lg=6
- 155 tests passing, 95.8% coverage ([befe480](https://github.com/waliaabhishek/chitragupta/commit/befe480ace04c102783ce92ec3080b5e77e42d4c))
- Fix: task-088 — Add summary stat cards to dashboard

Add SummaryStatCards component showing Total Cost, Usage Cost, and
Shared Cost at the top of CostDashboardPage. Update AggregationBucket
and AggregationResponse types to include usage_amount and shared_amount
fields matching the backend schema from task-084. Update existing test
fixtures and MSW handlers to include the new fields. ([df2bea5](https://github.com/waliaabhishek/chitragupta/commit/df2bea515c6dd6d8682d4d5107d615297e619997))
- Fix: task-087 — Add object inventory counts endpoint

Add GET /api/v1/tenants/{tenant_name}/inventory/summary endpoint that returns
counts of resources and identities grouped by type. Implements count_by_type()
on both ResourceRepository and IdentityRepository protocols with GROUP BY queries. ([5d5c575](https://github.com/waliaabhishek/chitragupta/commit/5d5c575ee9efae9ddde22fa1498f318bd8d99654))
- Fix: task-086 — Add data availability endpoint

Add GET /tenants/{tenant_name}/chargebacks/dates returning distinct
dates with chargeback facts for a tenant. Adds get_distinct_dates to
ChargebackRepository protocol and SQLModel implementation, using a
lightweight DISTINCT date(timestamp) query with tenant-scoped subquery. ([0e5ff1a](https://github.com/waliaabhishek/chitragupta/commit/0e5ff1a2433540c7a94ac4e6ba2eae23cfcba12e))
- Fix: task-085 — Add environment_id as groupable dimension in aggregate endpoint

- Add environment_id to _VALID_GROUP_BY in aggregation route
- Handle environment_id specially in aggregate() — maps to ResourceTable.parent_id
- Use conditional LEFT OUTER JOIN on ResourceTable only when environment_id requested
- 5 new integration tests covering environment grouping, org-wide costs, multi-dimension ([c492455](https://github.com/waliaabhishek/chitragupta/commit/c4924551498ab7f734a5b4bff5e052a795e1f705))
- Fix: task-084 — Split usage_amount and shared_amount in aggregate endpoint

Add usage_amount and shared_amount fields to aggregate endpoint response,
allowing callers to distinguish usage-driven vs shared/infrastructure costs
without separate filtered requests. Uses SQL CASE WHEN for single-pass
aggregation. Backward compatible — total_amount unchanged. ([8ea4073](https://github.com/waliaabhishek/chitragupta/commit/8ea407329ee43d671ceb6092f98503505c64612e))
- Fix: task-083 — Add shutdown_check to orchestrator for clean signal propagation

ChargebackOrchestrator now accepts optional shutdown_check callback.
When set, the run() loop checks it before each billing date iteration
and breaks cleanly if shutdown is requested. WorkflowRunner wires
_is_shutdown_requested as the callback, enabling single Ctrl+C shutdown. ([b1f89b2](https://github.com/waliaabhishek/chitragupta/commit/b1f89b2079c83052e1bb0ad913bde957cd57e92a))
- Fix: date range picker resets on change due to batched setSearchParams

Two sequential setFilter calls for start_date and end_date race under
React Router's batched setSearchParams — the second overwrites the first.
Add setFilters() batch setter and use it in FilterPanel's date picker. ([331a055](https://github.com/waliaabhishek/chitragupta/commit/331a0558a96c8b5d65a7272aa70a86c38c094fc9))
- Fix: task-082 — Batch chargeback fact writes with session.add_all()

Replace per-row session.merge() with batched session.add_all() for chargeback
facts. Adds upsert_batch() to ChargebackRepository protocol and implementation.
Renames _process_billing_line to _collect_billing_line_rows, accumulates rows
in CalculatePhase.run() and calls upsert_batch() once per date.

Performance: ~41K rows/day now written in single add_all() vs 41K merges. ([4f9fc3f](https://github.com/waliaabhishek/chitragupta/commit/4f9fc3f03bcfaeed6c88fa8a8fff338a16c52995))
- Fix: task-081 — Remove max_dates_per_run cap during backfill

Remove artificial date-processing limit that caused 90-day backfill
to take ~3 hours instead of ~30 minutes. The cap provided no benefit
since tenants run in parallel ThreadPoolExecutor threads.

- Remove max_dates_per_run field from TenantConfig
- Remove _max_dates_per_run and cap slice from ChargebackOrchestrator
- Update example configs and docs
- Add backward-compat tests for configs with extra field ([0a0f5a2](https://github.com/waliaabhishek/chitragupta/commit/0a0f5a28c6e56ae33ebbbdbecbc111c1e62c5ad7))
- Fix: task-077 — ChainModel construction-time validation

Add __post_init__ to ChainModel that enforces:
- Non-empty models sequence (ValueError if empty)
- Last model must be TerminalModel (ValueError if not)

Updated 13 existing tests to comply with new validation. ([afde827](https://github.com/waliaabhishek/chitragupta/commit/afde827cf0cc667bc5c1d61de263ad2866abedb6))
- Fix: task-076 — Remove deprecated allocation helpers

Remove allocate_evenly_with_fallback from helpers.py after migration to
SMK_INFRA_MODEL complete. Delete stale tests, update assertions to use
ChainModel-based allocators. No behavioral changes — cleanup only. ([88a540d](https://github.com/waliaabhishek/chitragupta/commit/88a540d39813ce7fe47e70848986e3e1288f50a4))
- Fix: task-079 — SMK infrastructure allocation models (COMPUTE/STORAGE)

Migrate SELF_KAFKA_COMPUTE and SELF_KAFKA_STORAGE from allocate_evenly_with_fallback
to composable ChainModel. Adds SMK_INFRA_MODEL with 3-tier chain:
- Tier 0: EvenSplit over metrics_derived (CostType.USAGE)
- Tier 1: EvenSplit over resource_active (NO_ACTIVE_IDENTITIES_LOCATED)
- Tier 2: Terminal to UNALLOCATED (NO_IDENTITIES_LOCATED)

Fixes behavioral gap where static identities in resource_active were bypassed
because allocate_evenly_with_fallback used tenant_period (always empty in SMK). ([d898c8a](https://github.com/waliaabhishek/chitragupta/commit/d898c8ada8f9c39efe4c63d0ee40c14c9c1cc692))
- Fix: task-080 — Generic Metrics-Only plugin composable models

Migrates GenericMetricsOnlyHandler from imperative allocation helpers
(allocate_evenly_with_fallback, _make_usage_ratio_allocator closure)
to declarative ChainModel composition.

- Add make_model_from_config() factory for even_split (2-tier) and
  usage_ratio (3-tier) ChainModels with proper AllocationDetail codes
- Replace _allocator_map with _model_map: dict[str, ChainModel]
- Fix TestCircularImports sys.modules pollution that broke isinstance
  checks across module reloads
- Add integration test through plugin.initialize() entry point ([b131a3f](https://github.com/waliaabhishek/chitragupta/commit/b131a3f47032262ca2ab446a089328ca2754efd6))
- Fix: task-075 — SMK allocation models migration to composable ChainModel

- Created allocation_models.py with SMK_INGRESS_MODEL and SMK_EGRESS_MODEL
- 3-tier ChainModel: UsageRatio → EvenSplit(resource_active) → Terminal
- Updated kafka.py to use models directly in _ALLOCATOR_MAP
- Removed kafka_allocators.py (imperative logic now in models)
- Updated all tests for new imports and behavioral delta (resource_active fallback) ([71369b2](https://github.com/waliaabhishek/chitragupta/commit/71369b2c92921af99dd6adcb03e12a5150d50286))
- Fix: task-074 — CCloud fallback allocator for unknown product types

Add get_fallback_allocator() to EcosystemPlugin protocol and wire through
EcosystemBundle. CCloud returns unknown_allocator (allocates to resource_id
with SHARED cost type), SMK/Generic return None. Orchestrator now dispatches
to fallback_allocator instead of inline UNALLOCATED allocation, preserving
cost lineage for unrecognized product types per reference UnknownAllocator. ([aef7724](https://github.com/waliaabhishek/chitragupta/commit/aef77247e7d2436c6c785225dccd27274fc32e1d))
- Fix: task-073 — CCloud Org-wide model with UNALLOCATED terminal

Add ORG_WIDE_MODEL ChainModel with explicit UNALLOCATED terminal for org-wide
costs (AUDIT_LOG_READ, SUPPORT). Fixes ALLOC-02 gap where org-wide costs were
terminating to resource_id instead of UNALLOCATED system identity.

- _ORG_WIDE_OWNER_TYPES excludes "principal" — only durable identity types
- EvenSplit tier 0 across tenant_period owners (SA, user, pool)
- TerminalModel tier 1 to UNALLOCATED with NO_IDENTITIES_LOCATED detail
- org_wide_allocator delegates to ORG_WIDE_MODEL
- 28 tests covering all verification scenarios
- Fixed pre-existing kafka_handler test import (kafka_num_cku_allocator) ([1771b50](https://github.com/waliaabhishek/chitragupta/commit/1771b50e58ffd8fbd250318c6df737c77f25c764))
- Fix: task-072 — CCloud Kafka CKU composition model

Migrate kafka_num_cku_allocator to composable DynamicCompositionModel:
- Add _extract_combined_usage helper (DRY: delegates to _extract_usage)
- Add CKU_USAGE_CHAIN (4-tier: usage ratio → merged_active → tenant_period → terminal)
- Add CKU_SHARED_CHAIN (3-tier: merged_active → tenant_period → terminal)
- Add make_dynamic_cku_model() and _CKU_DYNAMIC_MODEL singleton
- Add kafka_cku_allocator (single-line delegation to model)
- Remove kafka_num_cku_allocator
- Update kafka.py wiring for KAFKA_NUM_CKU/CKUS

27 new tests in test_cku_allocators.py. 100% coverage. ([e427a23](https://github.com/waliaabhishek/chitragupta/commit/e427a238f6660ec4cd394c363e230aae7248d57e))
- Fix: task-071 — KAFKA network models migration to composable ChainModel

- Add `_extract_usage` helper for single-metric-key usage extraction
- Add `make_network_model` factory producing 4-tier ChainModel
- Add BYTES_IN_MODEL, BYTES_OUT_MODEL, PARTITION_MODEL constants
- Add kafka_network_read_allocator, kafka_network_write_allocator, kafka_partition_allocator
- Update _KAFKA_ALLOCATORS dict with direction-specific allocators
- Fix metric direction blending: READ uses bytes_out, WRITE uses bytes_in ([cf5fa79](https://github.com/waliaabhishek/chitragupta/commit/cf5fa7977b4481838199e9ea579996c147b8d644))
- Fix: task-070 — FLINK_MODEL migration to composable allocation models

Migrates Flink allocators to use the composable ChainModel system:
- Define FLINK_MODEL as 4-tier ChainModel (UsageRatio → merged_active → tenant_period → terminal)
- Replace imperative flink_cfu_allocator with direct alias to FLINK_MODEL
- Fix terminal tier: resource_id instead of "UNALLOCATED", SHARED instead of USAGE
- Add tenant_period fallback tier (missing in original) ([b88bb79](https://github.com/waliaabhishek/chitragupta/commit/b88bb79f92c21fc4ad2ba1fe2f6ca50349ded3f4))
- Fix: task-069 — KSQLDB_MODEL migration to composable allocation models

Migrated ksqlDB allocator to use ChainModel with 3-tier fallback:
- Tier 0: EvenSplit over merged_active (USAGE)
- Tier 1: EvenSplit over tenant_period owners (SHARED, NO_ACTIVE_IDENTITIES_LOCATED)
- Tier 2: Terminal to resource_id (SHARED, NO_IDENTITIES_LOCATED)

Also: helpers.py allocate_evenly now allows None allocation_detail for
happy-path Tier 0 (user-approved design decision matching reference behavior). ([16d9ac2](https://github.com/waliaabhishek/chitragupta/commit/16d9ac26f1190b433aa215f54282b6f6a26dd55a))
- Fix: task-068 — CONNECTOR_MODEL migration to composable allocation models

- Add CONNECTOR_TASKS_MODEL (USAGE) and CONNECTOR_CAPACITY_MODEL (SHARED)
- Replace imperative connector allocators with model aliases
- Add AllocationDetail.NO_IDENTITIES_LOCATED on terminal tier
- Remove fragile post-processing loop for cost_type override
- Add 9 new tests including handler→allocator integration test ([d6f509d](https://github.com/waliaabhishek/chitragupta/commit/d6f509d4d76240e6223e13a1c1c93704b3536730))
- Fix: task-067 — SR_MODEL migration for Schema Registry allocator

Migrate schema_registry_allocator to composable allocation model:
- Create SR_MODEL ChainModel with 3 tiers (USAGE → SHARED → Terminal)
- Tier 0: EvenSplit over merged_active (CostType.USAGE)
- Tier 1: EvenSplit over tenant_period (CostType.SHARED + NO_ACTIVE_IDENTITIES_LOCATED)
- Tier 2: Terminal to resource_id (CostType.SHARED + NO_IDENTITIES_LOCATED)
- Fixes behavioral parity: Tier 0 now uses USAGE, Tier 2 uses resource_id not UNALLOCATED ([a3394ba](https://github.com/waliaabhishek/chitragupta/commit/a3394baa90fce29c0991928eb5503adbe89fe8e7))
- Fix: task-065 — CompositionModel and DynamicCompositionModel

Add composition models for splitting costs across multiple strategies:
- CompositionModel: fixed ratios validated at construction, last component absorbs rounding remainder
- DynamicCompositionModel: runtime-determined ratios via callable
- Both inject composition_index and composition_ratio metadata
- Both implement __call__ for CostAllocator compatibility ([590db30](https://github.com/waliaabhishek/chitragupta/commit/590db3098ef90c59fa92dec584d5d7caa8c70925))
- Fix: task-064 — ChainModel meta-model

Add AllocationError exception and ChainModel dataclass for composable
allocation fallback chains. ChainModel tries models in sequence,
injects chain_tier metadata, supports debug logging, raises
AllocationError on exhaustion. Includes 13 unit tests (100% coverage). ([c88c5c3](https://github.com/waliaabhishek/chitragupta/commit/c88c5c386308aae65c6c1c8293495c094e1590bb))
- Fix: task-063 — TerminalModel and DirectOwnerModel primitives

Add two composable allocation models:
- TerminalModel: always returns result, never None (chain termination)
- DirectOwnerModel: returns None when owner unresolved (fallback trigger)

Both implement allocate() for AllocationModel and __call__ for CostAllocator. ([d3d1def](https://github.com/waliaabhishek/chitragupta/commit/d3d1def425535c7abe224e42fb1027d16bc80b88))
- Fix: task-062 — EvenSplitModel and UsageRatioModel primitives

Add composable allocation model primitives for the CAM system:
- EvenSplitModel: splits cost evenly across identities from source callable
- UsageRatioModel: splits cost proportionally by usage values
- Both implement allocate() for chain composition (returns None for fallback)
- Both implement __call__() for CostAllocator compatibility (never returns None)
- Extended allocate_evenly() with allocation_detail and cost_type params
- Extended allocate_by_usage_ratio() with allocation_detail param ([0edf175](https://github.com/waliaabhishek/chitragupta/commit/0edf175a7bd7377a34ea4b433995622dfdc0de9e))
- Fix: task-061 — AllocationModel protocol and AllocationContext dataclass

Add foundational protocol for composable allocation model system:
- Create AllocationModel protocol with allocate() -> AllocationResult | None
- Add metadata field to AllocationResult for chain execution diagnostics
- 10 tests covering protocol compliance, dataclass behavior, no circular imports ([2ac0bca](https://github.com/waliaabhishek/chitragupta/commit/2ac0bca11194e23c7a165eb82b3f01cd752c833b))
- Fix: task-060 — Add frontend Docker container with compose profile

Multi-stage Dockerfile (node:22-alpine builder + nginx:1.27-alpine runtime)
with nginx proxy to backend API. Compose profile `ui` enables opt-in frontend
on port 8081. Includes security headers (X-Frame-Options, X-Content-Type-Options,
X-XSS-Protection). ([ed6fbc8](https://github.com/waliaabhishek/chitragupta/commit/ed6fbc839d8f92c0892aecc24cec9b0cf97016d4))
- Fix: task-059 — Filter tenant_period fallback to OWNER_IDENTITY_TYPES

allocate_evenly_with_fallback() and ksqldb_csu_allocator() now filter
tenant_period identities to OWNER_IDENTITY_TYPES, excluding api_key and
system identities from cost allocation. Added "principal" to
OWNER_IDENTITY_TYPES for self-managed Kafka support.

4 new tests verify filter behavior and terminal fallback paths. ([a9ca9fb](https://github.com/waliaabhishek/chitragupta/commit/a9ca9fbeb6f263bc543cb07b8e32a92df6924475))
- Fix: task-058 — Implement graceful shutdown handling

- Add _shutdown_event field and set_shutdown_event() to WorkflowRunner
- Replace blocking wait() with 1-second polling loop in run_once()
- Use executor.shutdown(wait=False, cancel_futures=True) for immediate exit
- Add signal handlers for run-once mode (standalone and both modes)
- Single Ctrl+C now exits within 2 seconds in all modes
- Clean log message on shutdown (no ugly stacktrace) ([18e065f](https://github.com/waliaabhishek/chitragupta/commit/18e065f2dfcdd445ff064eaa2200af0f6730c17a))
- Fix: task-057 — Fix alembic logging interference — restore root logger after migrations

Alembic's fileConfig() in env.py was overwriting the root logger configuration
when migrations ran, silencing INFO/DEBUG logs for the rest of the process.

Added save/restore pattern around command.upgrade() to preserve root logger
level and handlers across migration runs. ([d60e23f](https://github.com/waliaabhishek/chitragupta/commit/d60e23fa27367471624014ea5558ad76d13173ef))
- Fix: task-056 — Fix Frontend UI: Date Filter Refresh + Dark Mode Default

- Add useEffect in ChargebackGrid to purge AG Grid cache on filter change
- Create useTheme hook with localStorage persistence, dark mode default
- Integrate useTheme into App.tsx with ConfigProvider theme algorithm
- Add theme toggle button in Layout header
- Add comprehensive tests for all new functionality (142 tests pass) ([f36aaba](https://github.com/waliaabhishek/chitragupta/commit/f36aabacb250ececfe5c7c5cd96338ddf32f820c))
- Fix: task-055 — Fix API key identity resolution in metrics_derived path

API keys now correctly resolve to their owners in three places:
1. metrics_derived path in identity_resolution.py
2. _kafka_usage_allocation via context["api_key_to_owner"] remapping
3. tenant_period fallbacks now filter to OWNER_IDENTITY_TYPES

Also fixes pre-existing logger declaration in prometheus.py. ([f15b557](https://github.com/waliaabhishek/chitragupta/commit/f15b55717c6eb164d4f14dae3f9dd1fb4552c76f))
- Fix: task-054 — Use range query mode for Flink CFU metrics

Change query_mode from "instant" to "range" for _FLINK_METRICS_PRIMARY
and _FLINK_METRICS_FALLBACK. Same fix as task-051 (Kafka): instant
queries capture only one scrape interval, undercounting CFU usage
when billing windows exceed scrape frequency. ([f8c7fbc](https://github.com/waliaabhishek/chitragupta/commit/f8c7fbc607c0b5f7bba2033b4969946f89278bfb))
- Fix: task-053 — Add per-endpoint semaphore to PrometheusMetricsSource

Adds max_concurrent_requests config (default 20) and BoundedSemaphore
to limit total in-flight HTTP requests per Prometheus endpoint. Prevents
connection storms when parallel orchestrator query() calls compound. ([20ff550](https://github.com/waliaabhishek/chitragupta/commit/20ff550be490af38998c2fdbc0210d421a2939d1))
- Fix: task-052 — Simplify _aggregate_rows dual-dict pattern

Replace two parallel dicts (aggregated + templates) with single _Bucket
dataclass holding both total and template row. Eliminates implicit
coupling and redundant dict lookup in output loop. ([6980a77](https://github.com/waliaabhishek/chitragupta/commit/6980a77c86d02d70fafcff24fdec3ad6710eec41))
- Fix: task-051 — Use range query mode for CCloud Kafka metrics

Changed query_mode from "instant" to "range" for _KAFKA_READ_METRICS and
_KAFKA_WRITE_METRICS. Instant queries capture only one scrape interval's
worth of data; range queries sum all intervals across the billing window
for accurate principal allocation ratios. ([4459d46](https://github.com/waliaabhishek/chitragupta/commit/4459d4644e48af13b3620af7c8f5f0db3faba394))
- Fix: task-050 — Make discovery window configurable

Add discovery_window_hours config option to SelfManagedKafkaConfig
(default=1, gt=0). Pass through to run_combined_discovery() in both
_validate_principal_label and build_shared_context call sites.
Allows operators to extend lookback window for low-traffic clusters. ([0be237e](https://github.com/waliaabhishek/chitragupta/commit/0be237e50eedf0f01c9cb301c435c484663293e8))
- Fix: task-049 — Cache validation query for first gather cycle

Add _cached_discovery field to SelfManagedKafkaPlugin. Validation query
result is stored and consumed on first build_shared_context() call,
eliminating duplicate Prometheus round-trip per pipeline run. ([3c98edd](https://github.com/waliaabhishek/chitragupta/commit/3c98edd5d133fb221767188e3a0ec2963d9e17c9))
- Fix: task-048 — Add TTLCache for identity/resource repository lookups

Add repository-scoped TTLCache (cachetools) to SQLModelIdentityRepository and
SQLModelResourceRepository, eliminating redundant DB round-trips for repeated
get() calls within a single UoW session. Cache invalidation on upsert/mark_deleted.

- pyproject.toml: add cachetools>=5.0, types-cachetools>=5.0
- repositories.py: TTLCache with configurable maxsize/ttl, cache-check-first get()
- 20 new tests covering cache hits, invalidation, TTL expiry, LRU eviction ([2be0763](https://github.com/waliaabhishek/chitragupta/commit/2be076380e35a63aa298d34c8d4b04448f6fb47f))
- Fix: task-047 — Add metadata filtering to find_by_period for Flink

Add metadata_filter parameter to ResourceRepository.find_by_period for
DB-side JSON filtering. Flink handlers now pass compute_pool_id to filter
statements at the SQL layer instead of loading all statements and filtering
in Python. ([1da2405](https://github.com/waliaabhishek/chitragupta/commit/1da2405a9a8c7d07954495a856f95c580d69d6bf))
- Fix: task-046 — Use correlated subquery for chargeback delete

Replace two-phase Python-mediated DELETE with single atomic correlated
subquery. Eliminates memory overhead from dimension ID list materialization
and removes race condition window between SELECT and DELETE. ([66767f9](https://github.com/waliaabhishek/chitragupta/commit/66767f94ed235cda11033691327905360c71332f))
- Fix: task-045 — Cache billing_window() computation per line

Pre-compute billing windows once per billing line in CalculatePhase.run()
and pass cache to _compute_billing_windows, _prefetch_metrics, and
_process_billing_line. Reduces billing_window() calls from 3N to N. ([8315536](https://github.com/waliaabhishek/chitragupta/commit/8315536ae3e8be718d37a95f078ea0c10c4d6080))
- Fix: task-044 — Consolidate three Prometheus discovery queries into one

Replaces three separate MetricQuery objects (_BROKERS_QUERY, _TOPICS_QUERY,
PRINCIPALS_QUERY) with a single _COMBINED_DISCOVERY_QUERY that groups by all
three labels. Eliminates 2 redundant Prometheus round-trips per gather cycle.

Changes:
- prometheus.py: Added run_combined_discovery() + converter functions
- shared_context.py: Added discovered_brokers/topics/principals fields
- plugin.py: build_shared_context() now runs combined query once
- plugin.py: _validate_principal_label() uses run_combined_discovery()
- handlers/kafka.py: Uses cached discovery results from shared_ctx
- metrics.py: MetricQuery.resource_label now accepts str | None

Query count reduction:
- prometheus+prometheus: 3/cycle + 1 startup → 1/cycle + 1 startup
- prometheus+static: 2/cycle → 1/cycle

Tests: 1859 passed, coverage 98% ([ea1da2e](https://github.com/waliaabhishek/chitragupta/commit/ea1da2e58301d08322211a4c2e9b60d9ba4475d4))
- Fix: task-040 — Use bulk UPDATE for pipeline state mark_* methods

Replace SELECT-then-UPDATE pattern with direct UPDATE statements in
all 4 mark_* methods. Eliminates 90+ redundant queries per calculate
cycle for 30 billing dates. Add test_mark_resources_gathered for
complete test coverage. ([4e20093](https://github.com/waliaabhishek/chitragupta/commit/4e20093df7d8e6cfe05f94737d4021080239acbd))
- Fix: task-037 — Pass cached identity/resource data to handlers

Eliminates redundant find_by_period calls in handlers by passing
pre-built caches via ResolveContext parameter. Handlers now use
cached_identities/cached_resources when available, falling back
to DB queries only when context is None.

- Add ResolveContext TypedDict to protocols.py
- Orchestrator builds and passes context to handler.resolve_identities
- Kafka/SR handlers use cached_identities to skip identity queries
- Flink handlers use cached_resources with _get_flink_statement_resources helper
- All other handlers accept context parameter (signature-only change) ([dacee22](https://github.com/waliaabhishek/chitragupta/commit/dacee2226d4a93574e44edb4ab2d6f62de8c7e4d))
- Fix: task-043 — Add count=False parameter to skip COUNT query

Add count: bool = True parameter to ResourceRepository and IdentityRepository
find_active_at/find_by_period methods. When count=False, skips SELECT COUNT(*)
and returns 0 for total. Updates 6 internal callers that discard the count
to pass count=False, eliminating unnecessary database round-trips per billing cycle. ([7c296f3](https://github.com/waliaabhishek/chitragupta/commit/7c296f3e09c2c215a22bd9c41a9e1aa41e96f6e2))
- Fix: task-042 — Add indexes on temporal columns

Add indexes to created_at and deleted_at columns on resources and
identities tables for O(log N) temporal queries instead of full scans.

- base_tables.py: Add index=True to ResourceTable and IdentityTable
  temporal column declarations
- Migration 006: Creates 4 indexes (ix_resources_created_at,
  ix_resources_deleted_at, ix_identities_created_at,
  ix_identities_deleted_at)
- Tests: 4 tests covering index presence, migration upgrade/downgrade,
  and query plan verification
- Also fixes pre-existing logger declaration in cost_input.py ([4a6172c](https://github.com/waliaabhishek/chitragupta/commit/4a6172cf733ada0901b67b1b0ec0f0d2b5496b59))
- Fix: task-041 — Compute billing_windows once per calculate cycle

Compute _compute_billing_windows() once in run() and pass result to both
_build_tenant_period_cache() and _build_resource_cache(), eliminating
duplicate O(N) iteration over billing lines. ([31d3fe1](https://github.com/waliaabhishek/chitragupta/commit/31d3fe17b135a2cd1568ae3ee4f4e9a71be3e7a8))
- Fix: task-036 — Cache dimension lookups in ChargebackRepository

Add in-memory dimension cache to SQLModelChargebackRepository to eliminate
N+1 SELECT queries. Cache is scoped to repository instance (UoW lifetime).
Remove redundant session.get() call from upsert(). ([9a1fcae](https://github.com/waliaabhishek/chitragupta/commit/9a1fcae1d7d27abd70a790ebe528c063e6aa8b44))
- Fix: task-039 — Batch Prometheus queries in ConstructedCostInput

Single range query for full [start, end) window instead of N per-day calls.
Fallback to per-day queries on MetricsQueryError preserves partial billing. ([811c57f](https://github.com/waliaabhishek/chitragupta/commit/811c57fa681a32833533629416e209cb41dac722))
- Fix: task-038 — Parallelize Prometheus metrics prefetch loop

Parallelizes CalculatePhase._prefetch_metrics() using ThreadPoolExecutor
to reduce serial network wait time. Adds configurable metrics_prefetch_workers
to TenantConfig (default=4, range 1-20). Includes partial-failure handling
that logs warnings and returns empty dict for failing groups instead of
aborting the entire calculation. ([547d855](https://github.com/waliaabhishek/chitragupta/commit/547d855f0a3dcb8d7d39bc42a55daa0dc07ec8c3))
- Fix: task-035 — Remove per-entity flush from repository upserts

Removed 5 unnecessary session.flush() calls from upsert methods
(Resource, Identity, Billing, Chargeback, PipelineState). The UoW.commit()
already flushes all pending changes atomically at transaction end.

Preserved flush in _get_or_create_dimension() where auto-generated
dimension_id is needed as FK before fact row creation. ([416b980](https://github.com/waliaabhishek/chitragupta/commit/416b980162333969dab7fb815e851cd7b9d6502a))
- Fix: Accumulate billing costs for duplicate PKs from CCloud API

CCloud Billing API can return multiple rows with same PK containing
partial costs. Changed upsert() to sum costs instead of overwriting.
Added explicit PrimaryKeyConstraint for deterministic session.get() order. ([bc3a763](https://github.com/waliaabhishek/chitragupta/commit/bc3a76307de53eba01b65b85fabe06e616382d82))
- Fix: Wire plugin storage modules into storage backend creation

The CCloud billing infrastructure (CCloudBillingLineItem, CCloudBillingRepository,
CCloudStorageModule) was built but never connected to the runtime. create_storage_backend()
always used CoreStorageModule, ignoring the plugin's storage module.

Changes:
- create_storage_backend() now accepts optional storage_module parameter
- workflow_runner passes plugin.get_storage_module() when creating storage
- API dependencies use get_storage_module_for_ecosystem() for correct repo
- Move ecosystem→storage_module mapping to plugins/storage_modules.py (DIP compliance)

This ensures CCloud billing uses 7-field PK (with env_id) preventing cross-environment
billing collisions. ([14c56e2](https://github.com/waliaabhishek/chitragupta/commit/14c56e2c1cb14d6213ad4cafcd98db35077bb48f))
- Fix: task-033 — Wire up CCloudBillingLineItem in cost_input and add migration

The previous commit created the infrastructure but didn't wire it up:
- cost_input.py now uses CCloudBillingLineItem with env_id as direct field
- Added migration 006 to create ccloud_billing table with 7-field PK
- Migration includes data migration from billing to ccloud_billing for CCloud rows
- Fixed tests to expect env_id as direct field, not in metadata
- Excluded alembic migrations from logging coverage test ([9b405d6](https://github.com/waliaabhishek/chitragupta/commit/9b405d6eddee0b28beb0668f9c11c1a817bae4a4))
- Fix: task-033 — Plugin-Owned Storage Architecture

Move billing/resource/identity storage from core to plugins, fixing
CCloud billing collision where env_id was missing from PK.

Key changes:
- BillingLineItem, Resource, Identity converted from dataclasses to Protocols
- StorageModule protocol added; EcosystemPlugin gains get_storage_module()
- CCloudBillingLineItem with env_id (7-field composite PK)
- Each plugin owns storage package (tables, repositories, module)
- SMK/GMO inherit CoreStorageModule for shared core tables
- SQLModelUnitOfWork/Backend now require StorageModule param
- env.py imports plugin tables for Alembic discovery

4 review rounds, 98.09% coverage, 1815 tests passed. ([65f5153](https://github.com/waliaabhishek/chitragupta/commit/65f5153ea863039a1daf38173acf89cc66420cbd))
- Fix: task-032 — Billing table PK missing product_category

Add product_category to billing table primary key to prevent row collisions
when CCloud API returns billing lines with same (resource_id, product_type,
timestamp) but different product_category values.

Changes:
- BillingTable.product_category promoted to primary_key=True
- Added _billing_pk() helper for 6-field PK tuple extraction
- Changed increment_allocation_attempts() to accept BillingLineItem
- Updated RetryChecker/RetryManager signatures accordingly
- Added migration 005 to alter billing table PK
- Added 6 verification tests for PK and signature changes ([e5bbbf4](https://github.com/waliaabhishek/chitragupta/commit/e5bbbf4d5fcd47e8086cd312e24d95b6c36c6e29))
- Fix: TASK-031 — Comprehensive logging to all Python modules

Added logging infrastructure to 91 Python files:
- import logging + logger = logging.getLogger(__name__) boilerplate
- Debug logs at method entry with context params
- Info logs for significant events (counts, lifecycle)
- Warning logs for fallback decisions
- logger.exception() in all except blocks

Coverage: 98.22%, 1772 tests pass, 3 review rounds. ([7dade96](https://github.com/waliaabhishek/chitragupta/commit/7dade96011131f66684f8d2bb1b52feffaa6227e))
- Fix: TASK-030 — Annotated example YAML configs for all ecosystems

Create 8 annotated example configurations with corresponding .env.example
files in deployables/config/examples/:
- ccloud-minimal, ccloud-complete, ccloud-multi-tenant, ccloud-with-flink
- self-managed-minimal, self-managed-complete
- generic-postgres, generic-redis

Each config includes inline [Required|Optional] comments explaining every
field, env var placeholders with ${VAR:-default} syntax, and realistic
example values. All configs validated via load_config() in test suite. ([7fdf5fd](https://github.com/waliaabhishek/chitragupta/commit/7fdf5fd9fdd5c9f1cb171ad84012afedd91aaf28))
- Fix: TASK-028 — Identity resolution full-table scans

Replace O(N) scans with targeted lookups:
- connector/ksqldb: find_by_period + loop → uow.resources.get()
- connector/ksqldb: identity dict → direct uow.identities.get()
- flink: add resource_type="flink_statement" filter to find_by_period
- flink: identity dict → per-owner get() with resolved cache ([b6c8b2b](https://github.com/waliaabhishek/chitragupta/commit/b6c8b2b0a909a76d10285ae15026dc40fb16a71a))
- Fix: TASK-027 — Granularity extensibility via PluginSettingsBase

Add granularity_durations field to PluginSettingsBase allowing plugins
to define custom billing cadences (e.g., weekly: 168 hours) without
modifying core engine code. Resolves OCP violation in GRANULARITY_DURATION.

- Add granularity_durations: dict[str, int] with validator (min 1 hour)
- Rename GRANULARITY_DURATION to _DEFAULT_GRANULARITY_DURATION
- billing_window() accepts pre-merged durations from caller
- CalculatePhase pre-merges durations at init, passes to all call sites ([757d490](https://github.com/waliaabhishek/chitragupta/commit/757d49034d3e4d1d443286f591d14365fedc3ee8))
- Fix: TASK-026 — Export streaming with iter_by_filters

- Add iter_by_filters to ChargebackRepository Protocol for batched streaming
- Add _build_chargeback_where and _overlay_tags helpers to eliminate duplication
- Refactor find_by_filters and find_dimension_ids_by_filters to use helpers
- Replace find_by_filters(limit=100000) with iter_by_filters in export route
- No more silent 100K row truncation; memory bounded to batch_size rows ([aceb940](https://github.com/waliaabhishek/chitragupta/commit/aceb9401b4f4cad84053eaa435cffba8510805bd))
- Fix: TASK-024 — allocate_evenly_with_fallback helper for DRY allocator chain

Add core helper encoding standard fallback: merged_active → tenant_period → UNALLOCATED.
Delete 4 duplicate implementations across SMK and generic plugins.
Update SR allocator to use helper. 14 new tests, 1707 total passing. ([3acb8d4](https://github.com/waliaabhishek/chitragupta/commit/3acb8d4b93059ec513b43319cfda17b9489f9deb))
- Fix: TASK-023 — BaseServiceHandler convenience class for DRY handler boilerplate

Introduces opt-in BaseServiceHandler[ConnT, CfgT] base class that eliminates
~80 lines of duplicated scaffolding across 5 CCloud handlers:
- Standard 3-field __init__ (connection, config, ecosystem)
- Dict-lookup get_allocator() via class-level _ALLOCATOR_MAP
- Empty gather_identities() returning iter(())

Handlers adopting BaseServiceHandler:
- SchemaRegistryHandler, ConnectorHandler, KsqldbHandler: full adoption
- FlinkHandler: partial (keeps custom __init__ for _flink_regions)
- KafkaHandler: partial (keeps gather_identities() override)

OrgWide and Default handlers not migrated (different constructor signatures). ([8447efa](https://github.com/waliaabhishek/chitragupta/commit/8447efa0bda00272501237f80a6406f1d7bcad35))
- Fix: TASK-021 — resolve_date_range helper for DRY date conversion

Extract duplicated date→datetime conversion logic from 5 API routes
into a single resolve_date_range() helper in dependencies.py.

- Add resolve_date_range(start_date, end_date) -> tuple[datetime, datetime]
- Replace inline date logic in chargebacks, billing, aggregation, tags, export routes
- Fix bug: tags.py bulk_add_tags_by_filter now validates start_date <= end_date
- Clean up unused imports per file
- Add 6 tests covering defaults, explicit dates, ordering guard, edge cases ([02721c1](https://github.com/waliaabhishek/chitragupta/commit/02721c166c6c3f651ba4c4204f1981a950ca660b))
- Fix: TASK-020 — Retention cleanup reuses cached TenantRuntime storage

_cleanup_retention() now iterates _tenant_runtimes instead of _settings.tenants,
using runtime.storage instead of creating a fresh backend. Eliminates redundant
database engine creation and avoids SQLite single-writer conflicts. ([144e82b](https://github.com/waliaabhishek/chitragupta/commit/144e82b2a196a660d6503e8619be01f24eb55039))
- Fix: TASK-009 — GenericMetricsOnlyPlugin for YAML-only ecosystems

Adds generic_metrics_only plugin enabling new metrics-only ecosystems
via pure YAML config. No Python code required for new ecosystems.

- CostQuantityConfig discriminated union: fixed, storage_gib, network_gib
- CostTypeConfig with allocation_strategy: even_split or usage_ratio
- GenericIdentitySourceConfig: prometheus, static, or both
- Handler builds allocators and metrics queries from config at init
- CostInput constructs billing lines from YAML rates + Prometheus data

Self-managed Kafka expressible as generic plugin YAML config. ([6538840](https://github.com/waliaabhishek/chitragupta/commit/6538840e3e0878808fb7b190c82c390f67686cc2))
- Fix: TASK-025 — Global exception handler for FastAPI

Add global exception handler that catches unhandled exceptions, logs full
traceback server-side, and returns structured JSON error response with UUID
for correlation. HTTPException passthrough remains unaffected. ([7f57e0b](https://github.com/waliaabhishek/chitragupta/commit/7f57e0b88348cdaab5899292895ddb154853b33e))
- Fix: TASK-002 — Add Emitter protocol and CSV implementation

Implements pluggable output stage for chargeback results:
- Emitter protocol in core/plugin/protocols.py
- EmitterRegistry with name-based registration
- EmitPhase runs after calculate, supports per-emitter aggregation
- CsvEmitter implementation with idempotent overwrites
- EmitterSpec config model with aggregation validation
- import_attr helper extracted from load_protocol_callable

67 new tests, 1608 total passing, 98.71% coverage. ([7f7abce](https://github.com/waliaabhishek/chitragupta/commit/7f7abce306dbfc46bcc3d8286304b9ac22d5af09))
- Fix: TASK-022 — Extract temporal query validation helper

Extract duplicated temporal validation logic from resources.py and
identities.py into shared validate_temporal_params() function in
dependencies.py. Adds TemporalParams frozen dataclass to carry validated,
UTC-normalized values. ([4acb1ff](https://github.com/waliaabhishek/chitragupta/commit/4acb1ff15933388311c99ca35524f8c4f1516c5f))
- Fix: TASK-008 — Two-phase handler gather (LSP/DIP fix)

Eliminates implicit handler ordering via UoW side effects. Handlers now
receive pre-gathered shared context from plugin's build_shared_context(),
making them independently testable and substitutable.

Key changes:
- Add CCloudSharedContext and SMKSharedContext frozen dataclasses
- Add build_shared_context(tenant_id) to EcosystemPlugin protocol
- Add shared_ctx param to ServiceHandler.gather_resources
- GatherPhase calls build_shared_context once, threads to all handlers
- Handlers use shared_ctx instead of querying UoW for prior handler output
- SchemaRegistryHandler no longer calls gather_environments() (TD-028 resolved)
- Handler ordering no longer affects correctness (TD-027 properly resolved)

49 new tests, 1533 total passing, 98.57% coverage. ([b4b62fd](https://github.com/waliaabhishek/chitragupta/commit/b4b62fdd5aaa5df26b5f7b67cc00cf3a9331ef0d))
- Fix: TASK-006 — Decompose orchestrator god class into phases

Decompose ChargebackOrchestrator (590+ lines, 10+ responsibilities) into:
- GatherPhase: resource/identity/billing gather, deletion detection, throttle
- CalculatePhase: metrics prefetch, cache building, per-line allocation
- RetryManager: retry counter persistence with RetryChecker protocol
- ChargebackOrchestrator: thin coordinator (~150 lines with compat wrappers)

Module-level: _load_overrides (5-tuple), _ensure_unallocated_identity

30 new tests, 1484 total pass, 98.64% coverage, orchestrator.py 100%. ([f602b79](https://github.com/waliaabhishek/chitragupta/commit/f602b7994a753b67095fbed916c540d248cbe57a))
- Fix: TASK-014 — Plugin path configurable via AppSettings

Add plugins_path field to AppSettings with configurable override.
Default computed from __file__ for CWD-independence. ([42485f2](https://github.com/waliaabhishek/chitragupta/commit/42485f2a09212bc6b99cc4c9dc38fc5e5d984ddb))
- Fix: TASK-013 — Centralize metrics step configuration

Add metrics_step_seconds to PluginSettingsBase with default 3600s.
Eliminates 6 hardcoded timedelta(hours=1) call sites across orchestrator
and self_managed_kafka plugin. Step now flows from YAML config through
orchestrator, handlers, gathering functions, CostInput, and plugin validation. ([2d6b1a4](https://github.com/waliaabhishek/chitragupta/commit/2d6b1a4674e201cac1ef617945abd786043c6279))
- Fix: TASK-007 — Storage Backend Registry

Introduces StorageBackendRegistry pattern (mirrors PluginRegistry) to eliminate
hardcoded SQLModelBackend instantiation in workflow_runner.py and dependencies.py.
API now respects StorageConfig.backend instead of hardcoding.

- src/core/storage/registry.py: new file with StorageBackendRegistry, create_storage_backend()
- workflow_runner.py: removed _create_storage_backend, uses registry factory
- dependencies.py: get_or_create_backend accepts StorageConfig, uses registry
- tenants.py, pipeline.py: pass StorageConfig instead of connection_string
- 12 new tests for registry, 1427 total passing, 98.60% coverage ([633c4ab](https://github.com/waliaabhishek/chitragupta/commit/633c4ab28786ca5716e37c75ab6358ca789953af))
- Fix: TASK-019 — Eliminate DRY violation in _load_identity_resolver

Add IdentityResolver protocol to protocols.py and refactor
_load_identity_resolver to delegate to load_protocol_callable,
eliminating 25 lines of duplicated loading logic. Gains four
safety checks: ImportError wrapping, AttributeError wrapping,
class rejection, and protocol isinstance validation. ([1d8cec9](https://github.com/waliaabhishek/chitragupta/commit/1d8cec9cc24b07c7b6055e907e94e71639ee39c0))
- Fix: TASK-018 — Extract _detect_entity_deletions to eliminate DRY violation

Refactored _detect_deletions to use a single _detect_entity_deletions helper,
eliminating ~50 lines of duplicated zero-gather-protected deletion logic.
Replaced two separate counter attributes with _zero_gather_counters dict.
Added _EntityRepo Protocol for structural typing (TYPE_CHECKING only). ([9ddd121](https://github.com/waliaabhishek/chitragupta/commit/9ddd121450ef568dca98da5c58dd0c2553ba78b4))
- Fix: TASK-017 — Remove unused connection/config params from OrgWideCostHandler and DefaultHandler

- OrgWideCostHandler.__init__ now takes only ecosystem param
- DefaultHandler.__init__ same
- Removed dead TYPE_CHECKING imports (CCloudConnection, CCloudPluginConfig) from both handlers
- Updated plugin.py construction calls to pass only ecosystem
- Removed misleading docstring sentence from DefaultHandler
- Added constructor tests verifying new signature and rejecting old kwargs ([519d894](https://github.com/waliaabhishek/chitragupta/commit/519d894c6dafa0807fec92eadff1296dd1c8ca31))
- Fix: TASK-016 — Extract shared MetricsConnectionConfig and create_metrics_source factory

Consolidates duplicate Prometheus config models and factory logic from both
plugins into core.metrics.config. CCloudMetricsConfig and MetricsConfig were
byte-for-byte identical; inline factory blocks performed same unwrap/build
sequence. ([c5a1649](https://github.com/waliaabhishek/chitragupta/commit/c5a16499c7d3b00192d7fbeaa572da2aed3a60c8))
- Fix: TASK-015 — Standardize API routes on utc_today() instead of date.today()

Extract utc_today() helper in dependencies.py, replace date.today() in
billing, aggregation, tags, export routes. Update chargebacks to use
shared helper. Removes inline import in tags.py. All routes now use
UTC-based date for default windows, preventing timezone drift. ([407eb97](https://github.com/waliaabhishek/chitragupta/commit/407eb97dce1ee1e172957fad1e47d9b82bc8fd09))
- Fix: TASK-012 — Add close() to EcosystemPlugin/MetricsSource protocols, remove hasattr duck-typing

Add close() lifecycle method to both protocols, retype TenantRuntime.plugin
from object to EcosystemPlugin under TYPE_CHECKING, remove all hasattr guards
in TenantRuntime.close(), and add _metrics_source cleanup to both plugin
close() methods to prevent HTTP connection leaks. ([632e60a](https://github.com/waliaabhishek/chitragupta/commit/632e60a499c88b8363c7bdf5f41916ae89c0e5a5))
- Fix: TASK-011 — Move CCloud-specific enums out of core AllocationDetail

Remove NO_FLINK_STMT_NAME_TO_OWNER_MAP, FAILED_TO_LOCATE_FLINK_STATEMENT_OWNER,
and CLUSTER_LINKING_COST from core AllocationDetail enum (DIP violation).
Create plugins/confluent_cloud/constants.py with plain string constants.
Delete FAILED_TO_LOCATE_FLINK_STATEMENT_OWNER (dead code, no consumers). ([fd24249](https://github.com/waliaabhishek/chitragupta/commit/fd24249b0ebbbd8c9f486cd42d4033827eeaa892))
- Fix: TASK-010 — Validate plugin_settings at config load via PluginSettingsBase

Add PluginSettingsBase Pydantic model with orchestrator-consumed fields
(allocator_params, allocator_overrides, identity_resolution_overrides,
min_refresh_gap_seconds). TenantConfig.plugin_settings now validates at
load time instead of silently accepting any dict. Plugin configs inherit
PluginSettingsBase. Orchestrator uses typed attribute access. ([121ccf5](https://github.com/waliaabhishek/chitragupta/commit/121ccf51a54ebe11da4bc143ef9c37a1c34fabfe))
- Fix: TASK-004 — Replace sys.exit(1) with tenant suspension in WorkflowRunner

sys.exit(1) in worker threads is silently swallowed by CPython. This fix:
- Removes sys.exit(1) from _run_tenant, lets GatherFailureThresholdError propagate
- Adds _failed_tenants dict with lock for thread-safe tenant suspension tracking
- Adds _build_cached_fatal_result() and _mark_tenant_permanently_failed() helpers
- Updates run_once() and run_tenant() to skip/handle permanently failed tenants
- Adds get_failed_tenants() API for visibility into suspended tenants
- Adds all-tenants-failed CRITICAL alert in run_loop()
- Adds fatal: bool field to PipelineRunResult

9 new tests verify threshold breach handling, tenant skip logic, thread safety. ([7431224](https://github.com/waliaabhishek/chitragupta/commit/7431224d6e168e3287c49854a2ec11946748ed03))
- Fix: TASK-003 — Plugin discovery contract now works for both plugins

Both plugins now expose register() -> tuple[str, Callable] matching the
loader contract. Previously self_managed_kafka had wrong signature
(took registry arg) and confluent_cloud had no register() at all.

- self_managed_kafka/__init__.py: register() returns tuple, no args
- confluent_cloud/__init__.py: added register() returning tuple
- 16 new tests covering registration and discovery
- Removed stale tests calling old broken signature ([68da3db](https://github.com/waliaabhishek/chitragupta/commit/68da3db56623b2e590b200f5f57f80f8f0bca19a))
- Fix: TASK-005 — Single shared WorkflowRunner in both mode + per-tenant run guard

- Extract _create_runner() factory in main.py
- run_worker() accepts injected runner + shutdown_event for both mode
- Add _running_tenants set + lock to WorkflowRunner for per-tenant concurrency guard
- Add is_tenant_running() method for API pre-check
- Add drain(timeout) for graceful shutdown (waits for running tenants before close)
- Add already_running field to PipelineRunResult
- Add "skipped" status to PipelineRun for already-running cases
- trigger_pipeline returns 200 with status="already_running" when tenant is running
- app.py lifespan calls drain() via asyncio.to_thread before disposing backends ([a333499](https://github.com/waliaabhishek/chitragupta/commit/a3334998ae2e67d70febd7d559e5157ca2ca0482))
- Fix: TASK-001 — Flink fallback statement filter now uses is_stopped boolean

The _fallback_from_running_statements filter checked metadata["status"]
which gathering never writes. Replaced with metadata["is_stopped"] (bool)
to match what gathering actually populates, aligning with reference code
behavior. Updated existing test fixtures and added 4 targeted tests. ([a895d3f](https://github.com/waliaabhishek/chitragupta/commit/a895d3fbdff70874290d326503d2b08ddb3ae155))
- Fix: GAP-25 — Add X-RateLimit-Reset header support for rate limit handling

CCloudConnection now parses the legacy X-RateLimit-Reset header (Unix timestamp)
when handling 429 responses. The server-provided reset time is used instead of
falling back to generic exponential backoff. ([d3c4761](https://github.com/waliaabhishek/chitragupta/commit/d3c4761b00556055129d9b359a837436701f608d))
- Fix: GAP-24 — Connector fallback now uses resource-local instead of tenant-period

When no active identities are found for a connector, cost is now assigned
to the connector resource itself (identity_id=resource_id) instead of being
spread across all tenant identities. This matches legacy behavior where
unresolved connectors stay resource-local, not tenant-smeared. ([be33aa8](https://github.com/waliaabhishek/chitragupta/commit/be33aa827e2e3294a608bfecde3b73d34b107675))
- Fix: GAP-23 — Filter system identities from tenant-period cache

UNALLOCATED (identity_type="system") was leaking into tenant_period cache,
causing allocators to split costs N+1 ways instead of N ways. Fixed by
filtering system identities at cache population time in orchestrator. ([bcef628](https://github.com/waliaabhishek/chitragupta/commit/bcef6286ddaaafe2b0297c8d8015b71c6c7bc828))
- Fix: GAP-22 — Decimal split helpers can crash on non-0.0001 amounts

Add quantize-before-split and modulo wraparound to split_amount_evenly
and allocate_by_usage_ratio remainder loops. Prevents IndexError when
input amounts have precision beyond the 0.0001 quantization step. ([f0230c7](https://github.com/waliaabhishek/chitragupta/commit/f0230c74d4825deb638f01194c254ac7e40b68d7))
- Fix: GAP-21 — run_tenant() bootstrap flag now properly latched

Replace inline bootstrap in run_tenant() with delegation to
bootstrap_storage(). Ensures all tenants are bootstrapped and
_bootstrapped flag is set, preventing redundant DDL on subsequent calls. ([9aca32b](https://github.com/waliaabhishek/chitragupta/commit/9aca32b170a161e4d31224b8d02d97267d266f76))
- Fix: GAP-20 — Allocation retry attempts now persist across transaction rollback

When an allocator fails, the retry counter increment now uses a separate
UoW that commits independently, ensuring the attempt count survives the
main transaction's rollback. This enables proper escalation to UNALLOCATED
after the configured retry limit is reached. ([d915eef](https://github.com/waliaabhishek/chitragupta/commit/d915eef9564a6537ef6cf2e40c9f54962d11b24b))
- Fix: GAP-19 — Flink query uses shared resource-filter injection contract

Replace hardcoded {resource_id=~"lfcp-.+"} selector with {} placeholder
in both _FLINK_METRICS_PRIMARY and _FLINK_METRICS_FALLBACK. This enables
the shared _inject_resource_filter() mechanism to inject per-pool filters
consistently with other handlers. ([540999b](https://github.com/waliaabhishek/chitragupta/commit/540999b8e511ebba01246585200fd4a6cc961530))
- Fix: GAP-18 — Metrics query instant vs range mode support

Add query_mode field to MetricQuery with "instant" and "range" options.
CCloud handlers now use instant queries for parity with reference code.

- MetricQuery.query_mode defaults to "range" (preserves existing behavior)
- PrometheusMetricsSource routes by query_mode to /api/v1/query or /api/v1/query_range
- Extracted _execute_cached_post for DRY cache handling
- CCloud Kafka and Flink metrics use query_mode="instant"
- 13 new tests for instant mode routing, caching, and error handling ([53029c5](https://github.com/waliaabhishek/chitragupta/commit/53029c58336a260ceef2c35c7d2e85872f3f2d1b))
- Fix: GAP-17 — Malformed billing handling can drop or collide costs

- Add row_index parameter to _map_billing_item for unique fallback IDs
- Add _map_malformed_item to preserve hard-failure rows with metadata flag
- Update _fetch_window to use enumerate and yield malformed items instead of dropping
- Malformed rows get resource_id=malformed_billing_{idx} and metadata["malformed"]=True ([6897b78](https://github.com/waliaabhishek/chitragupta/commit/6897b78ad3a381c3631680c61bdf7eb35a531bd3))
- Fix: GAP-16 — Flink CFU metric name fallback support

Add dual-metric query support for Flink CFU metrics. Primary metric
(confluent_flink_num_cfu) takes precedence; falls back to legacy metric
(confluent_flink_statement_utilization_cfu_minutes_consumed) for tenants
still exporting the old metric name. Pre-filters metrics_data to prevent
double-counting when both metrics present. ([dd5589b](https://github.com/waliaabhishek/chitragupta/commit/dd5589bda85b257e9304f5b6fe6a541c631a2c97))
- Fix: GAP-15 — ksqlDB owner resolution reads wrong field

Read owner_id from top-level Resource field instead of metadata dict,
with metadata fallback for legacy resources. Also moved sentinel helper
to shared _identity_helpers module and applied Pythonic improvements. ([d4ea7d5](https://github.com/waliaabhishek/chitragupta/commit/d4ea7d558af72fde12081ff67d7127676d6f9b34))
- Fix: GAP-14 — Kafka network direction-specific allocation

Handler returns direction-specific metrics: READ → bytes_out only,
WRITE → bytes_in only, CKU → both. Prevents cross-attribution of
asymmetric producer/consumer traffic. ([f36090e](https://github.com/waliaabhishek/chitragupta/commit/f36090e2d55d12ba4d1c9accb7c0e1b06bd474bd))
- Fix: GAP-13 — Self-managed Kafka correctness issues

- Issue 1: Split self_kafka_network_allocator into ingress/egress variants
  to correctly attribute directional network costs (was conflating bytes_in
  and bytes_out, giving 50/50 split regardless of actual usage direction)
- Issue 2: Rename _BYTES_PER_GB to _BYTES_PER_GIB and config fields
  storage_per_gb_hourly, network_*_per_gb to _per_gib (breaking change:
  matches actual 2^30 math being performed)
- Issue 3: Add principal label validation in plugin.initialize() with
  static fallback when Prometheus lacks principal labels or is unreachable ([22860f0](https://github.com/waliaabhishek/chitragupta/commit/22860f0dbff9c75a4020162c0a422648ae470f9c))
- Fix: GAP-12 — Connector auth mode fallback probing + masked key detection

- Add auth mode fallback probing in gather_connectors(): when kafka.auth.mode
  is absent, probe for kafka.api.key or kafka.service.account.id to infer mode
- Add masked API key detection: keys that are all asterisks or empty string
  now return connector_api_key_masked sentinel instead of shared unknown
- Add distinct connector_api_key_not_found sentinel for keys not in DB
- UNKNOWN auth mode now uses connector_id as identity for per-connector
  attribution instead of shared connector_credentials_unknown sentinel ([edafc7f](https://github.com/waliaabhishek/chitragupta/commit/edafc7f6098677105d6644fde179e2dcc85248cd))
- Fix: GAP-11 — ksqlDB allocator fallback cost type semantics

ksqlDB CSU allocator now uses correct cost types for each fallback tier:
- merged_active identities → USAGE (attributed consumption)
- tenant_period fallback → SHARED (can't attribute specifically)
- No identities → assign to resource_id with SHARED via allocate_to_resource

Previously all paths were forced to USAGE, losing the semantic distinction
between attributed and unattributed costs. ([7cf3364](https://github.com/waliaabhishek/chitragupta/commit/7cf3364236ac8e437b733d6a6891d2fcbd9a156b))
- Fix: GAP-10 — Identity pool gathering and org-wide allocator deduplication

- Add gather_identity_providers + gather_identity_pools calls to KafkaHandler
- Add IdentitySet.ids_by_type() for filtering by identity type
- Filter org_wide_allocator to owner types (SA, user, pool) — excludes API keys
- Filter connector_allocators tenant_period fallback to owner types
- Add OWNER_IDENTITY_TYPES constant to eliminate DRY violation ([d50c451](https://github.com/waliaabhishek/chitragupta/commit/d50c451f5c4575bcf587bcbff0cc27b47c77738b))
- Fix: GAP-09 — Flink no-metrics fallback for cost attribution

Add secondary fallback path when Prometheus metrics are unavailable:
- Identity resolution queries running Flink statements from resource DB
- Allocator falls back to even split across merged_active identities
- Both paths preserve USAGE cost type for Flink consumption costs ([d10aca6](https://github.com/waliaabhishek/chitragupta/commit/d10aca6c488199213492958b621ef4aebe76d67f))
- Fix: GAP-08 — Add missing CUSTOM_CONNECT product types

Add CUSTOM_CONNECT_NUM_TASKS and CUSTOM_CONNECT_THROUGHPUT to
ConnectorHandler. Both map to connect_tasks_allocator (USAGE cost type),
matching reference code behavior. ([b1169c0](https://github.com/waliaabhishek/chitragupta/commit/b1169c062bd9ca396ceba7ce5a2080e843088383))
- Fix: GAP-07 — Per-date resource lookup cache

Pre-fetch resources per billing window and pass as dict to
_process_billing_line, eliminating redundant uow.resources.get()
calls. 10 billing lines sharing same resource_id now trigger
1 find_by_period instead of 10 get() calls. ([a952858](https://github.com/waliaabhishek/chitragupta/commit/a952858cb87a134ae9efef47e140900f2fa4149f))
- Fix: GAP-06 — Prometheus cache TTL alignment with LRU eviction

- Change cache_ttl_seconds default from 300s to 3600s (survives 30-min run cycle)
- Support cache_ttl_seconds=None for lifetime caching (matches reference lru_cache)
- Replace dict cache with OrderedDict for proper LRU semantics
- Add move_to_end() on cache hit for LRU promotion
- Add popitem(last=False) when full for LRU eviction (replaces skip-caching)
- Add race-condition guard in cache-store to handle concurrent fetches ([88dbfe8](https://github.com/waliaabhishek/chitragupta/commit/88dbfe8edcd5786d13a810debcabdbf40a104507))
- Fix: GAP-05 — Per-endpoint page size tuning for CCloud API

Add endpoint-specific page_size overrides to prevent timeouts on complex
Flink endpoints and align with reference code behavior:
- Flink compute pools: 50 (complex nested objects)
- Flink statements: 50 (complex nested objects)
- ksqlDB clusters: 100 (moderate complexity)
- API keys: 100 (moderate complexity)
- Schema Registry: 50 (moderate complexity) ([0f87905](https://github.com/waliaabhishek/chitragupta/commit/0f879052728cf9dd71aedf1182c0528e11268232))
- Fix: GAP-03 test logging pollution from alembic

Alembic's command.upgrade/downgrade calls logging.config.fileConfig()
which sets disable_existing_loggers=True, disabling the
core.storage.backends.sqlmodel.repositories logger. This broke the
billing revision test when run after migration tests.

Add fixture to restore logger state after each migration test. ([1a2338a](https://github.com/waliaabhishek/chitragupta/commit/1a2338a346f3e2213a8e7f2f6e794e18bf6ddddd))
- Fix: GAP-04 — API object refresh throttle with failure escalation

Add 30-minute (configurable) throttle to prevent excessive CCloud API calls
during orchestrator gather phase. Skip resource/identity/billing gather if
last successful gather was within min_refresh_gap.

Additionally, add gather failure escalation: after N consecutive gather
failures (default 5, configurable via gather_failure_threshold), raise
GatherFailureThresholdError causing program exit for operator attention.

- Add _last_resource_gather_at and _min_refresh_gap to orchestrator
- Add gather_failure_threshold to TenantConfig
- Add GatherFailureThresholdError exception with sys.exit(1) handler
- 10 new tests covering throttle and escalation behavior ([85dcff1](https://github.com/waliaabhishek/chitragupta/commit/85dcff1805500b892d98e936055221b0484ea347))
- Fix: GAP-03 — Billing revision detection with logging

Add change detection to SQLModelBillingRepository.upsert():
- Check for existing record before merge
- Log warning when total_cost differs (billing revision detected)
- Allow overwrite per approved design divergence

Tests: 3 new tests covering revision detection, no-op for same cost,
and no warning for new records. ([b9ba1d5](https://github.com/waliaabhishek/chitragupta/commit/b9ba1d5814a08fbe972a9ae3589a4f77e6499625))
- Fix: GAP-02 — Default/cluster-linking allocators assign to resource_id

Allocators now assign costs to billing_line.resource_id instead of
UNALLOCATED. Preserves resource lineage for Tableflow and cluster-linking
costs that can't be attributed to specific identities. ([cface12](https://github.com/waliaabhishek/chitragupta/commit/cface128ed808fc8c8aab98d9730b02dffe5f51e))
- Fix: GAP-01 — Network allocator tiered fallback for audit granularity

Adds 7-branch tiered fallback to Kafka network allocators matching reference
code behavior. Each fallback path now writes a distinct AllocationDetail code
so auditors can determine why a particular allocation path was taken.

Key changes:
- Add 3 AllocationDetail enum values for Tier 2/3 fallback branches
- Replace _kafka_shared_allocation with _fallback_no_metrics + _fallback_zero_usage
- Add _even_split_with_detail and _to_resource_with_detail helpers
- Terminal fallback assigns to resource_id (not UNALLOCATED)
- 12 new tests covering all 7 reachable branches + edge cases
- 100% coverage on kafka_allocators.py ([dfbd2d1](https://github.com/waliaabhishek/chitragupta/commit/dfbd2d148354961b8958f9443c0972fc5c37c053))
- Fix(ccloud): extract shared sentinel helper, fix connector identity nits ([433c0c6](https://github.com/waliaabhishek/chitragupta/commit/433c0c6ca9e935f921d15e31aed6cbd8e03ef944))
- Fix(ksqldb): address nits from quality review

- Replace redundant comment on cost type override with one that explains
  WHY allocate_evenly's SHARED default is overridden (compute consumption
  semantics require USAGE)
- Move ksqldb_csu_allocator import to module level in test file; remove
  six repeated local imports inside test methods ([fce1733](https://github.com/waliaabhishek/chitragupta/commit/fce17339d83c444ad10ab48144cff74a5b6686d3))
- Fix(kafka): correct PromQL query templates for resource filtering

Bug: Query templates used invalid {{kafka_id="{resource_id}"}} syntax.
- Double braces don't escape in regular strings (only f-strings/.format())
- {resource_id} and {step} placeholders were never substituted
- Result was invalid PromQL sent to Prometheus

Fix:
- Use proper {} placeholder pattern matching reference code
- _inject_resource_filter replaces {} with {kafka_id="lkc-xxx"}
- Corrected metric names: request_bytes/response_bytes (per reference)

This ensures metrics are filtered to the correct cluster and uses
valid PromQL syntax that Prometheus can parse. ([9bbe80d](https://github.com/waliaabhishek/chitragupta/commit/9bbe80dfc79c41f35be3422cd61aa93dffa80793))
- Fix: address review issues for chunk 2.2

MAJORS:
- Rename gather() params to match CostInput protocol (start/end)
- Fix uow type annotation (UnitOfWork, not Optional)

MINORS:
- Remove dead Z replace in _parse_iso_datetime (Python 3.14)
- Add comment for connector created_at omission
- Use json.dumps for deterministic hash in flink sentinel
- Replace global _tenant_counter with uuid

NITS:
- Add ECOSYSTEM constant comment
- Move _last_request_time update after request
- Add Flink cache key assumption comment
- Remove ValueError from _safe_decimal except
- Add floor guard to _get_rate_limit_wait
- Move cast to module-level import
- Remove redundant continue ([daec279](https://github.com/waliaabhishek/chitragupta/commit/daec2799bd8e93885513386b76e3be1fdcfeba9c))
- Fix(config): reject shared connection strings across tenants

Tenant isolation is convention-enforced (query-level), not structural.
Until full isolation is implemented (TD-025), prevent two tenants from
sharing a storage connection_string at config validation time. ([bd22efc](https://github.com/waliaabhishek/chitragupta/commit/bd22efc0ab4ab2e7e5adfc31db7cf5ba94ed7792))
- Fix: post-review hardening round 2

- Move create_tables() to bootstrap_storage() — called once at startup,
  not per-tenant per-cycle. run_once auto-bootstraps if not already done.
- Parenthesized except + fmt:skip for Python 3.12/3.13 compatibility
- Single-cycle path now logs results via shared _log_results()
- DRY: extract _log_results() used by both loop and single-cycle paths
- 4 new bootstrap tests, 1 new single-cycle logging test ([53106ee](https://github.com/waliaabhishek/chitragupta/commit/53106eef7b548be5115bd5aac4050782a4068d57))
- Fix: post-review hardening (items 1,3,7)

- loading.py: parenthesized except + fmt:skip for 3.12/3.13 compat
- workflow_runner.py: single-cycle path now logs results (parity with loop) ([d9aa30e](https://github.com/waliaabhishek/chitragupta/commit/d9aa30edf78a3ec13af3074388b50cce73d1950f))
- Fix(runner): move timeout=0 comment to effective_timeout line (AR-002) ([12852d1](https://github.com/waliaabhishek/chitragupta/commit/12852d1c1f247a9b84e834b027d5eb3cff44cea1))
- Fix(orchestrator): GAP-001+004+006 — resources_gathered per billing date, UTC reassignment, error propagation

GAP-001: Mark resources_gathered per billing date, not just today.
GAP-004: Reassign _ensure_utc() return values using dataclasses.replace().
GAP-006: _gather() returns errors list, propagated to PipelineRunResult. ([a9e00ba](https://github.com/waliaabhishek/chitragupta/commit/a9e00baa3dd6cc496cc44ba60cbaa1a156e1602e))
- Fix(mappers): reject naive datetimes on write path (GAP-014)

Split ensure_utc into permissive (read) and strict (write).
Write-path _to_table() functions now raise on naive datetimes.
Read-path _to_domain() functions remain permissive for DB compat. ([b06b855](https://github.com/waliaabhishek/chitragupta/commit/b06b855906e6d16dedf0fd2997752ccd7e724cde))
- Fix(helpers): use UNALLOCATED fallback in allocators (GAP-013) ([5f74e9e](https://github.com/waliaabhishek/chitragupta/commit/5f74e9eacb28c0a8e7860a97299164e389d1c57c))
- Fix(loading): correct Python 2 except syntax (GAP-012)

except ValueError, TypeError → except (ValueError, TypeError) ([03ac8cd](https://github.com/waliaabhishek/chitragupta/commit/03ac8cd11df23163349a94e081f574689bf809df))
- Fix(ccloud): type safety and lint fixes for chunk 2.1

- Add cast() for resp.json() return (mypy no-any-return)
- Add explicit type annotations in _calculate_backoff
- Add validation for auth_type='none' with credentials
- Add compare=False to _auth field
- Constrain allocator_params to primitive types with validator
- Add responses library for HTTP mocking in tests ([b5806d9](https://github.com/waliaabhishek/chitragupta/commit/b5806d95a63216fa0f42333c5e33c1be54444e19))


### Security
- Security: TASK-111 — Add .env exclusion, Dependabot, and dependency auditing

- Add .env* glob to .gitignore to prevent accidental secret commits
- Add .github/dependabot.yml for weekly pip ecosystem CVE scanning
- Add uv audit --frozen step to CI workflow (non-blocking initially) ([8ff2845](https://github.com/waliaabhishek/chitragupta/commit/8ff284555b58b187dc837e05c40e91dc0e7b5210))




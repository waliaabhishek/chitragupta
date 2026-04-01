from __future__ import annotations

import logging
from collections import defaultdict
from datetime import UTC, date, datetime, timedelta
from typing import TYPE_CHECKING, Any

from core.emitters.drivers import LifecycleDriver, PerDateDriver
from core.emitters.models import EmissionRecord, EmitManifest, EmitOutcome
from core.emitters.protocols import (
    ExpositionEmitter,
    LifecycleEmitter,
    PipelineAggregatedRowFetcher,
    PipelineDateSource,
    PipelineEmitterBuilder,
    PipelineRowFetcher,
    RowProvider,
)

if TYPE_CHECKING:
    from core.config.models import EmitterSpec
    from core.plugin.protocols import Emitter
    from core.storage.interface import StorageBackend

logger = logging.getLogger(__name__)

_GRANULARITY_ORDER: dict[str, int] = {"hourly": 0, "daily": 1, "monthly": 2}


class EmitterRunner:
    """Independent post-pipeline runner. Reads pipeline data from DB and drives emitters."""

    def __init__(
        self,
        ecosystem: str,
        storage_backend: StorageBackend,
        emitter_specs: list[EmitterSpec],
        date_source: PipelineDateSource,
        row_fetcher: PipelineRowFetcher[Any],
        emitter_builder: PipelineEmitterBuilder,
        pipeline: str,
        chargeback_granularity: str | None = None,
    ) -> None:
        if chargeback_granularity is not None:
            cb_level = _GRANULARITY_ORDER.get(chargeback_granularity, 0)
            for spec in emitter_specs:
                if spec.aggregation is not None:
                    req_level = _GRANULARITY_ORDER.get(spec.aggregation, -1)
                    if req_level < cb_level:
                        raise ValueError(
                            f"Emitter {spec.type!r} requests aggregation {spec.aggregation!r} "
                            f"which is finer than chargeback_granularity {chargeback_granularity!r}."
                        )
        self._ecosystem = ecosystem
        self._storage_backend = storage_backend
        self._emitter_specs = emitter_specs
        self._date_source = date_source
        self._row_fetcher = row_fetcher
        self._emitter_builder = emitter_builder
        self._pipeline = pipeline

    def run(self, tenant_id: str) -> None:
        """Emit all pending dates for one tenant. Idempotent — skips already-emitted dates."""
        all_dates = self._date_source.get_distinct_dates(self._ecosystem, tenant_id)
        for spec in self._emitter_specs:
            self._run_spec(tenant_id, spec, all_dates)

    def _run_spec(self, tenant_id: str, spec: EmitterSpec, all_dates: list[date]) -> None:
        with self._storage_backend.create_unit_of_work() as uow:
            emitted = uow.emissions.get_emitted_dates(self._ecosystem, tenant_id, spec.name, self._pipeline)
            failed = uow.emissions.get_failed_dates(self._ecosystem, tenant_id, spec.name, self._pipeline)

        if spec.lookback_days is not None:
            cutoff = (datetime.now(UTC) - timedelta(days=spec.lookback_days)).date()
            candidate_dates = [d for d in all_dates if d >= cutoff]
        else:
            candidate_dates = all_dates

        pending = [d for d in candidate_dates if d not in emitted]
        if not pending:
            return

        emitter = self._emitter_builder.build(spec)

        if spec.aggregation == "monthly":
            outcomes = self._run_monthly(tenant_id, emitter, pending, failed)
        else:
            manifest = EmitManifest(
                pending_dates=pending,
                total_rows_estimate=None,
                is_reemission=bool(failed & set(pending)),
            )

            def _row_provider(tid: str, dt: date) -> list[Any]:
                return self._fetch_rows(tid, dt, spec)

            if isinstance(emitter, ExpositionEmitter):
                outcomes = self._run_exposition(tenant_id, emitter, manifest, _row_provider)
            elif isinstance(emitter, LifecycleEmitter):
                outcomes = LifecycleDriver(emitter).run(tenant_id, manifest, _row_provider)
            else:
                outcomes = PerDateDriver(emitter).run(tenant_id, manifest, _row_provider)

        self._persist_outcomes(tenant_id, spec.name, outcomes)

    def _run_monthly(
        self,
        tenant_id: str,
        emitter: Emitter | LifecycleEmitter[Any] | ExpositionEmitter[Any],
        pending: list[date],
        failed: set[date],
    ) -> dict[date, EmitOutcome]:
        """Group pending dates by month, emit once per month, record outcome for each date.

        Dispatches by emitter type — LifecycleEmitter and ExpositionEmitter don't implement
        __call__, so plain-callable dispatch would TypeError at runtime.
        """
        month_groups: dict[date, list[date]] = defaultdict(list)
        for d in pending:
            month_groups[d.replace(day=1)].append(d)

        outcomes: dict[date, EmitOutcome] = {}
        for month_start, dates_in_month in sorted(month_groups.items()):
            # Uniform month-end arithmetic — no December special-case needed
            next_month_start = (month_start.replace(day=28) + timedelta(days=4)).replace(day=1)
            month_end = next_month_start - timedelta(days=1)

            if not isinstance(self._row_fetcher, PipelineAggregatedRowFetcher):
                raise ValueError(
                    "Monthly aggregation requested but the configured row_fetcher does not support fetch_aggregated."
                )
            rows = self._row_fetcher.fetch_aggregated(self._ecosystem, tenant_id, month_start, month_end, "monthly")

            if not rows:
                outcome = EmitOutcome.SKIPPED
            elif isinstance(emitter, ExpositionEmitter):
                # Default-arg capture binds `rows` at definition time, avoiding late-binding closure.
                def _month_provider(tid: str, dt: date, _rows: list[Any] = rows) -> list[Any]:
                    return _rows

                month_manifest = EmitManifest(
                    pending_dates=[month_start],
                    total_rows_estimate=len(rows),
                    is_reemission=bool(failed & set(dates_in_month)),
                )
                monthly_outcomes = self._run_exposition(tenant_id, emitter, month_manifest, _month_provider)
                outcome = monthly_outcomes.get(month_start, EmitOutcome.SKIPPED)
            elif isinstance(emitter, LifecycleEmitter):
                month_manifest = EmitManifest(
                    pending_dates=[month_start],
                    total_rows_estimate=len(rows),
                    is_reemission=bool(failed & set(dates_in_month)),
                )
                try:
                    emitter.open(tenant_id, month_manifest)
                    emitter.emit(tenant_id, month_start, rows)
                    result = emitter.close(tenant_id)
                    outcome = result.outcomes.get(month_start, EmitOutcome.EMITTED)
                except Exception:
                    logger.exception("Monthly LifecycleEmitter failed for tenant=%s month=%s", tenant_id, month_start)
                    outcome = EmitOutcome.FAILED
            else:
                # Plain Emitter protocol — __call__(tenant_id, date, rows)
                try:
                    emitter(tenant_id, month_start, rows)
                    outcome = EmitOutcome.EMITTED
                except Exception:
                    logger.exception("Monthly emitter failed for tenant=%s month=%s", tenant_id, month_start)
                    outcome = EmitOutcome.FAILED

            # Record outcome for ALL chargeback dates in this month group
            for d in dates_in_month:
                outcomes[d] = outcome

        return outcomes

    def _fetch_rows(self, tenant_id: str, dt: date, spec: EmitterSpec) -> list[Any]:
        """Fetch rows for a single date. For daily aggregation, uses SQL GROUP BY."""
        if spec.aggregation == "daily":
            if not isinstance(self._row_fetcher, PipelineAggregatedRowFetcher):
                raise ValueError(
                    f"Emitter spec {spec.name!r} requests aggregation='daily' "
                    "but the configured row_fetcher does not support fetch_aggregated."
                )
            return self._row_fetcher.fetch_aggregated(self._ecosystem, tenant_id, dt, dt, "daily")
        return self._row_fetcher.fetch_by_date(self._ecosystem, tenant_id, dt)

    def _run_exposition(
        self,
        tenant_id: str,
        emitter: ExpositionEmitter[Any],
        manifest: EmitManifest,
        row_provider: RowProvider[Any],
    ) -> dict[date, EmitOutcome]:
        emitter.load(tenant_id, manifest, row_provider)
        consumed = emitter.get_consumed(tenant_id)
        return {d: EmitOutcome.EMITTED if d in consumed else EmitOutcome.SKIPPED for d in manifest.pending_dates}

    def _persist_outcomes(
        self,
        tenant_id: str,
        emitter_name: str,
        outcomes: dict[date, EmitOutcome],
    ) -> None:
        with self._storage_backend.create_unit_of_work() as uow:
            for dt, outcome in outcomes.items():
                if outcome != EmitOutcome.SKIPPED:
                    uow.emissions.upsert(
                        EmissionRecord(
                            ecosystem=self._ecosystem,
                            tenant_id=tenant_id,
                            emitter_name=emitter_name,
                            pipeline=self._pipeline,
                            date=dt,
                            status=outcome.value,
                        )
                    )
            uow.commit()

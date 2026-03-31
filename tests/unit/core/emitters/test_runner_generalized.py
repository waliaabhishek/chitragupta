from __future__ import annotations

from collections import defaultdict
from datetime import UTC, date, datetime, timedelta
from typing import Any
from unittest.mock import MagicMock

import pytest

ECOSYSTEM = "test-eco"
TENANT_ID = "tenant-1"


# ---------- pipeline-aware mocks ----------


class MockEmissionRepoV2:
    """In-memory emission repo with pipeline discrimination."""

    def __init__(self) -> None:
        self._records: list[Any] = []
        # keyed by (pipeline, emitter_name)
        self._emitted: dict[tuple[str, str], set[date]] = defaultdict(set)
        self._failed: dict[tuple[str, str], set[date]] = defaultdict(set)

    def upsert(self, record: Any) -> None:
        self._records.append(record)
        key = (record.pipeline, record.emitter_name)
        if record.status == "emitted":
            self._emitted[key].add(record.date)
        elif record.status == "failed":
            self._failed[key].add(record.date)

    def get_emitted_dates(self, ecosystem: str, tenant_id: str, emitter_name: str, pipeline: str) -> set[date]:
        return self._emitted.get((pipeline, emitter_name), set()).copy()

    def get_failed_dates(self, ecosystem: str, tenant_id: str, emitter_name: str, pipeline: str) -> set[date]:
        return self._failed.get((pipeline, emitter_name), set()).copy()


class MockUnitOfWorkV2:
    def __init__(self, emission_repo: MockEmissionRepoV2) -> None:
        self.emissions = emission_repo

    def __enter__(self) -> MockUnitOfWorkV2:
        return self

    def __exit__(self, *args: Any) -> None:
        pass

    def commit(self) -> None:
        pass


class MockStorageBackendV2:
    """Storage backend that uses MockEmissionRepoV2 (no chargeback repo needed)."""

    def __init__(self, emission_repo: MockEmissionRepoV2 | None = None) -> None:
        self._emission_repo = emission_repo or MockEmissionRepoV2()

    def create_unit_of_work(self) -> MockUnitOfWorkV2:
        return MockUnitOfWorkV2(self._emission_repo)


class MockPipelineDateSource:
    def __init__(self, dates: list[date]) -> None:
        self._dates = dates

    def get_distinct_dates(self, ecosystem: str, tenant_id: str) -> list[date]:
        return sorted(self._dates)


class MockPipelineRowFetcher:
    """Implements PipelineRowFetcher only — no fetch_aggregated."""

    def fetch_by_date(self, ecosystem: str, tenant_id: str, dt: date) -> list[Any]:
        return [object()]  # non-empty stub — PerDateDriver skips empty rows


class MockPipelineEmitterBuilder:
    def __init__(self, emitter_fn: Any) -> None:
        self._emitter_fn = emitter_fn

    def build(self, spec: Any) -> Any:
        return self._emitter_fn


def _make_spec(
    name: str = "mock-emitter",
    aggregation: str | None = None,
    lookback_days: int | None = None,
) -> Any:
    from core.config.models import EmitterSpec

    return EmitterSpec(type=name, name=name, aggregation=aggregation, lookback_days=lookback_days)


def _noop_emitter(tenant_id: str, dt: date, rows: Any) -> None:
    pass


# ---------- Test 1: Chargeback idempotency regression ----------


class TestChargebackIdempotencyRegression:
    def test_second_run_emits_zero_dates(self) -> None:
        from core.emitters.runner import EmitterRunner

        call_log: list[date] = []

        def _fake_emitter(tenant_id: str, dt: date, rows: Any) -> None:
            call_log.append(dt)

        dates = [date(2025, 1, 1), date(2025, 1, 2), date(2025, 1, 3)]
        emission_repo = MockEmissionRepoV2()
        storage = MockStorageBackendV2(emission_repo)

        runner = EmitterRunner(
            ecosystem=ECOSYSTEM,
            storage_backend=storage,
            emitter_specs=[_make_spec()],
            date_source=MockPipelineDateSource(dates),
            row_fetcher=MockPipelineRowFetcher(),
            emitter_builder=MockPipelineEmitterBuilder(_fake_emitter),
            pipeline="chargeback",
        )

        runner.run(TENANT_ID)
        first_run_count = len(call_log)

        call_log.clear()
        runner.run(TENANT_ID)

        assert first_run_count == 3
        assert call_log == [], "Second run must emit zero dates — all already emitted"


# ---------- Test 2: Failed-date retry ----------


class TestFailedDateRetry:
    def test_failed_emission_record_retried_on_next_run(self) -> None:
        from core.emitters.models import EmissionRecord
        from core.emitters.runner import EmitterRunner

        call_log: list[date] = []

        def _fake_emitter(tenant_id: str, dt: date, rows: Any) -> None:
            call_log.append(dt)

        target_date = date(2025, 6, 15)
        emission_repo = MockEmissionRepoV2()

        # Pre-seed a failed record for the target date
        failed_record = EmissionRecord(
            ecosystem=ECOSYSTEM,
            tenant_id=TENANT_ID,
            emitter_name="mock-emitter",
            pipeline="chargeback",
            date=target_date,
            status="failed",
        )
        emission_repo.upsert(failed_record)

        storage = MockStorageBackendV2(emission_repo)
        runner = EmitterRunner(
            ecosystem=ECOSYSTEM,
            storage_backend=storage,
            emitter_specs=[_make_spec()],
            date_source=MockPipelineDateSource([target_date]),
            row_fetcher=MockPipelineRowFetcher(),
            emitter_builder=MockPipelineEmitterBuilder(_fake_emitter),
            pipeline="chargeback",
        )

        runner.run(TENANT_ID)

        assert target_date in call_log, "Failed date must be re-emitted on next run"


# ---------- Test 3: Topic attribution first run ----------


class TestTopicAttributionFirstRun:
    def test_all_dates_emitted_with_topic_attribution_pipeline(self) -> None:
        from core.emitters.runner import EmitterRunner

        call_log: list[date] = []

        def _ta_emitter(tenant_id: str, dt: date, rows: Any) -> None:
            call_log.append(dt)

        dates = [date(2025, 3, d) for d in range(1, 6)]
        emission_repo = MockEmissionRepoV2()
        storage = MockStorageBackendV2(emission_repo)

        runner = EmitterRunner(
            ecosystem=ECOSYSTEM,
            storage_backend=storage,
            emitter_specs=[_make_spec()],
            date_source=MockPipelineDateSource(dates),
            row_fetcher=MockPipelineRowFetcher(),
            emitter_builder=MockPipelineEmitterBuilder(_ta_emitter),
            pipeline="topic_attribution",
        )

        runner.run(TENANT_ID)

        assert sorted(call_log) == dates, "All dates must be emitted on first run"
        written_pipelines = {r.pipeline for r in emission_repo._records}
        assert written_pipelines == {"topic_attribution"}, (
            "EmissionRecords must be written with pipeline='topic_attribution'"
        )


# ---------- Test 4: Topic attribution second run skips ----------


class TestTopicAttributionSecondRunSkips:
    def test_second_run_emits_zero_dates(self) -> None:
        from core.emitters.runner import EmitterRunner

        call_log: list[date] = []

        def _ta_emitter(tenant_id: str, dt: date, rows: Any) -> None:
            call_log.append(dt)

        dates = [date(2025, 4, d) for d in range(1, 4)]
        emission_repo = MockEmissionRepoV2()
        storage = MockStorageBackendV2(emission_repo)

        runner = EmitterRunner(
            ecosystem=ECOSYSTEM,
            storage_backend=storage,
            emitter_specs=[_make_spec()],
            date_source=MockPipelineDateSource(dates),
            row_fetcher=MockPipelineRowFetcher(),
            emitter_builder=MockPipelineEmitterBuilder(_ta_emitter),
            pipeline="topic_attribution",
        )

        runner.run(TENANT_ID)
        call_log.clear()
        runner.run(TENANT_ID)

        emitted = emission_repo.get_emitted_dates(ECOSYSTEM, TENANT_ID, "mock-emitter", "topic_attribution")
        assert emitted == set(dates)
        assert call_log == [], "Second run must emit zero dates for topic_attribution"


# ---------- Test 5: Pipeline discriminator prevents collision ----------


class TestPipelineDiscriminatorPreventsCollision:
    def test_chargeback_and_topic_attribution_records_coexist(self) -> None:
        from core.emitters.models import EmissionRecord

        emission_repo = MockEmissionRepoV2()
        target_date = date(2025, 5, 10)

        cb_record = EmissionRecord(
            ecosystem=ECOSYSTEM,
            tenant_id=TENANT_ID,
            emitter_name="shared-emitter",
            pipeline="chargeback",
            date=target_date,
            status="emitted",
        )
        ta_record = EmissionRecord(
            ecosystem=ECOSYSTEM,
            tenant_id=TENANT_ID,
            emitter_name="shared-emitter",
            pipeline="topic_attribution",
            date=target_date,
            status="emitted",
        )

        emission_repo.upsert(cb_record)
        emission_repo.upsert(ta_record)

        assert len(emission_repo._records) == 2, "Both records must be accepted — pipeline discriminates them"
        cb_emitted = emission_repo.get_emitted_dates(ECOSYSTEM, TENANT_ID, "shared-emitter", "chargeback")
        ta_emitted = emission_repo.get_emitted_dates(ECOSYSTEM, TENANT_ID, "shared-emitter", "topic_attribution")
        assert target_date in cb_emitted
        assert target_date in ta_emitted


# ---------- Test 6: lookback_days for topic attribution ----------


class TestLookbackDaysTopicAttribution:
    def test_only_last_30_days_emitted_when_90_days_available(self) -> None:
        from core.emitters.runner import EmitterRunner

        call_log: list[date] = []

        def _ta_emitter(tenant_id: str, dt: date, rows: Any) -> None:
            call_log.append(dt)

        today = datetime.now(UTC).date()
        all_dates = [today - timedelta(days=d) for d in range(90)]
        cutoff = today - timedelta(days=30)
        expected_dates = [d for d in all_dates if d >= cutoff]

        emission_repo = MockEmissionRepoV2()
        storage = MockStorageBackendV2(emission_repo)
        spec = _make_spec(lookback_days=30)

        runner = EmitterRunner(
            ecosystem=ECOSYSTEM,
            storage_backend=storage,
            emitter_specs=[spec],
            date_source=MockPipelineDateSource(all_dates),
            row_fetcher=MockPipelineRowFetcher(),
            emitter_builder=MockPipelineEmitterBuilder(_ta_emitter),
            pipeline="topic_attribution",
        )

        runner.run(TENANT_ID)

        old_dates_emitted = [d for d in call_log if d < cutoff]
        assert old_dates_emitted == [], "Dates older than lookback_days must not be emitted"
        assert set(call_log) == set(expected_dates), "All dates within lookback_days must be emitted"


# ---------- Test 7: chargeback_granularity validation still runs ----------


class TestChargebackGranularityValidationRuns:
    def test_hourly_spec_with_daily_granularity_raises_value_error(self) -> None:
        from core.emitters.runner import EmitterRunner

        spec = _make_spec(aggregation="hourly")

        with pytest.raises(ValueError):
            EmitterRunner(
                ecosystem=ECOSYSTEM,
                storage_backend=MagicMock(),
                emitter_specs=[spec],
                date_source=MockPipelineDateSource([]),
                row_fetcher=MockPipelineRowFetcher(),
                emitter_builder=MockPipelineEmitterBuilder(_noop_emitter),
                pipeline="chargeback",
                chargeback_granularity="daily",
            )


# ---------- Test 8: chargeback_granularity not validated when absent ----------


class TestChargebackGranularityAbsentNoValidation:
    def test_no_chargeback_granularity_no_value_error(self) -> None:
        from core.emitters.runner import EmitterRunner

        spec = _make_spec(aggregation="hourly")

        # No chargeback_granularity → no ValueError regardless of spec.aggregation
        runner = EmitterRunner(
            ecosystem=ECOSYSTEM,
            storage_backend=MagicMock(),
            emitter_specs=[spec],
            date_source=MockPipelineDateSource([]),
            row_fetcher=MockPipelineRowFetcher(),
            emitter_builder=MockPipelineEmitterBuilder(_noop_emitter),
            pipeline="topic_attribution",
        )
        runner.run(TENANT_ID)  # no exception = test passes


# ---------- Test 9: TopicAttributionEmitterRunner deleted ----------


class TestTopicAttributionEmitterRunnerDeleted:
    def test_topic_attribution_emitter_runner_not_importable(self) -> None:
        with pytest.raises((ImportError, AttributeError)):
            from workflow_runner import TopicAttributionEmitterRunner  # noqa: F401


# ---------- Test 10: ISP compliance ----------


class TestISPCompliance:
    def test_topic_attribution_row_fetcher_is_not_aggregated_row_fetcher(self) -> None:
        from core.emitters.protocols import PipelineAggregatedRowFetcher
        from workflow_runner import TopicAttributionRowFetcher  # type: ignore[attr-defined]

        storage = MagicMock()
        fetcher = TopicAttributionRowFetcher(storage)

        assert not hasattr(fetcher, "fetch_aggregated"), "TopicAttributionRowFetcher must not have fetch_aggregated"
        assert not isinstance(fetcher, PipelineAggregatedRowFetcher), (
            "TopicAttributionRowFetcher must not satisfy PipelineAggregatedRowFetcher"
        )


# ---------- Test 11: Aggregation guard raises on wrong fetcher ----------


class TestAggregationGuardRaisesOnWrongFetcher:
    def test_fetch_rows_raises_value_error_for_non_aggregated_fetcher_with_daily_spec(self) -> None:
        from core.emitters.runner import EmitterRunner
        from workflow_runner import TopicAttributionRowFetcher  # type: ignore[attr-defined]

        spec = _make_spec(aggregation="daily")
        storage = MockStorageBackendV2()
        ta_fetcher = TopicAttributionRowFetcher(MagicMock())

        runner = EmitterRunner(
            ecosystem=ECOSYSTEM,
            storage_backend=storage,
            emitter_specs=[spec],
            date_source=MockPipelineDateSource([date(2025, 1, 1)]),
            row_fetcher=ta_fetcher,
            emitter_builder=MockPipelineEmitterBuilder(_noop_emitter),
            pipeline="topic_attribution",
        )

        with pytest.raises(ValueError, match="fetch_aggregated"):
            runner._fetch_rows(TENANT_ID, date(2025, 1, 1), spec)


# ---------- Test 12: emission_record_to_domain round-trips pipeline ----------


class TestEmissionRecordPipelineRoundTrip:
    def test_pipeline_field_preserved_through_mapper_round_trip(self) -> None:
        from core.emitters.models import EmissionRecord
        from core.storage.backends.sqlmodel.mappers import (
            emission_record_to_domain,
            emission_record_to_table,
        )

        original = EmissionRecord(
            ecosystem=ECOSYSTEM,
            tenant_id=TENANT_ID,
            emitter_name="ta-emitter",
            pipeline="topic_attribution",
            date=date(2025, 7, 4),
            status="emitted",
        )

        table_row = emission_record_to_table(original)
        restored = emission_record_to_domain(table_row)

        assert restored.pipeline == "topic_attribution", (
            "pipeline field must survive emission_record_to_table → emission_record_to_domain round-trip"
        )


# ---------- Test 13: Existing chargeback emitter smoke test with new constructor ----------


# ---------- Test: _run_monthly ValueError guard ----------


class TestMonthlyAggregationGuardRaisesOnNonAggregatedFetcher:
    def test_run_monthly_raises_value_error_for_non_aggregated_fetcher(self) -> None:
        from core.emitters.runner import EmitterRunner

        spec = _make_spec(aggregation="monthly")
        storage = MockStorageBackendV2()

        runner = EmitterRunner(
            ecosystem=ECOSYSTEM,
            storage_backend=storage,
            emitter_specs=[spec],
            date_source=MockPipelineDateSource([date(2025, 1, 15)]),
            row_fetcher=MockPipelineRowFetcher(),  # no fetch_aggregated
            emitter_builder=MockPipelineEmitterBuilder(_noop_emitter),
            pipeline="topic_attribution",
        )

        with pytest.raises(ValueError, match="fetch_aggregated"):
            runner.run(TENANT_ID)


# ---------- Test 13: Existing chargeback emitter smoke test with new constructor ----------


class TestExistingChargebackBehaviourWithNewConstructor:
    """Smoke test: chargeback path works with the generalized constructor."""

    def test_chargeback_emitter_runner_emits_all_pending_dates(self) -> None:
        from core.emitters.runner import EmitterRunner

        call_log: list[date] = []

        def _fake_emitter(tenant_id: str, dt: date, rows: Any) -> None:
            call_log.append(dt)

        dates = [date(2025, 8, 1), date(2025, 8, 2), date(2025, 8, 3)]
        emission_repo = MockEmissionRepoV2()
        storage = MockStorageBackendV2(emission_repo)

        runner = EmitterRunner(
            ecosystem=ECOSYSTEM,
            storage_backend=storage,
            emitter_specs=[_make_spec()],
            date_source=MockPipelineDateSource(dates),
            row_fetcher=MockPipelineRowFetcher(),
            emitter_builder=MockPipelineEmitterBuilder(_fake_emitter),
            pipeline="chargeback",
            chargeback_granularity="daily",
        )

        runner.run(TENANT_ID)

        assert sorted(call_log) == dates
        assert len(emission_repo._records) == 3
        assert all(r.status == "emitted" for r in emission_repo._records)
        assert all(r.pipeline == "chargeback" for r in emission_repo._records)

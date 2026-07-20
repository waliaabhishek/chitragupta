from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from core.config.models import StorageConfig, TenantConfig
from core.models.pipeline import PipelineState
from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend
from plugins.confluent_cloud.storage.module import CCloudStorageModule
from tests.unit.core.preview.test_service import (
    ControlledExecutor,
    _aggregate,
    _allocation,
    _runtime,
    _seed,
    _source,
)


def _backend(tmp_path: Path) -> SQLModelBackend:
    backend = SQLModelBackend(
        f"sqlite:///{tmp_path / 'preview-eligibility.db'}",
        CCloudStorageModule(),
        use_migrations=False,
    )
    backend.create_tables()
    return backend


def _tenant(
    connection_string: str,
    *,
    focus_preview: dict[str, object] | None,
    lookback_days: int = 10,
    cutoff_days: int = 5,
) -> TenantConfig:
    return TenantConfig.model_validate(
        {
            "ecosystem": "confluent_cloud",
            "tenant_id": "tenant-1",
            "lookback_days": lookback_days,
            "cutoff_days": cutoff_days,
            "storage": StorageConfig(connection_string=connection_string),
            "focus_preview": focus_preview,
        }
    )


def _block(**overrides: object) -> dict[str, object]:
    values: dict[str, object] = {
        "commercial_profile": "direct_payg",
        "billing_currency": "USD",
        "effective_start_date": "2020-01-01",
        "effective_end_date": "2030-01-01",
    }
    values.update(overrides)
    return values


def _state(tracking_date: date, *, calculated: bool) -> PipelineState:
    return PipelineState(
        ecosystem="confluent_cloud",
        tenant_id="tenant-1",
        tracking_date=tracking_date,
        billing_gathered=True,
        resources_gathered=True,
        chargeback_calculated=calculated,
        calculation_id="calculation-1" if calculated else None,
        calculation_completed_at=datetime(2026, 7, 3, tzinfo=UTC) if calculated else None,
        calculation_run_id=None,
    )


def _submit(
    runtime: object,
    backend: SQLModelBackend,
    tenant: TenantConfig,
    *,
    start_date: date,
    end_date: date,
) -> object:
    return runtime.submit(  # type: ignore[attr-defined,no-any-return]
        tenant_name="production",
        tenant_config=tenant,
        backend=backend,
        start_date=start_date,
        end_date=end_date,
        grain="daily",
        column_profile="full",
    )


def _failed(runtime: object, backend: SQLModelBackend, request_id: str) -> object:
    return runtime.get_request(  # type: ignore[attr-defined,no-any-return]
        backend=backend,
        request_id=request_id,
        ecosystem="confluent_cloud",
        tenant_id="tenant-1",
    )


def _assert_no_artifact(failed: object) -> None:
    assert failed.status.value == "failed"  # type: ignore[attr-defined]
    assert failed.source_snapshot is None  # type: ignore[attr-defined]
    assert failed.storage_key is None  # type: ignore[attr-defined]
    assert failed.package is None  # type: ignore[attr-defined]


def _replace_sources(backend: SQLModelBackend, sources: list[object]) -> None:
    with backend.create_unit_of_work() as uow:
        uow.billing.replace_source_window(
            "confluent_cloud",
            "tenant-1",
            datetime(2026, 6, 30, tzinfo=UTC),
            datetime(2026, 7, 3, tzinfo=UTC),
            sources,
        )
        uow.commit()


@pytest.mark.parametrize(
    ("tracking_date", "expected_code", "expected_retryable"),
    [
        (date(2026, 6, 23), "calculation_before_acquisition_lookback", False),
        (date(2026, 6, 24), "calculation_unavailable", True),
        (date(2026, 6, 28), "calculation_unavailable", True),
        (date(2026, 6, 29), "calculation_pending_cutoff_window", True),
        (date(2026, 7, 1), "calculation_pending_cutoff_window", True),
    ],
)
def test_calculation_lifecycle_uses_created_at_acquisition_boundaries(
    tmp_path: Path,
    tracking_date: date,
    expected_code: str,
    expected_retryable: bool,
) -> None:
    backend = _backend(tmp_path)
    _seed(backend, state=_state(tracking_date, calculated=False))
    executor = ControlledExecutor()
    runtime = _runtime(tmp_path, backend, executor)
    try:
        queued = _submit(
            runtime,
            backend,
            _tenant(backend._connection_string, focus_preview=_block()),
            start_date=tracking_date,
            end_date=tracking_date.replace(day=tracking_date.day + 1),
        )
        executor.run_all()
        failed = _failed(runtime, backend, queued.request_id)

        assert failed.diagnostic.code == expected_code
        assert failed.diagnostic.retryable is expected_retryable
        _assert_no_artifact(failed)
    finally:
        runtime.close()
        backend.dispose()


def test_outside_acquisition_diagnostic_never_promises_reconstruction_or_more_lookback(tmp_path: Path) -> None:
    backend = _backend(tmp_path)
    tracking_date = date(2025, 7, 1)
    _seed(backend, source=_source(), state=_state(tracking_date, calculated=False))
    executor = ControlledExecutor()
    runtime = _runtime(tmp_path, backend, executor)
    try:
        queued = _submit(
            runtime,
            backend,
            _tenant(
                backend._connection_string,
                focus_preview=_block(),
                lookback_days=364,
            ),
            start_date=tracking_date,
            end_date=date(2025, 7, 2),
        )
        executor.run_all()
        failed = _failed(runtime, backend, queued.request_id)

        assert failed.diagnostic.code == "calculation_before_acquisition_lookback"
        assert (
            failed.diagnostic.message
            == "Required retained calculation evidence is unavailable outside the current acquisition window."
        )
        assert "increase" not in failed.diagnostic.message.casefold()
        assert "reconstruct" not in failed.diagnostic.message.casefold()
        _assert_no_artifact(failed)
    finally:
        runtime.close()
        backend.dispose()


@pytest.mark.parametrize(
    ("start_date", "end_date", "expected_code"),
    [
        (date(2026, 6, 23), date(2026, 7, 2), "calculation_before_acquisition_lookback"),
        (date(2026, 6, 25), date(2026, 7, 2), "calculation_pending_cutoff_window"),
    ],
)
def test_mixed_missing_dates_use_outside_then_cutoff_precedence(
    tmp_path: Path,
    start_date: date,
    end_date: date,
    expected_code: str,
) -> None:
    backend = _backend(tmp_path)
    _seed(backend, state=_state(start_date, calculated=False))
    executor = ControlledExecutor()
    runtime = _runtime(tmp_path, backend, executor)
    try:
        queued = _submit(
            runtime,
            backend,
            _tenant(backend._connection_string, focus_preview=_block()),
            start_date=start_date,
            end_date=end_date,
        )
        executor.run_all()
        failed = _failed(runtime, backend, queued.request_id)

        assert failed.diagnostic.code == expected_code
    finally:
        runtime.close()
        backend.dispose()


def test_worker_uses_submission_clock_for_policy_even_when_queue_starts_later(tmp_path: Path) -> None:
    backend = _backend(tmp_path)
    tracking_date = date(2026, 6, 24)
    _seed(backend, state=_state(tracking_date, calculated=False))
    executor = ControlledExecutor()
    times = iter(
        [
            datetime(2026, 7, 4, tzinfo=UTC),
            datetime(2026, 8, 4, tzinfo=UTC),
            datetime(2026, 8, 4, 1, tzinfo=UTC),
        ]
    )
    artifacts = __import__("core.preview.artifacts", fromlist=["LocalPreviewArtifactStore"])
    service = __import__("core.preview.service", fromlist=["PreviewRuntime"])
    runtime = service.PreviewRuntime(
        artifact_store=artifacts.LocalPreviewArtifactStore(tmp_path / "artifacts"),
        max_workers=1,
        clock=lambda: next(times),
        request_id_factory=lambda: "request-clock",
        executor=executor,
    )
    try:
        queued = _submit(
            runtime,
            backend,
            _tenant(backend._connection_string, focus_preview=_block()),
            start_date=tracking_date,
            end_date=date(2026, 6, 25),
        )
        assert queued.created_at == datetime(2026, 7, 4, tzinfo=UTC)
        executor.run_all()
        failed = _failed(runtime, backend, queued.request_id)

        assert failed.diagnostic.code == "calculation_unavailable"
    finally:
        runtime.close()
        backend.dispose()


@pytest.mark.parametrize(
    ("focus_preview", "expected_code"),
    [
        (None, "preview_commercial_profile_unavailable"),
        (_block(billing_currency="EUR"), "preview_billing_currency_unsupported"),
        (
            _block(effective_start_date="2026-07-02", effective_end_date="2027-01-01"),
            "preview_commercial_profile_unavailable",
        ),
    ],
)
def test_commercial_and_currency_eligibility_fail_asynchronously_after_calculation(
    tmp_path: Path,
    focus_preview: dict[str, object] | None,
    expected_code: str,
) -> None:
    backend = _backend(tmp_path)
    _seed(backend, source=_source(), aggregate=_aggregate(), allocation=_allocation())
    executor = ControlledExecutor()
    runtime = _runtime(tmp_path, backend, executor)
    try:
        queued = _submit(
            runtime,
            backend,
            _tenant(backend._connection_string, focus_preview=focus_preview),
            start_date=date(2026, 7, 1),
            end_date=date(2026, 7, 2),
        )
        executor.run_all()
        failed = _failed(runtime, backend, queued.request_id)

        assert failed.diagnostic.code == expected_code
        assert failed.diagnostic.retryable is False
        _assert_no_artifact(failed)
    finally:
        runtime.close()
        backend.dispose()


def test_zero_source_and_zero_aggregate_fails_complete_coverage(tmp_path: Path) -> None:
    backend = _backend(tmp_path)
    _seed(backend)
    executor = ControlledExecutor()
    runtime = _runtime(tmp_path, backend, executor)
    try:
        queued = _submit(
            runtime,
            backend,
            _tenant(backend._connection_string, focus_preview=_block()),
            start_date=date(2026, 7, 1),
            end_date=date(2026, 7, 2),
        )
        executor.run_all()
        failed = _failed(runtime, backend, queued.request_id)

        assert failed.diagnostic.code == "preview_source_coverage_incomplete"
        assert failed.diagnostic.source_correlation_ids == ()
        _assert_no_artifact(failed)
    finally:
        runtime.close()
        backend.dispose()


@pytest.mark.parametrize(
    ("source_present", "aggregate_present"),
    [(False, True), (True, False)],
)
def test_one_sided_source_aggregate_coverage_fails_closed(
    tmp_path: Path,
    source_present: bool,
    aggregate_present: bool,
) -> None:
    backend = _backend(tmp_path)
    _seed(
        backend,
        source=_source() if source_present else None,
        aggregate=_aggregate() if aggregate_present else None,
    )
    executor = ControlledExecutor()
    runtime = _runtime(tmp_path, backend, executor)
    try:
        queued = _submit(
            runtime,
            backend,
            _tenant(backend._connection_string, focus_preview=_block()),
            start_date=date(2026, 7, 1),
            end_date=date(2026, 7, 2),
        )
        executor.run_all()
        failed = _failed(runtime, backend, queued.request_id)

        assert failed.diagnostic.code == "preview_source_coverage_incomplete"
        _assert_no_artifact(failed)
    finally:
        runtime.close()
        backend.dispose()


@pytest.mark.parametrize(
    ("source_changes", "expected_code"),
    [
        ({"malformed": True}, "preview_source_record_malformed"),
        ({"line_type": None}, "preview_source_line_type_unknown"),
        ({"line_type": "FUTURE_LINE"}, "preview_source_line_type_unsupported"),
        ({"line_type": "SUPPORT"}, "preview_charge_classification_ambiguous"),
        ({"line_type": "KAFKA_STREAMS"}, "preview_mapping_scope_unsupported"),
        ({"line_type": "PROMO_CREDIT"}, "preview_source_economics_unsupported"),
        ({"description": "Prior period refund"}, "preview_charge_classification_ambiguous"),
        ({"resource_id": None}, "preview_source_record_incomplete"),
        ({"amount": 0}, "preview_source_economics_unsupported"),
        ({"amount": 7}, "preview_source_reconciliation_failed"),
    ],
)
def test_source_eligibility_diagnostics_travel_through_worker_failure_path(
    tmp_path: Path,
    source_changes: dict[str, object],
    expected_code: str,
) -> None:
    backend = _backend(tmp_path)
    source = _source(**source_changes)
    _seed(backend, source=source, aggregate=_aggregate(), allocation=_allocation())
    executor = ControlledExecutor()
    runtime = _runtime(tmp_path, backend, executor)
    try:
        queued = _submit(
            runtime,
            backend,
            _tenant(backend._connection_string, focus_preview=_block()),
            start_date=date(2026, 7, 1),
            end_date=date(2026, 7, 2),
        )
        executor.run_all()
        failed = _failed(runtime, backend, queued.request_id)

        assert failed.diagnostic.code == expected_code
        assert failed.diagnostic.code != "preview_generation_failed"
        assert len(failed.diagnostic.source_correlation_ids) == 1
        assert "cost-1" not in failed.diagnostic.source_correlation_ids[0]
        if expected_code == "preview_mapping_scope_unsupported":
            assert failed.diagnostic.message == (
                "The complete source set exceeds the current Daily Full mapping scope."
            )
        _assert_no_artifact(failed)
    finally:
        runtime.close()
        backend.dispose()


def test_complete_scan_uses_highest_issue_precedence_including_kafka_streams(tmp_path: Path) -> None:
    backend = _backend(tmp_path)
    _seed(backend, aggregate=_aggregate(), allocation=_allocation())
    _replace_sources(
        backend,
        [
            _source(source_record_id="unsupported", provider_cost_id="unsupported", line_type="FUTURE_LINE"),
            _source(source_record_id="streams", provider_cost_id="streams", line_type="KAFKA_STREAMS"),
            _source(source_record_id="malformed", provider_cost_id="malformed", malformed=True),
            _source(source_record_id="valid", provider_cost_id="valid"),
        ],
    )
    executor = ControlledExecutor()
    runtime = _runtime(tmp_path, backend, executor)
    try:
        queued = _submit(
            runtime,
            backend,
            _tenant(backend._connection_string, focus_preview=_block()),
            start_date=date(2026, 7, 1),
            end_date=date(2026, 7, 2),
        )
        executor.run_all()
        failed = _failed(runtime, backend, queued.request_id)

        assert failed.diagnostic.code == "preview_source_record_malformed"
        assert len(failed.diagnostic.source_correlation_ids) == 1
        _assert_no_artifact(failed)
    finally:
        runtime.close()
        backend.dispose()


def test_more_than_twenty_blocking_sources_persist_smallest_twenty_safe_correlations(tmp_path: Path) -> None:
    eligibility = __import__("core.preview.eligibility", fromlist=["public_source_correlation_id"])
    backend = _backend(tmp_path)
    _seed(backend, aggregate=_aggregate(), allocation=_allocation())
    sources = [
        _source(
            source_record_id=f"provider:secret-{index:02}",
            provider_cost_id=f"secret-{index:02}",
            malformed=True,
        )
        for index in range(25)
    ]
    _replace_sources(backend, sources)
    expected = tuple(
        sorted(
            {
                eligibility.public_source_correlation_id(
                    ecosystem="confluent_cloud",
                    tenant_id="tenant-1",
                    source_record_id=source.source_record_id,
                )
                for source in sources
            }
        )[:20]
    )
    executor = ControlledExecutor()
    runtime = _runtime(tmp_path, backend, executor)
    try:
        queued = _submit(
            runtime,
            backend,
            _tenant(backend._connection_string, focus_preview=_block()),
            start_date=date(2026, 7, 1),
            end_date=date(2026, 7, 2),
        )
        executor.run_all()
        failed = _failed(runtime, backend, queued.request_id)

        assert failed.diagnostic.code == "preview_source_record_malformed"
        assert failed.diagnostic.source_correlation_ids == expected
        assert all("secret" not in item for item in expected)
        _assert_no_artifact(failed)
    finally:
        runtime.close()
        backend.dispose()


def test_more_than_twenty_valid_sources_use_independent_valid_correlation_accumulator(tmp_path: Path) -> None:
    eligibility = __import__("core.preview.eligibility", fromlist=["public_source_correlation_id"])
    backend = _backend(tmp_path)
    _seed(backend, aggregate=_aggregate(), allocation=_allocation())
    sources = [
        _source(
            source_record_id=f"provider:valid-{index:02}",
            provider_cost_id=f"valid-{index:02}",
        )
        for index in range(25)
    ]
    _replace_sources(backend, sources)
    expected = tuple(
        sorted(
            eligibility.public_source_correlation_id(
                ecosystem="confluent_cloud",
                tenant_id="tenant-1",
                source_record_id=source.source_record_id,
            )
            for source in sources
        )[:20]
    )
    executor = ControlledExecutor()
    runtime = _runtime(tmp_path, backend, executor)
    try:
        queued = _submit(
            runtime,
            backend,
            _tenant(backend._connection_string, focus_preview=_block()),
            start_date=date(2026, 7, 1),
            end_date=date(2026, 7, 2),
        )
        executor.run_all()
        failed = _failed(runtime, backend, queued.request_id)

        assert failed.diagnostic.code == "preview_mapping_scope_unsupported"
        assert failed.diagnostic.source_correlation_ids == expected
        _assert_no_artifact(failed)
    finally:
        runtime.close()
        backend.dispose()


@pytest.mark.parametrize(
    ("matched", "expected_code"),
    [
        (True, "preview_mapping_scope_unsupported"),
        (False, "preview_source_coverage_incomplete"),
    ],
)
def test_large_both_sided_streaming_coverage_is_complete_or_fails_closed(
    tmp_path: Path,
    matched: bool,
    expected_code: str,
) -> None:
    backend = _backend(tmp_path)
    _seed(backend)
    sources = [
        _source(
            source_record_id=f"provider:stream-{index:03}",
            provider_cost_id=f"stream-{index:03}",
            resource_id=f"lkc-{index:03}",
        )
        for index in range(300)
    ]
    _replace_sources(backend, sources)
    aggregate_count = len(sources) if matched else len(sources) - 1
    with backend.create_unit_of_work() as uow:
        for index in range(aggregate_count):
            uow.billing.upsert(_aggregate(resource_id=f"lkc-{index:03}"))
        uow.commit()
    executor = ControlledExecutor()
    runtime = _runtime(tmp_path, backend, executor)
    try:
        queued = _submit(
            runtime,
            backend,
            _tenant(backend._connection_string, focus_preview=_block()),
            start_date=date(2026, 7, 1),
            end_date=date(2026, 7, 2),
        )
        executor.run_all()
        failed = _failed(runtime, backend, queued.request_id)

        assert failed.diagnostic.code == expected_code
        assert len(failed.diagnostic.source_correlation_ids) == 20
        _assert_no_artifact(failed)
    finally:
        runtime.close()
        backend.dispose()


@pytest.mark.parametrize(
    ("currency", "expected_code"),
    [("", "preview_billing_currency_unknown"), ("EUR", "preview_billing_currency_unsupported")],
)
def test_selected_aggregate_currency_contradiction_fails_closed(
    tmp_path: Path,
    currency: str,
    expected_code: str,
) -> None:
    backend = _backend(tmp_path)
    _seed(
        backend,
        source=_source(),
        aggregate=_aggregate(currency=currency),
        allocation=_allocation(),
    )
    executor = ControlledExecutor()
    runtime = _runtime(tmp_path, backend, executor)
    try:
        queued = _submit(
            runtime,
            backend,
            _tenant(backend._connection_string, focus_preview=_block()),
            start_date=date(2026, 7, 1),
            end_date=date(2026, 7, 2),
        )
        executor.run_all()
        failed = _failed(runtime, backend, queued.request_id)

        assert failed.diagnostic.code == expected_code
        _assert_no_artifact(failed)
    finally:
        runtime.close()
        backend.dispose()

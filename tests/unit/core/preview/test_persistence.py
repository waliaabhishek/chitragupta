from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, get_type_hints

import pytest
from sqlalchemy import event

from core.models.pipeline import PipelineState
from core.storage.backends.sqlmodel.mappers import pipeline_state_to_domain, pipeline_state_to_table
from core.storage.backends.sqlmodel.module import CoreStorageModule
from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend
from plugins.confluent_cloud.storage.module import CCloudStorageModule
from tests.unit.core.preview.conftest import preview_module


def _backend(tmp_path: Path) -> SQLModelBackend:
    backend = SQLModelBackend(
        f"sqlite:///{tmp_path / 'preview.db'}",
        CCloudStorageModule(),
        use_migrations=False,
    )
    backend.create_tables()
    return backend


def _state(
    tracking_date: date,
    *,
    calculated: bool,
    calculation_id: str | None = None,
    completed_at: datetime | None = None,
    run_id: int | None = None,
) -> PipelineState:
    return PipelineState(
        ecosystem="confluent_cloud",
        tenant_id="tenant-1",
        tracking_date=tracking_date,
        billing_gathered=True,
        resources_gathered=True,
        chargeback_calculated=calculated,
        calculation_id=calculation_id,
        calculation_completed_at=completed_at,
        calculation_run_id=run_id,
    )


@pytest.mark.parametrize(
    ("calculation_id", "completed_at", "run_id", "usable"),
    [
        ("calculation-1", datetime(2026, 7, 3, tzinfo=UTC), None, True),
        (None, None, None, False),
        ("calculation-1", None, None, False),
        (None, datetime(2026, 7, 3, tzinfo=UTC), None, False),
    ],
)
def test_pipeline_state_mapper_round_trips_calculation_correlation(
    calculation_id: str | None,
    completed_at: datetime | None,
    run_id: int | None,
    usable: bool,
) -> None:
    state = _state(
        date(2026, 7, 1),
        calculated=True,
        calculation_id=calculation_id,
        completed_at=completed_at,
        run_id=run_id,
    )

    restored = pipeline_state_to_domain(pipeline_state_to_table(state))

    assert restored.calculation_id == calculation_id
    assert restored.calculation_completed_at == completed_at
    assert restored.calculation_run_id == run_id
    assert restored.has_usable_calculation is usable


def test_mark_calculated_writes_identity_time_and_fk_valid_provenance_atomically(tmp_path: Path) -> None:
    backend = _backend(tmp_path)
    completed_at = datetime(2026, 7, 3, 2, tzinfo=UTC)
    try:
        with backend.create_unit_of_work() as uow:
            run = uow.pipeline_runs.create_run("production", datetime(2026, 7, 3, tzinfo=UTC))
            assert run.id is not None
            uow.pipeline_state.upsert(_state(date(2026, 7, 1), calculated=False))
            uow.pipeline_state.mark_chargeback_calculated(
                "confluent_cloud",
                "tenant-1",
                date(2026, 7, 1),
                calculation_id="calculation-1",
                calculation_completed_at=completed_at,
                calculation_run_id=run.id,
            )
            uow.commit()

        with backend.create_read_only_unit_of_work() as uow:
            restored = uow.pipeline_state.get("confluent_cloud", "tenant-1", date(2026, 7, 1))
        assert restored is not None
        assert restored.chargeback_calculated is True
        assert restored.calculation_id == "calculation-1"
        assert restored.calculation_completed_at == completed_at
        assert restored.calculation_run_id == run.id
    finally:
        backend.dispose()


def test_mark_needs_recalculation_clears_every_success_field(tmp_path: Path) -> None:
    backend = _backend(tmp_path)
    try:
        with backend.create_unit_of_work() as uow:
            uow.pipeline_state.upsert(
                _state(
                    date(2026, 7, 1),
                    calculated=True,
                    calculation_id="calculation-1",
                    completed_at=datetime(2026, 7, 3, tzinfo=UTC),
                )
            )
            uow.pipeline_state.mark_needs_recalculation("confluent_cloud", "tenant-1", date(2026, 7, 1))
            uow.commit()

        with backend.create_read_only_unit_of_work() as uow:
            restored = uow.pipeline_state.get("confluent_cloud", "tenant-1", date(2026, 7, 1))
        assert restored is not None
        assert restored.chargeback_calculated is False
        assert restored.topic_attribution_calculated is False
        assert restored.calculation_id is None
        assert restored.calculation_completed_at is None
        assert restored.calculation_run_id is None
    finally:
        backend.dispose()


def test_find_needing_calculation_does_not_select_calculated_legacy_rows(tmp_path: Path) -> None:
    backend = _backend(tmp_path)
    try:
        with backend.create_unit_of_work() as uow:
            uow.pipeline_state.upsert(_state(date(2026, 7, 1), calculated=True))
            uow.pipeline_state.upsert(
                _state(
                    date(2026, 7, 2),
                    calculated=True,
                    calculation_id="partial",
                    completed_at=None,
                )
            )
            uow.pipeline_state.upsert(_state(date(2026, 7, 3), calculated=False))
            uow.commit()

        with backend.create_read_only_unit_of_work() as uow:
            pending = uow.pipeline_state.find_needing_calculation("confluent_cloud", "tenant-1")
        assert [item.tracking_date for item in pending] == [date(2026, 7, 3)]
    finally:
        backend.dispose()


@pytest.mark.parametrize(
    ("states", "expected_type", "usable", "missing", "incomplete"),
    [
        (
            [(True, "a", True)],
            "CompleteCalculationCoverage",
            (date(2026, 7, 1),),
            (),
            (),
        ),
        ([], "NoUsableCalculationCoverage", (), (date(2026, 7, 1),), ()),
        ([(False, None, False)], "NoUsableCalculationCoverage", (), (date(2026, 7, 1),), ()),
        ([(True, None, False)], "NoUsableCalculationCoverage", (), (), (date(2026, 7, 1),)),
        (
            [(True, "a", True), (False, None, False)],
            "PartialCalculationCoverage",
            (date(2026, 7, 1),),
            (date(2026, 7, 2),),
            (),
        ),
        (
            [(True, "a", True), (True, None, False)],
            "PartialCalculationCoverage",
            (date(2026, 7, 1),),
            (),
            (date(2026, 7, 2),),
        ),
        (
            [(True, None, False), (False, None, False)],
            "NoUsableCalculationCoverage",
            (),
            (date(2026, 7, 2),),
            (date(2026, 7, 1),),
        ),
        (
            [(True, "a", True), (True, None, False), (False, None, False)],
            "PartialCalculationCoverage",
            (date(2026, 7, 1),),
            (date(2026, 7, 3),),
            (date(2026, 7, 2),),
        ),
    ],
)
def test_find_current_coverage_returns_typed_partitioned_result(
    tmp_path: Path,
    states: list[tuple[bool, str | None, bool]],
    expected_type: str,
    usable: tuple[date, ...],
    missing: tuple[date, ...],
    incomplete: tuple[date, ...],
) -> None:
    backend = _backend(tmp_path)
    persistence = preview_module("persistence")
    request_days = max(1, len(states))
    try:
        with backend.create_unit_of_work() as uow:
            for offset, (calculated, calculation_id, has_time) in enumerate(states):
                uow.pipeline_state.upsert(
                    _state(
                        date(2026, 7, 1 + offset),
                        calculated=calculated,
                        calculation_id=calculation_id,
                        completed_at=datetime(2026, 7, 3, offset, tzinfo=UTC) if has_time else None,
                    )
                )
            uow.commit()

        with backend.create_read_only_unit_of_work() as uow:
            repository = persistence.SQLModelPreviewCalculationRepository(uow._session)
            result = repository.find_current_coverage(
                ecosystem="confluent_cloud",
                tenant_id="tenant-1",
                start_date=date(2026, 7, 1),
                end_date=date(2026, 7, 1 + request_days),
            )

        assert type(result).__name__ == expected_type
        assert tuple(item.tracking_date for item in getattr(result, "entries", ())) == usable
        assert result.missing_dates if missing else getattr(result, "missing_dates", ()) == ()
        assert getattr(result, "missing_dates", ()) == missing
        assert getattr(result, "incomplete_correlation_dates", ()) == incomplete
    finally:
        backend.dispose()


def test_coverage_repository_classifies_persisted_empty_id_and_invalid_timestamp_as_incomplete(
    tmp_path: Path,
) -> None:
    backend = _backend(tmp_path)
    persistence = preview_module("persistence")
    try:
        with backend.create_unit_of_work() as uow:
            uow.pipeline_state.upsert(
                _state(
                    date(2026, 7, 1),
                    calculated=True,
                    calculation_id="",
                    completed_at=datetime(2026, 7, 3, tzinfo=UTC),
                )
            )
            uow.pipeline_state.upsert(
                _state(
                    date(2026, 7, 2),
                    calculated=True,
                    calculation_id="calculation-2",
                    completed_at=datetime(2026, 7, 3, tzinfo=UTC),
                )
            )
            uow.commit()
        with backend._engine.begin() as connection:
            connection.exec_driver_sql(
                "UPDATE pipeline_state SET calculation_completed_at = 'not-a-timestamp' "
                "WHERE tracking_date = '2026-07-02'"
            )

        with backend.create_read_only_unit_of_work() as uow:
            repository = persistence.SQLModelPreviewCalculationRepository(uow._session)
            result = repository.find_current_coverage(
                ecosystem="confluent_cloud",
                tenant_id="tenant-1",
                start_date=date(2026, 7, 1),
                end_date=date(2026, 7, 3),
            )

        assert isinstance(result, persistence.NoUsableCalculationCoverage)
        assert result.missing_dates == ()
        assert result.incomplete_correlation_dates == (date(2026, 7, 1), date(2026, 7, 2))
    finally:
        backend.dispose()


def test_coverage_repository_value_error_branch_classifies_persisted_row_as_incomplete(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    backend = _backend(tmp_path)
    persistence = preview_module("persistence")
    try:
        with backend.create_unit_of_work() as uow:
            uow.pipeline_state.upsert(
                _state(
                    date(2026, 7, 1),
                    calculated=True,
                    calculation_id="calculation-1",
                    completed_at=datetime(2026, 7, 3, tzinfo=UTC),
                )
            )
            uow.commit()

        def reject_persisted_timestamp(**_kwargs: object) -> object:
            raise ValueError("invalid persisted calculation timestamp")

        monkeypatch.setattr(persistence, "PreviewCalculationCoverageEntry", reject_persisted_timestamp)
        with backend.create_read_only_unit_of_work() as uow:
            repository = persistence.SQLModelPreviewCalculationRepository(uow._session)
            result = repository.find_current_coverage(
                ecosystem="confluent_cloud",
                tenant_id="tenant-1",
                start_date=date(2026, 7, 1),
                end_date=date(2026, 7, 2),
            )

        assert isinstance(result, persistence.NoUsableCalculationCoverage)
        assert result.incomplete_correlation_dates == (date(2026, 7, 1),)
    finally:
        backend.dispose()


def test_coverage_query_is_bounded_ordered_and_does_not_join_pipeline_runs(tmp_path: Path) -> None:
    backend = _backend(tmp_path)
    persistence = preview_module("persistence")
    statements: list[tuple[str, Any]] = []

    def capture(_connection: object, _cursor: object, statement: str, parameters: Any, *_args: object) -> None:
        if "pipeline_state" in statement.lower() and statement.lstrip().lower().startswith("select"):
            statements.append((statement, parameters))

    event.listen(backend._ro_engine, "before_cursor_execute", capture)
    try:
        with backend.create_read_only_unit_of_work() as uow:
            repository = persistence.SQLModelPreviewCalculationRepository(uow._session)
            repository.find_current_coverage(
                ecosystem="confluent_cloud",
                tenant_id="tenant-1",
                start_date=date(2026, 7, 1),
                end_date=date(2026, 7, 3),
            )
        assert len(statements) == 1
        sql, parameters = statements[0]
        normalized = " ".join(sql.lower().split())
        assert "order by pipeline_state.tracking_date" in normalized
        assert "limit" in normalized
        assert "pipeline_runs" not in normalized
        assert 3 in tuple(parameters)
    finally:
        event.remove(backend._ro_engine, "before_cursor_execute", capture)
        backend.dispose()


def _queued_request(request_id: str = "request-1", tenant_id: str = "tenant-1") -> Any:
    mapping = preview_module("mapping")
    models = preview_module("models")
    return models.PreviewRequest(
        request_id=request_id,
        tenant_name="production",
        ecosystem="confluent_cloud",
        tenant_id=tenant_id,
        grain="daily",
        start_date=date(2026, 7, 1),
        end_date=date(2026, 7, 2),
        column_profile="full",
        effective_columns=mapping.FOCUS_1_4_FULL_PROFILE_COLUMNS,
        status=models.PreviewRequestStatus.QUEUED,
        created_at=datetime(2026, 7, 3, tzinfo=UTC),
        started_at=None,
        completed_at=None,
        expires_at=None,
        source_snapshot=None,
        diagnostic=None,
        storage_key=None,
        package=None,
    )


def test_request_repository_enforces_transitions_and_tenant_isolation(tmp_path: Path) -> None:
    backend = _backend(tmp_path)
    try:
        with backend.create_preview_write_unit_of_work() as uow:
            created = uow.requests.create_queued(_queued_request())
            assert created.status.value == "queued"
            running = uow.requests.mark_running("request-1", datetime(2026, 7, 3, 1, tzinfo=UTC))
            assert running is not None
            assert running.status.value == "running"
            assert running.started_at == datetime(2026, 7, 3, 1, tzinfo=UTC)
            assert uow.requests.mark_running("request-1", datetime(2026, 7, 3, 2, tzinfo=UTC)) is None
            uow.commit()

        with backend.create_preview_read_unit_of_work() as uow:
            assert uow.requests.get_for_owner("request-1", "confluent_cloud", "tenant-1") is not None
            assert uow.requests.get_for_owner("request-1", "confluent_cloud", "tenant-2") is None
    finally:
        backend.dispose()


def test_request_mapper_round_trips_persisted_grain_and_column_profile() -> None:
    persistence = preview_module("persistence")
    request = _queued_request()

    restored = persistence.request_to_domain(persistence.request_to_table(request))

    assert restored.grain == request.grain == "daily"
    assert restored.column_profile == request.column_profile == "full"


def test_request_mapper_round_trips_canonical_diagnostic_correlations() -> None:
    models = preview_module("models")
    persistence = preview_module("persistence")
    request = _queued_request()
    diagnostic = models.PreviewDiagnostic(
        code="preview_source_record_malformed",
        message="One or more persisted Confluent Costs API records are malformed.",
        retryable=False,
        source_correlation_ids=("src:v1:bbb", "src:v1:aaa", "src:v1:bbb"),
    )
    request = request.__class__(
        **{
            **request.__dict__,
            "status": models.PreviewRequestStatus.FAILED,
            "completed_at": datetime(2026, 7, 3, 1, tzinfo=UTC),
            "diagnostic": diagnostic,
        }
    )

    row = persistence.request_to_table(request)
    restored = persistence.request_to_domain(row)

    assert row.diagnostic_source_correlation_ids_json == '["src:v1:aaa","src:v1:bbb"]'
    assert restored.diagnostic is not None
    assert restored.diagnostic.source_correlation_ids == ("src:v1:aaa", "src:v1:bbb")


def test_request_mapper_hydrates_legacy_null_diagnostic_correlations_as_empty() -> None:
    models = preview_module("models")
    persistence = preview_module("persistence")
    request = _queued_request()
    diagnostic = models.PreviewDiagnostic(
        code="calculation_unavailable",
        message="No successful persisted calculation is available for the requested dates; run the pipeline and retry.",
        retryable=True,
    )
    request = request.__class__(
        **{
            **request.__dict__,
            "status": models.PreviewRequestStatus.FAILED,
            "completed_at": datetime(2026, 7, 3, 1, tzinfo=UTC),
            "diagnostic": diagnostic,
        }
    )
    row = persistence.request_to_table(request)
    row.diagnostic_source_correlation_ids_json = None

    restored = persistence.request_to_domain(row)

    assert restored.diagnostic is not None
    assert restored.diagnostic.source_correlation_ids == ()


def test_diagnostic_correlations_are_capped_before_persistence() -> None:
    models = preview_module("models")
    persistence = preview_module("persistence")
    request = _queued_request()
    diagnostic = models.PreviewDiagnostic(
        code="preview_source_record_malformed",
        message="One or more persisted Confluent Costs API records are malformed.",
        retryable=False,
        source_correlation_ids=tuple(f"src:v1:{index:064x}" for index in range(25)),
    )
    request = request.__class__(
        **{
            **request.__dict__,
            "status": models.PreviewRequestStatus.FAILED,
            "completed_at": datetime(2026, 7, 3, 1, tzinfo=UTC),
            "diagnostic": diagnostic,
        }
    )

    row = persistence.request_to_table(request)
    values = __import__("json").loads(row.diagnostic_source_correlation_ids_json)

    assert len(values) == 20
    assert values == sorted(set(values))


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [("grain", "hourly", "grain"), ("column_profile", "thin", "column profile")],
)
def test_request_mapper_rejects_unsupported_persisted_literals(field: str, value: str, message: str) -> None:
    persistence = preview_module("persistence")
    row = persistence.request_to_table(_queued_request())
    setattr(row, field, value)

    with pytest.raises(ValueError, match=message):
        persistence.request_to_domain(row)


@pytest.mark.parametrize(
    "field",
    ["request_id", "tenant_name", "ecosystem", "tenant_id"],
)
def test_request_mapper_rejects_blank_persisted_owner_identity(field: str) -> None:
    persistence = preview_module("persistence")
    row = persistence.request_to_table(_queued_request())
    setattr(row, field, "  ")

    with pytest.raises(ValueError, match=field):
        persistence.request_to_domain(row)


def test_request_mapper_rejects_unsupported_persisted_status() -> None:
    persistence = preview_module("persistence")
    row = persistence.request_to_table(_queued_request())
    row.status = "paused"

    with pytest.raises(ValueError, match="(?i)status"):
        persistence.request_to_domain(row)


@pytest.mark.parametrize(
    ("calculation_id", "completed_at", "message"),
    [
        ("", datetime(2026, 7, 3, tzinfo=UTC), "calculation_id"),
        ("calculation-1", datetime(2026, 7, 3), "calculation_completed_at"),
    ],
)
def test_calculation_coverage_entry_rejects_empty_identity_and_naive_completion(
    calculation_id: str,
    completed_at: datetime,
    message: str,
) -> None:
    models = preview_module("models")

    with pytest.raises(ValueError, match=message):
        models.PreviewCalculationCoverageEntry(
            tracking_date=date(2026, 7, 1),
            calculation_id=calculation_id,
            calculation_completed_at=completed_at,
            calculation_run_id=None,
        )


def test_preview_uows_expose_full_protocol_shape_and_close_sessions(tmp_path: Path) -> None:
    backend = _backend(tmp_path)
    persistence = preview_module("persistence")
    try:
        read_uow = backend.create_preview_read_unit_of_work()
        assert isinstance(read_uow, persistence.PreviewReadUnitOfWork)
        with read_uow as opened:
            assert opened.requests is not None
            assert opened.calculations is not None
            assert opened.cost_evidence is not None
            assert opened.allocation_evidence is not None
            assert opened.resources is not None
            assert opened.identities is not None
        assert read_uow._session is None

        write_uow = backend.create_preview_write_unit_of_work()
        assert isinstance(write_uow, persistence.PreviewWriteUnitOfWork)
        with write_uow as opened:
            assert opened.requests is not None
            opened.rollback()
        assert write_uow._session is None
    finally:
        backend.dispose()


def test_preview_repository_uow_and_backend_contracts_are_structural(tmp_path: Path) -> None:
    backend = _backend(tmp_path)
    persistence = preview_module("persistence")
    try:
        with backend.create_preview_write_unit_of_work() as write_uow:
            assert isinstance(write_uow.requests, persistence.PreviewRequestRepository)
        with backend.create_preview_read_unit_of_work() as read_uow:
            assert isinstance(read_uow.requests, persistence.PreviewRequestRepository)
            assert isinstance(read_uow.calculations, persistence.PreviewCalculationRepository)

        assert isinstance(backend, persistence.PreviewStorageBackend)
        assert get_type_hints(persistence.PreviewWriteUnitOfWork)["requests"] is persistence.PreviewRequestRepository
        read_hints = get_type_hints(persistence.PreviewReadUnitOfWork)
        assert read_hints["requests"] is persistence.PreviewRequestRepository
        assert read_hints["calculations"] is persistence.PreviewCalculationRepository
    finally:
        backend.dispose()


def test_preview_read_uow_closes_session_when_repository_validation_fails(tmp_path: Path) -> None:
    backend = SQLModelBackend(
        f"sqlite:///{tmp_path / 'unsupported.db'}",
        CoreStorageModule(),
        use_migrations=False,
    )
    backend.create_tables()
    read_uow = backend.create_preview_read_unit_of_work()
    try:
        with pytest.raises(TypeError, match="preview evidence support"):
            read_uow.__enter__()
        assert read_uow._session is None
    finally:
        read_uow.__exit__(None, None, None)
        backend.dispose()

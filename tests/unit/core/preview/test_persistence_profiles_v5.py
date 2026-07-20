from __future__ import annotations

from datetime import UTC, date, datetime
from importlib import import_module
from pathlib import Path
from typing import Any

import pytest

from tests.unit.core.preview.test_lifecycle_snapshot_v5 import _package, _request, _snapshot
from tests.unit.core.preview.test_persistence import _backend


def _persistence() -> Any:
    return import_module("core.preview.persistence")


def _mapping() -> Any:
    return import_module("core.preview.mapping")


def test_request_mapper_round_trips_explicit_effective_columns_and_daily_null_cutoff() -> None:
    persistence = _persistence()
    request = _request(
        status="queued",
        column_profile="custom",
        effective_columns=("Tags", "BilledCost"),
    )

    row = persistence.request_to_table(request)
    restored = persistence.request_to_domain(row)

    assert row.effective_columns_json == '["Tags","BilledCost"]'
    assert row.availability_cutoff_end_date is None
    assert restored.effective_columns == ("Tags", "BilledCost")
    assert restored.source_snapshot is None


def test_request_mapper_uses_one_mapping_validator_for_encode_and_explicit_decode(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    persistence = _persistence()
    calls: list[tuple[str, tuple[str, ...]]] = []
    original = persistence.validate_preview_effective_columns

    def validate(profile: str, columns: tuple[str, ...]) -> None:
        calls.append((profile, columns))
        original(profile, columns)

    monkeypatch.setattr(persistence, "validate_preview_effective_columns", validate)
    request = _request(status="queued")
    row = persistence.request_to_table(request)
    persistence.request_to_domain(row)

    assert calls == [
        ("full", _mapping().FOCUS_1_4_FULL_PROFILE_COLUMNS),
        ("full", _mapping().FOCUS_1_4_FULL_PROFILE_COLUMNS),
    ]


def test_legacy_null_effective_columns_use_frozen_v4_even_if_current_full_is_rebound(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    persistence = _persistence()
    mapping = _mapping()
    legacy = mapping.LEGACY_DAILY_FULL_V4_COLUMNS
    row = persistence.request_to_table(_request(status="queued"))
    row.effective_columns_json = None
    monkeypatch.setattr(mapping, "FOCUS_1_4_FULL_PROFILE_COLUMNS", ("FutureColumn",))

    restored = persistence.request_to_domain(row)

    assert restored.effective_columns is legacy
    assert restored.effective_columns == legacy
    assert restored.request_id == "request-1"
    assert restored.tenant_name == "production"
    assert restored.ecosystem == "confluent_cloud"
    assert restored.tenant_id == "tenant-1"
    assert restored.status.value == "queued"


def test_ready_v5_null_effective_coverage_fails_closed_while_legacy_null_columns_hydrate() -> None:
    persistence = _persistence()
    snapshot = _snapshot()
    ready = _request(
        status="ready",
        completed_at=datetime(2026, 7, 3, 2, tzinfo=UTC),
        source_snapshot=snapshot,
        storage_key="request-1",
        package=_package(),
    )
    row = persistence.request_to_table(ready)
    row.effective_coverage_start_date = None
    row.effective_coverage_end_date = None

    with pytest.raises(ValueError, match="effective coverage"):
        persistence.request_to_domain(row)

    row.effective_columns_json = None
    restored = persistence.request_to_domain(row)
    assert restored.effective_columns is _mapping().LEGACY_DAILY_FULL_V4_COLUMNS
    assert restored.source_snapshot is not None
    assert restored.source_snapshot.effective_coverage_start_date == ready.start_date
    assert restored.source_snapshot.effective_coverage_end_date == ready.end_date


def test_invalid_direct_domain_selection_fails_before_table_construction() -> None:
    persistence = _persistence()
    request = _request(status="queued", effective_columns=("BilledCost",))

    with pytest.raises(_mapping().PreviewEffectiveColumnsError):
        persistence.request_to_table(request)


def test_sqlite_hydration_normalizes_known_naive_utc_storage_boundary(tmp_path: Path) -> None:
    backend = _backend(tmp_path)
    try:
        with backend.create_preview_write_unit_of_work() as uow:
            uow.requests.create_queued(_request(status="queued"))
            uow.commit()
        with backend._engine.begin() as connection:
            connection.exec_driver_sql(
                "UPDATE preview_requests SET created_at = '2026-07-03 00:00:00' WHERE request_id = 'request-1'"
            )

        with backend.create_preview_read_unit_of_work() as uow:
            restored = uow.requests.get_for_owner("request-1", "confluent_cloud", "tenant-1")

        assert restored is not None
        assert restored.created_at == datetime(2026, 7, 3, tzinfo=UTC)
    finally:
        backend.dispose()


def test_mark_running_returns_validated_running_candidate_and_lost_transition_does_not_mutate(
    tmp_path: Path,
) -> None:
    backend = _backend(tmp_path)
    try:
        with backend.create_preview_write_unit_of_work() as uow:
            uow.requests.create_queued(_request(status="queued"))
            uow.commit()
        with backend.create_preview_write_unit_of_work() as uow:
            running = uow.requests.mark_running("request-1", datetime(2026, 7, 3, 1, tzinfo=UTC))
            uow.commit()

        assert running is not None
        assert running.status.value == "running"
        assert running.started_at == datetime(2026, 7, 3, 1, tzinfo=UTC)
        assert running.source_snapshot is None
        with backend.create_preview_write_unit_of_work() as uow:
            lost = uow.requests.mark_running("request-1", datetime(2026, 7, 3, 2, tzinfo=UTC))
            uow.commit()
        assert lost is None
        with backend.create_preview_read_unit_of_work() as uow:
            persisted = uow.requests.get_for_owner("request-1", "confluent_cloud", "tenant-1")
        assert persisted is not None
        assert persisted.started_at == datetime(2026, 7, 3, 1, tzinfo=UTC)
    finally:
        backend.dispose()


@pytest.mark.parametrize(
    "invalid_started_at",
    [datetime(2026, 7, 2, tzinfo=UTC), datetime(2026, 7, 3)],
)
def test_mark_running_rejects_invalid_candidate_before_sql_and_preserves_queued_row(
    tmp_path: Path,
    invalid_started_at: datetime,
) -> None:
    backend = _backend(tmp_path)
    try:
        with backend.create_preview_write_unit_of_work() as uow:
            uow.requests.create_queued(_request(status="queued"))
            uow.commit()
        with pytest.raises(ValueError), backend.create_preview_write_unit_of_work() as uow:
            uow.requests.mark_running("request-1", invalid_started_at)
            uow.commit()

        with backend.create_preview_read_unit_of_work() as uow:
            persisted = uow.requests.get_for_owner("request-1", "confluent_cloud", "tenant-1")
        assert persisted is not None
        assert persisted.status.value == "queued"
        assert persisted.started_at is None
    finally:
        backend.dispose()


def test_current_repository_has_no_expiry_transition() -> None:
    persistence = _persistence()

    assert not hasattr(persistence.SQLModelPreviewRequestRepository, "mark_expired")


def _stored_package() -> object:
    models = import_module("core.preview.models")
    package = _package()
    return models.PreviewStoredPackage(
        storage_key="request-1",
        manifest=package.manifest,
        files=package.files,
    )


def test_mark_ready_constructs_and_persists_complete_strict_candidate(tmp_path: Path) -> None:
    backend = _backend(tmp_path)
    snapshot = _snapshot()
    try:
        with backend.create_preview_write_unit_of_work() as uow:
            uow.requests.create_queued(_request(status="queued"))
            uow.commit()
        with backend.create_preview_write_unit_of_work() as uow:
            assert uow.requests.mark_running("request-1", datetime(2026, 7, 3, 1, tzinfo=UTC)) is not None
            uow.commit()
        with backend.create_preview_write_unit_of_work() as uow:
            assert (
                uow.requests.mark_ready(
                    "request-1",
                    datetime(2026, 7, 3, 2, tzinfo=UTC),
                    snapshot,
                    _stored_package(),
                )
                is True
            )
            uow.commit()

        with backend.create_preview_read_unit_of_work() as uow:
            ready = uow.requests.get_for_owner("request-1", "confluent_cloud", "tenant-1")
        assert ready is not None
        assert ready.status.value == "ready"
        assert ready.source_snapshot == snapshot
        assert ready.storage_key == "request-1"
        assert ready.package == _package()
    finally:
        backend.dispose()


def test_mark_ready_rejects_invalid_daily_cutoff_candidate_without_mutation(tmp_path: Path) -> None:
    backend = _backend(tmp_path)
    invalid = _snapshot(cutoff=date(2026, 7, 2))
    try:
        with backend.create_preview_write_unit_of_work() as uow:
            uow.requests.create_queued(_request(status="queued"))
            uow.commit()
        with backend.create_preview_write_unit_of_work() as uow:
            assert uow.requests.mark_running("request-1", datetime(2026, 7, 3, 1, tzinfo=UTC)) is not None
            uow.commit()
        with pytest.raises(ValueError), backend.create_preview_write_unit_of_work() as uow:
            uow.requests.mark_ready(
                "request-1",
                datetime(2026, 7, 3, 2, tzinfo=UTC),
                invalid,
                _stored_package(),
            )
            uow.commit()

        with backend.create_preview_read_unit_of_work() as uow:
            running = uow.requests.get_for_owner("request-1", "confluent_cloud", "tenant-1")
        assert running is not None
        assert running.status.value == "running"
        assert running.source_snapshot is None
        assert running.package is None
    finally:
        backend.dispose()


def test_mark_failed_constructs_complete_candidate_and_rejects_out_of_order_time(tmp_path: Path) -> None:
    models = import_module("core.preview.models")
    backend = _backend(tmp_path)
    diagnostic = models.PreviewDiagnostic("failed", "failed", False)
    try:
        with backend.create_preview_write_unit_of_work() as uow:
            uow.requests.create_queued(_request(status="queued"))
            uow.commit()
        with pytest.raises(ValueError), backend.create_preview_write_unit_of_work() as uow:
            uow.requests.mark_failed("request-1", datetime(2026, 7, 2, tzinfo=UTC), diagnostic)
            uow.commit()
        with backend.create_preview_read_unit_of_work() as uow:
            queued = uow.requests.get_for_owner("request-1", "confluent_cloud", "tenant-1")
        assert queued is not None
        assert queued.status.value == "queued"

        with backend.create_preview_write_unit_of_work() as uow:
            assert (
                uow.requests.mark_failed(
                    "request-1",
                    datetime(2026, 7, 3, 2, tzinfo=UTC),
                    diagnostic,
                )
                is True
            )
            uow.commit()
        with backend.create_preview_read_unit_of_work() as uow:
            failed = uow.requests.get_for_owner("request-1", "confluent_cloud", "tenant-1")
        assert failed is not None
        assert failed.status.value == "failed"
        assert failed.diagnostic == diagnostic
        assert failed.source_snapshot is None
        assert failed.package is None
    finally:
        backend.dispose()


def test_stale_ready_transition_returns_false_without_queued_mutation(tmp_path: Path) -> None:
    backend = _backend(tmp_path)
    try:
        with backend.create_preview_write_unit_of_work() as uow:
            uow.requests.create_queued(_request(status="queued"))
            uow.commit()
        with backend.create_preview_write_unit_of_work() as uow:
            assert (
                uow.requests.mark_ready(
                    "request-1",
                    datetime(2026, 7, 3, 2, tzinfo=UTC),
                    _snapshot(),
                    _stored_package(),
                )
                is False
            )
            uow.commit()
        with backend.create_preview_read_unit_of_work() as uow:
            queued = uow.requests.get_for_owner("request-1", "confluent_cloud", "tenant-1")
        assert queued is not None
        assert queued.status.value == "queued"
        assert queued.completed_at is None
        assert queued.package is None
    finally:
        backend.dispose()

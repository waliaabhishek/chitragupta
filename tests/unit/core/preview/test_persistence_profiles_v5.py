from __future__ import annotations

import time
from dataclasses import FrozenInstanceError, replace
from datetime import UTC, date, datetime, timedelta
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
    assert row.expires_at is None
    assert restored.effective_columns == ("Tags", "BilledCost")
    assert restored.source_snapshot is None
    assert restored.expires_at is None


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
        expires_at=datetime(2026, 7, 10, 2, tzinfo=UTC),
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


def test_page_and_expired_artifact_values_are_frozen_and_validate_identity() -> None:
    persistence = _persistence()
    item = _request(status="queued")
    page = persistence.PreviewRequestPage(items=(item,), next_cursor="request-1")
    expired = persistence.PreviewExpiredArtifact(request_id="request-1", storage_key="opaque-key")

    assert page.items == (item,)
    assert page.next_cursor == "request-1"
    assert expired.storage_key == "opaque-key"
    with pytest.raises(FrozenInstanceError):
        page.next_cursor = None
    with pytest.raises(ValueError):
        persistence.PreviewRequestPage(items=(), next_cursor="  ")
    with pytest.raises(ValueError):
        persistence.PreviewExpiredArtifact(request_id="../x", storage_key="opaque-key")


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
            ready_at = datetime(2026, 7, 3, 2, tzinfo=UTC)
            assert (
                uow.requests.mark_ready(
                    "request-1",
                    ready_at,
                    ready_at + timedelta(days=7),
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
        assert ready.completed_at == datetime(2026, 7, 3, 2, tzinfo=UTC)
        assert ready.expires_at == datetime(2026, 7, 10, 2, tzinfo=UTC)
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
                datetime(2026, 7, 10, 2, tzinfo=UTC),
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
                    datetime(2026, 7, 10, 2, tzinfo=UTC),
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
        assert queued.expires_at is None
        assert queued.package is None
    finally:
        backend.dispose()


@pytest.mark.parametrize(
    "expires_at",
    [
        datetime(2026, 7, 10, 1, 59, 59, tzinfo=UTC),
        datetime(2026, 7, 10, 2),
        datetime(2026, 7, 3, 2, tzinfo=UTC),
    ],
)
def test_mark_ready_rejects_invalid_expiry_before_sql(tmp_path: Path, expires_at: datetime) -> None:
    backend = _backend(tmp_path)
    ready_at = datetime(2026, 7, 3, 2, tzinfo=UTC)
    try:
        with backend.create_preview_write_unit_of_work() as uow:
            uow.requests.create_queued(_request(status="queued"))
            uow.commit()
        with backend.create_preview_write_unit_of_work() as uow:
            assert uow.requests.mark_running("request-1", datetime(2026, 7, 3, 1, tzinfo=UTC)) is not None
            uow.commit()
        with pytest.raises(ValueError, match="expires"), backend.create_preview_write_unit_of_work() as uow:
            uow.requests.mark_ready("request-1", ready_at, expires_at, _snapshot(), _stored_package())
            uow.commit()

        with backend.create_preview_read_unit_of_work() as uow:
            current = uow.requests.get_for_owner("request-1", "confluent_cloud", "tenant-1")
        assert current is not None
        assert current.status.value == "running"
        assert current.completed_at is None
        assert current.expires_at is None
    finally:
        backend.dispose()


def test_recent_request_keyset_pagination_is_stable_and_owner_scoped(tmp_path: Path) -> None:
    backend = _backend(tmp_path)
    created = datetime(2026, 7, 3, tzinfo=UTC)
    try:
        with backend.create_preview_write_unit_of_work() as uow:
            for request_id, tenant_id, offset in [
                ("request-a", "tenant-1", 0),
                ("request-b", "tenant-1", 0),
                ("request-c", "tenant-1", -1),
                ("request-foreign", "tenant-2", 2),
            ]:
                uow.requests.create_queued(
                    _request(
                        status="queued",
                        request_id=request_id,
                        tenant_id=tenant_id,
                        created_at=created + timedelta(seconds=offset),
                    )
                )
            uow.commit()

        with backend.create_preview_read_unit_of_work() as uow:
            first = uow.requests.list_recent_for_owner(
                ecosystem="confluent_cloud", tenant_id="tenant-1", limit=2, cursor_request_id=None
            )
            second = uow.requests.list_recent_for_owner(
                ecosystem="confluent_cloud",
                tenant_id="tenant-1",
                limit=2,
                cursor_request_id=first.next_cursor,
            )

        assert [item.request_id for item in first.items] == ["request-b", "request-a"]
        assert first.next_cursor == "request-a"
        assert [item.request_id for item in second.items] == ["request-c"]
        assert second.next_cursor is None
    finally:
        backend.dispose()


def test_recent_request_missing_and_foreign_cursor_use_same_typed_error(tmp_path: Path) -> None:
    backend = _backend(tmp_path)
    persistence = _persistence()
    try:
        with backend.create_preview_write_unit_of_work() as uow:
            uow.requests.create_queued(_request(status="queued", request_id="foreign", tenant_id="tenant-2"))
            uow.commit()

        for cursor in ("absent", "foreign"):
            with (
                pytest.raises(persistence.PreviewRequestCursorError) as raised,
                backend.create_preview_read_unit_of_work() as uow,
            ):
                uow.requests.list_recent_for_owner(
                    ecosystem="confluent_cloud",
                    tenant_id="tenant-1",
                    limit=20,
                    cursor_request_id=cursor,
                )
            assert str(raised.value) == "preview request cursor is invalid"
    finally:
        backend.dispose()


def test_recent_requests_include_every_lifecycle_state(tmp_path: Path) -> None:
    backend = _backend(tmp_path)
    ready_at = datetime(2026, 7, 3, 2, tzinfo=UTC)
    expires_at = ready_at + timedelta(days=7)
    request_ids = ("queued", "running", "failed", "ready", "expired")
    try:
        with backend.create_preview_write_unit_of_work() as uow:
            for request_id in request_ids:
                uow.requests.create_queued(_request(status="queued", request_id=request_id))
            uow.commit()
        for request_id in request_ids[1:]:
            with backend.create_preview_write_unit_of_work() as uow:
                assert uow.requests.mark_running(request_id, datetime(2026, 7, 3, 1, tzinfo=UTC)) is not None
                uow.commit()
        with backend.create_preview_write_unit_of_work() as uow:
            assert uow.requests.mark_failed(
                "failed",
                ready_at,
                import_module("core.preview.models").PreviewDiagnostic("failed", "failed", False),
            )
            for request_id in ("ready", "expired"):
                stored = _stored_package()
                stored = replace(stored, storage_key=request_id)
                assert uow.requests.mark_ready(request_id, ready_at, expires_at, _snapshot(), stored)
            uow.commit()
        with backend.create_preview_write_unit_of_work() as uow:
            assert (
                uow.requests.expire_ready_request(
                    request_id="expired",
                    ecosystem="confluent_cloud",
                    tenant_id="tenant-1",
                    now=expires_at,
                )
                is not None
            )
            uow.commit()

        with backend.create_preview_read_unit_of_work() as uow:
            page = uow.requests.list_recent_for_owner(
                ecosystem="confluent_cloud",
                tenant_id="tenant-1",
                limit=20,
                cursor_request_id=None,
            )
        assert {item.status.value for item in page.items} == set(request_ids)
    finally:
        backend.dispose()


def test_expiry_transition_is_exact_idempotent_and_clears_only_matching_key(tmp_path: Path) -> None:
    backend = _backend(tmp_path)
    ready_at = datetime(2026, 7, 3, 2, tzinfo=UTC)
    expires_at = ready_at + timedelta(days=7)
    try:
        with backend.create_preview_write_unit_of_work() as uow:
            uow.requests.create_queued(_request(status="queued"))
            uow.commit()
        with backend.create_preview_write_unit_of_work() as uow:
            assert uow.requests.mark_running("request-1", datetime(2026, 7, 3, 1, tzinfo=UTC)) is not None
            uow.commit()
        with backend.create_preview_write_unit_of_work() as uow:
            assert uow.requests.mark_ready("request-1", ready_at, expires_at, _snapshot(), _stored_package())
            uow.commit()

        with backend.create_preview_write_unit_of_work() as uow:
            assert (
                uow.requests.expire_ready_request(
                    request_id="request-1",
                    ecosystem="confluent_cloud",
                    tenant_id="tenant-1",
                    now=expires_at - timedelta(microseconds=1),
                )
                is None
            )
            artifact = uow.requests.expire_ready_request(
                request_id="request-1",
                ecosystem="confluent_cloud",
                tenant_id="tenant-1",
                now=expires_at,
            )
            uow.commit()
        assert artifact == _persistence().PreviewExpiredArtifact("request-1", "request-1")

        with backend.create_preview_write_unit_of_work() as uow:
            assert (
                uow.requests.expire_ready_request(
                    request_id="request-1",
                    ecosystem="confluent_cloud",
                    tenant_id="tenant-1",
                    now=expires_at,
                )
                is None
            )
            assert uow.requests.clear_expired_storage_key("request-1", "stale") is False
            assert uow.requests.clear_expired_storage_key("request-1", "request-1") is True
            assert uow.requests.clear_expired_storage_key("request-1", "request-1") is False
            uow.commit()

        with backend.create_preview_read_unit_of_work() as uow:
            expired = uow.requests.get_for_owner("request-1", "confluent_cloud", "tenant-1")
        assert expired is not None
        assert expired.status.value == "expired"
        assert expired.expires_at == expires_at
        assert expired.storage_key is None
        assert expired.package == _package()
    finally:
        backend.dispose()


def test_due_expiry_is_bounded_and_ordered_by_expiry_then_request_id(tmp_path: Path) -> None:
    backend = _backend(tmp_path)
    base = datetime(2026, 7, 3, 2, tzinfo=UTC)
    try:
        with backend.create_preview_write_unit_of_work() as uow:
            for request_id in ("request-b", "request-a", "request-c"):
                uow.requests.create_queued(_request(status="queued", request_id=request_id))
            uow.commit()
        for request_id in ("request-b", "request-a", "request-c"):
            with backend.create_preview_write_unit_of_work() as uow:
                assert uow.requests.mark_running(request_id, datetime(2026, 7, 3, 1, tzinfo=UTC)) is not None
                uow.commit()
            ready_at = base + (timedelta(seconds=1) if request_id == "request-c" else timedelta())
            with backend.create_preview_write_unit_of_work() as uow:
                assert uow.requests.mark_ready(
                    request_id,
                    ready_at,
                    ready_at + timedelta(days=7),
                    _snapshot(),
                    replace(_stored_package(), storage_key=request_id),
                )
                uow.commit()

        with backend.create_preview_write_unit_of_work() as uow:
            first = uow.requests.expire_ready_due(
                ecosystem="confluent_cloud",
                tenant_id="tenant-1",
                now=base + timedelta(days=7, seconds=1),
                limit=2,
            )
            second = uow.requests.expire_ready_due(
                ecosystem="confluent_cloud",
                tenant_id="tenant-1",
                now=base + timedelta(days=7, seconds=1),
                limit=2,
            )
            uow.commit()

        assert [item.request_id for item in first] == ["request-a", "request-b"]
        assert [item.request_id for item in second] == ["request-c"]
    finally:
        backend.dispose()


def test_interruption_recovery_uses_strict_whole_second_cutoff_and_keeps_same_second_row(
    tmp_path: Path,
) -> None:
    backend = _backend(tmp_path)
    models = import_module("core.preview.models")
    startup = datetime(2026, 7, 3, 1, tzinfo=UTC)
    diagnostic = models.PreviewDiagnostic(
        "preview_generation_interrupted",
        "FOCUS Mapping Preview generation was interrupted before completion.",
        True,
    )
    try:
        with backend.create_preview_write_unit_of_work() as uow:
            uow.requests.create_queued(
                _request(status="queued", request_id="strictly-before", created_at=startup - timedelta(microseconds=1))
            )
            uow.requests.create_queued(
                _request(status="queued", request_id="same-second", created_at=startup + timedelta(milliseconds=500))
            )
            uow.requests.create_queued(
                _request(status="queued", request_id="running-before", created_at=startup - timedelta(seconds=1))
            )
            assert (
                uow.requests.mark_running(
                    "running-before",
                    startup + timedelta(milliseconds=500),
                )
                is not None
            )
            uow.commit()
        with backend.create_preview_write_unit_of_work() as uow:
            result = uow.requests.fail_interrupted_before(
                ecosystem="confluent_cloud",
                tenant_id="tenant-1",
                startup_at=startup,
                lease_stale_at=startup,
                diagnostic=diagnostic,
            )
            uow.commit()
        assert result.failed_count == 2
        assert result.protected_count == 0
        with backend.create_preview_read_unit_of_work() as uow:
            before = uow.requests.get_for_owner("strictly-before", "confluent_cloud", "tenant-1")
            same = uow.requests.get_for_owner("same-second", "confluent_cloud", "tenant-1")
            running = uow.requests.get_for_owner("running-before", "confluent_cloud", "tenant-1")
        assert before is not None and before.status.value == "failed"
        assert before.diagnostic == diagnostic
        assert same is not None and same.status.value == "queued"
        assert running is not None and running.status.value == "failed"
        assert running.started_at == startup + timedelta(milliseconds=500)
        assert running.completed_at == running.started_at
        assert running.completed_at >= running.started_at
    finally:
        backend.dispose()


def test_multi_instance_recovery_respects_live_heartbeat_recovers_stale_owner_and_keeps_same_second_gap(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    backend = _backend(tmp_path)
    service = import_module("core.preview.service")
    artifacts = import_module("core.preview.artifacts")
    mapping = import_module("core.preview.mapping")
    from tests.unit.core.preview.test_service import _tenant_config
    from tests.unit.core.preview.test_service_delivery_v6 import ImmediateExecutor

    startup = datetime(2026, 7, 3, 1, tzinfo=UTC)
    old_created = startup - timedelta(minutes=1)
    live_worker = "worker-live-process"
    dead_worker = "worker-dead-process"
    live_clock = [old_created]
    monkeypatch.setattr(service, "_PREVIEW_HEARTBEAT_INTERVAL_SECONDS", 0.01)
    live_runtime = service.PreviewRuntime(
        artifact_store=artifacts.LocalPreviewArtifactStore(tmp_path / "live-artifacts"),
        max_workers=1,
        clock=lambda: live_clock[0],
        request_id_factory=lambda: "live-request",
        executor=ImmediateExecutor(),
        lease_owner_id=live_worker,
    )
    recovering_runtime = service.PreviewRuntime(
        artifact_store=artifacts.LocalPreviewArtifactStore(tmp_path / "recovery-artifacts"),
        max_workers=1,
        startup_at=startup,
        clock=lambda: startup,
        configured_owner_keys=(("production", "confluent_cloud", "tenant-1"),),
    )
    try:
        live_runtime.submit(
            tenant_name="production",
            tenant_config=_tenant_config(backend._connection_string),
            backend=backend,
            start_date=date(2026, 7, 1),
            end_date=date(2026, 7, 2),
            grain="daily",
            column_profile="full",
            effective_columns=mapping.FOCUS_1_4_FULL_PROFILE_COLUMNS,
        )
        with backend.create_preview_write_unit_of_work() as uow:
            uow.requests.create_queued(
                _request(status="queued", request_id="stale-request", created_at=old_created),
                worker_id=dead_worker,
                lease_expires_at=startup - timedelta(seconds=1),
            )
            uow.requests.create_queued(
                _request(
                    status="queued",
                    request_id="same-second-request",
                    created_at=startup + timedelta(milliseconds=500),
                )
            )
            uow.commit()
        with backend.create_preview_write_unit_of_work() as uow:
            assert (
                uow.requests.mark_running(
                    "live-request",
                    old_created,
                    worker_id=live_worker,
                    lease_expires_at=old_created + timedelta(seconds=30),
                )
                is not None
            )
            assert (
                uow.requests.mark_running(
                    "stale-request",
                    old_created,
                    worker_id=dead_worker,
                    lease_expires_at=startup - timedelta(seconds=1),
                )
                is not None
            )
            assert (
                uow.requests.renew_lease("live-request", "different-process", startup + timedelta(seconds=30)) is False
            )
            uow.commit()

        live_clock[0] = startup - timedelta(seconds=5)
        deadline = time.monotonic() + 1
        while time.monotonic() < deadline:
            with backend._engine.connect() as connection:
                live_lease = connection.exec_driver_sql(
                    "SELECT lease_expires_at FROM preview_requests WHERE request_id = 'live-request'"
                ).scalar_one()
            if live_lease == "2026-07-03 01:00:25.000000":
                break
            time.sleep(0.01)
        assert live_lease == "2026-07-03 01:00:25.000000"

        recovering_runtime.ensure_owner_recovered(
            backend=backend,
            tenant_name="production",
            ecosystem="confluent_cloud",
            tenant_id="tenant-1",
        )

        with backend.create_preview_read_unit_of_work() as uow:
            live = uow.requests.get_for_owner("live-request", "confluent_cloud", "tenant-1")
            stale = uow.requests.get_for_owner("stale-request", "confluent_cloud", "tenant-1")
            same_second = uow.requests.get_for_owner("same-second-request", "confluent_cloud", "tenant-1")
        assert live is not None and live.status.value == "running"
        assert stale is not None and stale.status.value == "failed"
        assert stale.diagnostic is not None and stale.diagnostic.code == "preview_generation_interrupted"
        assert same_second is not None and same_second.status.value == "queued"
        with backend._engine.connect() as connection:
            rows = {
                request_id: (worker_id, lease_expires_at)
                for request_id, worker_id, lease_expires_at in connection.exec_driver_sql(
                    "SELECT request_id, worker_id, lease_expires_at FROM preview_requests "
                    "WHERE request_id IN ('live-request', 'stale-request', 'same-second-request')"
                )
            }
        assert rows["live-request"] == (live_worker, "2026-07-03 01:00:25.000000")
        assert rows["stale-request"] == (None, None)
        assert rows["same-second-request"] == (None, None)
    finally:
        live_runtime.close()
        recovering_runtime.close()
        backend.dispose()

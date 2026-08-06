from __future__ import annotations

from collections.abc import Callable
from concurrent.futures import Future
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from importlib import import_module
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest

from tests.integration.core.api.backend_provider import FixedTenantBackendProvider
from tests.unit.core.preview.evidence_backend_double import preview_evidence_backend_double
from tests.unit.core.preview.test_lifecycle_snapshot_v5 import _request
from tests.unit.core.preview.test_service import _tenant_config


class NeverBackend:
    def create_preview_write_unit_of_work(self) -> object:
        raise AssertionError("invalid selection reached persistence")

    def create_preview_metadata_read_unit_of_work(self) -> object:
        raise AssertionError("empty/future Monthly evidence reached storage")


class NeverExecutor:
    def submit(self, _task: Callable[[], None]) -> Future[None]:
        raise AssertionError("invalid selection reached scheduling")

    def shutdown(self, wait: bool = True, *, cancel_futures: bool = False) -> None:
        del wait, cancel_futures


def test_submit_validates_direct_domain_effective_columns_before_persistence_or_scheduling(
    tmp_path: Path,
) -> None:
    service = import_module("core.preview.service")
    mapping = import_module("core.preview.mapping")
    runtime = service.PreviewRuntime(
        artifact_store=object(),
        backend_provider=FixedTenantBackendProvider(),
        max_workers=1,
        clock=lambda: datetime(2026, 7, 3, tzinfo=UTC),
        executor=NeverExecutor(),
    )

    with pytest.raises(mapping.PreviewEffectiveColumnsError):
        runtime.submit(
            tenant_name="production",
            tenant_config=_tenant_config(f"sqlite:///{tmp_path / 'unused.db'}"),
            backend=NeverBackend(),
            start_date=date(2026, 7, 1),
            end_date=date(2026, 7, 2),
            grain="daily",
            column_profile="full",
            effective_columns=("BilledCost",),
        )


def test_submit_calls_mapping_validator_before_strict_snapshot_validation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = import_module("core.preview.service")
    order: list[str] = []

    def validate_columns(_profile: str, _columns: tuple[str, ...]) -> None:
        order.append("columns")
        raise RuntimeError("stop after column validation")

    def validate_snapshot(**_kwargs: object) -> None:
        order.append("snapshot")

    monkeypatch.setattr(service, "validate_preview_effective_columns", validate_columns)
    monkeypatch.setattr(service, "validate_preview_request_snapshot", validate_snapshot)
    runtime = service.PreviewRuntime(
        artifact_store=object(),
        backend_provider=FixedTenantBackendProvider(),
        max_workers=1,
        clock=lambda: datetime(2026, 7, 3, tzinfo=UTC),
        executor=NeverExecutor(),
    )

    with pytest.raises(RuntimeError, match="stop after column validation"):
        runtime.submit(
            tenant_name="production",
            tenant_config=_tenant_config(f"sqlite:///{tmp_path / 'unused.db'}"),
            backend=NeverBackend(),
            start_date=date(2026, 7, 1),
            end_date=date(2026, 7, 2),
            grain="daily",
            column_profile="custom",
            effective_columns=("BilledCost",),
        )

    assert order == ["columns"]


def _policy(*, cutoff: date) -> Any:
    eligibility = import_module("core.preview.eligibility")
    return eligibility.PreviewEligibilityPolicy(
        commercial_profile="direct_payg",
        billing_currency="USD",
        effective_start_date=date(2020, 1, 1),
        effective_end_date=date(2030, 1, 1),
        acquisition_start_date=date(2026, 6, 1),
        acquisition_end_date=cutoff,
    )


def test_future_month_maps_to_existing_retryable_pending_diagnostic_before_storage() -> None:
    generator = import_module("core.preview.generator")
    request = _request(
        grain="monthly",
        created_at=datetime(2026, 6, 30, 23, 59, tzinfo=UTC),
        started_at=datetime(2026, 6, 30, 23, 59, 1, tzinfo=UTC),
    )
    package_generator = generator.PreviewPackageGenerator(max_csv_file_bytes=None)

    with pytest.raises(generator.PreviewGenerationError) as exc_info:
        package_generator.generate(backend=NeverBackend(), request=request, policy=_policy(cutoff=date(2026, 7, 1)))

    assert exc_info.value.diagnostic.code == "calculation_pending_cutoff_window"
    assert exc_info.value.diagnostic.retryable is True


def _header_only_request() -> Any:
    return _request(
        grain="monthly",
        created_at=datetime(2026, 7, 1, tzinfo=UTC),
        started_at=datetime(2026, 7, 1, 0, 0, 1, tzinfo=UTC),
    )


def _generation_backend_with_authority(authority: object | None) -> Any:
    backend = preview_evidence_backend_double()
    uow = MagicMock()
    uow.source_readiness.get_current_authority.return_value = authority
    backend.create_preview_generation_read_unit_of_work.return_value.__enter__.return_value = uow
    return backend


def test_empty_started_month_builds_header_only_package_for_absent_legacy_authority() -> None:
    generator = import_module("core.preview.generator")
    mapping = import_module("core.preview.mapping")
    request = _header_only_request()
    package_generator = generator.PreviewPackageGenerator(max_csv_file_bytes=None)
    backend = _generation_backend_with_authority(None)

    snapshot, draft = package_generator.generate(
        backend=backend,
        request=request,
        policy=_policy(cutoff=date(2026, 7, 1)),
    )

    assert snapshot.monthly_status == "provisional"
    assert snapshot.effective_coverage_start_date == snapshot.effective_coverage_end_date == date(2026, 7, 1)
    assert snapshot.calculation_timestamp is None
    assert snapshot.source_through is None
    assert draft.data_files[0].body == (",".join(mapping.FOCUS_1_4_FULL_PROFILE_COLUMNS) + "\n").encode()
    assert draft.reconciliation == mapping.PreviewPackageReconciliation(
        source_records=0,
        source_cost=Decimal(0),
        allocated_cost=Decimal(0),
        source_quantity=Decimal(0),
        allocated_quantity=Decimal(0),
    )


@pytest.mark.parametrize("status_name", ["PENDING", "FAILED"])
def test_header_only_package_fails_closed_for_newest_noncomplete_source_attempt(
    status_name: str,
) -> None:
    evidence = import_module("core.preview.evidence")
    generator = import_module("core.preview.generator")
    status = getattr(evidence.SourceAttemptStatus, status_name)
    attempt = evidence.PreviewSourceAttempt(
        attempt_sequence=1,
        ecosystem="confluent_cloud",
        tenant_id="tenant-1",
        refresh_token="refresh-1",
        refresh_start=datetime(2026, 6, 1, tzinfo=UTC),
        refresh_end=datetime(2026, 7, 1, tzinfo=UTC),
        status=status,
        started_at=datetime(2026, 7, 1, tzinfo=UTC),
        completed_at=(
            None
            if status is evidence.SourceAttemptStatus.PENDING
            else datetime(2026, 7, 1, tzinfo=UTC) + timedelta(seconds=1)
        ),
        failure_reason=(
            None
            if status is evidence.SourceAttemptStatus.PENDING
            else evidence.SourceAttemptFailureReason.PERSISTENCE_FAILED
        ),
    )

    with pytest.raises(generator.PreviewGenerationError) as exc_info:
        generator.PreviewPackageGenerator(max_csv_file_bytes=None).generate(
            backend=_generation_backend_with_authority(attempt),
            request=_header_only_request(),
            policy=_policy(cutoff=date(2026, 7, 1)),
        )

    assert exc_info.value.diagnostic.code == "preview_source_evidence_unavailable"
    assert exc_info.value.diagnostic.retryable is True


def test_header_only_package_fails_closed_when_evidence_storage_is_unavailable() -> None:
    generator = import_module("core.preview.generator")
    availability = import_module("core.preview.storage_availability")
    backend = preview_evidence_backend_double()
    backend.create_preview_generation_read_unit_of_work.side_effect = availability.PreviewEvidenceUnavailableError(
        "private schema details"
    )

    with pytest.raises(generator.PreviewGenerationError) as exc_info:
        generator.PreviewPackageGenerator(max_csv_file_bytes=None).generate(
            backend=backend,
            request=_header_only_request(),
            policy=_policy(cutoff=date(2026, 7, 1)),
        )

    assert exc_info.value.diagnostic.code == "preview_evidence_storage_unavailable"
    assert exc_info.value.diagnostic.retryable is False
    assert "private schema details" not in exc_info.value.diagnostic.message


def test_unavailable_generation_uow_maps_to_closed_evidence_storage_diagnostic() -> None:
    generator = import_module("core.preview.generator")
    availability = import_module("core.preview.storage_availability")
    backend = preview_evidence_backend_double()
    backend.create_preview_generation_read_unit_of_work.side_effect = availability.PreviewEvidenceUnavailableError(
        "private schema details"
    )

    with pytest.raises(generator.PreviewGenerationError) as exc_info:
        generator.PreviewPackageGenerator(max_csv_file_bytes=None).generate(
            backend=backend,
            request=_request(grain="daily"),
            policy=_policy(cutoff=date(2026, 7, 2)),
        )

    assert exc_info.value.diagnostic.code == "preview_evidence_storage_unavailable"
    assert exc_info.value.diagnostic.retryable is False
    assert "private schema details" not in exc_info.value.diagnostic.message

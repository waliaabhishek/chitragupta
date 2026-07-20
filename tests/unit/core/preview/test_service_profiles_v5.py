from __future__ import annotations

from collections.abc import Callable
from concurrent.futures import Future
from datetime import UTC, date, datetime
from decimal import Decimal
from importlib import import_module
from pathlib import Path
from typing import Any

import pytest

from tests.unit.core.preview.test_lifecycle_snapshot_v5 import _request
from tests.unit.core.preview.test_service import _tenant_config


class NeverBackend:
    def create_preview_write_unit_of_work(self) -> object:
        raise AssertionError("invalid selection reached persistence")

    def create_preview_read_unit_of_work(self) -> object:
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
    service = import_module("core.preview.service")
    request = _request(
        grain="monthly",
        created_at=datetime(2026, 6, 30, 23, 59, tzinfo=UTC),
        started_at=datetime(2026, 6, 30, 23, 59, 1, tzinfo=UTC),
    )
    runtime = service.PreviewRuntime(
        artifact_store=object(),
        max_workers=1,
        executor=NeverExecutor(),
    )

    with pytest.raises(service._PreviewFailureError) as exc_info:
        runtime._generate(NeverBackend(), request, _policy(cutoff=date(2026, 7, 1)))

    assert exc_info.value.diagnostic.code == "calculation_pending_cutoff_window"
    assert exc_info.value.diagnostic.retryable is True


def test_empty_started_month_builds_header_only_provisional_package_without_storage() -> None:
    service = import_module("core.preview.service")
    mapping = import_module("core.preview.mapping")
    request = _request(
        grain="monthly",
        created_at=datetime(2026, 7, 1, tzinfo=UTC),
        started_at=datetime(2026, 7, 1, 0, 0, 1, tzinfo=UTC),
    )
    runtime = service.PreviewRuntime(
        artifact_store=object(),
        max_workers=1,
        executor=NeverExecutor(),
    )

    snapshot, package = runtime._generate(NeverBackend(), request, _policy(cutoff=date(2026, 7, 1)))

    assert snapshot.monthly_status == "provisional"
    assert snapshot.effective_coverage_start_date == snapshot.effective_coverage_end_date == date(2026, 7, 1)
    assert snapshot.calculation_timestamp is None
    assert snapshot.source_through is None
    assert package.data_files[0].body == (",".join(mapping.FOCUS_1_4_FULL_PROFILE_COLUMNS) + "\n").encode()
    manifest = __import__("json").loads(package.manifest_body)
    assert manifest["reconciliation"] == {
        "source_cost": str(Decimal(0)),
        "allocated_cost": str(Decimal(0)),
        "difference": str(Decimal(0)),
    }

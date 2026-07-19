from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal
from importlib import import_module
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest


def preview_module(name: str) -> ModuleType:
    """Import one production preview module from inside a test.

    Keeping imports out of module scope makes the TDD-red report enumerate the
    missing preview contracts instead of stopping collection at the first file.
    """

    return import_module(f"core.preview.{name}")


@pytest.fixture
def preview_artifact_root(tmp_path: Path) -> Path:
    return tmp_path / "focus-preview"


@pytest.fixture
def valid_source_evidence() -> Any:
    evidence = preview_module("evidence")
    return evidence.PreviewSourceEvidence(
        source_record_id="provider:cost-1",
        identity_scheme="provider_cost_id",
        provider_cost_id="cost-1",
        source_period_start=datetime(2026, 7, 1, tzinfo=UTC),
        source_period_end=datetime(2026, 7, 2, tzinfo=UTC),
        collection_window_start=datetime(2026, 6, 30, tzinfo=UTC),
        collection_window_end=datetime(2026, 7, 3, tzinfo=UTC),
        evidence_scope_start=datetime(2026, 7, 1, tzinfo=UTC),
        evidence_scope_end=datetime(2026, 7, 2, tzinfo=UTC),
        allocation_timestamp=datetime(2026, 7, 1, tzinfo=UTC),
        granularity="DAILY",
        native_product="KAFKA",
        native_line_type="KAFKA_STORAGE",
        amount=Decimal("8"),
        original_amount=Decimal("10"),
        discount_amount=Decimal("2"),
        price=Decimal("2"),
        quantity=Decimal("5"),
        unit="GB",
        native_description="Kafka storage usage",
        native_network_access_type="PUBLIC_INTERNET",
        resource_id="lkc-1",
        resource_name="Orders",
        environment_id="env-1",
        native_tier_dimensions=(("lower_bound", "0"), ("upper_bound", "100")),
        malformed=False,
        diagnostics=(),
    )


@pytest.fixture
def valid_aggregate_evidence() -> Any:
    evidence = preview_module("evidence")
    return evidence.PreviewAggregateEvidence(
        timestamp=datetime(2026, 7, 1, tzinfo=UTC),
        environment_id="env-1",
        resource_id="lkc-1",
        native_product="KAFKA",
        native_line_type="KAFKA_STORAGE",
        quantity=Decimal("5"),
        unit_price=Decimal("2"),
        total_cost=Decimal("8"),
        compatibility_currency="USD",
        granularity="daily",
    )


@pytest.fixture
def valid_allocation_evidence() -> Any:
    evidence = preview_module("evidence")
    return evidence.PreviewAllocationEvidence(
        timestamp=datetime(2026, 7, 1, tzinfo=UTC),
        environment_id="env-1",
        resource_id="lkc-1",
        native_product="KAFKA",
        native_line_type="KAFKA_STORAGE",
        allocation_target_id="sa-1",
        allocation_method="direct",
        amount=Decimal("8"),
    )


@pytest.fixture
def complete_coverage() -> Any:
    models = preview_module("models")
    persistence = preview_module("persistence")
    entry = models.PreviewCalculationCoverageEntry(
        tracking_date=date(2026, 7, 1),
        calculation_id="calculation-1",
        calculation_completed_at=datetime(2026, 7, 3, 2, tzinfo=UTC),
        calculation_run_id=17,
    )
    return persistence.CompleteCalculationCoverage(entries=(entry,))

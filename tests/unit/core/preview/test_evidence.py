from __future__ import annotations

import inspect
from datetime import UTC, datetime
from pathlib import Path

import pytest

from tests.unit.core.preview.conftest import preview_module


def test_core_preview_evidence_has_no_plugin_dependency() -> None:
    module = preview_module("evidence")
    source = Path(module.__file__).read_text(encoding="utf-8")

    assert "plugins." not in source


@pytest.mark.parametrize(
    ("protocol_name", "method_name", "parameter_names"),
    [
        ("PreviewCostEvidenceReader", "find_preview_source_candidates", ["self", "scope"]),
        (
            "PreviewCostEvidenceReader",
            "find_preview_aggregate_candidates",
            ["self", "scope", "source"],
        ),
        (
            "PreviewAllocationEvidenceReader",
            "find_preview_allocation_candidates",
            ["self", "scope", "source"],
        ),
    ],
)
def test_evidence_protocols_expose_intrinsically_bounded_reads(
    protocol_name: str,
    method_name: str,
    parameter_names: list[str],
) -> None:
    module = preview_module("evidence")
    method = getattr(getattr(module, protocol_name), method_name)

    assert list(inspect.signature(method).parameters) == parameter_names
    assert "limit" not in inspect.signature(method).parameters


def test_source_tier_dimensions_are_canonical_and_immutable(valid_source_evidence: object) -> None:
    assert valid_source_evidence.native_tier_dimensions == (("lower_bound", "0"), ("upper_bound", "100"))  # type: ignore[attr-defined]
    with pytest.raises((AttributeError, TypeError)):
        valid_source_evidence.native_tier_dimensions[0] = ("changed", "1")  # type: ignore[attr-defined,index]


def test_confluent_repositories_satisfy_core_preview_protocols() -> None:
    from plugins.confluent_cloud.storage.repositories import CCloudBillingRepository, CCloudChargebackRepository

    evidence = preview_module("evidence")

    assert isinstance(CCloudBillingRepository.__new__(CCloudBillingRepository), evidence.PreviewCostEvidenceReader)
    assert isinstance(
        CCloudChargebackRepository.__new__(CCloudChargebackRepository),
        evidence.PreviewAllocationEvidenceReader,
    )


def test_source_scope_model_requires_aware_ordered_bounds() -> None:
    evidence = preview_module("evidence")

    scope = evidence.PreviewEvidenceScope(
        ecosystem="confluent_cloud",
        tenant_id="tenant-1",
        start=datetime(2026, 7, 1, tzinfo=UTC),
        end=datetime(2026, 7, 2, tzinfo=UTC),
    )
    assert scope.start < scope.end

    with pytest.raises(ValueError):
        evidence.PreviewEvidenceScope(
            ecosystem="confluent_cloud",
            tenant_id="tenant-1",
            start=datetime(2026, 7, 2, tzinfo=UTC),
            end=datetime(2026, 7, 1, tzinfo=UTC),
        )

    with pytest.raises(ValueError):
        evidence.PreviewEvidenceScope(
            ecosystem="confluent_cloud",
            tenant_id="tenant-1",
            start=datetime(2026, 7, 1),
            end=datetime(2026, 7, 2),
        )

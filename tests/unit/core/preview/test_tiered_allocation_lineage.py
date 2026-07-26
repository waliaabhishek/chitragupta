from __future__ import annotations

from dataclasses import replace
from datetime import UTC, date, datetime
from decimal import Context, Decimal, localcontext
from types import SimpleNamespace
from typing import Any

import pytest

from tests.unit.core.preview.conftest import preview_module

START = datetime(2026, 7, 1, tzinfo=UTC)
END = datetime(2026, 7, 2, tzinfo=UTC)
COMPLETED_AT = datetime(2026, 7, 3, tzinfo=UTC)


def _selected(mapping: Any, source: Any) -> Any:
    classification = mapping.classify_daily_full_source(
        request_start=START,
        request_end=END,
        source=source,
    )
    assert isinstance(classification, mapping.AcceptedPreviewSource)
    assert source.amount is not None
    return mapping.SelectedSourceProjection(
        source,
        classification.semantics,
        mapping.project_financials(
            source=source,
            semantics=classification.semantics,
            billed_share=source.amount,
        ),
    )


def _source(valid_source_evidence: Any, source_id: str, *, amount: str, original: str, quantity: str) -> Any:
    return replace(
        valid_source_evidence,
        source_record_id=source_id,
        provider_cost_id=source_id,
        amount=Decimal(amount),
        original_amount=Decimal(original),
        discount_amount=Decimal(original) - Decimal(amount),
        price=Decimal("2"),
        quantity=Decimal(quantity),
        native_tier_dimensions=(("tier", source_id),),
        billing_timestamp=START,
        billing_env_id="env-1",
        billing_resource_id="lkc-1",
        billing_product_type="KAFKA_STORAGE",
        billing_product_category="KAFKA",
    )


def _aggregate(evidence: Any, source: Any) -> Any:
    return evidence.PreviewAggregateEvidence(
        timestamp=START,
        environment_id="env-1",
        resource_id="lkc-1",
        native_product="KAFKA",
        native_line_type="KAFKA_STORAGE",
        quantity=source.quantity,
        unit_price=source.price,
        total_cost=source.amount,
        compatibility_currency="USD",
        granularity="daily",
        source_record_id=source.source_record_id,
        evidence_scope_start=source.evidence_scope_start,
        evidence_scope_end=source.evidence_scope_end,
    )


def _allocation(
    evidence: Any,
    source: Any,
    *,
    ordinal: int = 0,
    cost: str | None = None,
    quantity: str | None = None,
    original: str | None = None,
    source_id: str | None = None,
) -> Any:
    allocated_cost = source.amount if cost is None else Decimal(cost)
    allocated_quantity = source.quantity if quantity is None else Decimal(quantity)
    allocated_original = source.original_amount if original is None else Decimal(original)
    assert allocated_cost is not None
    assert allocated_quantity is not None
    assert allocated_original is not None
    assert source.amount is not None
    with localcontext(Context(prec=38)):
        allocation_ratio = allocated_cost / source.amount
    return evidence.PreviewAllocationEvidence(
        timestamp=START,
        environment_id="env-1",
        resource_id="lkc-1",
        native_product="KAFKA",
        native_line_type="KAFKA_STORAGE",
        allocation_target_id="sa-1",
        allocation_method="direct",
        amount=allocated_cost,
        calculation_id="calculation-1",
        portion_ordinal=ordinal,
        target_kind="identity",
        target_id="sa-1",
        allocated_cost=allocated_cost,
        allocated_quantity=allocated_quantity,
        allocation_ratio=allocation_ratio,
        method_id="direct",
        method_version="v1",
        method_details_json='{"allocation_detail":"direct","metadata":{},"target_kind":"identity"}',
        origin_total_cost=source.amount,
        origin_quantity=source.quantity,
        origin_unit_price=source.price,
        origin_currency="USD",
        origin_granularity="daily",
        source_record_id=source.source_record_id if source_id is None else source_id,
        evidence_scope_start=source.evidence_scope_start,
        evidence_scope_end=source.evidence_scope_end,
        allocated_original_cost=allocated_original,
        origin_original_cost=source.original_amount,
    )


def test_two_same_compatibility_key_tiers_reconcile_as_distinct_exact_origins(
    valid_source_evidence: Any,
) -> None:
    evidence = preview_module("evidence")
    mapping = preview_module("mapping")
    sources = (
        _source(valid_source_evidence, "provider:tier-a", amount="3", original="4", quantity="2"),
        _source(valid_source_evidence, "provider:tier-b", amount="5", original="6", quantity="3"),
    )
    aggregates = tuple(_aggregate(evidence, source) for source in sources)

    selected_by_origin, aggregates_by_origin = mapping.reconcile_source_aggregate_stream(
        selected_sources=tuple(_selected(mapping, source) for source in sources),
        aggregates=aggregates,
    )

    assert len(selected_by_origin) == len(aggregates_by_origin) == 2
    assert {selected.source.source_record_id for selected in selected_by_origin.values()} == {
        "provider:tier-a",
        "provider:tier-b",
    }
    assert len({_key for _key in selected_by_origin}) == 2


def test_allocation_stream_uses_derived_preview_count_not_compatibility_count(
    valid_source_evidence: Any,
) -> None:
    evidence = preview_module("evidence")
    mapping = preview_module("mapping")
    sources = (
        _source(valid_source_evidence, "provider:tier-a", amount="3", original="4", quantity="2"),
        _source(valid_source_evidence, "provider:tier-b", amount="5", original="6", quantity="3"),
    )
    aggregates = tuple(_aggregate(evidence, source) for source in sources)
    selected_by_origin, aggregates_by_origin = mapping.reconcile_source_aggregate_stream(
        selected_sources=tuple(_selected(mapping, source) for source in sources),
        aggregates=aggregates,
    )
    del selected_by_origin
    run = evidence.PreviewAllocationRunEvidence(
        ecosystem="confluent_cloud",
        tenant_id="tenant-1",
        tracking_date=date(2026, 7, 1),
        calculation_id="calculation-1",
        calculation_completed_at=COMPLETED_AT,
        capture_status=evidence.AllocationLineageRunStatus.COMPLETE,
        capture_reason=None,
        portion_count=1,
        preview_portion_count=2,
    )

    by_origin = mapping.reconcile_allocation_lineage_stream(
        aggregates_by_origin=aggregates_by_origin,
        expected_completion_by_run={(date(2026, 7, 1), "calculation-1"): COMPLETED_AT},
        runs=(run,),
        allocations=tuple(_allocation(evidence, source) for source in sources),
    )

    assert len(by_origin) == 2
    assert all(len(portions) == 1 for portions in by_origin.values())


def test_project_allocated_financials_uses_exact_persisted_original_cost_not_rounded_ratio(
    valid_source_evidence: Any,
) -> None:
    evidence = preview_module("evidence")
    mapping = preview_module("mapping")
    source = _source(valid_source_evidence, "provider:tier-a", amount="3", original="10", quantity="5")
    allocation = _allocation(
        evidence,
        source,
        cost="1",
        quantity="1.6666666666666666666666666666666666667",
        original="3.34",
    )

    projected = mapping.project_allocated_financials(
        selected=_selected(mapping, source),
        allocation=allocation,
    )

    with localcontext(Context(prec=38)):
        expected_ratio = Decimal(1) / Decimal(3)
    assert allocation.allocation_ratio == expected_ratio
    assert projected.billed_cost == Decimal("1")
    assert projected.contracted_cost == Decimal("3.34")
    assert projected.list_cost == Decimal("3.34")


def test_exact_source_mismatch_and_original_cost_reconciliation_fail_closed(
    valid_source_evidence: Any,
) -> None:
    evidence = preview_module("evidence")
    mapping = preview_module("mapping")
    source = _source(valid_source_evidence, "provider:tier-a", amount="3", original="10", quantity="5")
    aggregate = _aggregate(evidence, source)
    wrong_source = _allocation(evidence, source, source_id="provider:tier-b")
    with pytest.raises(mapping.PreviewAllocationLineageError):
        mapping.validate_allocation_lineage_evidence(
            aggregate=aggregate,
            allocations=(wrong_source,),
        )

    portions = (
        _allocation(evidence, source, ordinal=0, cost="1", quantity="2", original="3"),
        _allocation(evidence, source, ordinal=1, cost="2", quantity="3", original="6"),
    )
    with pytest.raises(mapping.PreviewFinancialReconciliationError):
        mapping.validate_allocation_lineage_evidence(
            aggregate=aggregate,
            allocations=portions,
        )


@pytest.mark.parametrize("persisted_ratio", [Decimal("0"), Decimal("0.7")])
def test_persisted_ratio_must_equal_precision_38_recomputation(
    valid_source_evidence: Any,
    persisted_ratio: Decimal,
) -> None:
    evidence = preview_module("evidence")
    mapping = preview_module("mapping")
    source = _source(valid_source_evidence, "provider:tier-a", amount="4", original="4", quantity="4")
    aggregate = _aggregate(evidence, source)
    allocation = replace(
        _allocation(evidence, source, cost="3", quantity="3", original="3"),
        allocation_ratio=persisted_ratio,
    )

    with pytest.raises(mapping.PreviewAllocationLineageError):
        mapping.validate_allocation_lineage_evidence(
            aggregate=aggregate,
            allocations=(allocation,),
        )


def test_compatibility_source_totals_use_preview_context_independent_of_order(
    valid_source_evidence: Any,
) -> None:
    evidence = preview_module("evidence")
    mapping = preview_module("mapping")
    sources = (
        replace(
            _source(valid_source_evidence, "provider:large", amount="1E+37", original="1E+37", quantity="1"),
            discount_amount=Decimal(0),
            price=Decimal("1E+37"),
        ),
        replace(
            _source(valid_source_evidence, "provider:unit", amount="1", original="1", quantity="1"),
            discount_amount=Decimal(0),
            price=Decimal(1),
        ),
        replace(
            _source(valid_source_evidence, "provider:refund", amount="-1E+37", original="-1E+37", quantity="-1"),
            discount_amount=Decimal(0),
            price=Decimal("1E+37"),
            native_description="Refund Kafka storage usage",
        ),
    )
    aggregates = tuple(
        replace(
            _aggregate(evidence, source),
            compatibility_total_cost=Decimal(1),
            compatibility_quantity=Decimal(1),
        )
        for source in sources
    )
    selected = tuple(_selected(mapping, source) for source in sources)

    with localcontext(Context(prec=6)):
        for selected_order, aggregate_order in (
            (selected, aggregates),
            (tuple(reversed(selected)), tuple(reversed(aggregates))),
        ):
            selected_by_origin, aggregates_by_origin = mapping.reconcile_source_aggregate_stream(
                selected_sources=selected_order,
                aggregates=aggregate_order,
            )
            assert len(selected_by_origin) == len(aggregates_by_origin) == 3


def test_exact_source_columns_reconcile_independently_with_compatibility_portion(
    valid_source_evidence: Any,
) -> None:
    evidence = preview_module("evidence")
    mapping = preview_module("mapping")
    sources = (
        _source(valid_source_evidence, "provider:tier-a", amount="3", original="3", quantity="2"),
        _source(valid_source_evidence, "provider:tier-b", amount="5", original="5", quantity="3"),
    )
    allocations = tuple(
        replace(
            _allocation(evidence, source),
            compatibility_allocated_cost=Decimal(8),
            compatibility_allocated_quantity=Decimal(5),
        )
        for source in sources
    )
    mapping.reconcile_compatibility_allocation_columns(allocations)

    with pytest.raises(mapping.PreviewFinancialReconciliationError):
        mapping.reconcile_compatibility_allocation_columns(
            tuple(replace(allocation, compatibility_allocated_cost=Decimal(7)) for allocation in allocations)
        )


def test_generator_spool_keeps_same_key_tiers_distinct_and_reconciles_exact_totals(
    valid_source_evidence: Any,
) -> None:
    evidence = preview_module("evidence")
    generator = preview_module("generator")
    mapping = preview_module("mapping")
    sources = (
        _source(valid_source_evidence, "provider:tier-a", amount="3", original="4", quantity="2"),
        _source(valid_source_evidence, "provider:tier-b", amount="5", original="6", quantity="3"),
    )
    aggregates = tuple(_aggregate(evidence, source) for source in sources)
    allocations = tuple(_allocation(evidence, source) for source in sources)
    run = evidence.PreviewAllocationRunEvidence(
        ecosystem="confluent_cloud",
        tenant_id="tenant-1",
        tracking_date=date(2026, 7, 1),
        calculation_id="calculation-1",
        calculation_completed_at=COMPLETED_AT,
        capture_status=evidence.AllocationLineageRunStatus.COMPLETE,
        capture_reason=None,
        portion_count=1,
        preview_portion_count=2,
    )
    spool = generator._PreviewEvidenceSpool(limit_bytes=10_000_000)
    try:
        for source in sources:
            spool.add_selected(_selected(mapping, source))
        for aggregate in aggregates:
            spool.add_aggregate(aggregate)
        spool.reconcile_sources(
            SimpleNamespace(
                ecosystem="confluent_cloud",
                tenant_id="tenant-1",
                source_snapshot=None,
            )
        )
        spool.add_and_reconcile_allocations(
            expected_completion_by_run={(date(2026, 7, 1), "calculation-1"): COMPLETED_AT},
            runs=(run,),
            allocations=allocations,
        )

        origins = tuple(spool.iter_origins())
        assert len(origins) == 2
        assert {selected.source.source_record_id for _origin, selected, _aggregate in origins} == {
            "provider:tier-a",
            "provider:tier-b",
        }
        assert spool.reconciliation.source_cost == Decimal("8")
        assert spool.reconciliation.allocated_cost == Decimal("8")
        assert spool.reconciliation.source_quantity == Decimal("5")
        assert spool.reconciliation.allocated_quantity == Decimal("5")
    finally:
        spool.close()

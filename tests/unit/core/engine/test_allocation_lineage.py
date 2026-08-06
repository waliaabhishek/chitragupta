from __future__ import annotations

import json
from datetime import UTC, datetime
from decimal import Decimal
from importlib import import_module
from typing import Any

import pytest

from core.models.chargeback import ChargebackRow, CostType
from plugins.confluent_cloud.models.billing import CCloudBillingLineItem


def _lineage() -> Any:
    return import_module("core.engine.allocation_lineage")


def _origin(**overrides: object) -> CCloudBillingLineItem:
    values: dict[str, object] = {
        "ecosystem": "confluent_cloud",
        "tenant_id": "org-1",
        "timestamp": datetime(2026, 7, 1, tzinfo=UTC),
        "env_id": "env-1",
        "resource_id": "lkc-1",
        "product_category": "KAFKA",
        "product_type": "KAFKA_STORAGE",
        "quantity": Decimal("1.000"),
        "unit_price": Decimal("3"),
        "total_cost": Decimal("3"),
        "currency": "USD",
        "granularity": "daily",
    }
    values.update(overrides)
    return CCloudBillingLineItem(**values)  # type: ignore[arg-type]


def _row(*, target: str, amount: str, **overrides: object) -> ChargebackRow:
    values: dict[str, object] = {
        "ecosystem": "confluent_cloud",
        "tenant_id": "org-1",
        "timestamp": datetime(2026, 7, 1, tzinfo=UTC),
        "resource_id": "lkc-1",
        "product_category": "KAFKA",
        "product_type": "KAFKA_STORAGE",
        "identity_id": target,
        "cost_type": CostType.USAGE,
        "amount": Decimal(amount),
        "allocation_method": "usage_ratio",
        "allocation_detail": "usage_ratio_allocation",
        "metadata": {"env_id": "env-1"},
    }
    values.update(overrides)
    return ChargebackRow(**values)  # type: ignore[arg-type]


def test_builder_preserves_actual_order_types_targets_and_exact_realized_values() -> None:
    lineage = _lineage()
    origin = _origin()
    rows = (
        _row(target="sa-2", amount="1"),
        _row(target="lkc-1", amount="1"),
        _row(target="UNALLOCATED", amount="1"),
    )

    capture = lineage.build_allocation_lineage_capture(origin=origin, rows=rows)

    assert capture.status is lineage.LineageCaptureStatus.COMPLETE
    assert capture.reason is None
    assert capture.origin_timestamp == origin.timestamp
    assert capture.origin_env_id == "env-1"
    assert capture.origin_resource_id == "lkc-1"
    assert capture.origin_product_type == "KAFKA_STORAGE"
    assert capture.origin_product_category == "KAFKA"
    assert [fact.portion_ordinal for fact in capture.facts] == [0, 1, 2]
    assert [fact.target_kind for fact in capture.facts] == [
        lineage.AllocationTargetKind.IDENTITY,
        lineage.AllocationTargetKind.RESOURCE,
        lineage.AllocationTargetKind.UNALLOCATED,
    ]
    assert [fact.target_id for fact in capture.facts] == ["sa-2", "lkc-1", None]
    assert [fact.allocated_cost for fact in capture.facts] == [Decimal("1")] * 3
    assert [fact.allocation_ratio for fact in capture.facts] == [
        Decimal("0.33333333333333333333333333333333333333")
    ] * 3
    assert [fact.allocated_quantity for fact in capture.facts] == [
        Decimal("0.333"),
        Decimal("0.333"),
        Decimal("0.334"),
    ]
    assert sum((fact.allocated_quantity for fact in capture.facts), Decimal()) == origin.quantity
    assert [fact.method_id for fact in capture.facts] == ["usage_ratio"] * 3
    assert [fact.method_version for fact in capture.facts] == ["v1"] * 3


def test_builder_canonicalizes_typed_method_details_without_using_allocator_ratio_as_provenance() -> None:
    lineage = _lineage()
    row = _row(
        target="sa-1",
        amount="2",
        metadata={
            "ratio": Decimal("0.9990"),
            "composition_ratio": 0.25,
            "composition_index": 2,
            "nested": {"z": None, "a": [True, "value", 4]},
        },
    )

    fact = lineage.build_allocation_lineage_capture(origin=_origin(), rows=(row,)).facts[0]

    assert fact.allocation_ratio == Decimal("0.66666666666666666666666666666666666667")
    assert fact.method_details_json == (
        '{"allocation_detail":"usage_ratio_allocation","metadata":'
        '{"composition_index":2,"composition_ratio":0.25,"nested":'
        '{"a":[true,"value",4],"z":null},"ratio":{"decimal":"0.999"}},'
        '"target_kind":"identity"}'
    )
    assert json.loads(fact.method_details_json)["metadata"]["ratio"] == {"decimal": "0.999"}


@pytest.mark.parametrize(
    ("origin_overrides", "rows", "reason_name"),
    [
        ({"total_cost": Decimal("0")}, (_row(target="sa-1", amount="0"),), "ZERO_ORIGIN_COST"),
        ({}, (), "NO_PORTIONS"),
        ({}, (_row(target=" ", amount="3"),), None),
        ({}, (_row(target="sa-1", amount="NaN"),), "INVALID_ROW_COST"),
        ({}, (_row(target="sa-1", amount="-1"),), "INVALID_RATIO"),
        ({}, (_row(target="sa-1", amount="3", allocation_method=""),), "INVALID_METHOD"),
        ({}, (_row(target="sa-1", amount="3", metadata={"bad": object()}),), "INVALID_METADATA"),
        (
            {},
            (_row(target="sa-1", amount="6"), _row(target="sa-2", amount="0")),
            "INVALID_QUANTITY",
        ),
    ],
)
def test_builder_returns_closed_invalid_capture_for_unanalyzable_inputs(
    origin_overrides: dict[str, object],
    rows: tuple[ChargebackRow, ...],
    reason_name: str | None,
) -> None:
    lineage = _lineage()

    capture = lineage.build_allocation_lineage_capture(origin=_origin(**origin_overrides), rows=rows)

    assert capture.status is lineage.LineageCaptureStatus.INVALID
    if reason_name is None:
        assert capture.reason in tuple(lineage.LineageCaptureReason)
    else:
        assert capture.reason is getattr(lineage.LineageCaptureReason, reason_name)
    assert capture.facts == ()


def test_builder_persists_actual_shortfall_without_inventing_remainder_portion() -> None:
    lineage = _lineage()
    rows = (_row(target="sa-1", amount="1"), _row(target="sa-2", amount="1"))

    capture = lineage.build_allocation_lineage_capture(origin=_origin(), rows=rows)

    assert capture.status is lineage.LineageCaptureStatus.COMPLETE
    assert len(capture.facts) == len(rows)
    assert [fact.allocated_cost for fact in capture.facts] == [Decimal("1"), Decimal("1")]
    assert sum((fact.allocated_cost for fact in capture.facts), Decimal()) == Decimal("2")
    assert capture.facts[-1].allocated_quantity == Decimal("0.667")


@pytest.mark.parametrize(
    "metadata",
    [
        {"value": Decimal("NaN")},
        {"value": float("inf")},
        {1: "non-string-key"},
    ],
    ids=("non-finite-decimal", "non-finite-float", "non-string-mapping-key"),
)
def test_builder_rejects_closed_metadata_codec_inputs(metadata: dict[object, object]) -> None:
    lineage = _lineage()

    capture = lineage.build_allocation_lineage_capture(
        origin=_origin(),
        rows=(_row(target="sa-1", amount="3", metadata=metadata),),
    )

    assert capture.status is lineage.LineageCaptureStatus.INVALID
    assert capture.reason is lineage.LineageCaptureReason.INVALID_METADATA
    assert capture.facts == ()


def test_builder_zero_quantity_emits_zero_for_every_actual_portion() -> None:
    lineage = _lineage()
    capture = lineage.build_allocation_lineage_capture(
        origin=_origin(quantity=Decimal("0.000")),
        rows=(_row(target="sa-1", amount="1"), _row(target="sa-2", amount="2")),
    )

    assert [fact.allocated_quantity for fact in capture.facts] == [Decimal("0.000"), Decimal("0.000")]

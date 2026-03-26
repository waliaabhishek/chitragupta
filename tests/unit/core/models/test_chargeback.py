from __future__ import annotations

from dataclasses import asdict
from datetime import UTC, datetime
from decimal import Decimal

from core.models.chargeback import ChargebackRow, CostType

_NOW = datetime(2024, 1, 15, 12, 0, 0, tzinfo=UTC)


class TestCostType:
    def test_values(self) -> None:
        assert CostType.USAGE == "usage"
        assert CostType.SHARED == "shared"

    def test_string_conversion(self) -> None:
        assert str(CostType.USAGE) == "usage"
        assert CostType("shared") is CostType.SHARED


class TestChargebackRow:
    def test_construction_usage(self) -> None:
        row = ChargebackRow(
            ecosystem="confluent",
            tenant_id="t-001",
            timestamp=_NOW,
            resource_id="lkc-abc",
            product_category="kafka",
            product_type="ckus",
            identity_id="u-1",
            cost_type=CostType.USAGE,
            amount=Decimal("10.50"),
            allocation_method="direct",
            allocation_detail="100% owner",
            tags=["prod"],
            metadata={"note": "test"},
        )
        assert row.ecosystem == "confluent"
        assert row.cost_type is CostType.USAGE
        assert row.amount == Decimal("10.50")
        assert row.allocation_method == "direct"
        assert row.tags == ["prod"]

    def test_construction_shared(self) -> None:
        row = ChargebackRow(
            ecosystem="confluent",
            tenant_id="t-001",
            timestamp=_NOW,
            resource_id=None,
            product_category="support",
            product_type="enterprise_support",
            identity_id="u-1",
            cost_type=CostType.SHARED,
        )
        assert row.cost_type is CostType.SHARED
        assert row.resource_id is None

    def test_nullable_resource_id(self) -> None:
        row = ChargebackRow(
            ecosystem="confluent",
            tenant_id="t-001",
            timestamp=_NOW,
            resource_id=None,
            product_category="org",
            product_type="org_cost",
            identity_id="u-1",
            cost_type=CostType.SHARED,
        )
        assert row.resource_id is None

    def test_defaults(self) -> None:
        row = ChargebackRow(
            ecosystem="confluent",
            tenant_id="t-001",
            timestamp=_NOW,
            resource_id="r-1",
            product_category="kafka",
            product_type="ckus",
            identity_id="u-1",
            cost_type=CostType.USAGE,
        )
        assert row.amount == Decimal("0")
        assert row.allocation_method is None
        assert row.allocation_detail is None
        assert row.tags == {}
        assert row.metadata == {}

    def test_tags_mutation(self) -> None:
        row = ChargebackRow(
            ecosystem="confluent",
            tenant_id="t-001",
            timestamp=_NOW,
            resource_id="r-1",
            product_category="kafka",
            product_type="ckus",
            identity_id="u-1",
            cost_type=CostType.USAGE,
        )
        row.tags["new-key"] = "new-val"
        assert "new-key" in row.tags

    def test_asdict_round_trip(self) -> None:
        row = ChargebackRow(
            ecosystem="confluent",
            tenant_id="t-001",
            timestamp=_NOW,
            resource_id="lkc-abc",
            product_category="kafka",
            product_type="ckus",
            identity_id="u-1",
            cost_type=CostType.USAGE,
            amount=Decimal("5.00"),
            tags=["a", "b"],
        )
        d = asdict(row)
        row2 = ChargebackRow(**d)
        assert row == row2

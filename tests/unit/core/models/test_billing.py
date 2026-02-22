from __future__ import annotations

from dataclasses import asdict
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from core.models.billing import BillingLineItem

_NOW = datetime(2024, 1, 15, 12, 0, 0, tzinfo=UTC)


class TestBillingLineItem:
    def test_construction(self) -> None:
        b = BillingLineItem(
            ecosystem="confluent",
            tenant_id="t-001",
            timestamp=_NOW,
            resource_id="lkc-abc",
            product_category="kafka",
            product_type="kafka_num_ckus",
            quantity=Decimal("100"),
            unit_price=Decimal("0.01"),
            total_cost=Decimal("1.00"),
            currency="EUR",
            granularity="hourly",
            metadata={"invoice": "INV-001"},
        )
        assert b.ecosystem == "confluent"
        assert b.total_cost == Decimal("1.00")
        assert b.currency == "EUR"
        assert b.granularity == "hourly"
        assert b.metadata == {"invoice": "INV-001"}

    def test_defaults(self) -> None:
        b = BillingLineItem(
            ecosystem="aws",
            tenant_id="t-001",
            timestamp=_NOW,
            resource_id="r-1",
            product_category="compute",
            product_type="ec2",
            quantity=Decimal("1"),
            unit_price=Decimal("0.10"),
            total_cost=Decimal("0.10"),
        )
        assert b.currency == "USD"
        assert b.granularity == "daily"
        assert b.metadata == {}

    def test_frozen_enforcement(self) -> None:
        b = BillingLineItem(
            ecosystem="confluent",
            tenant_id="t-001",
            timestamp=_NOW,
            resource_id="lkc-abc",
            product_category="kafka",
            product_type="kafka_num_ckus",
            quantity=Decimal("1"),
            unit_price=Decimal("1"),
            total_cost=Decimal("1"),
        )
        with pytest.raises(AttributeError):
            b.total_cost = Decimal("999")  # type: ignore[misc] -- intentional write to frozen field

    def test_asdict_round_trip(self) -> None:
        b = BillingLineItem(
            ecosystem="confluent",
            tenant_id="t-001",
            timestamp=_NOW,
            resource_id="lkc-abc",
            product_category="kafka",
            product_type="kafka_num_ckus",
            quantity=Decimal("100.5"),
            unit_price=Decimal("0.01"),
            total_cost=Decimal("1.005"),
        )
        d = asdict(b)
        b2 = BillingLineItem(**d)
        assert b == b2

    def test_decimal_precision(self) -> None:
        b = BillingLineItem(
            ecosystem="confluent",
            tenant_id="t-001",
            timestamp=_NOW,
            resource_id="r-1",
            product_category="kafka",
            product_type="ckus",
            quantity=Decimal("0.001"),
            unit_price=Decimal("0.00001"),
            total_cost=Decimal("0.00000001"),
        )
        assert b.quantity == Decimal("0.001")
        assert b.unit_price == Decimal("0.00001")
        assert b.total_cost == Decimal("0.00000001")

    def test_zero_total_cost(self) -> None:
        b = BillingLineItem(
            ecosystem="confluent",
            tenant_id="t-001",
            timestamp=_NOW,
            resource_id="r-1",
            product_category="kafka",
            product_type="ckus",
            quantity=Decimal("0"),
            unit_price=Decimal("0"),
            total_cost=Decimal("0"),
        )
        assert b.total_cost == Decimal("0")

    def test_negative_total_cost(self) -> None:
        b = BillingLineItem(
            ecosystem="confluent",
            tenant_id="t-001",
            timestamp=_NOW,
            resource_id="r-1",
            product_category="kafka",
            product_type="credit",
            quantity=Decimal("1"),
            unit_price=Decimal("-5.00"),
            total_cost=Decimal("-5.00"),
        )
        assert b.total_cost == Decimal("-5.00")

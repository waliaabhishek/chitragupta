from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest

# After implementation: BillingLineItem becomes a runtime_checkable Protocol.
# Currently it is a frozen dataclass — isinstance structural subtyping won't work.
from core.models.billing import BillingLineItem

# After implementation: CCloudBillingLineItem is the concrete dataclass with env_id.
# Currently does not exist — ImportError causes test collection failure (red state).
from plugins.confluent_cloud.models.billing import CCloudBillingLineItem

_NOW = datetime(2024, 1, 15, 12, 0, 0, tzinfo=UTC)


def _make_ccloud_line(env_id: str = "env-abc") -> CCloudBillingLineItem:
    return CCloudBillingLineItem(
        ecosystem="confluent_cloud",
        tenant_id="org-001",
        timestamp=_NOW,
        env_id=env_id,
        resource_id="lkc-xxxxx",
        product_category="kafka",
        product_type="kafka_num_ckus",
        quantity=Decimal("10"),
        unit_price=Decimal("0.10"),
        total_cost=Decimal("1.00"),
        currency="USD",
        granularity="hourly",
    )


class TestBillingLineItemProtocol:
    def test_billing_line_item_is_runtime_checkable_protocol(self) -> None:
        """BillingLineItem must be a runtime_checkable Protocol, not a dataclass."""
        from typing import Protocol

        assert issubclass(BillingLineItem, Protocol)  # type: ignore[arg-type]

    def test_ccloud_billing_line_item_isinstance_billing_line_item(self) -> None:
        """CCloudBillingLineItem satisfies BillingLineItem Protocol structurally."""
        line = _make_ccloud_line()
        assert isinstance(line, BillingLineItem)

    def test_billing_line_item_protocol_properties(self) -> None:
        """All required Protocol properties are accessible on CCloudBillingLineItem."""
        line = _make_ccloud_line()
        assert line.ecosystem == "confluent_cloud"
        assert line.tenant_id == "org-001"
        assert line.timestamp == _NOW
        assert line.resource_id == "lkc-xxxxx"
        assert line.product_category == "kafka"
        assert line.product_type == "kafka_num_ckus"
        assert line.total_cost == Decimal("1.00")
        assert line.currency == "USD"
        assert line.granularity == "hourly"

    def test_ccloud_billing_line_item_has_env_id(self) -> None:
        """CCloudBillingLineItem exposes env_id — not part of core Protocol."""
        line = _make_ccloud_line(env_id="env-xyz")
        assert line.env_id == "env-xyz"

    def test_ccloud_billing_line_item_is_frozen(self) -> None:
        """CCloudBillingLineItem is immutable (frozen dataclass)."""
        line = _make_ccloud_line()
        with pytest.raises(AttributeError):
            line.total_cost = Decimal("999")  # type: ignore[misc]

    def test_two_ccloud_lines_differ_only_by_env_id(self) -> None:
        """Two CCloudBillingLineItems with different env_id are not equal."""
        line_a = _make_ccloud_line(env_id="env-aaa")
        line_b = _make_ccloud_line(env_id="env-bbb")
        assert line_a != line_b
        assert line_a.env_id != line_b.env_id
        # Both satisfy the Protocol
        assert isinstance(line_a, BillingLineItem)
        assert isinstance(line_b, BillingLineItem)

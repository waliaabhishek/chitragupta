from __future__ import annotations

from datetime import UTC, date, datetime, timezone
from decimal import Decimal
from typing import Any

import pytest

from core.models.billing import BillingLineItem
from core.models.chargeback import ChargebackRow, CostType, CustomTag
from core.models.identity import Identity
from core.models.pipeline import PipelineState
from core.models.resource import Resource, ResourceStatus
from core.storage.backends.sqlmodel.mappers import (
    billing_to_domain,
    billing_to_table,
    chargeback_to_dimension,
    chargeback_to_domain,
    chargeback_to_fact,
    ensure_utc,
    ensure_utc_strict,
    identity_to_domain,
    identity_to_table,
    pipeline_state_to_domain,
    pipeline_state_to_table,
    resource_to_domain,
    resource_to_table,
    tag_to_domain,
    tag_to_table,
)
from core.storage.backends.sqlmodel.tables import (
    ChargebackDimensionTable,
    ChargebackFactTable,
)


class TestEnsureUtc:
    def test_none_returns_none(self):
        assert ensure_utc(None) is None

    def test_naive_gets_utc(self):
        dt = datetime(2026, 1, 1, 12, 0, 0)
        result = ensure_utc(dt)
        assert result is not None
        assert result.tzinfo is UTC

    def test_utc_unchanged(self):
        dt = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)
        result = ensure_utc(dt)
        assert result == dt
        assert result.tzinfo is UTC


class TestResourceMapper:
    def _make_resource(self, **overrides: Any) -> Resource:
        defaults = dict(
            ecosystem="ccloud",
            tenant_id="t1",
            resource_id="r1",
            resource_type="kafka",
            display_name="My Cluster",
            parent_id="env-1",
            owner_id="u-1",
            status=ResourceStatus.ACTIVE,
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
            deleted_at=None,
            last_seen_at=datetime(2026, 1, 15, tzinfo=UTC),
            metadata={"cloud": "aws", "region": "us-east-1", "extra": "val"},
        )
        defaults.update(overrides)
        return Resource(**defaults)

    def test_round_trip(self):
        r = self._make_resource()
        table = resource_to_table(r)
        # Promoted columns
        assert table.cloud == "aws"
        assert table.region == "us-east-1"
        # Remaining metadata
        assert '"extra"' in (table.metadata_json or "")
        domain = resource_to_domain(table)
        assert domain.ecosystem == r.ecosystem
        assert domain.resource_id == r.resource_id
        assert domain.metadata["cloud"] == "aws"
        assert domain.metadata["region"] == "us-east-1"
        assert domain.metadata["extra"] == "val"
        assert domain.status == ResourceStatus.ACTIVE

    def test_empty_metadata(self):
        r = self._make_resource(metadata={})
        table = resource_to_table(r)
        assert table.cloud is None
        assert table.region is None
        assert table.metadata_json is None
        domain = resource_to_domain(table)
        assert domain.metadata == {}

    def test_null_created_at(self):
        r = self._make_resource(created_at=None)
        table = resource_to_table(r)
        assert table.created_at is None
        domain = resource_to_domain(table)
        assert domain.created_at is None

    def test_deleted_status(self):
        r = self._make_resource(status=ResourceStatus.DELETED)
        table = resource_to_table(r)
        assert table.status == "deleted"
        domain = resource_to_domain(table)
        assert domain.status == ResourceStatus.DELETED

    def test_naive_datetime_raises_on_write(self):
        """GAP-014: Write path rejects naive datetimes."""
        r = self._make_resource(created_at=datetime(2026, 1, 1))
        with pytest.raises(ValueError, match="Naive datetime"):
            resource_to_table(r)


class TestIdentityMapper:
    def _make_identity(self, **overrides: Any) -> Identity:
        defaults = dict(
            ecosystem="ccloud",
            tenant_id="t1",
            identity_id="u1",
            identity_type="user",
            display_name="Alice",
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
            deleted_at=None,
            last_seen_at=datetime(2026, 1, 15, tzinfo=UTC),
            metadata={"role": "admin"},
        )
        defaults.update(overrides)
        return Identity(**defaults)

    def test_round_trip(self):
        i = self._make_identity()
        table = identity_to_table(i)
        domain = identity_to_domain(table)
        assert domain.ecosystem == i.ecosystem
        assert domain.identity_id == i.identity_id
        assert domain.metadata == {"role": "admin"}

    def test_empty_metadata(self):
        i = self._make_identity(metadata={})
        table = identity_to_table(i)
        assert table.metadata_json is None
        domain = identity_to_domain(table)
        assert domain.metadata == {}


class TestBillingMapper:
    def _make_billing(self, **overrides: Any) -> BillingLineItem:
        defaults = dict(
            ecosystem="ccloud",
            tenant_id="t1",
            timestamp=datetime(2026, 1, 1, tzinfo=UTC),
            resource_id="r1",
            product_category="compute",
            product_type="kafka",
            quantity=Decimal("100.5"),
            unit_price=Decimal("0.01"),
            total_cost=Decimal("1.005"),
            currency="USD",
            granularity="daily",
            metadata={},
        )
        defaults.update(overrides)
        return BillingLineItem(**defaults)

    def test_round_trip(self):
        b = self._make_billing()
        table = billing_to_table(b)
        assert table.quantity == "100.5"
        assert table.unit_price == "0.01"
        assert table.total_cost == "1.005"
        domain = billing_to_domain(table)
        assert domain.quantity == Decimal("100.5")
        assert domain.total_cost == Decimal("1.005")

    def test_high_precision_decimal(self):
        b = self._make_billing(quantity=Decimal("0.000000001"))
        table = billing_to_table(b)
        domain = billing_to_domain(table)
        assert domain.quantity == Decimal("0.000000001")


class TestChargebackMapper:
    def _make_row(self, **overrides: Any) -> ChargebackRow:
        defaults = dict(
            ecosystem="ccloud",
            tenant_id="t1",
            timestamp=datetime(2026, 1, 1, tzinfo=UTC),
            resource_id="r1",
            product_category="compute",
            product_type="kafka",
            identity_id="u1",
            cost_type=CostType.USAGE,
            amount=Decimal("50.25"),
            allocation_method="direct",
            allocation_detail=None,
            tags=["team:platform", "env:prod"],
            metadata={"debug": "info"},
        )
        defaults.update(overrides)
        return ChargebackRow(**defaults)

    def test_to_dimension(self):
        row = self._make_row()
        dim = chargeback_to_dimension(row)
        assert dim.ecosystem == "ccloud"
        assert dim.cost_type == "usage"
        assert dim.dimension_id is None  # not saved yet

    def test_to_fact(self):
        row = self._make_row()
        fact = chargeback_to_fact(row, dimension_id=42)
        assert fact.dimension_id == 42
        assert fact.amount == "50.25"
        assert '"team:platform"' in fact.tags_json

    def test_to_domain(self):
        dim = ChargebackDimensionTable(
            dimension_id=1,
            ecosystem="ccloud",
            tenant_id="t1",
            resource_id="r1",
            product_category="compute",
            product_type="kafka",
            identity_id="u1",
            cost_type="usage",
            allocation_method="direct",
            allocation_detail=None,
        )
        fact = ChargebackFactTable(
            timestamp=datetime(2026, 1, 1, tzinfo=UTC),
            dimension_id=1,
            amount="50.25",
            tags_json='["team:platform"]',
        )
        domain = chargeback_to_domain(dim, fact)
        assert domain.amount == Decimal("50.25")
        assert domain.tags == ["team:platform"]
        assert domain.metadata == {}  # metadata is transient
        assert domain.cost_type == CostType.USAGE

    def test_null_resource_id(self):
        row = self._make_row(resource_id=None)
        dim = chargeback_to_dimension(row)
        assert dim.resource_id is None
        # Full round-trip: dimension + fact → domain
        dim.dimension_id = 99
        fact = chargeback_to_fact(row, dimension_id=99)
        domain = chargeback_to_domain(dim, fact)
        assert domain.resource_id is None
        assert domain.amount == row.amount


class TestPipelineStateMapper:
    def test_round_trip(self):
        ps = PipelineState(
            ecosystem="ccloud",
            tenant_id="t1",
            tracking_date=date(2026, 1, 1),
            billing_gathered=True,
            resources_gathered=False,
            chargeback_calculated=False,
        )
        table = pipeline_state_to_table(ps)
        assert table.billing_gathered is True
        domain = pipeline_state_to_domain(table)
        assert domain.billing_gathered is True
        assert domain.tracking_date == date(2026, 1, 1)


class TestCustomTagMapper:
    def test_round_trip(self):
        tag = CustomTag(
            tag_id=1,
            dimension_id=42,
            tag_key="team",
            tag_value="platform",
            created_by="admin",
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
        )
        table = tag_to_table(tag)
        assert table.tag_key == "team"
        domain = tag_to_domain(table)
        assert domain.tag_id == 1
        assert domain.tag_key == "team"
        assert domain.created_at is not None
        assert domain.created_at.tzinfo is UTC

    def test_none_created_at_gets_default(self):
        tag = CustomTag(
            tag_id=None,
            dimension_id=42,
            tag_key="k",
            tag_value="v",
            created_by="admin",
            created_at=None,
        )
        table = tag_to_table(tag)
        assert table.created_at is not None


# --- GAP-014 regression tests ---

_UTC5 = timezone(offset=__import__("datetime").timedelta(hours=5))


class TestEnsureUtcStrictGap014:
    """GAP-014: Write-path strict UTC enforcement."""

    def test_raises_on_naive(self):
        with pytest.raises(ValueError, match="Naive datetime"):
            ensure_utc_strict(datetime(2026, 1, 1))

    def test_converts_non_utc(self):
        dt = datetime(2026, 1, 1, 17, 0, 0, tzinfo=_UTC5)
        result = ensure_utc_strict(dt)
        assert result is not None
        assert result.tzinfo is UTC
        assert result.hour == 12

    def test_none_returns_none(self):
        assert ensure_utc_strict(None) is None

    def test_utc_passthrough(self):
        dt = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)
        result = ensure_utc_strict(dt)
        assert result == dt


class TestReadPathPermissiveGap014:
    """GAP-014: Read path tolerates naive datetimes from DB."""

    def test_resource_to_domain_naive_created_at(self):
        from core.storage.backends.sqlmodel.tables import ResourceTable

        t = ResourceTable(
            ecosystem="test",
            tenant_id="t1",
            resource_id="r1",
            resource_type="kafka",
            status="active",
            created_at=datetime(2026, 1, 1),  # naive from DB
        )
        domain = resource_to_domain(t)
        assert domain.created_at is not None
        assert domain.created_at.tzinfo is UTC

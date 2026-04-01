from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

ECOSYSTEM = "aws"
TENANT_ID = "t1"
TIMESTAMP = datetime(2024, 1, 15, 0, 0, 0, tzinfo=UTC)


def _make_billing_line(
    *,
    ecosystem: str = ECOSYSTEM,
    resource_id: str = "r1",
    product_type: str = "ec2",
    product_category: str = "compute",
    total_cost: Decimal = Decimal("20.00"),
) -> Any:
    from core.models.billing import CoreBillingLineItem

    return CoreBillingLineItem(
        ecosystem=ecosystem,
        tenant_id=TENANT_ID,
        timestamp=TIMESTAMP,
        resource_id=resource_id,
        product_category=product_category,
        product_type=product_type,
        quantity=Decimal("1"),
        unit_price=total_cost,
        total_cost=total_cost,
    )


def _make_resource(
    *,
    ecosystem: str = ECOSYSTEM,
    resource_id: str = "cluster-1",
    resource_type: str = "kafka_cluster",
) -> Any:
    from core.models.resource import CoreResource

    return CoreResource(
        ecosystem=ecosystem,
        tenant_id=TENANT_ID,
        resource_id=resource_id,
        resource_type=resource_type,
        last_seen_at=TIMESTAMP,
    )


def _make_identity(
    *,
    ecosystem: str = ECOSYSTEM,
    identity_id: str = "user-1",
    identity_type: str = "service_account",
) -> Any:
    from core.models.identity import CoreIdentity

    return CoreIdentity(
        ecosystem=ecosystem,
        tenant_id=TENANT_ID,
        identity_id=identity_id,
        identity_type=identity_type,
        last_seen_at=TIMESTAMP,
    )


class TestBillingEmitRow:
    def test_billing_emit_row_importable(self) -> None:
        from core.emitters.emit_rows import BillingEmitRow  # noqa: F401

    def test_csv_fields_is_empty_tuple(self) -> None:
        from core.emitters.emit_rows import BillingEmitRow

        assert BillingEmitRow.__csv_fields__ == ()

    def test_has_prometheus_metrics_classvar(self) -> None:
        from core.emitters.emit_rows import BillingEmitRow
        from core.models.emit_descriptors import MetricDescriptor

        assert hasattr(BillingEmitRow, "__prometheus_metrics__")
        assert isinstance(BillingEmitRow.__prometheus_metrics__, tuple)
        assert len(BillingEmitRow.__prometheus_metrics__) >= 1
        assert all(isinstance(d, MetricDescriptor) for d in BillingEmitRow.__prometheus_metrics__)

    def test_prometheus_metric_name(self) -> None:
        from core.emitters.emit_rows import BillingEmitRow

        names = {d.name for d in BillingEmitRow.__prometheus_metrics__}
        assert "chitragupta_billing_amount" in names

    def test_from_line_maps_total_cost_to_amount(self) -> None:
        from core.emitters.emit_rows import BillingEmitRow

        line = _make_billing_line(total_cost=Decimal("42.50"))
        row = BillingEmitRow.from_line(line, TENANT_ID, TIMESTAMP)
        assert row.amount == Decimal("42.50")

    def test_from_line_copies_ecosystem(self) -> None:
        from core.emitters.emit_rows import BillingEmitRow

        line = _make_billing_line(ecosystem="ccloud")
        row = BillingEmitRow.from_line(line, TENANT_ID, TIMESTAMP)
        assert row.ecosystem == "ccloud"

    def test_from_line_copies_resource_id(self) -> None:
        from core.emitters.emit_rows import BillingEmitRow

        line = _make_billing_line(resource_id="lkc-abc")
        row = BillingEmitRow.from_line(line, TENANT_ID, TIMESTAMP)
        assert row.resource_id == "lkc-abc"

    def test_from_line_copies_product_type(self) -> None:
        from core.emitters.emit_rows import BillingEmitRow

        line = _make_billing_line(product_type="KAFKA_CKU")
        row = BillingEmitRow.from_line(line, TENANT_ID, TIMESTAMP)
        assert row.product_type == "KAFKA_CKU"

    def test_from_line_sets_timestamp(self) -> None:
        from core.emitters.emit_rows import BillingEmitRow

        ts = datetime(2024, 3, 1, 0, 0, 0, tzinfo=UTC)
        line = _make_billing_line()
        row = BillingEmitRow.from_line(line, TENANT_ID, ts)
        assert row.timestamp == ts

    def test_satisfies_row_protocol(self) -> None:
        from core.emitters.emit_rows import BillingEmitRow
        from core.emitters.protocols import Row

        row = BillingEmitRow(
            tenant_id=TENANT_ID,
            ecosystem=ECOSYSTEM,
            resource_id="r1",
            product_type="ec2",
            product_category="compute",
            amount=Decimal("10.00"),
            timestamp=TIMESTAMP,
        )
        assert isinstance(row, Row)


class TestResourceEmitRow:
    def test_resource_emit_row_importable(self) -> None:
        from core.emitters.emit_rows import ResourceEmitRow  # noqa: F401

    def test_csv_fields_is_empty_tuple(self) -> None:
        from core.emitters.emit_rows import ResourceEmitRow

        assert ResourceEmitRow.__csv_fields__ == ()

    def test_has_prometheus_metrics_classvar(self) -> None:
        from core.emitters.emit_rows import ResourceEmitRow
        from core.models.emit_descriptors import MetricDescriptor

        assert hasattr(ResourceEmitRow, "__prometheus_metrics__")
        assert all(isinstance(d, MetricDescriptor) for d in ResourceEmitRow.__prometheus_metrics__)

    def test_prometheus_metric_name(self) -> None:
        from core.emitters.emit_rows import ResourceEmitRow

        names = {d.name for d in ResourceEmitRow.__prometheus_metrics__}
        assert "chitragupta_resource_active" in names

    def test_from_resource_sets_amount_to_one(self) -> None:
        from core.emitters.emit_rows import ResourceEmitRow

        r = _make_resource()
        row = ResourceEmitRow.from_resource(r, TENANT_ID, TIMESTAMP)
        assert row.amount == Decimal(1)

    def test_from_resource_copies_resource_id(self) -> None:
        from core.emitters.emit_rows import ResourceEmitRow

        r = _make_resource(resource_id="lkc-xyz")
        row = ResourceEmitRow.from_resource(r, TENANT_ID, TIMESTAMP)
        assert row.resource_id == "lkc-xyz"

    def test_from_resource_copies_resource_type(self) -> None:
        from core.emitters.emit_rows import ResourceEmitRow

        r = _make_resource(resource_type="schema_registry")
        row = ResourceEmitRow.from_resource(r, TENANT_ID, TIMESTAMP)
        assert row.resource_type == "schema_registry"

    def test_from_resource_sets_timestamp(self) -> None:
        from core.emitters.emit_rows import ResourceEmitRow

        ts = datetime(2024, 6, 1, 0, 0, 0, tzinfo=UTC)
        r = _make_resource()
        row = ResourceEmitRow.from_resource(r, TENANT_ID, ts)
        assert row.timestamp == ts

    def test_satisfies_row_protocol(self) -> None:
        from core.emitters.emit_rows import ResourceEmitRow
        from core.emitters.protocols import Row

        row = ResourceEmitRow(
            tenant_id=TENANT_ID,
            ecosystem=ECOSYSTEM,
            resource_id="r1",
            resource_type="kafka_cluster",
            amount=Decimal(1),
            timestamp=TIMESTAMP,
        )
        assert isinstance(row, Row)


class TestIdentityEmitRow:
    def test_identity_emit_row_importable(self) -> None:
        from core.emitters.emit_rows import IdentityEmitRow  # noqa: F401

    def test_csv_fields_is_empty_tuple(self) -> None:
        from core.emitters.emit_rows import IdentityEmitRow

        assert IdentityEmitRow.__csv_fields__ == ()

    def test_has_prometheus_metrics_classvar(self) -> None:
        from core.emitters.emit_rows import IdentityEmitRow
        from core.models.emit_descriptors import MetricDescriptor

        assert hasattr(IdentityEmitRow, "__prometheus_metrics__")
        assert all(isinstance(d, MetricDescriptor) for d in IdentityEmitRow.__prometheus_metrics__)

    def test_prometheus_metric_name(self) -> None:
        from core.emitters.emit_rows import IdentityEmitRow

        names = {d.name for d in IdentityEmitRow.__prometheus_metrics__}
        assert "chitragupta_identity_active" in names

    def test_from_identity_sets_amount_to_one(self) -> None:
        from core.emitters.emit_rows import IdentityEmitRow

        i = _make_identity()
        row = IdentityEmitRow.from_identity(i, TENANT_ID, TIMESTAMP)
        assert row.amount == Decimal(1)

    def test_from_identity_copies_identity_id(self) -> None:
        from core.emitters.emit_rows import IdentityEmitRow

        i = _make_identity(identity_id="sa-123")
        row = IdentityEmitRow.from_identity(i, TENANT_ID, TIMESTAMP)
        assert row.identity_id == "sa-123"

    def test_from_identity_copies_identity_type(self) -> None:
        from core.emitters.emit_rows import IdentityEmitRow

        i = _make_identity(identity_type="user")
        row = IdentityEmitRow.from_identity(i, TENANT_ID, TIMESTAMP)
        assert row.identity_type == "user"

    def test_from_identity_sets_timestamp(self) -> None:
        from core.emitters.emit_rows import IdentityEmitRow

        ts = datetime(2024, 9, 1, 0, 0, 0, tzinfo=UTC)
        i = _make_identity()
        row = IdentityEmitRow.from_identity(i, TENANT_ID, ts)
        assert row.timestamp == ts

    def test_satisfies_row_protocol(self) -> None:
        from core.emitters.emit_rows import IdentityEmitRow
        from core.emitters.protocols import Row

        row = IdentityEmitRow(
            tenant_id=TENANT_ID,
            ecosystem=ECOSYSTEM,
            identity_id="u1",
            identity_type="user",
            amount=Decimal(1),
            timestamp=TIMESTAMP,
        )
        assert isinstance(row, Row)

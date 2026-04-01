from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock

import pytest

ECOSYSTEM = "aws"
TENANT_ID = "t1"
DATE = date(2024, 1, 15)
BILLING_TS = datetime(2024, 1, 15, 0, 0, 0, tzinfo=UTC)


# ---------------------------------------------------------------------------
# Fake storage helpers
# ---------------------------------------------------------------------------


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
        timestamp=BILLING_TS,
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
        last_seen_at=BILLING_TS,
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
        last_seen_at=BILLING_TS,
    )


@pytest.fixture()
def fake_storage() -> Any:
    billing_data = [_make_billing_line(total_cost=Decimal("42.50"))]
    resources = [
        _make_resource(resource_id="cluster-1"),
        _make_resource(resource_id="sr-1", resource_type="schema_registry"),
    ]
    identities = [_make_identity(identity_id="user-1"), _make_identity(identity_id="sa-1", identity_type="user")]

    uow = MagicMock()
    uow.__enter__ = MagicMock(return_value=uow)
    uow.__exit__ = MagicMock(return_value=False)
    uow.billing.find_by_date.return_value = billing_data
    uow.resources.find_active_at.return_value = (resources, len(resources))
    uow.identities.find_active_at.return_value = (identities, len(identities))
    uow.chargebacks.get_distinct_dates.return_value = [DATE]
    uow.topic_attributions.get_distinct_dates.return_value = [DATE]
    uow.topic_attributions.find_by_date.return_value = []

    storage = MagicMock()
    storage.billing_data = billing_data
    storage.resources_data = resources
    storage.identities_data = identities
    storage.create_unit_of_work.return_value = uow
    storage.create_read_only_unit_of_work.return_value = uow
    return storage


# ---------------------------------------------------------------------------
# BillingRowFetcher
# ---------------------------------------------------------------------------


class TestBillingRowFetcher:
    def test_importable(self) -> None:
        from core.emitters.sources import BillingRowFetcher  # noqa: F401

    def test_fetch_by_date_returns_billing_emit_rows(self, fake_storage: Any) -> None:
        from core.emitters.emit_rows import BillingEmitRow
        from core.emitters.sources import BillingRowFetcher

        fetcher = BillingRowFetcher(fake_storage)
        rows = fetcher.fetch_by_date(ECOSYSTEM, TENANT_ID, DATE)
        assert len(rows) == 1
        assert isinstance(rows[0], BillingEmitRow)

    def test_fetch_by_date_maps_total_cost_to_amount(self, fake_storage: Any) -> None:
        """Verification test 7 from design doc."""
        from core.emitters.sources import BillingRowFetcher

        fetcher = BillingRowFetcher(fake_storage)
        rows = fetcher.fetch_by_date(ECOSYSTEM, TENANT_ID, DATE)
        assert all(isinstance(r.amount, Decimal) for r in rows)
        assert rows[0].amount == fake_storage.billing_data[0].total_cost

    def test_fetch_by_date_timestamp_is_midnight_utc(self, fake_storage: Any) -> None:
        """Verification test 7 from design doc."""
        from core.emitters.sources import BillingRowFetcher

        fetcher = BillingRowFetcher(fake_storage)
        rows = fetcher.fetch_by_date(ECOSYSTEM, TENANT_ID, DATE)
        assert rows[0].timestamp == datetime(2024, 1, 15, 0, 0, 0, tzinfo=UTC)

    def test_satisfies_pipeline_row_fetcher_protocol(self, fake_storage: Any) -> None:
        from core.emitters.protocols import PipelineRowFetcher
        from core.emitters.sources import BillingRowFetcher

        fetcher = BillingRowFetcher(fake_storage)
        assert isinstance(fetcher, PipelineRowFetcher)


# ---------------------------------------------------------------------------
# ResourceRowFetcher
# ---------------------------------------------------------------------------


class TestResourceRowFetcher:
    def test_importable(self) -> None:
        from core.emitters.sources import ResourceRowFetcher  # noqa: F401

    def test_fetch_by_date_returns_resource_emit_rows(self, fake_storage: Any) -> None:
        from core.emitters.emit_rows import ResourceEmitRow
        from core.emitters.sources import ResourceRowFetcher

        fetcher = ResourceRowFetcher(fake_storage)
        rows = fetcher.fetch_by_date(ECOSYSTEM, TENANT_ID, DATE)
        assert len(rows) == 2
        assert all(isinstance(r, ResourceEmitRow) for r in rows)

    def test_fetch_by_date_amount_is_one(self, fake_storage: Any) -> None:
        """Verification test 8 from design doc — active indicator."""
        from core.emitters.sources import ResourceRowFetcher

        fetcher = ResourceRowFetcher(fake_storage)
        rows = fetcher.fetch_by_date(ECOSYSTEM, TENANT_ID, DATE)
        assert all(r.amount == Decimal(1) for r in rows)

    def test_fetch_by_date_timestamp_is_midnight_utc(self, fake_storage: Any) -> None:
        """Verification test 8 from design doc."""
        from core.emitters.sources import ResourceRowFetcher

        fetcher = ResourceRowFetcher(fake_storage)
        rows = fetcher.fetch_by_date(ECOSYSTEM, TENANT_ID, DATE)
        assert all(r.timestamp == datetime(2024, 1, 15, 0, 0, 0, tzinfo=UTC) for r in rows)

    def test_satisfies_pipeline_row_fetcher_protocol(self, fake_storage: Any) -> None:
        from core.emitters.protocols import PipelineRowFetcher
        from core.emitters.sources import ResourceRowFetcher

        fetcher = ResourceRowFetcher(fake_storage)
        assert isinstance(fetcher, PipelineRowFetcher)


# ---------------------------------------------------------------------------
# IdentityRowFetcher
# ---------------------------------------------------------------------------


class TestIdentityRowFetcher:
    def test_importable(self) -> None:
        from core.emitters.sources import IdentityRowFetcher  # noqa: F401

    def test_fetch_by_date_returns_identity_emit_rows(self, fake_storage: Any) -> None:
        from core.emitters.emit_rows import IdentityEmitRow
        from core.emitters.sources import IdentityRowFetcher

        fetcher = IdentityRowFetcher(fake_storage)
        rows = fetcher.fetch_by_date(ECOSYSTEM, TENANT_ID, DATE)
        assert len(rows) == 2
        assert all(isinstance(r, IdentityEmitRow) for r in rows)

    def test_fetch_by_date_amount_is_one(self, fake_storage: Any) -> None:
        """Verification test 8 from design doc — active indicator."""
        from core.emitters.sources import IdentityRowFetcher

        fetcher = IdentityRowFetcher(fake_storage)
        rows = fetcher.fetch_by_date(ECOSYSTEM, TENANT_ID, DATE)
        assert all(r.amount == Decimal(1) for r in rows)

    def test_fetch_by_date_timestamp_is_midnight_utc(self, fake_storage: Any) -> None:
        from core.emitters.sources import IdentityRowFetcher

        fetcher = IdentityRowFetcher(fake_storage)
        rows = fetcher.fetch_by_date(ECOSYSTEM, TENANT_ID, DATE)
        assert all(r.timestamp == datetime(2024, 1, 15, 0, 0, 0, tzinfo=UTC) for r in rows)

    def test_satisfies_pipeline_row_fetcher_protocol(self, fake_storage: Any) -> None:
        from core.emitters.protocols import PipelineRowFetcher
        from core.emitters.sources import IdentityRowFetcher

        fetcher = IdentityRowFetcher(fake_storage)
        assert isinstance(fetcher, PipelineRowFetcher)


# ---------------------------------------------------------------------------
# TopicAttributionDateSource (moved from workflow_runner)
# ---------------------------------------------------------------------------


class TestTopicAttributionDateSource:
    def test_importable_from_sources(self) -> None:
        from core.emitters.sources import TopicAttributionDateSource  # noqa: F401

    def test_get_distinct_dates(self, fake_storage: Any) -> None:
        from core.emitters.sources import TopicAttributionDateSource

        source = TopicAttributionDateSource(fake_storage)
        dates = source.get_distinct_dates(ECOSYSTEM, TENANT_ID)
        assert dates == [DATE]

    def test_not_importable_from_workflow_runner(self) -> None:
        with pytest.raises((ImportError, AttributeError)):
            from workflow_runner import TopicAttributionDateSource  # type: ignore[attr-defined]  # noqa: F401


# ---------------------------------------------------------------------------
# TopicAttributionRowFetcher (moved from workflow_runner)
# ---------------------------------------------------------------------------


class TestTopicAttributionRowFetcher:
    def test_importable_from_sources(self) -> None:
        from core.emitters.sources import TopicAttributionRowFetcher  # noqa: F401

    def test_fetch_by_date(self, fake_storage: Any) -> None:
        from core.emitters.sources import TopicAttributionRowFetcher

        fetcher = TopicAttributionRowFetcher(fake_storage)
        rows = fetcher.fetch_by_date(ECOSYSTEM, TENANT_ID, DATE)
        assert isinstance(rows, list)
        uow = fake_storage.create_read_only_unit_of_work.return_value.__enter__.return_value
        uow.topic_attributions.find_by_date.assert_called_once_with(ECOSYSTEM, TENANT_ID, DATE)

    def test_not_importable_from_workflow_runner(self) -> None:
        with pytest.raises((ImportError, AttributeError)):
            from workflow_runner import TopicAttributionRowFetcher  # type: ignore[attr-defined]  # noqa: F401

    def test_satisfies_pipeline_row_fetcher_protocol(self, fake_storage: Any) -> None:
        from core.emitters.protocols import PipelineRowFetcher
        from core.emitters.sources import TopicAttributionRowFetcher

        fetcher = TopicAttributionRowFetcher(fake_storage)
        assert isinstance(fetcher, PipelineRowFetcher)

    def test_does_not_satisfy_aggregated_row_fetcher(self, fake_storage: Any) -> None:
        from core.emitters.protocols import PipelineAggregatedRowFetcher
        from core.emitters.sources import TopicAttributionRowFetcher

        fetcher = TopicAttributionRowFetcher(fake_storage)
        assert not isinstance(fetcher, PipelineAggregatedRowFetcher)
        assert not hasattr(fetcher, "fetch_aggregated")

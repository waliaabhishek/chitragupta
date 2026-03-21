from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from unittest.mock import MagicMock

from core.engine.allocation import AllocatorRegistry
from core.engine.orchestrator import CalculatePhase
from core.models.billing import CoreBillingLineItem
from core.models.identity import IdentityResolution, IdentitySet
from core.plugin.registry import EcosystemBundle

_NOW = datetime(2026, 2, 22, 12, 0, 0, tzinfo=UTC)
_ECO = "confluent_cloud"
_TENANT = "t-1"
_PRODUCT_TYPE = "KAFKA_CKU"


def _raising_allocator(ctx: object) -> None:
    raise RuntimeError("allocation failed — test-triggered")


def _make_calculate_phase() -> CalculatePhase:
    mock_handler = MagicMock()
    mock_handler.service_type = "kafka"
    mock_handler.handles_product_types = [_PRODUCT_TYPE]
    mock_handler.resolve_identities.return_value = IdentityResolution(
        resource_active=IdentitySet(),
        metrics_derived=IdentitySet(),
        tenant_period=IdentitySet(),
    )
    mock_handler.get_allocator.return_value = _raising_allocator

    bundle = EcosystemBundle(
        plugin=MagicMock(),
        handlers={"kafka": mock_handler},
        product_type_to_handler={_PRODUCT_TYPE: mock_handler},
        fallback_allocator=None,
    )

    mock_retry_checker = MagicMock()
    mock_retry_checker.increment_and_check.return_value = (3, True)

    return CalculatePhase(
        ecosystem=_ECO,
        tenant_id=_TENANT,
        bundle=bundle,
        retry_checker=mock_retry_checker,
        metrics_source=None,
        allocator_registry=AllocatorRegistry(),  # empty → KeyError → handler fallback
        identity_overrides={},
        allocator_params={},
        metrics_step=timedelta(hours=1),
    )


class TestAllocateToUnallocatedEnvId:
    """Gap 1: env_id must be carried into UNALLOCATED row when allocation fallback triggers."""

    def test_ccloud_billing_line_env_id_carried_to_unallocated_row(self) -> None:
        """Item 1: CCloudBillingLineItem(env_id='env-abc') triggers fallback → row.metadata['env_id'] == 'env-abc'."""
        from plugins.confluent_cloud.models.billing import CCloudBillingLineItem

        line = CCloudBillingLineItem(
            ecosystem=_ECO,
            tenant_id=_TENANT,
            timestamp=_NOW,
            env_id="env-abc",
            resource_id="cluster-1",
            product_category="kafka",
            product_type=_PRODUCT_TYPE,
            quantity=Decimal(1),
            unit_price=Decimal("100"),
            total_cost=Decimal("100"),
        )

        phase = _make_calculate_phase()
        uow = MagicMock()
        b_start = _NOW
        b_end = _NOW + timedelta(hours=24)
        b_dur = timedelta(hours=24)

        rows = phase._collect_billing_line_rows(
            line=line,
            uow=uow,
            prefetched_metrics={},
            failed_metric_keys=frozenset(),
            tenant_period_cache={(b_start, b_end): IdentitySet()},
            resource_cache={},
            line_window_cache={id(line): (b_start, b_end, b_dur)},
        )

        assert len(rows) == 1
        assert rows[0].identity_id == "UNALLOCATED"
        assert rows[0].metadata["env_id"] == "env-abc"

    def test_plain_billing_line_no_env_id_unallocated_row_has_empty_metadata(self) -> None:
        """Item 2: CoreBillingLineItem (no env_id attr) triggers fallback → row.metadata == {}."""
        line = CoreBillingLineItem(
            ecosystem=_ECO,
            tenant_id=_TENANT,
            timestamp=_NOW,
            resource_id="cluster-1",
            product_category="kafka",
            product_type=_PRODUCT_TYPE,
            quantity=Decimal(1),
            unit_price=Decimal("100"),
            total_cost=Decimal("100"),
            granularity="daily",
        )

        phase = _make_calculate_phase()
        uow = MagicMock()
        b_start = _NOW
        b_end = _NOW + timedelta(hours=24)
        b_dur = timedelta(hours=24)

        rows = phase._collect_billing_line_rows(
            line=line,
            uow=uow,
            prefetched_metrics={},
            failed_metric_keys=frozenset(),
            tenant_period_cache={(b_start, b_end): IdentitySet()},
            resource_cache={},
            line_window_cache={id(line): (b_start, b_end, b_dur)},
        )

        assert len(rows) == 1
        assert rows[0].identity_id == "UNALLOCATED"
        assert rows[0].metadata == {}

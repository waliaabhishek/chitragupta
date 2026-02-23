from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from core.engine.allocation import AllocationContext, AllocationResult, AllocatorRegistry
from core.models import ChargebackRow, CostType, MetricRow

from .conftest import make_billing_line, make_identity_resolution, stub_allocator, stub_allocator_2

_NOW = datetime(2024, 1, 15, 12, 0, 0, tzinfo=UTC)


class TestAllocationContext:
    def test_construction_all_fields(self) -> None:
        bl = make_billing_line()
        ir = make_identity_resolution()
        metrics = {"cpu": [MetricRow(timestamp=_NOW, metric_key="cpu", value=42.0)]}
        ctx = AllocationContext(
            timeslice=_NOW,
            billing_line=bl,
            identities=ir,
            split_amount=Decimal("10.00"),
            metrics_data=metrics,
            params={"ratio": 0.7},
        )
        assert ctx.timeslice == _NOW
        assert ctx.billing_line is bl
        assert ctx.identities is ir
        assert ctx.split_amount == Decimal("10.00")
        assert ctx.metrics_data == metrics
        assert ctx.params == {"ratio": 0.7}

    def test_defaults(self) -> None:
        ctx = AllocationContext(
            timeslice=_NOW,
            billing_line=make_billing_line(),
            identities=make_identity_resolution(),
        )
        assert ctx.split_amount == Decimal(0)
        assert ctx.metrics_data is None
        assert ctx.params == {}


class TestAllocationResult:
    def test_construction_empty(self) -> None:
        result = AllocationResult()
        assert result.rows == []

    def test_row_append(self) -> None:
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
        )
        result = AllocationResult()
        result.rows.append(row)
        assert len(result.rows) == 1
        assert result.rows[0].identity_id == "u-1"


class TestAllocatorRegistryBase:
    def test_register_and_get(self) -> None:
        reg = AllocatorRegistry()
        reg.register("KAFKA_STORAGE", stub_allocator)
        assert reg.get("KAFKA_STORAGE") is stub_allocator

    def test_duplicate_register_raises(self) -> None:
        reg = AllocatorRegistry()
        reg.register("KAFKA_STORAGE", stub_allocator)
        with pytest.raises(ValueError, match="Duplicate base"):
            reg.register("KAFKA_STORAGE", stub_allocator)

    def test_unknown_product_type_raises(self) -> None:
        reg = AllocatorRegistry()
        with pytest.raises(KeyError, match="No allocator"):
            reg.get("NONEXISTENT")

    def test_list_product_types(self) -> None:
        reg = AllocatorRegistry()
        reg.register("B_TYPE", stub_allocator)
        reg.register("A_TYPE", stub_allocator)
        assert reg.list_product_types() == ["A_TYPE", "B_TYPE"]


class TestAllocatorRegistryOverrides:
    def test_register_override_and_get(self) -> None:
        reg = AllocatorRegistry()
        reg.register("SKU", stub_allocator)
        reg.register_override("SKU", stub_allocator_2)
        assert reg.get("SKU") is stub_allocator_2

    def test_override_without_base(self) -> None:
        reg = AllocatorRegistry()
        reg.register_override("NEW_SKU", stub_allocator)
        assert reg.get("NEW_SKU") is stub_allocator

    def test_duplicate_override_last_write_wins(self) -> None:
        reg = AllocatorRegistry()
        reg.register_override("SKU", stub_allocator)
        reg.register_override("SKU", stub_allocator_2)
        assert reg.get("SKU") is stub_allocator_2

    def test_list_overrides(self) -> None:
        reg = AllocatorRegistry()
        reg.register_override("Z_SKU", stub_allocator)
        reg.register_override("A_SKU", stub_allocator)
        assert reg.list_overrides() == ["A_SKU", "Z_SKU"]


class TestAllocatorRegistryCombined:
    def test_base_and_override_coexist(self) -> None:
        reg = AllocatorRegistry()
        reg.register("SKU_A", stub_allocator)
        reg.register("SKU_B", stub_allocator)
        reg.register_override("SKU_A", stub_allocator_2)
        assert reg.get("SKU_A") is stub_allocator_2
        assert reg.get("SKU_B") is stub_allocator

    def test_list_product_types_includes_both(self) -> None:
        reg = AllocatorRegistry()
        reg.register("BASE_ONLY", stub_allocator)
        reg.register_override("OVERRIDE_ONLY", stub_allocator)
        types = reg.list_product_types()
        assert "BASE_ONLY" in types
        assert "OVERRIDE_ONLY" in types

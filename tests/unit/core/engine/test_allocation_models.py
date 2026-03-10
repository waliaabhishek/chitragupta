from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from .conftest import make_billing_line, make_identity_resolution

_NOW = datetime(2024, 1, 15, 12, 0, 0, tzinfo=UTC)


class TestAllocationModelImport:
    def test_import_succeeds(self) -> None:
        from core.engine.allocation_models import AllocationModel  # noqa: F401

    def test_allocation_model_is_runtime_checkable(self) -> None:
        from core.engine.allocation_models import AllocationModel

        assert hasattr(AllocationModel, "__protocol_attrs__") or hasattr(AllocationModel, "_is_runtime_protocol")


class TestAllocationModelProtocolConformance:
    def test_conforming_class_satisfies_protocol(self) -> None:
        from core.engine.allocation import AllocationContext, AllocationResult
        from core.engine.allocation_models import AllocationModel

        class GoodAllocator:
            def allocate(self, ctx: AllocationContext) -> AllocationResult | None:
                return AllocationResult()

        assert isinstance(GoodAllocator(), AllocationModel)

    def test_non_conforming_class_does_not_satisfy_protocol(self) -> None:
        from core.engine.allocation_models import AllocationModel

        class BadAllocator:
            def compute(self) -> None:
                pass

        assert not isinstance(BadAllocator(), AllocationModel)

    def test_class_missing_allocate_entirely_does_not_satisfy(self) -> None:
        from core.engine.allocation_models import AllocationModel

        class EmptyClass:
            pass

        assert not isinstance(EmptyClass(), AllocationModel)


class TestAllocationResultMetadata:
    def test_default_metadata_is_empty_dict(self) -> None:
        from core.engine.allocation import AllocationResult

        result = AllocationResult(rows=[])
        assert result.metadata == {}

    def test_metadata_stores_and_retrieves_values(self) -> None:
        from core.engine.allocation import AllocationResult

        result = AllocationResult(rows=[], metadata={"tier": "merged_active"})
        assert result.metadata["tier"] == "merged_active"

    def test_metadata_default_is_independent_per_instance(self) -> None:
        from core.engine.allocation import AllocationResult

        r1 = AllocationResult(rows=[])
        r2 = AllocationResult(rows=[])
        r1.metadata["key"] = "value"
        assert "key" not in r2.metadata


class TestAllocationContextFields:
    def test_all_fields_accessible(self) -> None:
        from core.engine.allocation import AllocationContext
        from core.models import MetricRow

        bl = make_billing_line()
        ir = make_identity_resolution()
        metrics = {"cpu": [MetricRow(timestamp=_NOW, metric_key="cpu", value=42.0)]}
        ctx = AllocationContext(
            timeslice=_NOW,
            billing_line=bl,
            split_amount=Decimal("10.00"),
            metrics_data=metrics,
            identities=ir,
            params={"ratio": 0.5},
        )
        assert ctx.billing_line is bl
        assert ctx.split_amount == Decimal("10.00")
        assert ctx.metrics_data == metrics
        assert ctx.identities is ir
        assert ctx.params == {"ratio": 0.5}
        assert ctx.timeslice == _NOW


class TestCircularImports:
    def test_no_circular_import_with_allocation(self) -> None:
        import sys

        # Remove cached modules to force fresh import
        for key in list(sys.modules.keys()):
            if "core.engine.allocation" in key:
                del sys.modules[key]

        import core.engine.allocation  # noqa: F401
        import core.engine.allocation_models  # noqa: F401

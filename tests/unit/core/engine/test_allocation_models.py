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


# ---------------------------------------------------------------------------
# task-062: EvenSplitModel and UsageRatioModel tests (TDD RED phase)
# ---------------------------------------------------------------------------


def _make_ctx(split_amount: Decimal = Decimal("10.00")) -> AllocationContext:
    from core.engine.allocation import AllocationContext

    return AllocationContext(
        timeslice=_NOW,
        billing_line=make_billing_line(),
        identities=make_identity_resolution(),
        split_amount=split_amount,
    )


class TestEvenSplitModelAllocate:
    def test_two_identities_returns_two_rows_summing_exact(self) -> None:
        from core.engine.allocation_models import EvenSplitModel

        ctx = _make_ctx(split_amount=Decimal("1.00"))
        model = EvenSplitModel(source=lambda c: ["a", "b"])
        result = model.allocate(ctx)
        assert result is not None
        assert len(result.rows) == 2
        assert sum(r.amount for r in result.rows) == Decimal("1.00")

    def test_three_identities_returns_three_rows_summing_exact(self) -> None:
        from core.engine.allocation_models import EvenSplitModel

        ctx = _make_ctx(split_amount=Decimal("1.00"))
        model = EvenSplitModel(source=lambda c: ["a", "b", "c"])
        result = model.allocate(ctx)
        assert result is not None
        assert len(result.rows) == 3
        assert sum(r.amount for r in result.rows) == Decimal("1.00")

    def test_five_identities_returns_five_rows_summing_exact(self) -> None:
        from core.engine.allocation_models import EvenSplitModel

        ctx = _make_ctx(split_amount=Decimal("1.00"))
        model = EvenSplitModel(source=lambda c: list("abcde"))
        result = model.allocate(ctx)
        assert result is not None
        assert len(result.rows) == 5
        assert sum(r.amount for r in result.rows) == Decimal("1.00")

    def test_empty_source_returns_none(self) -> None:
        from core.engine.allocation_models import EvenSplitModel

        ctx = _make_ctx(split_amount=Decimal("1.00"))
        model = EvenSplitModel(source=lambda c: [])
        result = model.allocate(ctx)
        assert result is None

    def test_custom_detail_propagated_to_rows(self) -> None:
        from core.engine.allocation_models import EvenSplitModel

        ctx = _make_ctx(split_amount=Decimal("10.00"))
        model = EvenSplitModel(source=lambda c: ["a", "b"], detail="custom_even")
        result = model.allocate(ctx)
        assert result is not None
        assert all(r.allocation_detail == "custom_even" for r in result.rows)

    def test_custom_cost_type_propagated_to_rows(self) -> None:
        from core.engine.allocation_models import EvenSplitModel
        from core.models.chargeback import CostType

        ctx = _make_ctx(split_amount=Decimal("10.00"))
        model = EvenSplitModel(source=lambda c: ["a", "b"], cost_type=CostType.USAGE)
        result = model.allocate(ctx)
        assert result is not None
        assert all(r.cost_type == CostType.USAGE for r in result.rows)

    def test_default_cost_type_is_shared(self) -> None:
        from core.engine.allocation_models import EvenSplitModel
        from core.models.chargeback import CostType

        ctx = _make_ctx(split_amount=Decimal("10.00"))
        model = EvenSplitModel(source=lambda c: ["a", "b"])
        result = model.allocate(ctx)
        assert result is not None
        assert all(r.cost_type == CostType.SHARED for r in result.rows)


class TestUsageRatioModelAllocate:
    def test_thirty_seventy_split_returns_correct_amounts(self) -> None:
        from core.engine.allocation_models import UsageRatioModel

        ctx = _make_ctx(split_amount=Decimal("10.00"))
        model = UsageRatioModel(usage_source=lambda c: {"a": 30.0, "b": 70.0})
        result = model.allocate(ctx)
        assert result is not None
        assert len(result.rows) == 2
        amounts = {r.identity_id: r.amount for r in result.rows}
        assert amounts["a"] == Decimal("3.00")
        assert amounts["b"] == Decimal("7.00")

    def test_empty_usage_dict_returns_none(self) -> None:
        from core.engine.allocation_models import UsageRatioModel

        ctx = _make_ctx(split_amount=Decimal("10.00"))
        model = UsageRatioModel(usage_source=lambda c: {})
        result = model.allocate(ctx)
        assert result is None

    def test_all_zero_usage_returns_none(self) -> None:
        from core.engine.allocation_models import UsageRatioModel

        ctx = _make_ctx(split_amount=Decimal("10.00"))
        model = UsageRatioModel(usage_source=lambda c: {"a": 0.0, "b": 0.0})
        result = model.allocate(ctx)
        assert result is None

    def test_custom_detail_propagated_to_rows(self) -> None:
        from core.engine.allocation_models import UsageRatioModel

        ctx = _make_ctx(split_amount=Decimal("10.00"))
        model = UsageRatioModel(usage_source=lambda c: {"a": 50.0, "b": 50.0}, detail="custom_usage")
        result = model.allocate(ctx)
        assert result is not None
        assert all(r.allocation_detail == "custom_usage" for r in result.rows)

    def test_cost_type_is_always_usage(self) -> None:
        from core.engine.allocation_models import UsageRatioModel
        from core.models.chargeback import CostType

        ctx = _make_ctx(split_amount=Decimal("10.00"))
        model = UsageRatioModel(usage_source=lambda c: {"a": 40.0, "b": 60.0})
        result = model.allocate(ctx)
        assert result is not None
        assert all(r.cost_type == CostType.USAGE for r in result.rows)


class TestEvenSplitModelCostAllocatorContract:
    def test_empty_source_returns_unallocated_row_not_none(self) -> None:
        from core.engine.allocation_models import EvenSplitModel
        from core.models.chargeback import AllocationDetail

        ctx = _make_ctx(split_amount=Decimal("10.00"))
        model = EvenSplitModel(source=lambda c: [])
        result = model(ctx)
        assert result is not None
        assert len(result.rows) == 1
        assert result.rows[0].identity_id == "UNALLOCATED"
        assert result.rows[0].allocation_detail == AllocationDetail.NO_IDENTITIES_LOCATED

    def test_even_split_model_satisfies_cost_allocator_protocol(self) -> None:
        from core.engine.allocation_models import EvenSplitModel
        from core.plugin.protocols import CostAllocator

        model = EvenSplitModel(source=lambda c: ["a", "b"])
        assert isinstance(model, CostAllocator)

    def test_call_with_valid_source_returns_real_result(self) -> None:
        from core.engine.allocation_models import EvenSplitModel

        ctx = _make_ctx(split_amount=Decimal("10.00"))
        model = EvenSplitModel(source=lambda c: ["a", "b"])
        result = model(ctx)
        assert len(result.rows) == 2
        assert all(r.identity_id != "UNALLOCATED" for r in result.rows)
        assert sum(r.amount for r in result.rows) == Decimal("10.00")


class TestUsageRatioModelCostAllocatorContract:
    def test_empty_usage_returns_unallocated_row_not_none(self) -> None:
        from core.engine.allocation_models import UsageRatioModel
        from core.models.chargeback import AllocationDetail

        ctx = _make_ctx(split_amount=Decimal("10.00"))
        model = UsageRatioModel(usage_source=lambda c: {})
        result = model(ctx)
        assert result is not None
        assert len(result.rows) == 1
        assert result.rows[0].identity_id == "UNALLOCATED"
        assert result.rows[0].allocation_detail == AllocationDetail.NO_USAGE_FOR_ACTIVE_IDENTITIES

    def test_usage_ratio_model_satisfies_cost_allocator_protocol(self) -> None:
        from core.engine.allocation_models import UsageRatioModel
        from core.plugin.protocols import CostAllocator

        model = UsageRatioModel(usage_source=lambda c: {"a": 1.0})
        assert isinstance(model, CostAllocator)

    def test_call_with_valid_usage_returns_real_result(self) -> None:
        from core.engine.allocation_models import UsageRatioModel

        ctx = _make_ctx(split_amount=Decimal("10.00"))
        model = UsageRatioModel(usage_source=lambda c: {"a": 30.0, "b": 70.0})
        result = model(ctx)
        assert len(result.rows) == 2
        assert all(r.identity_id != "UNALLOCATED" for r in result.rows)


class TestAllocationModelProtocolConformanceNew:
    def test_even_split_model_satisfies_allocation_model_protocol(self) -> None:
        from core.engine.allocation_models import AllocationModel, EvenSplitModel

        model = EvenSplitModel(source=lambda c: ["a"])
        assert isinstance(model, AllocationModel)

    def test_usage_ratio_model_satisfies_allocation_model_protocol(self) -> None:
        from core.engine.allocation_models import AllocationModel, UsageRatioModel

        model = UsageRatioModel(usage_source=lambda c: {"a": 1.0})
        assert isinstance(model, AllocationModel)


class TestBackwardCompatibility:
    def test_allocate_evenly_unchanged_positional_signature(self) -> None:
        from core.engine.helpers import allocate_evenly

        ctx = _make_ctx(split_amount=Decimal("10.00"))
        result = allocate_evenly(ctx, ["x", "y"])
        assert result is not None
        assert len(result.rows) == 2
        assert sum(r.amount for r in result.rows) == Decimal("10.00")

    def test_allocate_by_usage_ratio_unchanged_positional_signature(self) -> None:
        from core.engine.helpers import allocate_by_usage_ratio

        ctx = _make_ctx(split_amount=Decimal("10.00"))
        result = allocate_by_usage_ratio(ctx, {"x": 25.0, "y": 75.0})
        assert result is not None
        assert len(result.rows) == 2
        assert sum(r.amount for r in result.rows) == Decimal("10.00")


# ---------------------------------------------------------------------------
# task-063: TerminalModel and DirectOwnerModel tests (TDD RED phase)
# ---------------------------------------------------------------------------


class TestTerminalModelAllocate:
    def test_static_identity_returns_single_row_with_correct_fields(self) -> None:
        from core.engine.allocation_models import TerminalModel
        from core.models.chargeback import CostType

        ctx = _make_ctx(split_amount=Decimal("10.00"))
        model = TerminalModel(identity_id="sa-123")
        result = model.allocate(ctx)
        assert result is not None
        assert len(result.rows) == 1
        assert result.rows[0].identity_id == "sa-123"
        assert result.rows[0].allocation_method == "terminal"
        assert result.rows[0].amount == Decimal("10.00")
        assert result.rows[0].cost_type == CostType.SHARED

    def test_allocate_never_returns_none(self) -> None:
        from core.engine.allocation import AllocationResult
        from core.engine.allocation_models import TerminalModel

        ctx = _make_ctx(split_amount=Decimal("10.00"))
        model = TerminalModel(identity_id="sa-123")
        result = model.allocate(ctx)
        assert isinstance(result, AllocationResult)

    def test_callable_identity_id_evaluated_against_ctx(self) -> None:
        from core.engine.allocation_models import TerminalModel

        ctx = _make_ctx(split_amount=Decimal("10.00"))
        model = TerminalModel(identity_id=lambda c: c.billing_line.resource_id)
        result = model.allocate(ctx)
        assert result is not None
        assert len(result.rows) == 1
        assert result.rows[0].identity_id == ctx.billing_line.resource_id

    def test_custom_detail_and_cost_type_propagated(self) -> None:
        from core.engine.allocation_models import TerminalModel
        from core.models.chargeback import CostType

        ctx = _make_ctx(split_amount=Decimal("10.00"))
        model = TerminalModel(identity_id="sa-123", detail="fallback_sa", cost_type=CostType.USAGE)
        result = model.allocate(ctx)
        assert result is not None
        assert result.rows[0].allocation_detail == "fallback_sa"
        assert result.rows[0].cost_type == CostType.USAGE

    def test_call_delegates_to_allocate(self) -> None:
        from core.engine.allocation_models import TerminalModel

        ctx = _make_ctx(split_amount=Decimal("10.00"))
        model = TerminalModel(identity_id="sa-123")
        result_allocate = model.allocate(ctx)
        result_call = model(ctx)
        assert result_call is not None
        assert len(result_call.rows) == 1
        assert result_call.rows[0].identity_id == result_allocate.rows[0].identity_id
        assert result_call.rows[0].amount == result_allocate.rows[0].amount

    def test_terminal_model_satisfies_allocation_model_protocol(self) -> None:
        from core.engine.allocation_models import AllocationModel, TerminalModel

        model = TerminalModel(identity_id="x")
        assert isinstance(model, AllocationModel)


class TestTerminalModelCostAllocatorContract:
    def test_terminal_model_satisfies_cost_allocator_protocol(self) -> None:
        from core.engine.allocation_models import TerminalModel
        from core.plugin.protocols import CostAllocator

        model = TerminalModel(identity_id="sa-123")
        assert isinstance(model, CostAllocator)

    def test_call_always_returns_allocation_result_not_none(self) -> None:
        from core.engine.allocation import AllocationResult
        from core.engine.allocation_models import TerminalModel

        ctx = _make_ctx(split_amount=Decimal("10.00"))
        model = TerminalModel(identity_id="sa-123")
        result = model(ctx)
        assert isinstance(result, AllocationResult)


class TestDirectOwnerModelAllocate:
    def test_callable_owner_source_returns_single_row(self) -> None:
        from core.engine.allocation_models import DirectOwnerModel
        from core.models.chargeback import CostType

        ctx = _make_ctx(split_amount=Decimal("10.00"))
        model = DirectOwnerModel(owner_source=lambda c: "sa-456")
        result = model.allocate(ctx)
        assert result is not None
        assert len(result.rows) == 1
        assert result.rows[0].identity_id == "sa-456"
        assert result.rows[0].allocation_method == "direct_owner"
        assert result.rows[0].amount == Decimal("10.00")
        assert result.rows[0].cost_type == CostType.USAGE

    def test_static_string_owner_source_returns_single_row(self) -> None:
        from core.engine.allocation_models import DirectOwnerModel

        ctx = _make_ctx(split_amount=Decimal("10.00"))
        model = DirectOwnerModel(owner_source="sa-456")
        result = model.allocate(ctx)
        assert result is not None
        assert len(result.rows) == 1
        assert result.rows[0].identity_id == "sa-456"

    def test_none_owner_source_returns_none(self) -> None:
        from core.engine.allocation_models import DirectOwnerModel

        ctx = _make_ctx(split_amount=Decimal("10.00"))
        model = DirectOwnerModel(owner_source=lambda c: None)
        result = model.allocate(ctx)
        assert result is None

    def test_empty_string_owner_source_returns_none(self) -> None:
        from core.engine.allocation_models import DirectOwnerModel

        ctx = _make_ctx(split_amount=Decimal("10.00"))
        model = DirectOwnerModel(owner_source=lambda c: "")
        result = model.allocate(ctx)
        assert result is None

    def test_custom_detail_and_cost_type_propagated(self) -> None:
        from core.engine.allocation_models import DirectOwnerModel
        from core.models.chargeback import CostType

        ctx = _make_ctx(split_amount=Decimal("10.00"))
        model = DirectOwnerModel(
            owner_source=lambda c: "sa-456",
            detail="topic_owner",
            cost_type=CostType.SHARED,
        )
        result = model.allocate(ctx)
        assert result is not None
        assert result.rows[0].allocation_detail == "topic_owner"
        assert result.rows[0].cost_type == CostType.SHARED

    def test_direct_owner_model_satisfies_allocation_model_protocol(self) -> None:
        from core.engine.allocation_models import AllocationModel, DirectOwnerModel

        model = DirectOwnerModel(owner_source=lambda c: None)
        assert isinstance(model, AllocationModel)


class TestDirectOwnerModelCostAllocatorContract:
    def test_none_owner_call_returns_unallocated_row_not_none(self) -> None:
        from core.engine.allocation_models import DirectOwnerModel
        from core.models.chargeback import AllocationDetail

        ctx = _make_ctx(split_amount=Decimal("10.00"))
        model = DirectOwnerModel(owner_source=lambda c: None)
        result = model(ctx)
        assert result is not None
        assert len(result.rows) == 1
        assert result.rows[0].identity_id == "UNALLOCATED"
        assert result.rows[0].allocation_detail == AllocationDetail.NO_IDENTITIES_LOCATED

    def test_direct_owner_model_satisfies_cost_allocator_protocol(self) -> None:
        from core.engine.allocation_models import DirectOwnerModel
        from core.plugin.protocols import CostAllocator

        model = DirectOwnerModel(owner_source=lambda c: None)
        assert isinstance(model, CostAllocator)

    def test_call_with_valid_owner_returns_real_result(self) -> None:
        from core.engine.allocation_models import DirectOwnerModel

        ctx = _make_ctx(split_amount=Decimal("10.00"))
        model = DirectOwnerModel(owner_source=lambda c: "sa-456")
        result = model(ctx)
        assert result is not None
        assert len(result.rows) == 1
        assert result.rows[0].identity_id == "sa-456"

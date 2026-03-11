from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING

from .conftest import make_billing_line, make_identity_resolution

if TYPE_CHECKING:
    from core.engine.allocation import AllocationContext

_NOW = datetime(2024, 1, 15, 12, 0, 0, tzinfo=UTC)


class TestAllocationModelImport:
    def test_import_succeeds(self) -> None:
        from core.engine.allocation_models import AllocationModel  # noqa: F401

    def test_allocation_model_is_runtime_checkable(self) -> None:
        from core.engine.allocation_models import AllocationModel

        assert hasattr(AllocationModel, "__protocol_attrs__") or hasattr(AllocationModel, "_is_runtime_protocol")


class TestAllocationModelProtocolConformance:
    def test_conforming_class_satisfies_protocol(self) -> None:
        from core.engine.allocation import AllocationResult
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

        saved = {k: v for k, v in sys.modules.items() if "core.engine.allocation" in k}
        try:
            for key in list(sys.modules.keys()):
                if "core.engine.allocation" in key:
                    del sys.modules[key]

            import core.engine.allocation  # noqa: F401
            import core.engine.allocation_models  # noqa: F401
        finally:
            sys.modules.update(saved)


# ---------------------------------------------------------------------------
# task-062: EvenSplitModel and UsageRatioModel tests (TDD RED phase)
# ---------------------------------------------------------------------------


def _make_ctx(
    split_amount: Decimal = Decimal("10.00"),
    params: dict | None = None,
) -> AllocationContext:
    from core.engine.allocation import AllocationContext

    return AllocationContext(
        timeslice=_NOW,
        billing_line=make_billing_line(),
        identities=make_identity_resolution(),
        split_amount=split_amount,
        params=params or {},
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


# ---------------------------------------------------------------------------
# task-064: ChainModel tests (TDD RED phase)
# ---------------------------------------------------------------------------


class _ReturnsNoneModel:
    def __init__(self) -> None:
        self.call_count = 0

    def allocate(self, ctx: object) -> None:
        self.call_count += 1
        return None


class _ReturnsResultModel:
    def __init__(self, rows: list | None = None) -> None:
        from core.engine.allocation import AllocationResult

        self.rows = rows or []
        self._result_class = AllocationResult

    def allocate(self, ctx: object) -> object:
        from core.engine.allocation import AllocationResult

        return AllocationResult(rows=self.rows)


class TestChainModelFirstMatch:
    def test_first_match_returns_result_from_second_model(self) -> None:
        from core.engine.allocation_models import ChainModel

        none_model = _ReturnsNoneModel()
        result_model = _ReturnsResultModel()
        chain = ChainModel(models=[none_model, result_model])
        ctx = _make_ctx()
        result = chain.allocate(ctx)
        assert result is not None
        assert none_model.call_count == 1

    def test_first_model_allocate_called_exactly_once_on_skip(self) -> None:
        from core.engine.allocation_models import ChainModel

        none_model = _ReturnsNoneModel()
        result_model = _ReturnsResultModel()
        chain = ChainModel(models=[none_model, result_model])
        ctx = _make_ctx()
        chain.allocate(ctx)
        assert none_model.call_count == 1


class TestChainModelChainTierMetadata:
    def test_chain_tier_0_on_primary_hit(self) -> None:
        from core.engine.allocation_models import ChainModel
        from core.models.chargeback import ChargebackRow, CostType

        rows = [
            ChargebackRow(
                ecosystem="confluent",
                tenant_id="t-001",
                timestamp=_NOW,
                resource_id="lkc-abc123",
                product_category="kafka",
                product_type="kafka_num_ckus",
                identity_id="a",
                amount=Decimal("10.00"),
                cost_type=CostType.SHARED,
                allocation_method="test",
                allocation_detail="test",
            )
        ]
        result_model = _ReturnsResultModel(rows=rows)
        chain = ChainModel(models=[result_model])
        ctx = _make_ctx()
        result = chain.allocate(ctx)
        assert result is not None
        assert all(row.metadata["chain_tier"] == 0 for row in result.rows)

    def test_chain_tier_1_on_first_fallback(self) -> None:
        from core.engine.allocation_models import ChainModel
        from core.models.chargeback import ChargebackRow, CostType

        rows = [
            ChargebackRow(
                ecosystem="confluent",
                tenant_id="t-001",
                timestamp=_NOW,
                resource_id="lkc-abc123",
                product_category="kafka",
                product_type="kafka_num_ckus",
                identity_id="a",
                amount=Decimal("10.00"),
                cost_type=CostType.SHARED,
                allocation_method="test",
                allocation_detail="test",
            )
        ]
        none_model = _ReturnsNoneModel()
        result_model = _ReturnsResultModel(rows=rows)
        chain = ChainModel(models=[none_model, result_model])
        ctx = _make_ctx()
        result = chain.allocate(ctx)
        assert result is not None
        assert all(row.metadata["chain_tier"] == 1 for row in result.rows)


class TestChainModelAllocationError:
    def test_exhausted_chain_raises_allocation_error(self) -> None:
        import pytest

        from core.engine.allocation_models import AllocationError, ChainModel

        chain = ChainModel(models=[_ReturnsNoneModel(), _ReturnsNoneModel()])
        ctx = _make_ctx()
        with pytest.raises(AllocationError):
            chain.allocate(ctx)

    def test_allocation_error_message_includes_resource_id(self) -> None:
        import pytest

        from core.engine.allocation_models import AllocationError, ChainModel

        chain = ChainModel(models=[_ReturnsNoneModel()])
        ctx = _make_ctx()
        with pytest.raises(AllocationError, match=ctx.billing_line.resource_id):
            chain.allocate(ctx)


class TestChainModelLogFallbacks:
    def test_log_fallbacks_false_default_no_debug_call(self, caplog: object) -> None:
        from core.engine.allocation_models import ChainModel
        from core.models.chargeback import ChargebackRow, CostType

        rows = [
            ChargebackRow(
                ecosystem="confluent",
                tenant_id="t-001",
                timestamp=_NOW,
                resource_id="lkc-abc123",
                product_category="kafka",
                product_type="kafka_num_ckus",
                identity_id="a",
                amount=Decimal("10.00"),
                cost_type=CostType.SHARED,
                allocation_method="test",
                allocation_detail="test",
            )
        ]
        none_model = _ReturnsNoneModel()
        result_model = _ReturnsResultModel(rows=rows)
        chain = ChainModel(models=[none_model, result_model], log_fallbacks=False)
        ctx = _make_ctx()
        import logging as _logging

        with caplog.at_level(_logging.DEBUG, logger="core.engine.allocation_models"):  # type: ignore[union-attr]
            chain.allocate(ctx)
        assert not any(r.levelno == _logging.DEBUG for r in caplog.records)

    def test_log_fallbacks_true_tier_0_hit_no_debug_call(self, caplog: object) -> None:
        import logging as _logging

        from core.engine.allocation_models import ChainModel
        from core.models.chargeback import ChargebackRow, CostType

        rows = [
            ChargebackRow(
                ecosystem="confluent",
                tenant_id="t-001",
                timestamp=_NOW,
                resource_id="lkc-abc123",
                product_category="kafka",
                product_type="kafka_num_ckus",
                identity_id="a",
                amount=Decimal("10.00"),
                cost_type=CostType.SHARED,
                allocation_method="test",
                allocation_detail="test",
            )
        ]
        result_model = _ReturnsResultModel(rows=rows)
        chain = ChainModel(models=[result_model], log_fallbacks=True)
        ctx = _make_ctx()
        with caplog.at_level(_logging.DEBUG, logger="core.engine.allocation_models"):  # type: ignore[union-attr]
            chain.allocate(ctx)
        assert not any(r.levelno == _logging.DEBUG for r in caplog.records)

    def test_log_fallbacks_true_tier_1_hit_debug_called_once(self, caplog: object) -> None:
        import logging as _logging

        from core.engine.allocation_models import ChainModel
        from core.models.chargeback import ChargebackRow, CostType

        rows = [
            ChargebackRow(
                ecosystem="confluent",
                tenant_id="t-001",
                timestamp=_NOW,
                resource_id="lkc-abc123",
                product_category="kafka",
                product_type="kafka_num_ckus",
                identity_id="a",
                amount=Decimal("10.00"),
                cost_type=CostType.SHARED,
                allocation_method="test",
                allocation_detail="test",
            )
        ]
        none_model = _ReturnsNoneModel()
        result_model = _ReturnsResultModel(rows=rows)
        chain = ChainModel(models=[none_model, result_model], log_fallbacks=True)
        ctx = _make_ctx()
        with caplog.at_level(_logging.DEBUG, logger="core.engine.allocation_models"):  # type: ignore[union-attr]
            chain.allocate(ctx)
        debug_records = [r for r in caplog.records if r.levelno == _logging.DEBUG]
        assert len(debug_records) == 1
        assert "1" in debug_records[0].getMessage()
        assert ctx.billing_line.resource_id in debug_records[0].getMessage()
        assert ctx.billing_line.product_type in debug_records[0].getMessage()


class TestChainModelCostAllocatorProtocol:
    def test_call_delegates_to_allocate(self) -> None:
        from core.engine.allocation_models import ChainModel
        from core.models.chargeback import ChargebackRow, CostType

        rows = [
            ChargebackRow(
                ecosystem="confluent",
                tenant_id="t-001",
                timestamp=_NOW,
                resource_id="lkc-abc123",
                product_category="kafka",
                product_type="kafka_num_ckus",
                identity_id="a",
                amount=Decimal("10.00"),
                cost_type=CostType.SHARED,
                allocation_method="test",
                allocation_detail="test",
            )
        ]
        result_model = _ReturnsResultModel(rows=rows)
        chain = ChainModel(models=[result_model])
        ctx = _make_ctx()
        result_via_allocate = chain.allocate(ctx)
        result_via_call = chain(ctx)
        assert result_via_call is not None
        assert len(result_via_call.rows) == len(result_via_allocate.rows)
        assert result_via_call.rows[0].identity_id == result_via_allocate.rows[0].identity_id

    def test_chain_model_satisfies_cost_allocator_protocol(self) -> None:
        from core.engine.allocation_models import ChainModel
        from core.plugin.protocols import CostAllocator

        chain = ChainModel(models=[_ReturnsResultModel()])
        assert isinstance(chain, CostAllocator)


class TestChainModelMetadataNonDestructive:
    def test_existing_metadata_keys_retained_after_chain_tier_injection(self) -> None:
        from core.engine.allocation_models import ChainModel
        from core.models.chargeback import ChargebackRow, CostType

        row = ChargebackRow(
            ecosystem="confluent",
            tenant_id="t-001",
            timestamp=_NOW,
            resource_id="lkc-abc123",
            product_category="kafka",
            product_type="kafka_num_ckus",
            identity_id="a",
            amount=Decimal("10.00"),
            cost_type=CostType.SHARED,
            allocation_method="test",
            allocation_detail="test",
        )
        row.metadata["existing_key"] = "existing_value"
        result_model = _ReturnsResultModel(rows=[row])
        chain = ChainModel(models=[result_model])
        ctx = _make_ctx()
        result = chain.allocate(ctx)
        assert result is not None
        assert result.rows[0].metadata["existing_key"] == "existing_value"
        assert result.rows[0].metadata["chain_tier"] == 0


class TestChainModelIntegration:
    def test_chain_model_with_real_models_falls_back_to_terminal(self) -> None:
        from core.engine.allocation_models import ChainModel, DirectOwnerModel, TerminalModel

        chain = ChainModel([DirectOwnerModel(owner_source=lambda c: None), TerminalModel(identity_id="UNALLOCATED")])
        ctx = _make_ctx()
        result = chain.allocate(ctx)
        assert result.rows[0].identity_id == "UNALLOCATED"
        assert result.rows[0].metadata["chain_tier"] == 1


# ---------------------------------------------------------------------------
# task-065: CompositionModel and DynamicCompositionModel tests (TDD RED phase)
# ---------------------------------------------------------------------------


class TestCompositionModelSplit:
    def test_seventy_thirty_split_produces_correct_amounts(self) -> None:
        from core.engine.allocation_models import CompositionModel, TerminalModel

        t1 = TerminalModel(identity_id="sa-001")
        t2 = TerminalModel(identity_id="sa-002")
        model = CompositionModel([(Decimal("0.70"), t1), (Decimal("0.30"), t2)])
        ctx = _make_ctx(split_amount=Decimal("10.00"))
        result = model.allocate(ctx)
        assert result is not None
        amounts = {r.identity_id: r.amount for r in result.rows}
        assert amounts["sa-001"] == Decimal("7.00")
        assert amounts["sa-002"] == Decimal("3.00")

    def test_seventy_thirty_split_sets_composition_index(self) -> None:
        from core.engine.allocation_models import CompositionModel, TerminalModel

        t1 = TerminalModel(identity_id="sa-001")
        t2 = TerminalModel(identity_id="sa-002")
        model = CompositionModel([(Decimal("0.70"), t1), (Decimal("0.30"), t2)])
        ctx = _make_ctx(split_amount=Decimal("10.00"))
        result = model.allocate(ctx)
        indices = {r.identity_id: r.metadata["composition_index"] for r in result.rows}
        assert indices["sa-001"] == 0
        assert indices["sa-002"] == 1

    def test_seventy_thirty_split_sets_composition_ratio(self) -> None:
        from core.engine.allocation_models import CompositionModel, TerminalModel

        t1 = TerminalModel(identity_id="sa-001")
        t2 = TerminalModel(identity_id="sa-002")
        model = CompositionModel([(Decimal("0.70"), t1), (Decimal("0.30"), t2)])
        ctx = _make_ctx(split_amount=Decimal("10.00"))
        result = model.allocate(ctx)
        ratios = {r.identity_id: r.metadata["composition_ratio"] for r in result.rows}
        assert ratios["sa-001"] == 0.7
        assert ratios["sa-002"] == 0.3

    def test_fifty_fifty_split_produces_equal_amounts(self) -> None:
        from core.engine.allocation_models import CompositionModel, TerminalModel

        t1 = TerminalModel(identity_id="sa-001")
        t2 = TerminalModel(identity_id="sa-002")
        model = CompositionModel([(Decimal("0.50"), t1), (Decimal("0.50"), t2)])
        ctx = _make_ctx(split_amount=Decimal("10.00"))
        result = model.allocate(ctx)
        amounts = {r.identity_id: r.amount for r in result.rows}
        assert amounts["sa-001"] == Decimal("5.00")
        assert amounts["sa-002"] == Decimal("5.00")

    def test_fifty_fifty_split_sets_composition_ratio_half(self) -> None:
        from core.engine.allocation_models import CompositionModel, TerminalModel

        t1 = TerminalModel(identity_id="sa-001")
        t2 = TerminalModel(identity_id="sa-002")
        model = CompositionModel([(Decimal("0.50"), t1), (Decimal("0.50"), t2)])
        ctx = _make_ctx(split_amount=Decimal("10.00"))
        result = model.allocate(ctx)
        for row in result.rows:
            assert row.metadata["composition_ratio"] == 0.5

    def test_rounding_remainder_amounts_sum_exactly_to_split_amount(self) -> None:
        from core.engine.allocation_models import CompositionModel, TerminalModel

        t1 = TerminalModel(identity_id="sa-001")
        t2 = TerminalModel(identity_id="sa-002")
        t3 = TerminalModel(identity_id="sa-003")
        model = CompositionModel(
            [
                (Decimal("0.333"), t1),
                (Decimal("0.333"), t2),
                (Decimal("0.334"), t3),
            ]
        )
        ctx = _make_ctx(split_amount=Decimal("10.00"))
        result = model.allocate(ctx)
        assert sum(r.amount for r in result.rows) == Decimal("10.00")


class TestCompositionModelValidation:
    def test_ratios_summing_to_over_one_raises_value_error(self) -> None:
        import pytest

        from core.engine.allocation_models import CompositionModel, TerminalModel

        t1 = TerminalModel(identity_id="sa-001")
        t2 = TerminalModel(identity_id="sa-002")
        with pytest.raises(ValueError, match="1.2"):
            CompositionModel([(Decimal("0.60"), t1), (Decimal("0.60"), t2)])

    def test_value_error_message_contains_actual_sum(self) -> None:
        import pytest

        from core.engine.allocation_models import CompositionModel, TerminalModel

        t1 = TerminalModel(identity_id="sa-001")
        t2 = TerminalModel(identity_id="sa-002")
        with pytest.raises(ValueError) as exc_info:
            CompositionModel([(Decimal("0.60"), t1), (Decimal("0.60"), t2)])
        assert "1.2" in str(exc_info.value)

    def test_component_returning_none_raises_allocation_error(self) -> None:
        import pytest

        from core.engine.allocation_models import AllocationError, CompositionModel, TerminalModel

        t1 = TerminalModel(identity_id="sa-001")
        model = CompositionModel([(Decimal("0.70"), t1), (Decimal("0.30"), _ReturnsNoneModel())])
        ctx = _make_ctx(split_amount=Decimal("10.00"))
        with pytest.raises(AllocationError):
            model.allocate(ctx)

    def test_allocation_error_message_includes_resource_id(self) -> None:
        import pytest

        from core.engine.allocation_models import AllocationError, CompositionModel, TerminalModel

        t1 = TerminalModel(identity_id="sa-001")
        model = CompositionModel([(Decimal("0.70"), t1), (Decimal("0.30"), _ReturnsNoneModel())])
        ctx = _make_ctx(split_amount=Decimal("10.00"))
        with pytest.raises(AllocationError, match=ctx.billing_line.resource_id):
            model.allocate(ctx)


class TestCompositionModelMetadataPreservation:
    def test_chain_tier_metadata_preserved_after_composition_injection(self) -> None:
        from core.engine.allocation_models import ChainModel, CompositionModel, TerminalModel

        t1 = TerminalModel(identity_id="sa-001")
        t2 = TerminalModel(identity_id="sa-002")
        # ChainModel wrapping TerminalModel will inject chain_tier=0
        chain_component = ChainModel(models=[t1])
        model = CompositionModel(
            [
                (Decimal("0.70"), chain_component),
                (Decimal("0.30"), t2),
            ]
        )
        ctx = _make_ctx(split_amount=Decimal("10.00"))
        result = model.allocate(ctx)
        chain_rows = [r for r in result.rows if r.identity_id == "sa-001"]
        assert len(chain_rows) == 1
        assert chain_rows[0].metadata["chain_tier"] == 0
        assert chain_rows[0].metadata["composition_index"] == 0
        assert chain_rows[0].metadata["composition_ratio"] == 0.7


class TestCompositionModelCostAllocatorProtocol:
    def test_call_returns_same_result_as_allocate(self) -> None:
        from core.engine.allocation_models import CompositionModel, TerminalModel

        t1 = TerminalModel(identity_id="sa-001")
        t2 = TerminalModel(identity_id="sa-002")
        model = CompositionModel([(Decimal("0.70"), t1), (Decimal("0.30"), t2)])
        ctx = _make_ctx(split_amount=Decimal("10.00"))
        result_allocate = model.allocate(ctx)
        result_call = model(ctx)
        assert result_call is not None
        assert len(result_call.rows) == len(result_allocate.rows)
        call_amounts = sorted(r.amount for r in result_call.rows)
        allocate_amounts = sorted(r.amount for r in result_allocate.rows)
        assert call_amounts == allocate_amounts

    def test_composition_model_satisfies_allocation_model_protocol(self) -> None:
        from core.engine.allocation_models import AllocationModel, CompositionModel, TerminalModel

        t1 = TerminalModel(identity_id="sa-001")
        t2 = TerminalModel(identity_id="sa-002")
        model = CompositionModel([(Decimal("0.50"), t1), (Decimal("0.50"), t2)])
        assert isinstance(model, AllocationModel)

    def test_composition_model_satisfies_cost_allocator_protocol(self) -> None:
        from core.engine.allocation_models import CompositionModel, TerminalModel
        from core.plugin.protocols import CostAllocator

        t1 = TerminalModel(identity_id="sa-001")
        t2 = TerminalModel(identity_id="sa-002")
        model = CompositionModel([(Decimal("0.50"), t1), (Decimal("0.50"), t2)])
        assert isinstance(model, CostAllocator)


class TestDynamicCompositionModelRuntime:
    def test_ratio_source_reads_params_at_runtime(self) -> None:
        from core.engine.allocation_models import DynamicCompositionModel, TerminalModel

        t1 = TerminalModel(identity_id="sa-001")
        t2 = TerminalModel(identity_id="sa-002")

        def ratio_source(ctx: object) -> list:
            usage_ratio = Decimal(str(ctx.params["usage_ratio"]))  # type: ignore[union-attr]
            shared_ratio = Decimal("1") - usage_ratio
            return [(usage_ratio, t1), (shared_ratio, t2)]

        model = DynamicCompositionModel(ratio_source=ratio_source)

        ctx_70 = _make_ctx(split_amount=Decimal("10.00"), params={"usage_ratio": "0.70"})
        result_70 = model.allocate(ctx_70)
        amounts_70 = {r.identity_id: r.amount for r in result_70.rows}

        ctx_30 = _make_ctx(split_amount=Decimal("10.00"), params={"usage_ratio": "0.30"})
        result_30 = model.allocate(ctx_30)
        amounts_30 = {r.identity_id: r.amount for r in result_30.rows}

        assert amounts_70["sa-001"] == Decimal("7.00")
        assert amounts_30["sa-001"] == Decimal("3.00")

    def test_different_params_produce_different_split_amounts(self) -> None:
        from core.engine.allocation_models import DynamicCompositionModel, TerminalModel

        t1 = TerminalModel(identity_id="sa-001")
        t2 = TerminalModel(identity_id="sa-002")

        def ratio_source(ctx: object) -> list:
            ratio = Decimal(str(ctx.params["ratio"]))  # type: ignore[union-attr]
            return [(ratio, t1), (Decimal("1") - ratio, t2)]

        model = DynamicCompositionModel(ratio_source=ratio_source)

        ctx_a = _make_ctx(split_amount=Decimal("10.00"), params={"ratio": "0.60"})
        result_a = model.allocate(ctx_a)

        ctx_b = _make_ctx(split_amount=Decimal("10.00"), params={"ratio": "0.40"})
        result_b = model.allocate(ctx_b)

        amounts_a = {r.identity_id: r.amount for r in result_a.rows}
        amounts_b = {r.identity_id: r.amount for r in result_b.rows}
        assert amounts_a["sa-001"] != amounts_b["sa-001"]

    def test_invalid_ratios_at_allocation_time_raises_value_error(self) -> None:
        import pytest

        from core.engine.allocation_models import DynamicCompositionModel, TerminalModel

        t1 = TerminalModel(identity_id="sa-001")
        t2 = TerminalModel(identity_id="sa-002")

        def bad_ratio_source(ctx: object) -> list:
            return [(Decimal("0.50"), t1), (Decimal("0.30"), t2)]  # sums to 0.80

        model = DynamicCompositionModel(ratio_source=bad_ratio_source)
        ctx = _make_ctx(split_amount=Decimal("10.00"))
        with pytest.raises(ValueError):
            model.allocate(ctx)

    def test_dynamic_composition_model_satisfies_allocation_model_protocol(self) -> None:
        from core.engine.allocation_models import AllocationModel, DynamicCompositionModel, TerminalModel

        t1 = TerminalModel(identity_id="sa-001")
        t2 = TerminalModel(identity_id="sa-002")
        model = DynamicCompositionModel(ratio_source=lambda ctx: [(Decimal("0.50"), t1), (Decimal("0.50"), t2)])
        assert isinstance(model, AllocationModel)

    def test_dynamic_composition_model_satisfies_cost_allocator_protocol(self) -> None:
        from core.engine.allocation_models import DynamicCompositionModel, TerminalModel
        from core.plugin.protocols import CostAllocator

        t1 = TerminalModel(identity_id="sa-001")
        t2 = TerminalModel(identity_id="sa-002")
        model = DynamicCompositionModel(ratio_source=lambda ctx: [(Decimal("0.50"), t1), (Decimal("0.50"), t2)])
        assert isinstance(model, CostAllocator)

    def test_call_returns_same_result_as_allocate(self) -> None:
        from core.engine.allocation_models import DynamicCompositionModel, TerminalModel

        t1 = TerminalModel(identity_id="sa-001")
        t2 = TerminalModel(identity_id="sa-002")
        model = DynamicCompositionModel(ratio_source=lambda ctx: [(Decimal("0.70"), t1), (Decimal("0.30"), t2)])
        ctx = _make_ctx(split_amount=Decimal("10.00"))
        result_allocate = model.allocate(ctx)
        result_call = model(ctx)
        assert result_call is not None
        assert len(result_call.rows) == len(result_allocate.rows)
        call_amounts = sorted(r.amount for r in result_call.rows)
        allocate_amounts = sorted(r.amount for r in result_allocate.rows)
        assert call_amounts == allocate_amounts


class TestChainModelWithCompositionModel:
    def test_chain_model_delegates_to_composition_model_and_injects_chain_tier(self) -> None:
        from core.engine.allocation_models import ChainModel, CompositionModel, TerminalModel

        t1 = TerminalModel(identity_id="a")
        t2 = TerminalModel(identity_id="b")
        composition = CompositionModel(components=[(Decimal("0.60"), t1), (Decimal("0.40"), t2)])
        chain = ChainModel(models=[composition])

        ctx = _make_ctx(split_amount=Decimal("10.00"))
        result = chain.allocate(ctx)

        assert result is not None
        assert len(result.rows) == 2
        # Verify composition metadata present
        assert all(row.metadata["composition_index"] in (0, 1) for row in result.rows)
        assert all("composition_ratio" in row.metadata for row in result.rows)
        # Verify chain_tier metadata injected on top
        assert all(row.metadata["chain_tier"] == 0 for row in result.rows)
        # Verify amounts
        amounts = sorted(r.amount for r in result.rows)
        assert amounts == [Decimal("4.00"), Decimal("6.00")]

from __future__ import annotations

import logging
from collections.abc import Callable, Sequence
from dataclasses import dataclass, replace
from decimal import ROUND_HALF_UP, Decimal
from typing import Protocol, runtime_checkable

from core.engine.allocation import AllocationContext, AllocationResult
from core.engine.helpers import _CENT, allocate_by_usage_ratio, allocate_evenly, make_row
from core.models import CostType
from core.models.chargeback import AllocationDetail

logger = logging.getLogger(__name__)


class AllocationError(Exception):
    """Raised when an allocation chain exhausts all models without a result."""


@runtime_checkable
class AllocationModel(Protocol):
    """Protocol for allocation models in the composable allocation system.

    Models return AllocationResult on success, or None to signal that the
    next model in the chain should be tried. Terminal models must always
    return a result (never None).

    This protocol is distinct from CostAllocator (which uses __call__ and
    always succeeds). AllocationModel uses an explicit allocate() method to
    make the optional return semantics visible in the type signature.

    Models that want to be usable as CostAllocator can implement __call__
    in addition to allocate().
    """

    def allocate(self, ctx: AllocationContext) -> AllocationResult | None: ...


@dataclass(frozen=True)
class EvenSplitModel:
    """Split cost evenly across identities from source.

    Returns None if source yields no identities (signals fallback via AllocationModel.allocate).
    __call__ satisfies CostAllocator protocol: never returns None, falls back to UNALLOCATED row.
    """

    source: Callable[[AllocationContext], Sequence[str]]
    detail: str | None = None
    cost_type: CostType = CostType.SHARED

    def allocate(self, ctx: AllocationContext) -> AllocationResult | None:
        identity_ids = self.source(ctx)
        if not identity_ids:
            return None
        return allocate_evenly(
            ctx,
            identity_ids,
            allocation_detail=self.detail,
            cost_type=self.cost_type,
        )

    def __call__(self, ctx: AllocationContext) -> AllocationResult:
        result = self.allocate(ctx)
        if result is None:
            row = make_row(
                ctx,
                identity_id="UNALLOCATED",
                cost_type=self.cost_type,
                amount=ctx.split_amount,
                allocation_method="even_split",
                allocation_detail=AllocationDetail.NO_IDENTITIES_LOCATED,
            )
            return AllocationResult(rows=[row])
        return result


@dataclass(frozen=True)
class UsageRatioModel:
    """Split cost proportionally by usage values.

    Returns None if no usage data or all-zero values (signals fallback via AllocationModel.allocate).
    __call__ satisfies CostAllocator protocol: never returns None, falls back to UNALLOCATED row.
    cost_type is always CostType.USAGE — usage-ratio rows are always usage-attributed.
    """

    usage_source: Callable[[AllocationContext], dict[str, float]]
    detail: str | None = None

    def allocate(self, ctx: AllocationContext) -> AllocationResult | None:
        usage = self.usage_source(ctx)
        if not usage or sum(usage.values()) == 0:
            return None
        return allocate_by_usage_ratio(ctx, usage, allocation_detail=self.detail)

    def __call__(self, ctx: AllocationContext) -> AllocationResult:
        result = self.allocate(ctx)
        if result is None:
            row = make_row(
                ctx,
                identity_id="UNALLOCATED",
                cost_type=CostType.SHARED,
                amount=ctx.split_amount,
                allocation_method="usage_ratio",
                allocation_detail=AllocationDetail.NO_USAGE_FOR_ACTIVE_IDENTITIES,
            )
            return AllocationResult(rows=[row])
        return result


@dataclass(frozen=True)
class TerminalModel:
    """Always succeeds — assigns full cost to a single identity.

    Used as the last model in a chain. Never returns None.
    Implements __call__ for standalone use as CostAllocator.
    """

    identity_id: str | Callable[[AllocationContext], str]
    detail: str | None = None
    cost_type: CostType = CostType.SHARED

    def allocate(self, ctx: AllocationContext) -> AllocationResult:
        ident = self.identity_id(ctx) if callable(self.identity_id) else self.identity_id
        row = make_row(
            ctx,
            identity_id=ident,
            cost_type=self.cost_type,
            amount=ctx.split_amount,
            allocation_method="terminal",
            allocation_detail=self.detail,
        )
        return AllocationResult(rows=[row])

    def __call__(self, ctx: AllocationContext) -> AllocationResult:
        return self.allocate(ctx)


@dataclass(frozen=True)
class DirectOwnerModel:
    """Assigns full cost to a specific owner identity.

    Returns None if owner cannot be resolved (signals fallback).
    Implements __call__ for standalone use as CostAllocator — when owner
    is unresolvable, falls back to an UNALLOCATED row (never returns None).
    """

    owner_source: str | Callable[[AllocationContext], str | None]
    detail: str | None = None
    cost_type: CostType = CostType.USAGE

    def allocate(self, ctx: AllocationContext) -> AllocationResult | None:
        owner = self.owner_source(ctx) if callable(self.owner_source) else self.owner_source
        if not owner:
            return None
        row = make_row(
            ctx,
            identity_id=owner,
            cost_type=self.cost_type,
            amount=ctx.split_amount,
            allocation_method="direct_owner",
            allocation_detail=self.detail,
        )
        return AllocationResult(rows=[row])

    def __call__(self, ctx: AllocationContext) -> AllocationResult:
        result = self.allocate(ctx)
        if result is None:
            row = make_row(
                ctx,
                identity_id="UNALLOCATED",
                cost_type=CostType.SHARED,
                amount=ctx.split_amount,
                allocation_method="direct_owner",
                allocation_detail=AllocationDetail.NO_IDENTITIES_LOCATED,
            )
            return AllocationResult(rows=[row])
        return result


@dataclass(frozen=True)
class ChainModel:
    """Try models in sequence until one succeeds.

    The last model should be a TerminalModel to guarantee a result.
    Raises AllocationError if the chain exhausts without a result —
    this indicates a misconfigured chain (missing terminal).

    Injects chain_tier metadata (0-based index of the succeeding model)
    into every ChargebackRow for production observability.

    Implements __call__ for CostAllocator protocol compatibility.
    """

    models: Sequence[AllocationModel]
    log_fallbacks: bool = False

    def __post_init__(self) -> None:
        if not self.models:
            raise ValueError("ChainModel requires at least one model; got empty sequence")
        if not isinstance(self.models[-1], TerminalModel):
            raise ValueError(f"ChainModel last model must be a TerminalModel; got {type(self.models[-1]).__name__}")

    def allocate(self, ctx: AllocationContext) -> AllocationResult:
        for i, model in enumerate(self.models):
            result = model.allocate(ctx)
            if result is not None:
                if self.log_fallbacks and i > 0:
                    logger.debug(
                        "Chain fell back to tier %d for resource=%s product=%s",
                        i,
                        ctx.billing_line.resource_id,
                        ctx.billing_line.product_type,
                    )
                for row in result.rows:
                    row.metadata["chain_tier"] = i
                return result
        raise AllocationError(f"Chain exhausted without result for {ctx.billing_line.resource_id}")

    def __call__(self, ctx: AllocationContext) -> AllocationResult:
        """Allow ChainModel to be used directly as CostAllocator."""
        return self.allocate(ctx)


@dataclass(frozen=True)
class CompositionModel:
    """Split cost across multiple models by ratio.

    Each component is a (ratio, model) tuple. Ratios must sum to 1.0.
    All component models must return a result (use ChainModel for fallbacks).

    Injects composition_index (0-based) and composition_ratio (float) into
    every row's metadata for production auditability.

    Implements __call__ for CostAllocator protocol compatibility.
    """

    components: Sequence[tuple[Decimal, AllocationModel]]

    def __post_init__(self) -> None:
        total = sum(ratio for ratio, _ in self.components)
        if abs(total - Decimal("1")) > Decimal("0.0001"):
            raise ValueError(f"Composition ratios must sum to 1.0, got {total}")

    def allocate(self, ctx: AllocationContext) -> AllocationResult:
        rows = []
        remaining = ctx.split_amount
        for i, (ratio, model) in enumerate(self.components):
            if i < len(self.components) - 1:
                sub_amount = (ctx.split_amount * ratio).quantize(_CENT, rounding=ROUND_HALF_UP)
                remaining -= sub_amount
            else:
                sub_amount = remaining  # last component absorbs rounding remainder
            sub_ctx = replace(ctx, split_amount=sub_amount)
            result = model.allocate(sub_ctx)
            if result is None:
                raise AllocationError(
                    f"CompositionModel: component {i} returned None for "
                    f"{ctx.billing_line.resource_id} — wrap in ChainModel for fallbacks"
                )
            for row in result.rows:
                row.metadata["composition_index"] = i
                row.metadata["composition_ratio"] = float(ratio)
            rows.extend(result.rows)
        return AllocationResult(rows=rows)

    def __call__(self, ctx: AllocationContext) -> AllocationResult:
        return self.allocate(ctx)


@dataclass(frozen=True)
class DynamicCompositionModel:
    """Split cost with runtime-determined ratios.

    ratio_source receives AllocationContext and returns a sequence of
    (ratio, model) tuples. Useful when ratios come from ctx.params or
    other runtime configuration.

    Delegates to CompositionModel for ratio validation and execution —
    a ValueError is raised if returned ratios do not sum to 1.0.

    Implements __call__ for CostAllocator protocol compatibility.
    """

    ratio_source: Callable[[AllocationContext], Sequence[tuple[Decimal, AllocationModel]]]

    def allocate(self, ctx: AllocationContext) -> AllocationResult:
        components = self.ratio_source(ctx)
        return CompositionModel(tuple(components)).allocate(ctx)

    def __call__(self, ctx: AllocationContext) -> AllocationResult:
        return self.allocate(ctx)

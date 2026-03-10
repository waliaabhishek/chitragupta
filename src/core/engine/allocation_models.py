from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol, runtime_checkable

from core.engine.allocation import AllocationContext, AllocationResult
from core.engine.helpers import allocate_by_usage_ratio, allocate_evenly, make_row
from core.models import CostType
from core.models.chargeback import AllocationDetail

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from collections.abc import Sequence


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

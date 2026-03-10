from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Protocol, runtime_checkable

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from core.engine.allocation import AllocationContext, AllocationResult


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

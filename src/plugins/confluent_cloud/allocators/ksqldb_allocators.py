"""ksqlDB allocators for CCloud cost distribution.

ksqlDB CSU costs use simple even split across active identities.
CSU (Confluent Streaming Units) represent compute capacity.
"""

from __future__ import annotations

from dataclasses import replace
from typing import TYPE_CHECKING

from core.engine.helpers import allocate_evenly
from core.models import CostType

if TYPE_CHECKING:
    # AllocationContext/AllocationResult are runtime-available but imported under
    # TYPE_CHECKING for lightweight module loading. Works because
    # `from __future__ import annotations` makes all annotations strings.
    from core.engine.allocation import AllocationContext, AllocationResult


def ksqldb_csu_allocator(ctx: AllocationContext) -> AllocationResult:
    """Even split across active identities for ksqlDB CSU costs.

    Uses USAGE cost type (compute resource consumption).

    Fallback chain:
    1. merged_active (resource_active union metrics_derived)
    2. tenant_period (all tenant identities in billing window)
    3. UNALLOCATED (no identities found)
    """
    identity_ids = list(ctx.identities.merged_active.ids())
    if not identity_ids:
        identity_ids = list(ctx.identities.tenant_period.ids())

    result = allocate_evenly(ctx, identity_ids)

    # Override cost type: allocate_evenly defaults to SHARED, but CSU represents
    # compute consumption billed to specific users, so USAGE is semantically correct.
    result.rows = [replace(row, cost_type=CostType.USAGE) for row in result.rows]

    return result

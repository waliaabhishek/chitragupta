"""ksqlDB allocators for CCloud cost distribution.

ksqlDB CSU costs use simple even split across active identities.
CSU (Confluent Streaming Units) represent compute capacity.
"""

from __future__ import annotations

import logging
from dataclasses import replace
from typing import TYPE_CHECKING

from core.engine.helpers import allocate_evenly, allocate_to_resource
from core.models import CostType

if TYPE_CHECKING:
    # AllocationContext/AllocationResult are runtime-available but imported under
    # TYPE_CHECKING for lightweight module loading. Works because
    # `from __future__ import annotations` makes all annotations strings.
    from core.engine.allocation import AllocationContext, AllocationResult
logger = logging.getLogger(__name__)


def ksqldb_csu_allocator(ctx: AllocationContext) -> AllocationResult:
    """Even split across active identities for ksqlDB CSU costs.

    Uses USAGE cost type when attributing to known consumers (merged_active).
    Falls back to SHARED when attribution is uncertain (tenant_period or no identities).

    Fallback chain:
    1. merged_active (resource_active union metrics_derived) → USAGE
    2. tenant_period (all tenant identities in billing window) → SHARED
    3. resource itself (no identities found) → SHARED via allocate_to_resource
    """
    identity_ids = list(ctx.identities.merged_active.ids())
    if identity_ids:
        result = allocate_evenly(ctx, identity_ids)
        # Resource-specific identities → USAGE (attributed consumption)
        result.rows = [replace(row, cost_type=CostType.USAGE) for row in result.rows]
        return result

    # Fallback to tenant_period — keep as SHARED (can't attribute specifically)
    identity_ids = list(ctx.identities.tenant_period.ids())
    if identity_ids:
        return allocate_evenly(ctx, identity_ids)  # SHARED is the default from allocate_evenly

    # Terminal fallback — no identities at all, assign to resource (SHARED per allocate_to_resource)
    return allocate_to_resource(ctx)

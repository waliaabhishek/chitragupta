"""Schema Registry allocators for CCloud cost distribution.

Schema Registry costs use simple even split across active identities.
No metrics needed — SR doesn't track per-principal usage.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from core.engine.helpers import allocate_evenly

if TYPE_CHECKING:
    # AllocationContext/AllocationResult are runtime-available but imported under
    # TYPE_CHECKING for lightweight module loading. Works because
    # `from __future__ import annotations` makes all annotations strings.
    from core.engine.allocation import AllocationContext, AllocationResult


def schema_registry_allocator(ctx: AllocationContext) -> AllocationResult:
    """Even split across active identities for SR costs.

    Fallback chain:
    1. merged_active (resource_active ∪ metrics_derived)
    2. tenant_period (all tenant identities in billing window)
    3. UNALLOCATED (no identities found)
    """
    identity_ids = list(ctx.identities.merged_active.ids())
    if not identity_ids:
        identity_ids = list(ctx.identities.tenant_period.ids())
    return allocate_evenly(ctx, identity_ids)

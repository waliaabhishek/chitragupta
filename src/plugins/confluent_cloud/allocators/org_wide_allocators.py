"""Org-wide allocators for CCloud cost distribution.

Org-wide costs (AUDIT_LOG_READ, SUPPORT) are split evenly across ALL
tenant-period identities. Uses tenant_period scope (not merged_active)
because org-wide costs apply to the entire tenant, not specific resources.

Fallback: UNALLOCATED if no identities exist.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from core.engine.helpers import allocate_evenly

if TYPE_CHECKING:
    from core.engine.allocation import AllocationContext, AllocationResult


def org_wide_allocator(ctx: AllocationContext) -> AllocationResult:
    """Even split across ALL tenant-period identities (SHARED cost type).

    Uses tenant_period (not merged_active) because org-wide costs apply to
    the entire tenant, not just resource-active or metrics-derived identities.

    Falls back to UNALLOCATED if no identities exist.
    """
    identity_ids = sorted(ctx.identities.tenant_period.ids())
    return allocate_evenly(ctx, identity_ids)

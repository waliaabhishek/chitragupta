"""Org-wide allocators for CCloud cost distribution.

Org-wide costs (AUDIT_LOG_READ, SUPPORT) are split evenly across ALL
tenant-period identities. Uses tenant_period scope (not merged_active)
because org-wide costs apply to the entire tenant, not specific resources.

Fallback: UNALLOCATED if no identities exist.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from core.engine.helpers import allocate_evenly
from core.models import OWNER_IDENTITY_TYPES

if TYPE_CHECKING:
    from core.engine.allocation import AllocationContext, AllocationResult


def org_wide_allocator(ctx: AllocationContext) -> AllocationResult:
    """Even split across owner identities (SAs, users, pools) — not API keys.

    Uses tenant_period (not merged_active) because org-wide costs apply to
    the entire tenant, not just resource-active or metrics-derived identities.

    Excludes API keys (identity_type="api_key") and UNALLOCATED (identity_type="system")
    to match reference code deduplication behavior.

    Falls back to UNALLOCATED if no owner identities exist.
    """
    identity_ids = sorted(ctx.identities.tenant_period.ids_by_type(*OWNER_IDENTITY_TYPES))
    return allocate_evenly(ctx, identity_ids)

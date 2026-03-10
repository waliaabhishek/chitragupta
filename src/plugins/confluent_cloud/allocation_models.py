"""Composable allocation model definitions for CCloud plugins.

Each MODEL constant is a ChainModel (or single AllocationModel) that encodes
the full allocation strategy for a product type. Allocators in allocators/
import and call these models.
"""

from __future__ import annotations

import logging

from core.engine.allocation_models import ChainModel, EvenSplitModel, TerminalModel
from core.models import OWNER_IDENTITY_TYPES, CostType
from core.models.chargeback import AllocationDetail

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Schema Registry allocation model
#
# Mirrors Active_Total_Resource_CostAllocator from reference:
#   Tier 0: active identities      -> usage_cost   (CostType.USAGE)
#   Tier 1: tenant-period owners   -> shared_cost  + NO_ACTIVE_IDENTITIES_LOCATED
#   Tier 2: resource_id (terminal) -> shared_cost  + NO_IDENTITIES_LOCATED
# ---------------------------------------------------------------------------

SR_MODEL = ChainModel(
    models=[
        EvenSplitModel(
            source=lambda ctx: sorted(ctx.identities.merged_active.ids()),
            cost_type=CostType.USAGE,
        ),
        EvenSplitModel(
            source=lambda ctx: sorted(ctx.identities.tenant_period.ids_by_type(*OWNER_IDENTITY_TYPES)),
            cost_type=CostType.SHARED,
            detail=AllocationDetail.NO_ACTIVE_IDENTITIES_LOCATED,
        ),
        TerminalModel(
            identity_id=lambda ctx: ctx.billing_line.resource_id,
            cost_type=CostType.SHARED,
            detail=AllocationDetail.NO_IDENTITIES_LOCATED,
        ),
    ],
    log_fallbacks=True,
)

# ---------------------------------------------------------------------------
# Connector allocation models
#
# Two models separate cost type semantics by product type:
#
# CONNECTOR_TASKS_MODEL — task/throughput-based costs (USAGE):
#   Tier 0: active identities      -> usage_cost   (CostType.USAGE)
#   Tier 1: resource_id (terminal) -> shared_cost  + NO_IDENTITIES_LOCATED
#
# CONNECTOR_CAPACITY_MODEL — infrastructure costs (SHARED):
#   Tier 0: active identities      -> shared_cost  (CostType.SHARED)
#   Tier 1: resource_id (terminal) -> shared_cost  + NO_IDENTITIES_LOCATED
#
# Both models: no tenant_period fallback — matches reference behavior
# and preserves GAP-24 fix (resource-local terminal when no active identities).
# ---------------------------------------------------------------------------

CONNECTOR_TASKS_MODEL = ChainModel(
    models=[
        EvenSplitModel(
            source=lambda ctx: sorted(ctx.identities.merged_active.ids()),
            cost_type=CostType.USAGE,
        ),
        TerminalModel(
            identity_id=lambda ctx: ctx.billing_line.resource_id,
            cost_type=CostType.SHARED,
            detail=AllocationDetail.NO_IDENTITIES_LOCATED,
        ),
    ],
    log_fallbacks=True,
)

CONNECTOR_CAPACITY_MODEL = ChainModel(
    models=[
        EvenSplitModel(
            source=lambda ctx: sorted(ctx.identities.merged_active.ids()),
            cost_type=CostType.SHARED,
        ),
        TerminalModel(
            identity_id=lambda ctx: ctx.billing_line.resource_id,
            cost_type=CostType.SHARED,
            detail=AllocationDetail.NO_IDENTITIES_LOCATED,
        ),
    ],
    log_fallbacks=True,
)

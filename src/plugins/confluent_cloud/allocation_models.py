"""Composable allocation model definitions for CCloud plugins.

Each MODEL constant is a ChainModel (or single AllocationModel) that encodes
the full allocation strategy for a product type. Allocators in allocators/
import and call these models.
"""

from __future__ import annotations

import logging

from core.engine.allocation_models import ChainModel, EvenSplitModel, TerminalModel, UsageRatioModel
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
# KsqlDB allocation model
#
# Mirrors KSQLNumCSUCostAllocator from reference:
#   Tier 0: active identities      -> usage_cost   (CostType.USAGE)
#   Tier 1: tenant-period owners   -> shared_cost  + NO_ACTIVE_IDENTITIES_LOCATED
#   Tier 2: resource_id (terminal) -> shared_cost  + NO_IDENTITIES_LOCATED
# ---------------------------------------------------------------------------

KSQLDB_MODEL = ChainModel(
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

# ---------------------------------------------------------------------------
# Flink allocation model
#
# Mirrors FlinkNumCFUCostAllocator from reference:
#   Tier 0: UsageRatio by stmt_owner_cfu     -> usage_cost  (CostType.USAGE)
#   Tier 1: EvenSplit across merged_active   -> usage_cost  + NO_USAGE_FOR_ACTIVE_IDENTITIES
#   Tier 2: EvenSplit across tenant_period   -> shared_cost + NO_ACTIVE_IDENTITIES_LOCATED
#   Tier 3: resource_id (terminal)           -> shared_cost + NO_IDENTITIES_LOCATED
# ---------------------------------------------------------------------------

FLINK_MODEL = ChainModel(
    models=[
        UsageRatioModel(
            usage_source=lambda ctx: ctx.identities.context.get("stmt_owner_cfu", {}),
        ),
        EvenSplitModel(
            source=lambda ctx: sorted(ctx.identities.merged_active.ids()),
            cost_type=CostType.USAGE,
            detail=AllocationDetail.NO_USAGE_FOR_ACTIVE_IDENTITIES,
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

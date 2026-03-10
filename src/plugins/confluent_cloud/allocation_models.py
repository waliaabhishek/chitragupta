"""Composable allocation model definitions for CCloud plugins.

Each MODEL constant is a ChainModel (or single AllocationModel) that encodes
the full allocation strategy for a product type. Allocators in allocators/
import and call these models.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from core.engine.allocation_models import ChainModel, EvenSplitModel, TerminalModel, UsageRatioModel
from core.models import OWNER_IDENTITY_TYPES, CostType
from core.models.chargeback import AllocationDetail

if TYPE_CHECKING:
    from core.engine.allocation import AllocationContext

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


# ---------------------------------------------------------------------------
# Kafka network / partition allocation helpers and models
#
# Mirrors UsageBasedNetworkCostAllocator from reference:
#   Tier 0: usage ratio by bytes       -> usage_cost  (UsageRatioModel)
#   Tier 1: merged_active even split   -> shared_cost + NO_METRICS_LOCATED
#   Tier 2: tenant_period even split   -> shared_cost + NO_ACTIVE_IDENTITIES_LOCATED
#   Tier 3: resource_id (terminal)     -> shared_cost + NO_IDENTITIES_LOCATED
# ---------------------------------------------------------------------------


def _extract_usage(
    ctx: AllocationContext,
    metric_key: str,
    principal_label: str,
) -> dict[str, float]:
    """Extract per-owner usage from a single metric key, resolving API keys to owners.

    Returns empty dict when metrics_data is absent, key missing, or all values <= 0.
    Used as usage_source for UsageRatioModel — empty result signals fallback to next tier.
    """
    if not ctx.metrics_data:
        return {}
    api_key_to_owner: dict[str, str] = ctx.identities.context.get("api_key_to_owner", {})
    result: dict[str, float] = {}
    for row in ctx.metrics_data.get(metric_key, []):
        principal = row.labels.get(principal_label)
        if principal and row.value > 0:
            owner = api_key_to_owner.get(principal, principal)
            result[owner] = result.get(owner, 0.0) + row.value
    return result


def make_network_model(metric_key: str, principal_label: str) -> ChainModel:
    """Create a 4-tier network cost allocation ChainModel.

    Tier 0 — UsageRatioModel: split by byte consumption per identity.
              Returns None when metrics absent or all values zero.
    Tier 1 — EvenSplitModel(merged_active): fires when Tier 0 returns None
              (no metrics, or zero usage). Returns None when merged_active is empty.
    Tier 2 — EvenSplitModel(tenant_period owners): fires when no active identities.
              Returns None when no owner-type identities in period.
    Tier 3 — TerminalModel(resource_id): always succeeds.

    Args:
        metric_key: Key into ctx.metrics_data (e.g. "bytes_in", "bytes_out").
        principal_label: MetricRow label key for principal ID (e.g. "principal_id").
    """

    def usage_source(ctx: AllocationContext) -> dict[str, float]:
        return _extract_usage(ctx, metric_key, principal_label)

    return ChainModel(
        models=[
            UsageRatioModel(
                usage_source=usage_source,
                detail=AllocationDetail.USAGE_RATIO_ALLOCATION,
            ),
            EvenSplitModel(
                source=lambda ctx: sorted(ctx.identities.merged_active.ids()),
                cost_type=CostType.SHARED,
                detail=AllocationDetail.NO_METRICS_LOCATED,
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


BYTES_IN_MODEL = make_network_model(metric_key="bytes_in", principal_label="principal_id")
BYTES_OUT_MODEL = make_network_model(metric_key="bytes_out", principal_label="principal_id")

# KAFKA_PARTITION: no metrics configured in handler (get_metrics_for_product_type returns []).
# Model always falls through Tier 0 to even-split fallbacks (Tiers 1-3).
PARTITION_MODEL = make_network_model(metric_key="partition_count", principal_label="principal_id")

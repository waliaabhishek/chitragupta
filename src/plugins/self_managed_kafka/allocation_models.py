"""Composable allocation model definitions for Self-Managed Kafka plugin.

SMK_INGRESS_MODEL and SMK_EGRESS_MODEL encode the full 3-tier network
allocation strategy using ChainModel, parallel to CCloud's make_network_model.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from core.engine.allocation_models import ChainModel, EvenSplitModel, TerminalModel, UsageRatioModel
from core.models import CostType
from core.models.chargeback import AllocationDetail

if TYPE_CHECKING:
    from core.engine.allocation import AllocationContext

logger = logging.getLogger(__name__)


def _extract_smk_usage(ctx: AllocationContext, metric_key: str) -> dict[str, float]:
    """Extract per-principal byte usage from metrics_data for the given metric key.

    SMK principals appear directly in the 'principal' label — no API key resolution
    needed (contrast with CCloud which resolves api_key_to_owner via context).

    Returns empty dict when metrics_data absent, key missing, or all values <= 0.
    Empty result signals UsageRatioModel to return None and trigger next tier.
    """
    if not ctx.metrics_data:
        return {}
    result: dict[str, float] = {}
    for row in ctx.metrics_data.get(metric_key, []):
        principal = row.labels.get("principal")
        if principal and row.value > 0:
            result[principal] = result.get(principal, 0.0) + row.value
    return result


def make_smk_network_model(metric_key: str) -> ChainModel:
    """Create a 3-tier SMK network cost allocation ChainModel."""

    def usage_source(ctx: AllocationContext) -> dict[str, float]:
        return _extract_smk_usage(ctx, metric_key)

    return ChainModel(
        models=[
            UsageRatioModel(
                usage_source=usage_source,
                detail=AllocationDetail.USAGE_RATIO_ALLOCATION,
            ),
            EvenSplitModel(
                source=lambda ctx: sorted(ctx.identities.resource_active.ids()),
                cost_type=CostType.SHARED,
                detail=AllocationDetail.NO_METRICS_LOCATED,
            ),
            TerminalModel(
                identity_id="UNALLOCATED",
                cost_type=CostType.SHARED,
                detail=AllocationDetail.NO_IDENTITIES_LOCATED,
            ),
        ],
        log_fallbacks=True,
    )


# ---------------------------------------------------------------------------
# SMK network allocation models
#
# 3-tier chain:
#   Tier 0: UsageRatio by principal bytes (Prometheus)   -> usage_ratio_allocation
#   Tier 1: EvenSplit over resource_active (static cfg)  -> no_metrics_located
#   Tier 2: Terminal to UNALLOCATED                      -> no_identities_located
# ---------------------------------------------------------------------------

SMK_INGRESS_MODEL = make_smk_network_model(metric_key="bytes_in_per_principal")
SMK_EGRESS_MODEL = make_smk_network_model(metric_key="bytes_out_per_principal")

"""Composable allocation model definitions for Self-Managed Kafka plugin.

SMK_INGRESS_MODEL and SMK_EGRESS_MODEL encode the full 3-tier network
allocation strategy using ChainModel, parallel to CCloud's make_network_model.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING

from core.engine.allocation import AllocationResult
from core.engine.allocation_models import EvenSplitModel
from core.engine.helpers import make_row
from core.models import CostType
from core.models.chargeback import AllocationDetail
from plugins.self_managed_kafka.telemetry_contract import (
    SMK_DETAIL_PRINCIPAL_TELEMETRY_INVALID,
    SMK_DETAIL_PRINCIPAL_TELEMETRY_NOT_OBSERVED,
)

if TYPE_CHECKING:
    from core.engine.allocation import AllocationContext

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class StaticPolicyAllocationModel:
    """Allocate only configured static identities; telemetry is evidence, not usage."""

    def __call__(self, ctx: AllocationContext) -> AllocationResult:
        evidence = ctx.identities.context
        metadata = {
            "principal_attribution_status": evidence.get("principal_attribution_status", "policy_only_configured"),
            "principal_attribution_detail": evidence.get("principal_attribution_detail", ""),
            "measured_usage": False,
        }
        if "metrics_scope_status" in evidence:
            metadata["metrics_scope_status"] = evidence["metrics_scope_status"]
        if "metrics_scope_detail" in evidence:
            metadata["metrics_scope_detail"] = evidence["metrics_scope_detail"]
        policy = EvenSplitModel(
            source=lambda context: sorted(context.identities.resource_active.ids()),
            detail=AllocationDetail.EVEN_SPLIT_ALLOCATION,
            cost_type=CostType.SHARED,
        ).allocate(ctx)
        if policy is not None:
            for row in policy.rows:
                row.metadata.update(metadata)
                row.metadata["allocation_basis"] = "static_policy"
            return policy

        status = metadata["principal_attribution_status"]
        detail: str | AllocationDetail
        if status == "not_observed":
            detail = SMK_DETAIL_PRINCIPAL_TELEMETRY_NOT_OBSERVED
        elif status == "invalid":
            detail = SMK_DETAIL_PRINCIPAL_TELEMETRY_INVALID
        elif status == "transient_failure":
            detail = AllocationDetail.METRICS_FETCH_FAILED
        else:
            detail = AllocationDetail.NO_IDENTITIES_LOCATED
        row = make_row(
            ctx,
            identity_id="UNALLOCATED",
            cost_type=CostType.SHARED,
            amount=ctx.split_amount,
            allocation_method="static_policy",
            allocation_detail=detail,
        )
        row.metadata.update(metadata)
        return AllocationResult(rows=[row])


SMK_INGRESS_MODEL = StaticPolicyAllocationModel()
SMK_EGRESS_MODEL = StaticPolicyAllocationModel()
SMK_INFRA_MODEL = StaticPolicyAllocationModel()

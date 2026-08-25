"""Composable allocation model definitions for Self-Managed Kafka plugin.

SMK_INGRESS_MODEL and SMK_EGRESS_MODEL encode the full 3-tier network
allocation strategy using ChainModel, parallel to CCloud's make_network_model.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from decimal import ROUND_HALF_UP, Decimal
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

PRINCIPAL_CLIENT_ONLY_RESIDUAL_DETAIL = "principal_client_only_residual"
PRINCIPAL_ROUNDING_RESIDUAL_DETAIL = "principal_rounding_residual"
PRINCIPAL_POLICY_UNATTRIBUTED_DETAIL = "principal_policy_unattributed"


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


@dataclass(frozen=True)
class QuotaPrincipalAllocationModel:
    """Allocate one network pool from already-evaluated quota evidence."""

    direction: str

    def __call__(self, ctx: AllocationContext) -> AllocationResult:
        from plugins.self_managed_kafka.principal_attribution import (
            PrincipalAttributionState,
            allocate_principal_money,
        )

        evidence = ctx.identities.context.get("principal_telemetry_evidence")
        evaluation = getattr(evidence, self.direction, None)
        pool = ctx.split_amount.quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
        if evaluation is None:
            return AllocationResult(
                rows=[
                    make_row(
                        ctx,
                        identity_id="UNALLOCATED",
                        cost_type=CostType.SHARED,
                        amount=pool,
                        allocation_method="principal_quota_unavailable_v1",
                        allocation_detail=AllocationDetail.METRICS_FETCH_FAILED,
                    )
                ]
            )
        allocation = allocate_principal_money(evaluation, pool=pool)
        method = {
            PrincipalAttributionState.READY: "principal_quota_ready_v1",
            PrincipalAttributionState.DEGRADED: "principal_quota_degraded_v1",
            PrincipalAttributionState.UNAVAILABLE: "principal_quota_unavailable_v1",
            PrincipalAttributionState.ZERO_USAGE: "principal_quota_zero_usage_v1",
        }[evaluation.state]
        if evaluation.state in {PrincipalAttributionState.READY, PrincipalAttributionState.DEGRADED}:
            rows = []
            for weight, amount in allocation.user_amounts:
                row = make_row(
                    ctx,
                    identity_id=weight.identity_id,
                    cost_type=CostType.USAGE,
                    amount=amount,
                    allocation_method=method,
                    allocation_detail=AllocationDetail.USAGE_RATIO_ALLOCATION,
                )
                row.metadata["team"] = weight.team
                rows.append(row)
            if evaluation.state is PrincipalAttributionState.DEGRADED and evaluation.client_only_weight > Decimal("0"):
                rows.append(
                    make_row(
                        ctx,
                        identity_id="UNALLOCATED",
                        cost_type=CostType.SHARED,
                        amount=allocation.client_only_amount,
                        allocation_method=method,
                        allocation_detail=PRINCIPAL_CLIENT_ONLY_RESIDUAL_DETAIL,
                    )
                )
            if allocation.rounding_residual > Decimal("0"):
                rows.append(
                    make_row(
                        ctx,
                        identity_id="UNALLOCATED",
                        cost_type=CostType.SHARED,
                        amount=allocation.rounding_residual,
                        allocation_method=method,
                        allocation_detail=PRINCIPAL_ROUNDING_RESIDUAL_DETAIL,
                    )
                )
            return AllocationResult(rows=rows)
        detail = (
            "principal_zero_usage" if evaluation.state is PrincipalAttributionState.ZERO_USAGE else evaluation.detail
        )
        return AllocationResult(
            rows=[
                make_row(
                    ctx,
                    identity_id="UNALLOCATED",
                    cost_type=CostType.SHARED,
                    amount=allocation.client_only_amount,
                    allocation_method=method,
                    allocation_detail=detail,
                )
            ]
        )


@dataclass(frozen=True)
class FixedPrincipalPolicyAllocationModel:
    """Apply one explicitly configured fixed policy without measured ownership."""

    policy: str
    identities: tuple[str, ...] = ()

    def __call__(self, ctx: AllocationContext) -> AllocationResult:
        from plugins.self_managed_kafka.principal_attribution import allocate_static_even

        pool = ctx.split_amount.quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
        if self.policy != "static_even_v1":
            return AllocationResult(
                rows=[
                    make_row(
                        ctx,
                        identity_id="UNALLOCATED",
                        cost_type=CostType.SHARED,
                        amount=pool,
                        allocation_method="principal_unattributed_v1",
                        allocation_detail=PRINCIPAL_POLICY_UNATTRIBUTED_DETAIL,
                    )
                ]
            )
        allocation = allocate_static_even(identities=self.identities, pool=pool)
        if not allocation.user_amounts:
            return AllocationResult(
                rows=[
                    make_row(
                        ctx,
                        identity_id="UNALLOCATED",
                        cost_type=CostType.SHARED,
                        amount=allocation.client_only_amount,
                        allocation_method="principal_unattributed_v1",
                        allocation_detail=PRINCIPAL_POLICY_UNATTRIBUTED_DETAIL,
                    )
                ]
            )
        rows = [
            make_row(
                ctx,
                identity_id=weight.identity_id,
                cost_type=CostType.SHARED,
                amount=amount,
                allocation_method="static_even_v1",
                allocation_detail=AllocationDetail.EVEN_SPLIT_ALLOCATION,
            )
            for weight, amount in allocation.user_amounts
        ]
        if allocation.rounding_residual > Decimal("0"):
            rows.append(
                make_row(
                    ctx,
                    identity_id="UNALLOCATED",
                    cost_type=CostType.SHARED,
                    amount=allocation.rounding_residual,
                    allocation_method="static_even_v1",
                    allocation_detail=PRINCIPAL_ROUNDING_RESIDUAL_DETAIL,
                )
            )
        return AllocationResult(rows=rows)

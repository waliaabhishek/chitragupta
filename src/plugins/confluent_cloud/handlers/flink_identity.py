"""Flink identity resolution helper for Confluent Cloud Flink compute pools.

Unlike connectors/ksqlDB which resolve identity from resource metadata alone,
Flink uses a two-step process:
1. Metrics identify active statement names for a compute pool
2. Statement resources are looked up to find the owner (principal)

The stmt_owner_cfu map is stored in IdentityResolution.context for the allocator.
"""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

from core.models import IdentityResolution, IdentitySet

from ._identity_helpers import create_flink_sentinel, create_sentinel_from_id

if TYPE_CHECKING:
    from core.models import MetricRow
    from core.storage.interface import UnitOfWork


# Sentinel identity IDs for Flink
FLINK_STMT_OWNER_UNKNOWN = "flink_stmt_owner_unknown"


def _extract_active_statements(
    metrics_data: dict[str, list[MetricRow]],
    pool_id: str,
) -> dict[str, float]:
    """Extract statement names with CFU usage for this pool.

    Returns dict mapping statement_name -> total CFU value.
    Filters out zero-value metrics rows.
    """
    stmt_cfu: dict[str, float] = {}
    for rows in metrics_data.values():
        for row in rows:
            cp_id = row.labels.get("compute_pool_id", "")
            if cp_id != pool_id:
                continue
            if row.value <= 0:
                continue
            stmt_name = row.labels.get("flink_statement_name", "")
            if stmt_name:
                stmt_cfu[stmt_name] = stmt_cfu.get(stmt_name, 0.0) + row.value
    return stmt_cfu


def _fallback_from_running_statements(
    compute_pool_id: str,
    tenant_id: str,
    billing_start: datetime,
    billing_end: datetime,
    uow: UnitOfWork,
    ecosystem: str,
) -> tuple[dict[str, float], IdentitySet]:
    """Find running statements for a compute pool when metrics are unavailable.

    Queries the resources table for flink_statement resources whose
    metadata.compute_pool_id matches the given pool. Assigns equal CFU
    weight to each running statement (since we don't have actual metrics).

    Implicit allocatability guard: if no Flink API credentials are configured
    for the pool's region, the gathering phase will not have fetched any
    flink_statement resources for that pool. This query then returns empty,
    producing the same gate as the old `is_chargeback_allocatable` check
    without requiring an explicit credential check here.
    """
    all_resources, _ = uow.resources.find_by_period(
        ecosystem=ecosystem,
        tenant_id=tenant_id,
        start=billing_start,
        end=billing_end,
    )

    # Filter to running statements belonging to this compute pool
    pool_stmts = [
        s
        for s in all_resources
        if s.resource_type == "flink_statement"
        and s.metadata.get("compute_pool_id") == compute_pool_id
        and s.metadata.get("status", "") not in ("COMPLETED", "FAILED", "STOPPED")
    ]

    if not pool_stmts:
        return {}, IdentitySet()

    all_identities, _ = uow.identities.find_by_period(
        ecosystem=ecosystem,
        tenant_id=tenant_id,
        start=billing_start,
        end=billing_end,
    )
    identity_by_id = {i.identity_id: i for i in all_identities}

    # Equal weight per statement (no metrics to differentiate)
    resource_active = IdentitySet()
    owner_weight: dict[str, float] = {}
    for stmt in pool_stmts:
        owner_id = stmt.owner_id or stmt.metadata.get("owner_id")
        if owner_id:
            owner_weight[owner_id] = owner_weight.get(owner_id, 0.0) + 1.0
            identity = identity_by_id.get(owner_id)
            if identity:
                resource_active.add(identity)
            else:
                sentinel = create_sentinel_from_id(owner_id, tenant_id, ecosystem)
                resource_active.add(sentinel)

    return owner_weight, resource_active


def _resolve_statement_owners(
    stmt_cfu: dict[str, float],
    resource_id: str,
    tenant_id: str,
    billing_start: datetime,
    billing_end: datetime,
    uow: UnitOfWork,
    ecosystem: str,
) -> tuple[dict[str, float], IdentitySet]:
    """Resolve statement owners from resource DB for the metrics-driven primary path.

    Returns (stmt_owner_cfu mapping owner_id->cfu, resource_active IdentitySet).
    """
    resources, _ = uow.resources.find_by_period(
        ecosystem=ecosystem,
        tenant_id=tenant_id,
        start=billing_start,
        end=billing_end,
    )

    # Build statement_name -> owner_id map from flink_statement resources
    # TD-033: Filter by compute_pool_id to avoid cross-pool collisions
    stmt_name_to_owner: dict[str, str | None] = {}
    for r in resources:
        if r.resource_type == "flink_statement":
            pool_id = r.metadata.get("compute_pool_id", "")
            if pool_id != resource_id:
                continue  # Statement belongs to different pool
            display_name = r.display_name or r.metadata.get("statement_name", "")
            if display_name:
                stmt_name_to_owner[display_name] = r.owner_id

    all_identities, _ = uow.identities.find_by_period(
        ecosystem=ecosystem,
        tenant_id=tenant_id,
        start=billing_start,
        end=billing_end,
    )
    identity_by_id = {i.identity_id: i for i in all_identities}

    resource_active = IdentitySet()
    owner_cfu: dict[str, float] = {}
    for stmt_name, cfu_value in stmt_cfu.items():
        owner_id = stmt_name_to_owner.get(stmt_name)

        if not owner_id:
            # Statement not found in resources or has no owner
            sentinel = create_flink_sentinel(
                FLINK_STMT_OWNER_UNKNOWN,
                tenant_id,
                ecosystem,
                "Flink Statement Owner Unknown",
            )
            resource_active.add(sentinel)
            key = FLINK_STMT_OWNER_UNKNOWN
            owner_cfu[key] = owner_cfu.get(key, 0.0) + cfu_value
            continue

        owner = identity_by_id.get(owner_id)
        if owner is None:
            owner = create_sentinel_from_id(owner_id, tenant_id, ecosystem)

        resource_active.add(owner)
        owner_cfu[owner.identity_id] = owner_cfu.get(owner.identity_id, 0.0) + cfu_value

    return owner_cfu, resource_active


def resolve_flink_identity(
    tenant_id: str,
    resource_id: str,
    billing_start: datetime,
    billing_end: datetime,
    metrics_data: dict[str, list[MetricRow]] | None,
    uow: UnitOfWork,
    ecosystem: str,
) -> IdentityResolution:
    """Resolve identities for a Flink compute pool.

    Two-step process:
    1. Extract active statement names from metrics where compute_pool_id matches resource_id
    2. Look up flink_statement resources by display_name to find owner (principal)

    Falls back to querying running statements from the resource DB when metrics are absent.

    Returns IdentityResolution with:
    - resource_active: owners of statements with CFU usage in billing window
    - metrics_derived: empty
    - tenant_period: empty (orchestrator fills this)
    - context["stmt_owner_cfu"]: dict[str, float] mapping owner_id -> total CFU
    """
    # Primary path: extract from metrics
    if metrics_data:
        active_stmts = _extract_active_statements(metrics_data, resource_id)

        if active_stmts:
            stmt_owner_cfu, resource_active = _resolve_statement_owners(
                active_stmts, resource_id, tenant_id, billing_start, billing_end, uow, ecosystem
            )
            if stmt_owner_cfu:
                return IdentityResolution(
                    resource_active=resource_active,
                    metrics_derived=IdentitySet(),
                    tenant_period=IdentitySet(),
                    context={"stmt_owner_cfu": stmt_owner_cfu},
                )

    # Secondary path: no metrics or no active statements from metrics
    # Find running Flink statements for this compute pool from resource DB
    stmt_owner_cfu, resource_active = _fallback_from_running_statements(
        resource_id, tenant_id, billing_start, billing_end, uow, ecosystem
    )
    if stmt_owner_cfu:
        return IdentityResolution(
            resource_active=resource_active,
            metrics_derived=IdentitySet(),
            tenant_period=IdentitySet(),
            context={"stmt_owner_cfu": stmt_owner_cfu},
        )

    # No statements found at all
    return IdentityResolution(
        resource_active=IdentitySet(),
        metrics_derived=IdentitySet(),
        tenant_period=IdentitySet(),
        context={},
    )

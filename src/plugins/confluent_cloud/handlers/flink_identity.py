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

    Returns IdentityResolution with:
    - resource_active: owners of statements with CFU usage in billing window
    - metrics_derived: empty
    - tenant_period: empty (orchestrator fills this)
    - context["stmt_owner_cfu"]: dict[str, float] mapping owner_id -> total CFU
    """
    resource_active = IdentitySet()
    stmt_owner_cfu: dict[str, float] = {}

    # No metrics -> no statements can be identified
    if not metrics_data:
        return IdentityResolution(
            resource_active=resource_active,
            metrics_derived=IdentitySet(),
            tenant_period=IdentitySet(),
            context={},
        )

    # Step 1: Extract active statement names and CFU usage from metrics
    stmt_cfu = _extract_active_statements(metrics_data, resource_id)

    # No active statements found in metrics
    if not stmt_cfu:
        return IdentityResolution(
            resource_active=resource_active,
            metrics_derived=IdentitySet(),
            tenant_period=IdentitySet(),
            context={},
        )

    # Step 2: Look up statement resources to find owners
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

    # Get all identities in billing window for lookup
    all_identities, _ = uow.identities.find_by_period(
        ecosystem=ecosystem,
        tenant_id=tenant_id,
        start=billing_start,
        end=billing_end,
    )
    identity_by_id = {i.identity_id: i for i in all_identities}

    # Step 3: Map statement CFU to owners
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
            stmt_owner_cfu[key] = stmt_owner_cfu.get(key, 0.0) + cfu_value
            continue

        # Resolve owner identity
        owner = identity_by_id.get(owner_id)
        if owner is None:
            owner = create_sentinel_from_id(owner_id, tenant_id, ecosystem)

        resource_active.add(owner)
        stmt_owner_cfu[owner.identity_id] = stmt_owner_cfu.get(owner.identity_id, 0.0) + cfu_value

    return IdentityResolution(
        resource_active=resource_active,
        metrics_derived=IdentitySet(),
        tenant_period=IdentitySet(),
        context={"stmt_owner_cfu": stmt_owner_cfu},
    )

"""ksqlDB identity resolution helper for Confluent Cloud ksqlDB apps.

This module provides identity resolution for ksqlDB apps based on their owner_id.
Unlike connectors which have auth mode branching, ksqlDB uses direct owner_id lookup.
"""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

from core.models import IdentityResolution, IdentitySet

from ._identity_helpers import create_ksqldb_sentinel, create_sentinel_from_id

if TYPE_CHECKING:
    from core.storage.interface import UnitOfWork


# Sentinel identity IDs for ksqlDB
KSQLDB_DELETED_SENTINEL = "ksqldb_deleted_when_calc_started"
KSQLDB_OWNER_UNKNOWN = "ksqldb_owner_unknown"


def resolve_ksqldb_identity(
    tenant_id: str,
    resource_id: str,
    billing_start: datetime,
    billing_end: datetime,
    uow: UnitOfWork,
    ecosystem: str,
) -> IdentityResolution:
    """Resolve identity for a ksqlDB app.

    ksqlDB apps have direct owner_id field on Resource (credential_identity from CCloud API).

    Args:
        tenant_id: The tenant ID.
        resource_id: The ksqlDB resource ID.
        billing_start: Start of billing window.
        billing_end: End of billing window.
        uow: Unit of work for database access.
        ecosystem: The ecosystem name.

    Returns:
        IdentityResolution with:
        - resource_active: The ksqlDB owner (or sentinel if unknown/deleted)
        - metrics_derived: Empty (ksqlDB doesn't have metrics-based identity)
        - tenant_period: Empty (orchestrator fills this)
    """
    resource_active = IdentitySet()
    metrics_derived = IdentitySet()
    tenant_period = IdentitySet()

    # Find all resources in the billing period
    resources, _ = uow.resources.find_by_period(
        ecosystem=ecosystem,
        tenant_id=tenant_id,
        start=billing_start,
        end=billing_end,
    )

    # Filter to the specific ksqlDB resource
    ksqldb_app = next((r for r in resources if r.resource_id == resource_id), None)

    # Resource not found -> deleted sentinel
    if ksqldb_app is None:
        sentinel = create_ksqldb_sentinel(
            KSQLDB_DELETED_SENTINEL,
            tenant_id,
            ecosystem,
            "ksqlDB Deleted When Calculation Started",
        )
        resource_active.add(sentinel)
        return IdentityResolution(
            resource_active=resource_active,
            metrics_derived=metrics_derived,
            tenant_period=tenant_period,
        )

    # Get owner_id from top-level field (metadata fallback for legacy)
    owner_id = ksqldb_app.owner_id or ksqldb_app.metadata.get("owner_id")

    # No owner_id -> unknown sentinel
    if not owner_id:
        sentinel = create_ksqldb_sentinel(
            KSQLDB_OWNER_UNKNOWN,
            tenant_id,
            ecosystem,
            "ksqlDB Owner Unknown",
        )
        resource_active.add(sentinel)
        return IdentityResolution(
            resource_active=resource_active,
            metrics_derived=metrics_derived,
            tenant_period=tenant_period,
        )

    # Get all identities in billing window for lookup
    all_identities, _ = uow.identities.find_by_period(
        ecosystem=ecosystem,
        tenant_id=tenant_id,
        start=billing_start,
        end=billing_end,
    )
    identity_by_id = {i.identity_id: i for i in all_identities}

    # Look up owner in identities
    owner = identity_by_id.get(owner_id)
    if owner is None:
        # Owner not in DB -> create sentinel from ID (parses prefix for type)
        owner = create_sentinel_from_id(owner_id, tenant_id, ecosystem)

    resource_active.add(owner)

    return IdentityResolution(
        resource_active=resource_active,
        metrics_derived=metrics_derived,
        tenant_period=tenant_period,
    )

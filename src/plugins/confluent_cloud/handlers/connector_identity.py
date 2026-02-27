"""Connector identity resolution helper for Confluent Cloud connectors.

This module provides identity resolution for connectors based on their authentication mode.
Unlike Kafka/SR which use API key lookups, connectors have direct owner information in metadata.
"""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

from core.models import IdentityResolution, IdentitySet

from ._identity_helpers import create_connector_sentinel, create_sentinel_from_id

if TYPE_CHECKING:
    from core.models import Identity
    from core.storage.interface import UnitOfWork


# Sentinel identity IDs for unknown connector owners
CONNECTOR_CREDENTIALS_UNKNOWN = "connector_credentials_unknown"
CONNECTOR_CREDENTIALS_MASKED = "connector_credentials_masked"


def resolve_connector_identity(
    tenant_id: str,
    resource_id: str,
    billing_start: datetime,
    billing_end: datetime,
    uow: UnitOfWork,
    ecosystem: str,
) -> IdentityResolution:
    """Resolve identity for a connector based on its authentication mode.

    Args:
        tenant_id: The tenant ID.
        resource_id: The connector resource ID.
        billing_start: Start of billing window.
        billing_end: End of billing window.
        uow: Unit of work for database access.
        ecosystem: The ecosystem name.

    Returns:
        IdentityResolution with:
        - resource_active: The connector owner (or sentinel if unknown/masked)
        - metrics_derived: Empty (connectors don't have metrics-based identity)
        - tenant_period: Empty (orchestrator fills this)
    """
    resource_active = IdentitySet()
    metrics_derived = IdentitySet()
    tenant_period = IdentitySet()

    # Find the connector resource in the billing period
    resources, _ = uow.resources.find_by_period(
        ecosystem=ecosystem,
        tenant_id=tenant_id,
        start=billing_start,
        end=billing_end,
    )

    # Filter to the specific connector resource
    connector = None
    for r in resources:
        if r.resource_id == resource_id:
            connector = r
            break

    # Resource not found -> masked sentinel
    if connector is None:
        sentinel = create_connector_sentinel(CONNECTOR_CREDENTIALS_MASKED, tenant_id, ecosystem, is_masked=True)
        resource_active.add(sentinel)
        return IdentityResolution(
            resource_active=resource_active,
            metrics_derived=metrics_derived,
            tenant_period=tenant_period,
        )

    # Get auth mode from metadata
    auth_mode = connector.metadata.get("kafka_auth_mode")

    # Get all identities in billing window for lookup
    all_identities, _ = uow.identities.find_by_period(
        ecosystem=ecosystem,
        tenant_id=tenant_id,
        start=billing_start,
        end=billing_end,
    )
    identity_by_id = {i.identity_id: i for i in all_identities}

    owner: Identity | None = None

    if auth_mode == "SERVICE_ACCOUNT":
        # Direct owner from service account ID
        sa_id = connector.metadata.get("kafka_service_account_id")
        if sa_id:
            owner = identity_by_id.get(sa_id) or create_sentinel_from_id(sa_id, tenant_id, ecosystem)
        else:
            # No service account ID in metadata
            owner = create_connector_sentinel(CONNECTOR_CREDENTIALS_UNKNOWN, tenant_id, ecosystem, is_masked=False)

    elif auth_mode == "KAFKA_API_KEY":
        # Look up API key, then resolve its owner
        api_key_id = connector.metadata.get("kafka_api_key")
        if api_key_id:
            api_key = identity_by_id.get(api_key_id)
            if api_key:
                owner_id = api_key.metadata.get("owner_id")
                if owner_id:
                    owner = identity_by_id.get(owner_id) or create_sentinel_from_id(owner_id, tenant_id, ecosystem)
                else:
                    # API key has no owner_id
                    owner = create_connector_sentinel(
                        CONNECTOR_CREDENTIALS_UNKNOWN, tenant_id, ecosystem, is_masked=False
                    )
            else:
                # API key not found in DB
                owner = create_connector_sentinel(CONNECTOR_CREDENTIALS_UNKNOWN, tenant_id, ecosystem, is_masked=False)
        else:
            # No API key in metadata
            owner = create_connector_sentinel(CONNECTOR_CREDENTIALS_UNKNOWN, tenant_id, ecosystem, is_masked=False)

    else:
        # UNKNOWN mode or missing auth_mode
        owner = create_connector_sentinel(CONNECTOR_CREDENTIALS_UNKNOWN, tenant_id, ecosystem, is_masked=False)

    resource_active.add(owner)

    return IdentityResolution(
        resource_active=resource_active,
        metrics_derived=metrics_derived,
        tenant_period=tenant_period,
    )

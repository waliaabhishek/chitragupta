"""Identity resolution helper for Kafka and Schema Registry handlers.

This module provides temporal identity resolution for CCloud billing allocation.
The critical fix from reference code: filter by billing window, not current state.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import TYPE_CHECKING

from core.models import IdentityResolution, IdentitySet

from ._identity_helpers import create_sentinel_from_id

if TYPE_CHECKING:
    from core.models import Identity, MetricRow
    from core.storage.interface import UnitOfWork
logger = logging.getLogger(__name__)


def resolve_kafka_sr_identities(
    tenant_id: str,
    resource_id: str,
    billing_start: datetime,
    billing_end: datetime,
    metrics_data: dict[str, list[MetricRow]] | None,
    uow: UnitOfWork,
    ecosystem: str,
    cached_identities: IdentitySet | None = None,
) -> IdentityResolution:
    """Resolve identities for Kafka/SR billing with temporal awareness.

    Args:
        tenant_id: The tenant ID.
        resource_id: The Kafka cluster ID (lkc-xxx) or SR ID (lsrc-xxx).
        billing_start: Start of billing window.
        billing_end: End of billing window.
        metrics_data: Prometheus metrics data (may be None).
        uow: Unit of work for database access.
        ecosystem: The ecosystem name.

    Returns:
        IdentityResolution with:
        - resource_active: API key owners for this resource during billing window
        - metrics_derived: Principal IDs from metrics (sentinels if not in DB)
        - tenant_period: Empty (orchestrator fills this)
    """
    logger.debug(
        "resolve_kafka_sr_identities tenant=%s resource=%s",
        tenant_id,
        resource_id,
    )
    resource_active = IdentitySet()
    metrics_derived = IdentitySet()
    tenant_period = IdentitySet()  # Orchestrator fills this

    # 1. Get all identities in billing window (single query, or use cache)
    all_identities: IdentitySet | list[Identity]
    if cached_identities is not None:
        all_identities = cached_identities
    else:
        all_identities, _ = uow.identities.find_by_period(
            ecosystem=ecosystem,
            tenant_id=tenant_id,
            start=billing_start,
            end=billing_end,
            count=False,
        )

    # Build lookup dict for O(1) owner resolution
    identity_by_id = {i.identity_id: i for i in all_identities}

    # 2. Filter to API keys for this cluster (metadata.resource_id per gathering.py)
    for identity in all_identities:
        if identity.identity_type != "api_key":
            continue
        if identity.metadata.get("resource_id") != resource_id:
            continue
        owner_id = identity.metadata.get("owner_id")
        if owner_id:
            owner = identity_by_id.get(owner_id) or create_sentinel_from_id(owner_id, tenant_id, ecosystem)
            resource_active.add(owner)

    # 3. Extract principals from metrics; build api_key_to_owner for allocator use
    api_key_to_owner: dict[str, str] = {}
    if metrics_data:
        principals = _extract_principals_from_metrics(metrics_data)
        for principal_id in principals:
            identity = identity_by_id.get(principal_id) or create_sentinel_from_id(principal_id, tenant_id, ecosystem)
            if identity.identity_type == "api_key":
                owner_id = identity.metadata.get("owner_id")
                if owner_id:
                    owner = identity_by_id.get(owner_id) or create_sentinel_from_id(owner_id, tenant_id, ecosystem)
                    metrics_derived.add(owner)
                    api_key_to_owner[principal_id] = owner_id
                # If no owner_id on API key, drop — unresolvable API key cannot be billed
            else:
                metrics_derived.add(identity)

    logger.debug(
        "resolve_kafka_sr_identities resolved=%d identities resource=%s",
        len(resource_active.ids()) + len(metrics_derived.ids()),
        resource_id,
    )
    return IdentityResolution(
        resource_active=resource_active,
        metrics_derived=metrics_derived,
        tenant_period=tenant_period,
        context={"api_key_to_owner": api_key_to_owner},
    )


def _extract_principals_from_metrics(
    metrics_data: dict[str, list[MetricRow]],
) -> set[str]:
    """Extract unique principal IDs from metrics labels.

    Args:
        metrics_data: Dict mapping metric key to list of MetricRow.

    Returns:
        Set of unique principal_id values.
    """
    principals: set[str] = set()
    for rows in metrics_data.values():
        for row in rows:
            principal_id = row.labels.get("principal_id")
            if principal_id:
                principals.add(principal_id)
    return principals

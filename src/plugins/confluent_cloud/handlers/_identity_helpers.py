"""Shared identity helper functions for Confluent Cloud handlers.

Extracted to avoid DRY violations between identity_resolution.py and connector_identity.py.
"""

from __future__ import annotations

from core.models import Identity

# Prefix-to-type mapping for CCloud identity IDs
_PREFIX_TYPE_MAP = {"sa": "service_account", "u": "user", "pool": "identity_pool"}


def create_sentinel_from_id(identity_id: str, tenant_id: str, ecosystem: str) -> Identity:
    """Create a sentinel identity for unknown identity IDs.

    Parses the identity type from the ID prefix:
    - sa-xxx -> service_account
    - u-xxx -> user
    - pool-xxx -> identity_pool
    - other -> unknown

    Args:
        identity_id: The identity ID to create sentinel for.
        tenant_id: The tenant ID.
        ecosystem: The ecosystem name.

    Returns:
        A sentinel Identity object.
    """
    prefix = identity_id.split("-")[0] if "-" in identity_id else ""
    identity_type = _PREFIX_TYPE_MAP.get(prefix, "unknown")

    return Identity(
        ecosystem=ecosystem,
        tenant_id=tenant_id,
        identity_id=identity_id,
        identity_type=identity_type,
        display_name=f"Unknown {identity_type}",
    )


def create_connector_sentinel(identity_id: str, tenant_id: str, ecosystem: str, *, is_masked: bool) -> Identity:
    """Create a connector-specific sentinel identity.

    Used for connector_credentials_unknown and connector_credentials_masked.

    Args:
        identity_id: The sentinel identity ID.
        tenant_id: The tenant ID.
        ecosystem: The ecosystem name.
        is_masked: True if credentials are known but hidden, False if unknown.

    Returns:
        A connector_credentials sentinel Identity.
    """
    display_name = "Connector Credentials Masked" if is_masked else "Connector Credentials Unknown"
    return Identity(
        ecosystem=ecosystem,
        tenant_id=tenant_id,
        identity_id=identity_id,
        identity_type="connector_credentials",
        display_name=display_name,
    )

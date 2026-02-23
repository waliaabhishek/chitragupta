"""Custom identity resolver for integration test override scenario."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any

from core.models.identity import Identity, IdentityResolution, IdentitySet

if TYPE_CHECKING:
    from core.models.metrics import MetricRow


def my_resolver(
    tenant_id: str,
    resource_id: str,
    billing_timestamp: datetime,
    billing_duration: timedelta,
    metrics_data: dict[str, list[MetricRow]] | None,
    uow: Any,
) -> IdentityResolution:
    """Custom resolver that returns a fixed 'custom-resolved' identity."""
    ra = IdentitySet()
    ra.add(
        Identity(
            ecosystem="test-eco",
            tenant_id=tenant_id,
            identity_id="custom-resolved",
            identity_type="service_account",
            display_name="Custom Resolved",
        )
    )
    return IdentityResolution(
        resource_active=ra,
        metrics_derived=IdentitySet(),
        tenant_period=IdentitySet(),
        context={"override": True},
    )

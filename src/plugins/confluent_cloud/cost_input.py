from __future__ import annotations

import logging
from collections.abc import Iterable, Iterator
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from core.storage.interface import UnitOfWork
    from plugins.confluent_cloud.config import CCloudPluginConfig
    from plugins.confluent_cloud.connections import CCloudConnection

from core.models import BillingLineItem
from core.plugin.protocols import CostInput

LOGGER = logging.getLogger(__name__)
BILLING_API_PATH = "/billing/v1/costs"
BILLING_PAGE_SIZE = 2000
ECOSYSTEM = "confluent_cloud"


def _generate_date_windows(
    start: datetime,
    end: datetime,
    days_per_query: int,
) -> Iterator[tuple[datetime, datetime]]:
    """Yield (window_start, window_end) pairs covering [start, end)."""
    current = start
    delta = timedelta(days=days_per_query)
    while current < end:
        window_end = min(current + delta, end)
        yield current, window_end
        current = window_end


def _parse_billing_date(date_str: str) -> datetime:
    """Parse 'YYYY-MM-DD' to UTC-aware midnight datetime."""
    try:
        return datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=UTC)
    except ValueError as exc:
        raise ValueError(
            f"Invalid billing date format '{date_str}': expected YYYY-MM-DD"
        ) from exc


def _safe_decimal(value: Any) -> Decimal:
    """Convert value to Decimal safely. Returns 0 and logs warning on failure."""
    if value is None:
        return Decimal("0")
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        LOGGER.warning(
            "Could not convert billing value to Decimal: %r — defaulting to 0", value
        )
        return Decimal("0")


def _map_billing_item(
    item: dict[str, Any],
    ecosystem: str,
    tenant_id: str,
) -> BillingLineItem:
    """Map a CCloud billing API response item to BillingLineItem."""
    resource = item.get("resource", {})

    # Build metadata, excluding None values
    metadata: dict[str, Any] = {}
    env_id = resource.get("environment", {}).get("id")
    if env_id:
        metadata["env_id"] = env_id
    resource_name = resource.get("display_name")
    if resource_name:
        metadata["resource_name"] = resource_name
    if "original_amount" in item:
        metadata["original_amount"] = _safe_decimal(item["original_amount"])

    return BillingLineItem(
        ecosystem=ecosystem,
        tenant_id=tenant_id,
        timestamp=_parse_billing_date(item["start_date"]),
        resource_id=resource.get("id") or "unresolved_billing_resource",
        product_category=item.get("product", ""),
        product_type=item.get("line_type", ""),
        quantity=_safe_decimal(item.get("quantity")),
        unit_price=_safe_decimal(item.get("price")),
        total_cost=_safe_decimal(item.get("amount")),
        granularity="daily",
        currency="USD",
        metadata=metadata,
    )


class CCloudBillingCostInput(CostInput):
    """CostInput implementation that gathers billing data from CCloud Billing API.

    This is an API-billed CostInput: it retrieves itemized billing line items
    directly from the Confluent Cloud billing API.
    """

    def __init__(
        self,
        connection: CCloudConnection,
        config: CCloudPluginConfig,
    ) -> None:
        self._connection = connection
        self._config = config

    def gather(
        self,
        tenant_id: str,
        period_start: datetime,
        period_end: datetime,
        uow: UnitOfWork | None,  # Not used by billing API cost input
    ) -> Iterable[BillingLineItem]:
        """Gather billing line items from CCloud Billing API.

        Yields BillingLineItem for each billing record in the period.
        Splits large date ranges into smaller windows per days_per_query config.
        """
        days_per_query = self._config.billing_api.days_per_query

        for window_start, window_end in _generate_date_windows(
            period_start, period_end, days_per_query
        ):
            yield from self._fetch_window(tenant_id, window_start, window_end)

    def _fetch_window(
        self,
        tenant_id: str,
        start: datetime,
        end: datetime,
    ) -> Iterable[BillingLineItem]:
        """Fetch billing items for a single date window."""
        params = {
            "start_date": start.strftime("%Y-%m-%d"),
            "end_date": end.strftime("%Y-%m-%d"),
            "page_size": BILLING_PAGE_SIZE,
        }

        for raw_item in self._connection.get(BILLING_API_PATH, params=params):
            try:
                yield _map_billing_item(raw_item, ECOSYSTEM, tenant_id)
            except (KeyError, ValueError) as exc:
                # Malformed item — log warning and skip
                resource_id = raw_item.get("resource", {}).get("id", "unknown")
                LOGGER.warning(
                    "Skipping malformed billing item (resource=%s): %s",
                    resource_id,
                    exc,
                )
                continue

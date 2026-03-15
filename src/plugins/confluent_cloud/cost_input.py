from __future__ import annotations

import logging
from collections.abc import Iterable, Iterator
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from core.models import BillingLineItem
    from core.storage.interface import UnitOfWork
    from plugins.confluent_cloud.config import CCloudPluginConfig
    from plugins.confluent_cloud.connections import CCloudConnection

from core.plugin.protocols import CostInput
from plugins.confluent_cloud.models.billing import CCloudBillingLineItem

logger = logging.getLogger(__name__)
BILLING_API_PATH = "/billing/v1/costs"
BILLING_PAGE_SIZE = 2000
# Billing is inherently CCloud-specific; constant avoids parameter threading
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
        raise ValueError(f"Invalid billing date format '{date_str}': expected YYYY-MM-DD") from exc


def _safe_decimal(value: Any) -> Decimal:
    """Convert value to Decimal safely. Returns 0 and logs warning on failure."""
    if value is None:
        return Decimal("0")
    try:
        return Decimal(str(value))
    except InvalidOperation:
        logger.warning("Could not convert billing value to Decimal: %r — defaulting to 0", value)
        return Decimal("0")


def _map_billing_item(
    item: dict[str, Any],
    ecosystem: str,
    tenant_id: str,
    row_index: int = 0,
) -> BillingLineItem:
    """Map a CCloud billing API response item to CCloudBillingLineItem."""
    resource = item.get("resource", {})

    # Extract env_id - required for CCloud billing PK to prevent collisions
    env_id = resource.get("environment", {}).get("id") or ""

    # Build metadata, excluding None values
    metadata: dict[str, Any] = {}
    resource_name = resource.get("display_name")
    if resource_name:
        metadata["resource_name"] = resource_name
    if "original_amount" in item:
        metadata["original_amount"] = _safe_decimal(item["original_amount"])

    return CCloudBillingLineItem(
        ecosystem=ecosystem,
        tenant_id=tenant_id,
        timestamp=_parse_billing_date(item["start_date"]),
        env_id=env_id,  # CCloud-specific: part of 7-field PK
        resource_id=resource.get("id") or f"unresolved_billing_{row_index}",
        product_category=item.get("product", ""),
        product_type=item.get("line_type", ""),
        quantity=_safe_decimal(item.get("quantity")),
        unit_price=_safe_decimal(item.get("price")),
        total_cost=_safe_decimal(item.get("amount")),
        granularity="daily",
        currency="USD",
        metadata=metadata,
    )


def _map_malformed_item(
    item: dict[str, Any], ecosystem: str, tenant_id: str, idx: int, exc: Exception
) -> BillingLineItem:
    """Create a billing line from a malformed API row with best-effort field extraction."""
    resource = item.get("resource", {})
    env_id = resource.get("environment", {}).get("id") or ""

    return CCloudBillingLineItem(
        ecosystem=ecosystem,
        tenant_id=tenant_id,
        timestamp=_parse_billing_date(item.get("start_date", "1970-01-01")),
        env_id=env_id,
        resource_id=f"malformed_billing_{idx}",
        product_category=item.get("product", f"MALFORMED_{idx}"),
        product_type=item.get("line_type", f"MALFORMED_{idx}"),
        quantity=_safe_decimal(item.get("quantity")),
        unit_price=_safe_decimal(item.get("price")),
        total_cost=_safe_decimal(item.get("amount")),
        granularity="daily",
        currency="USD",
        metadata={"malformed": True, "parse_error": str(exc)},
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
        start: datetime,
        end: datetime,
        uow: UnitOfWork,
    ) -> Iterable[BillingLineItem]:
        """Gather billing line items from CCloud Billing API.

        Yields BillingLineItem for each billing record in the period.
        Splits large date ranges into smaller windows per days_per_query config.
        """
        days_per_query = self._config.billing_api.days_per_query

        for window_start, window_end in _generate_date_windows(start, end, days_per_query):
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

        for idx, raw_item in enumerate(self._connection.get(BILLING_API_PATH, params=params)):
            try:
                yield _map_billing_item(raw_item, ECOSYSTEM, tenant_id, row_index=idx)
            except (KeyError, ValueError) as exc:
                # Preserve malformed row instead of dropping
                logger.debug("Preserving malformed billing item %d as sentinel row: %s", idx, exc)
                yield _map_malformed_item(raw_item, ECOSYSTEM, tenant_id, idx, exc)

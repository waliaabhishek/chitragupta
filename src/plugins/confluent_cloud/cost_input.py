from __future__ import annotations

import hashlib
import json
import logging
from collections import defaultdict
from collections.abc import Iterable, Iterator
from copy import deepcopy
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from core.models import BillingLineItem
    from core.storage.interface import UnitOfWork
    from plugins.confluent_cloud.config import CCloudPluginConfig
    from plugins.confluent_cloud.connections import CCloudConnection

from core.plugin.protocols import CostInput
from plugins.confluent_cloud.models.billing import (
    CCloudBillingLineItem,
    CCloudCostSourceRecord,
    CCloudSourceWindowWriter,
    billing_natural_key,
)

logger = logging.getLogger(__name__)
BILLING_API_PATH = "/billing/v1/costs"
BILLING_PAGE_SIZE = 2000
# Billing is inherently CCloud-specific; constant avoids parameter threading
ECOSYSTEM = "confluent_cloud"
_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
_NATIVE_DECIMAL_FIELDS = ("amount", "original_amount", "discount_amount", "price", "quantity")


@dataclass(frozen=True)
class _SourceCandidate:
    raw_payload: dict[str, Any]
    collection_window_start: datetime
    collection_window_end: datetime
    ordinal: int


def _canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _hash_identity(prefix: str, value: Any) -> str:
    digest = hashlib.sha256(_canonical_json(value).encode()).hexdigest()
    return f"{prefix}{digest}"


def _normalize_gather_bound(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("Billing gather bounds must be timezone-aware")
    utc_value = value.astimezone(UTC)
    return datetime(utc_value.year, utc_value.month, utc_value.day, tzinfo=UTC)


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
) -> CCloudBillingLineItem:
    """Map a CCloud billing API response item to CCloudBillingLineItem."""
    resource_value = item.get("resource", {})
    if not isinstance(resource_value, dict):
        raise ValueError("Invalid billing resource shape: expected object")
    resource = resource_value

    # Extract env_id - required for CCloud billing PK to prevent collisions
    environment_value = resource.get("environment", {})
    if not isinstance(environment_value, dict):
        raise ValueError("Invalid billing environment shape: expected object")
    environment = environment_value
    env_id = environment.get("id") or ""

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
) -> CCloudBillingLineItem:
    """Create a billing line from a malformed API row with best-effort field extraction."""
    resource_value = item.get("resource", {})
    resource = resource_value if isinstance(resource_value, dict) else {}
    environment_value = resource.get("environment", {})
    environment = environment_value if isinstance(environment_value, dict) else {}
    env_id = environment.get("id") or ""

    try:
        timestamp = _parse_billing_date(item.get("start_date", "1970-01-01"))
    except TypeError, ValueError:
        timestamp = _EPOCH

    return CCloudBillingLineItem(
        ecosystem=ecosystem,
        tenant_id=tenant_id,
        timestamp=timestamp,
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


def _strict_date(raw: dict[str, Any], field: str, diagnostics: list[str]) -> datetime | None:
    value = raw.get(field)
    if value is None or value == "":
        diagnostics.append(f"missing_required:{field}")
        return None
    if not isinstance(value, str):
        diagnostics.append(f"invalid_date:{field}")
        return None
    try:
        return _parse_billing_date(value)
    except ValueError:
        diagnostics.append(f"invalid_date:{field}")
        return None


def _strict_decimal(raw: dict[str, Any], field: str, diagnostics: list[str]) -> Decimal | None:
    if field not in raw or raw[field] is None or raw[field] == "":
        if field == "original_amount":
            diagnostics.append("missing_required:original_amount")
        return None
    try:
        return Decimal(str(raw[field]))
    except InvalidOperation, ValueError:
        diagnostics.append(f"invalid_decimal:{field}")
        return None


def _optional_string(raw: dict[str, Any], field: str) -> str | None:
    value = raw.get(field)
    return value if isinstance(value, str) else None


def _source_shape(
    raw: dict[str, Any], diagnostics: list[str]
) -> tuple[str | None, str | None, str | None, dict[str, str]]:
    resource_id: str | None = None
    resource_name: str | None = None
    environment_id: str | None = None
    resource = raw.get("resource")
    if resource is not None:
        if not isinstance(resource, dict):
            diagnostics.append("invalid_shape:resource")
        else:
            resource_id = resource.get("id") if isinstance(resource.get("id"), str) else None
            resource_name = resource.get("display_name") if isinstance(resource.get("display_name"), str) else None
            environment = resource.get("environment")
            if environment is not None:
                if isinstance(environment, dict):
                    environment_id = environment.get("id") if isinstance(environment.get("id"), str) else None
                else:
                    diagnostics.append("invalid_shape:environment")

    tier_dimensions: dict[str, str] = {}
    raw_tiers = raw.get("tier_dimensions")
    if raw_tiers is not None:
        if isinstance(raw_tiers, dict) and all(
            isinstance(key, str) and isinstance(value, str) for key, value in raw_tiers.items()
        ):
            tier_dimensions = dict(raw_tiers)
        else:
            diagnostics.append("invalid_shape:tier_dimensions")
    return resource_id, resource_name, environment_id, tier_dimensions


def _map_source_record(
    candidate: _SourceCandidate,
    tenant_id: str,
    source_record_id: str,
    identity_scheme: str,
    provider_cost_id: str | None,
    extra_diagnostics: tuple[str, ...] = (),
) -> CCloudCostSourceRecord:
    raw = candidate.raw_payload
    diagnostics: list[str] = []

    raw_id = raw.get("id")
    if not isinstance(raw_id, str) or not raw_id.strip():
        diagnostics.append("missing_required:id")
    line_type = _optional_string(raw, "line_type")
    unit = _optional_string(raw, "unit")
    if not unit and line_type != "PROMO_CREDIT":
        diagnostics.append("missing_required:unit")

    source_start = _strict_date(raw, "start_date", diagnostics)
    source_end = _strict_date(raw, "end_date", diagnostics)
    if source_start is not None and source_end is not None and source_end <= source_start:
        diagnostics.append("invalid_date:end_date")

    decimals = {field: _strict_decimal(raw, field, diagnostics) for field in _NATIVE_DECIMAL_FIELDS}
    resource_id, resource_name, environment_id, tier_dimensions = _source_shape(raw, diagnostics)
    diagnostics.extend(extra_diagnostics)

    if source_start is not None and source_end is not None and source_end > source_start:
        evidence_start = source_start
        evidence_end = source_end
    else:
        evidence_start = candidate.collection_window_start
        evidence_end = candidate.collection_window_end
    allocation_timestamp = source_start if source_start is not None else _EPOCH
    retention_timestamp = allocation_timestamp if source_start is not None else evidence_end

    return CCloudCostSourceRecord(
        ecosystem=ECOSYSTEM,
        tenant_id=tenant_id,
        source_record_id=source_record_id,
        identity_scheme=identity_scheme,
        provider_cost_id=provider_cost_id,
        source_period_start=source_start,
        source_period_end=source_end,
        collection_window_start=candidate.collection_window_start,
        collection_window_end=candidate.collection_window_end,
        evidence_scope_start=evidence_start,
        evidence_scope_end=evidence_end,
        allocation_timestamp=allocation_timestamp,
        retention_timestamp=retention_timestamp,
        granularity=_optional_string(raw, "granularity"),
        product=_optional_string(raw, "product"),
        line_type=line_type,
        amount=decimals["amount"],
        original_amount=decimals["original_amount"],
        discount_amount=decimals["discount_amount"],
        price=decimals["price"],
        quantity=decimals["quantity"],
        unit=unit,
        description=_optional_string(raw, "description"),
        network_access_type=_optional_string(raw, "network_access_type"),
        resource_id=resource_id,
        resource_name=resource_name,
        environment_id=environment_id,
        tier_dimensions=tier_dimensions,
        malformed=bool(diagnostics),
        diagnostics=tuple(diagnostics),
        raw_payload=deepcopy(raw),
    )


def _assign_source_records(
    candidates: list[_SourceCandidate], tenant_id: str, overall_start: datetime, overall_end: datetime
) -> list[CCloudCostSourceRecord]:
    provider_groups: defaultdict[str, list[tuple[int, _SourceCandidate]]] = defaultdict(list)
    for index, candidate in enumerate(candidates):
        provider_id = candidate.raw_payload.get("id")
        if isinstance(provider_id, str) and provider_id.strip():
            provider_groups[provider_id].append((index, candidate))

    collision_ordinals: dict[int, int] = {}
    for matching in provider_groups.values():
        if len(matching) <= 1:
            continue
        matching.sort(key=lambda pair: _canonical_json(pair[1].raw_payload))
        for ordinal, (index, _) in enumerate(matching):
            collision_ordinals[index] = ordinal

    records: list[CCloudCostSourceRecord] = []
    for index, candidate in enumerate(candidates):
        raw_id = candidate.raw_payload.get("id")
        current_provider_id = raw_id if isinstance(raw_id, str) and raw_id.strip() else None
        if current_provider_id is None:
            source_id = _hash_identity(
                "composite:v1:",
                {
                    "ecosystem": ECOSYSTEM,
                    "tenant_id": tenant_id,
                    "collection_window_start": candidate.collection_window_start.isoformat(),
                    "collection_window_end": candidate.collection_window_end.isoformat(),
                    "ordinal": candidate.ordinal,
                    "raw_payload": candidate.raw_payload,
                },
            )
            records.append(_map_source_record(candidate, tenant_id, source_id, "composite_v1", None))
        elif len(provider_groups[current_provider_id]) == 1:
            records.append(
                _map_source_record(
                    candidate,
                    tenant_id,
                    f"provider:{current_provider_id}",
                    "provider_cost_id",
                    current_provider_id,
                )
            )
        else:
            source_id = _hash_identity(
                "provider-collision:v1:",
                {
                    "ecosystem": ECOSYSTEM,
                    "tenant_id": tenant_id,
                    "overall_start": overall_start.isoformat(),
                    "overall_end": overall_end.isoformat(),
                    "provider_cost_id": current_provider_id,
                    "raw_payload": candidate.raw_payload,
                    "collision_ordinal": collision_ordinals[index],
                },
            )
            records.append(
                _map_source_record(
                    candidate,
                    tenant_id,
                    source_id,
                    "provider_id_collision_v1",
                    current_provider_id,
                    (f"duplicate_provider_id:{current_provider_id}",),
                )
            )
    return records


def _aggregate_tiers(items: list[CCloudBillingLineItem]) -> CCloudBillingLineItem:
    """Merge billing rows that share the same 7-field key but differ by pricing tier.

    Single-row groups pass through unchanged (preserving original unit_price).
    Multi-row groups sum total_cost and quantity, zero out unit_price (meaningless
    after aggregation), and stash per-tier breakdown in metadata["tiers"].
    """
    if len(items) == 1:
        return items[0]

    first = items[0]
    total_cost = sum((item.total_cost for item in items), Decimal("0"))
    total_qty = sum((item.quantity for item in items), Decimal("0"))

    tiers = [
        {"price": str(item.unit_price), "quantity": str(item.quantity), "cost": str(item.total_cost)} for item in items
    ]

    merged_metadata = dict(first.metadata)
    merged_metadata["tiers"] = tiers

    return CCloudBillingLineItem(
        ecosystem=first.ecosystem,
        tenant_id=first.tenant_id,
        timestamp=first.timestamp,
        env_id=first.env_id,
        resource_id=first.resource_id,
        product_category=first.product_category,
        product_type=first.product_type,
        quantity=total_qty,
        unit_price=Decimal("0"),
        total_cost=total_cost,
        currency=first.currency,
        granularity=first.granularity,
        metadata=merged_metadata,
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
        writer = uow.billing
        if not isinstance(writer, CCloudSourceWindowWriter):
            raise RuntimeError("CCloud billing repository does not satisfy CCloudSourceWindowWriter")

        overall_start = _normalize_gather_bound(start)
        overall_end = _normalize_gather_bound(end)
        if overall_start >= overall_end:
            return

        candidates: list[_SourceCandidate] = []
        aggregates: list[CCloudBillingLineItem] = []
        days_per_query = self._config.billing_api.days_per_query
        for window_start, window_end in _generate_date_windows(overall_start, overall_end, days_per_query):
            window_aggregates, window_candidates = self._fetch_window(tenant_id, window_start, window_end)
            aggregates.extend(window_aggregates)
            candidates.extend(window_candidates)

        source_records = _assign_source_records(candidates, tenant_id, overall_start, overall_end)
        writer.replace_source_window(ECOSYSTEM, tenant_id, overall_start, overall_end, source_records)
        yield from aggregates

    def _fetch_window(
        self,
        tenant_id: str,
        start: datetime,
        end: datetime,
    ) -> tuple[list[CCloudBillingLineItem], list[_SourceCandidate]]:
        """Fetch billing items for a single date window.

        Collects all rows per window, then aggregates rows that share the same
        7-field billing key (tiered pricing produces multiple rows per key).
        Each HTTP subwindow is grouped independently, while gather() retains all
        subwindow aggregates and source candidates for the tenant lookback interval
        until the authoritative source-window replacement succeeds.
        """
        params = {
            "start_date": start.strftime("%Y-%m-%d"),
            "end_date": end.strftime("%Y-%m-%d"),
            "page_size": BILLING_PAGE_SIZE,
        }

        groups: defaultdict[tuple[str, str, datetime, str, str, str, str], list[CCloudBillingLineItem]] = defaultdict(
            list
        )
        candidates: list[_SourceCandidate] = []
        for idx, raw_item in enumerate(self._connection.get(BILLING_API_PATH, params=params)):
            candidates.append(
                _SourceCandidate(
                    raw_payload=deepcopy(raw_item),
                    collection_window_start=start,
                    collection_window_end=end,
                    ordinal=idx,
                )
            )
            try:
                item = _map_billing_item(raw_item, ECOSYSTEM, tenant_id, row_index=idx)
            except (AttributeError, KeyError, TypeError, ValueError) as exc:
                logger.debug("Preserving malformed billing item %d as sentinel row: %s", idx, exc)
                item = _map_malformed_item(raw_item, ECOSYSTEM, tenant_id, idx, exc)
            groups[billing_natural_key(item)].append(item)

        return [_aggregate_tiers(group) for group in groups.values()], candidates

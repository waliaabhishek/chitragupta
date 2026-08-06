from __future__ import annotations

import json
import logging
import math
from collections.abc import Mapping, Sequence
from decimal import ROUND_HALF_UP, Context, Decimal, InvalidOperation, localcontext
from typing import TYPE_CHECKING, Any, cast

from core.storage.interface import (
    AllocationLineageCapture,
    AllocationLineageFact,
    AllocationTargetKind,
    LineageCaptureReason,
    LineageCaptureStatus,
)

if TYPE_CHECKING:
    from core.models.billing import BillingLineItem
    from core.models.chargeback import ChargebackRow

_RATIO_CONTEXT = Context(prec=38)
logger = logging.getLogger(__name__)


class _InvalidMetadataError(ValueError):
    pass


def _decimal_text(value: Decimal) -> str:
    text = format(value, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


def _canonical_metadata(value: Any) -> Any:
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise _InvalidMetadataError
        return {"decimal": _decimal_text(value)}
    if isinstance(value, float):
        if not math.isfinite(value):
            raise _InvalidMetadataError
        return value
    if isinstance(value, Mapping):
        if not all(isinstance(key, str) for key in value):
            raise _InvalidMetadataError
        if set(value) == {"decimal"}:
            raise _InvalidMetadataError
        return {key: _canonical_metadata(item) for key, item in value.items()}
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [_canonical_metadata(item) for item in value]
    raise _InvalidMetadataError


def _invalid(origin: BillingLineItem, reason: LineageCaptureReason) -> AllocationLineageCapture:
    logger.warning(
        "Allocation lineage capture is invalid for resource=%s product_type=%s reason=%s",
        origin.resource_id,
        origin.product_type,
        reason.value,
    )
    return AllocationLineageCapture(
        origin_timestamp=origin.timestamp,
        origin_env_id=str(getattr(origin, "env_id", "")),
        origin_resource_id=origin.resource_id,
        origin_product_type=origin.product_type,
        origin_product_category=origin.product_category,
        status=LineageCaptureStatus.INVALID,
        reason=reason,
        facts=(),
    )


def build_allocation_lineage_capture(
    *,
    origin: BillingLineItem,
    rows: tuple[ChargebackRow, ...],
) -> AllocationLineageCapture:
    """Freeze the actual allocation result for one existing billing origin."""
    if not rows:
        return _invalid(origin, LineageCaptureReason.NO_PORTIONS)
    if not origin.total_cost.is_finite() or origin.total_cost == 0:
        return _invalid(origin, LineageCaptureReason.ZERO_ORIGIN_COST)
    if not origin.quantity.is_finite():
        return _invalid(origin, LineageCaptureReason.INVALID_QUANTITY)

    prepared: list[tuple[ChargebackRow, AllocationTargetKind, str | None, Decimal, str]] = []
    for row in rows:
        if not row.amount.is_finite():
            return _invalid(origin, LineageCaptureReason.INVALID_ROW_COST)
        method_id = row.allocation_method
        if not method_id or not method_id.strip():
            return _invalid(origin, LineageCaptureReason.INVALID_METHOD)
        target = row.identity_id
        if not target.strip():
            return _invalid(origin, LineageCaptureReason.INVALID_METADATA)
        kind = (
            AllocationTargetKind.UNALLOCATED
            if target == "UNALLOCATED"
            else AllocationTargetKind.RESOURCE
            if target == origin.resource_id
            else AllocationTargetKind.IDENTITY
        )
        target_id = None if kind is AllocationTargetKind.UNALLOCATED else target
        try:
            with localcontext(_RATIO_CONTEXT):
                ratio = row.amount / origin.total_cost
        except InvalidOperation, ZeroDivisionError:
            return _invalid(origin, LineageCaptureReason.INVALID_RATIO)
        if not ratio.is_finite() or ratio < 0:
            return _invalid(origin, LineageCaptureReason.INVALID_RATIO)
        try:
            details = json.dumps(
                _canonical_metadata(
                    {
                        "allocation_detail": row.allocation_detail,
                        "metadata": row.metadata,
                        "target_kind": kind.value,
                    }
                ),
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=False,
            )
        except _InvalidMetadataError, ValueError:
            return _invalid(origin, LineageCaptureReason.INVALID_METADATA)
        prepared.append((row, kind, target_id, ratio, details))

    quantities: list[Decimal] = []
    if origin.quantity == 0:
        quantities = [origin.quantity for _ in prepared]
    else:
        try:
            with localcontext(_RATIO_CONTEXT):
                quantum = Decimal(1).scaleb(cast("int", origin.quantity.as_tuple().exponent))
                allocated = Decimal(0)
                for _row, _kind, _target_id, ratio, _details in prepared[:-1]:
                    quantity = (origin.quantity * ratio).quantize(quantum, rounding=ROUND_HALF_UP)
                    quantities.append(quantity)
                    allocated += quantity
                final_quantity = origin.quantity - allocated
        except InvalidOperation:
            return _invalid(origin, LineageCaptureReason.INVALID_QUANTITY)
        sign_mismatch = (origin.quantity > 0 and final_quantity < 0) or (origin.quantity < 0 and final_quantity > 0)
        if sign_mismatch or not final_quantity.is_finite():
            return _invalid(origin, LineageCaptureReason.INVALID_QUANTITY)
        quantities.append(final_quantity)

    facts = tuple(
        AllocationLineageFact(
            portion_ordinal=ordinal,
            target_kind=kind,
            target_id=target_id,
            allocated_cost=row.amount,
            allocated_quantity=quantities[ordinal],
            allocation_ratio=ratio,
            method_id=cast("str", row.allocation_method),
            method_version="v1",
            method_details_json=details,
        )
        for ordinal, (row, kind, target_id, ratio, details) in enumerate(prepared)
    )
    return AllocationLineageCapture(
        origin_timestamp=origin.timestamp,
        origin_env_id=str(getattr(origin, "env_id", "")),
        origin_resource_id=origin.resource_id,
        origin_product_type=origin.product_type,
        origin_product_category=origin.product_category,
        status=LineageCaptureStatus.COMPLETE,
        reason=None,
        facts=facts,
    )

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, Literal, cast, overload

from core.emitters.models import EmissionRecord
from core.models.billing import CoreBillingLineItem
from core.models.chargeback import ChargebackRow, CostType
from core.models.entity_tag import EntityTag
from core.models.identity import CoreIdentity
from core.models.pipeline import PipelineRun, PipelineState
from core.models.resource import CoreResource, ResourceStatus
from core.storage.backends.sqlmodel.base_tables import BillingTable, IdentityTable, ResourceTable
from core.storage.backends.sqlmodel.tables import (
    ChargebackDimensionTable,
    ChargebackFactTable,
    EmissionRecordTable,
    EntityTagTable,
    PipelineRunTable,
    PipelineStateTable,
)

logger = logging.getLogger(__name__)


@overload
def ensure_utc(dt: datetime) -> datetime: ...
@overload
def ensure_utc(dt: None) -> None: ...
def ensure_utc(dt: datetime | None) -> datetime | None:
    """Read-path: ensure UTC. Naive datetimes assumed UTC (for DB compatibility)."""
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


@overload
def ensure_utc_strict(dt: datetime) -> datetime: ...
@overload
def ensure_utc_strict(dt: None) -> None: ...
def ensure_utc_strict(dt: datetime | None) -> datetime | None:
    """Write-path: ensure UTC. Raises on naive datetimes."""
    if dt is None:
        return None
    if dt.tzinfo is None:
        msg = f"Naive datetime not allowed — must be UTC-aware: {dt}"
        raise ValueError(msg)
    return dt.astimezone(UTC)


def _metadata_to_json(metadata: dict[str, Any]) -> str | None:
    """Serialize metadata dict to JSON string. Empty dict → None."""
    if not metadata:
        return None
    return json.dumps(metadata, default=str)


def _json_to_metadata(json_str: str | None) -> dict[str, Any]:
    """Deserialize JSON string to metadata dict. None/empty → {}."""
    if not json_str:
        return {}
    return json.loads(json_str)  # type: ignore[no-any-return]  # json.loads returns Any; callers expect dict[str, Any]


# --- Resource ---


def resource_to_table(r: CoreResource) -> ResourceTable:
    remaining = dict(r.metadata)
    cloud = remaining.pop("cloud", None)
    region = remaining.pop("region", None)
    return ResourceTable(
        ecosystem=r.ecosystem,
        tenant_id=r.tenant_id,
        resource_id=r.resource_id,
        resource_type=r.resource_type,
        display_name=r.display_name,
        parent_id=r.parent_id,
        owner_id=r.owner_id,
        status=r.status.value,
        cloud=cloud,
        region=region,
        created_at=ensure_utc_strict(r.created_at),
        deleted_at=ensure_utc_strict(r.deleted_at),
        last_seen_at=ensure_utc_strict(r.last_seen_at),
        metadata_json=_metadata_to_json(remaining),
    )


def resource_to_domain(t: ResourceTable) -> CoreResource:
    metadata = _json_to_metadata(t.metadata_json)
    if t.cloud is not None:
        metadata["cloud"] = t.cloud
    if t.region is not None:
        metadata["region"] = t.region
    return CoreResource(
        ecosystem=t.ecosystem,
        tenant_id=t.tenant_id,
        resource_id=t.resource_id,
        resource_type=t.resource_type,
        display_name=t.display_name,
        parent_id=t.parent_id,
        owner_id=t.owner_id,
        status=ResourceStatus(t.status),
        created_at=ensure_utc(t.created_at),
        deleted_at=ensure_utc(t.deleted_at),
        last_seen_at=ensure_utc(t.last_seen_at),
        metadata=metadata,
    )


# --- Identity ---


def identity_to_table(i: CoreIdentity) -> IdentityTable:
    return IdentityTable(
        ecosystem=i.ecosystem,
        tenant_id=i.tenant_id,
        identity_id=i.identity_id,
        identity_type=i.identity_type,
        display_name=i.display_name,
        created_at=ensure_utc_strict(i.created_at),
        deleted_at=ensure_utc_strict(i.deleted_at),
        last_seen_at=ensure_utc_strict(i.last_seen_at),
        metadata_json=_metadata_to_json(i.metadata),
    )


def identity_to_domain(t: IdentityTable) -> CoreIdentity:
    return CoreIdentity(
        ecosystem=t.ecosystem,
        tenant_id=t.tenant_id,
        identity_id=t.identity_id,
        identity_type=t.identity_type,
        display_name=t.display_name,
        created_at=ensure_utc(t.created_at),
        deleted_at=ensure_utc(t.deleted_at),
        last_seen_at=ensure_utc(t.last_seen_at),
        metadata=_json_to_metadata(t.metadata_json),
    )


# --- Billing ---


def billing_to_table(b: CoreBillingLineItem) -> BillingTable:
    return BillingTable(
        ecosystem=b.ecosystem,
        tenant_id=b.tenant_id,
        timestamp=ensure_utc_strict(b.timestamp),
        resource_id=b.resource_id,
        product_type=b.product_type,
        product_category=b.product_category,
        quantity=str(b.quantity),
        unit_price=str(b.unit_price),
        total_cost=str(b.total_cost),
        currency=b.currency,
        granularity=b.granularity,
        metadata_json=_metadata_to_json(b.metadata),
    )


def billing_to_domain(t: BillingTable) -> CoreBillingLineItem:
    return CoreBillingLineItem(
        ecosystem=t.ecosystem,
        tenant_id=t.tenant_id,
        timestamp=ensure_utc(t.timestamp),
        resource_id=t.resource_id,
        product_type=t.product_type,
        product_category=t.product_category,
        quantity=Decimal(t.quantity),
        unit_price=Decimal(t.unit_price),
        total_cost=Decimal(t.total_cost),
        currency=t.currency,
        granularity=t.granularity,
        metadata=_json_to_metadata(t.metadata_json),
    )


# --- Chargeback (star schema) ---


def chargeback_to_dimension(row: ChargebackRow) -> ChargebackDimensionTable:
    return ChargebackDimensionTable(
        ecosystem=row.ecosystem,
        tenant_id=row.tenant_id,
        resource_id=row.resource_id,
        product_category=row.product_category,
        product_type=row.product_type,
        identity_id=row.identity_id,
        cost_type=row.cost_type.value,
        allocation_method=row.allocation_method,
        allocation_detail=row.allocation_detail,
        env_id=row.metadata.get("env_id", ""),
    )


def chargeback_to_fact(row: ChargebackRow, dimension_id: int) -> ChargebackFactTable:
    return ChargebackFactTable(
        timestamp=ensure_utc_strict(row.timestamp),
        dimension_id=dimension_id,
        amount=str(row.amount),
        tags_json=json.dumps(row.tags),
    )


def chargeback_to_domain(dim: ChargebackDimensionTable, fact: ChargebackFactTable) -> ChargebackRow:
    return ChargebackRow(
        ecosystem=dim.ecosystem,
        tenant_id=dim.tenant_id,
        timestamp=ensure_utc(fact.timestamp),
        resource_id=dim.resource_id,
        product_category=dim.product_category,
        product_type=dim.product_type,
        identity_id=dim.identity_id,
        cost_type=CostType(dim.cost_type),
        amount=Decimal(fact.amount),
        allocation_method=dim.allocation_method,
        allocation_detail=dim.allocation_detail,
        tags={},
        metadata={"env_id": dim.env_id} if dim.env_id else {},
        dimension_id=dim.dimension_id,
    )


# --- PipelineState ---


def pipeline_state_to_table(p: PipelineState) -> PipelineStateTable:
    return PipelineStateTable(
        ecosystem=p.ecosystem,
        tenant_id=p.tenant_id,
        tracking_date=p.tracking_date,
        billing_gathered=p.billing_gathered,
        resources_gathered=p.resources_gathered,
        chargeback_calculated=p.chargeback_calculated,
    )


def pipeline_state_to_domain(t: PipelineStateTable) -> PipelineState:
    return PipelineState(
        ecosystem=t.ecosystem,
        tenant_id=t.tenant_id,
        tracking_date=t.tracking_date,
        billing_gathered=t.billing_gathered,
        resources_gathered=t.resources_gathered,
        chargeback_calculated=t.chargeback_calculated,
    )


# --- PipelineRun ---


def pipeline_run_to_table(run: PipelineRun) -> PipelineRunTable:
    return PipelineRunTable(
        id=run.id,
        tenant_name=run.tenant_name,
        started_at=ensure_utc_strict(run.started_at),
        ended_at=ensure_utc_strict(run.ended_at),
        status=run.status,
        stage=run.stage,
        current_date=run.current_date,
        dates_gathered=run.dates_gathered,
        dates_calculated=run.dates_calculated,
        rows_written=run.rows_written,
        error_message=run.error_message,
    )


def pipeline_run_to_domain(t: PipelineRunTable) -> PipelineRun:
    return PipelineRun(
        id=t.id,
        tenant_name=t.tenant_name,
        started_at=ensure_utc(t.started_at),
        ended_at=ensure_utc(t.ended_at),
        status=cast("Literal['running', 'completed', 'failed']", t.status),
        stage=t.stage,
        current_date=t.current_date,
        dates_gathered=t.dates_gathered,
        dates_calculated=t.dates_calculated,
        rows_written=t.rows_written,
        error_message=t.error_message,
    )


# --- EntityTag ---


def entity_tag_to_domain(t: EntityTagTable) -> EntityTag:
    return EntityTag(
        tag_id=t.tag_id,
        tenant_id=t.tenant_id,
        entity_type=t.entity_type,
        entity_id=t.entity_id,
        tag_key=t.tag_key,
        tag_value=t.tag_value,
        created_by=t.created_by,
        created_at=ensure_utc(t.created_at),
    )


def emission_record_to_table(record: EmissionRecord) -> EmissionRecordTable:
    return EmissionRecordTable(
        ecosystem=record.ecosystem,
        tenant_id=record.tenant_id,
        emitter_name=record.emitter_name,
        date=record.date,
        status=record.status,
        attempt_count=record.attempt_count,
    )


def emission_record_to_domain(row: EmissionRecordTable) -> EmissionRecord:
    return EmissionRecord(
        ecosystem=row.ecosystem,
        tenant_id=row.tenant_id,
        emitter_name=row.emitter_name,
        date=row.date,
        status=row.status,
        attempt_count=row.attempt_count,
    )

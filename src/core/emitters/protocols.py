from __future__ import annotations

import logging
from collections.abc import Callable, Sequence
from datetime import date, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Literal, Protocol, runtime_checkable

if TYPE_CHECKING:
    from core.config.models import EmitterSpec
    from core.emitters.models import EmitManifest, EmitResult

logger = logging.getLogger(__name__)


@runtime_checkable
class Row(Protocol):
    """Structural protocol satisfied by all pipeline row types.

    ChargebackRow, TopicAttributionRow, BillingEmitRow, ResourceEmitRow,
    IdentityEmitRow all have these four attributes — no subclassing required.
    """

    ecosystem: str
    tenant_id: str
    timestamp: datetime
    amount: Decimal


# PEP 695 generic type alias — RowT bound to Row
type RowProvider[RowT: Row] = Callable[[str, date], Sequence[RowT]]


@runtime_checkable
class LifecycleEmitter[RowT: Row](Protocol):
    def open(self, tenant_id: str, manifest: EmitManifest) -> None: ...
    def emit(self, tenant_id: str, date: date, rows: Sequence[RowT]) -> None: ...
    def close(self, tenant_id: str) -> EmitResult: ...


@runtime_checkable
class ExpositionEmitter[RowT: Row](Protocol):
    def load(self, tenant_id: str, manifest: EmitManifest, rows: RowProvider[RowT]) -> None: ...
    def get_consumed(self, tenant_id: str) -> set[date]: ...


@runtime_checkable
class PipelineDateSource(Protocol):
    """Returns the set of candidate emission dates for a pipeline."""

    def get_distinct_dates(self, ecosystem: str, tenant_id: str) -> list[date]: ...


@runtime_checkable
class PipelineRowFetcher[RowT: Row](Protocol):
    """Fetches data rows for emission by date. All pipeline row fetchers implement this."""

    def fetch_by_date(self, ecosystem: str, tenant_id: str, dt: date) -> list[RowT]: ...


@runtime_checkable
class PipelineAggregatedRowFetcher[RowT: Row](PipelineRowFetcher[RowT], Protocol):
    """Extends PipelineRowFetcher with aggregated range fetch. Chargeback only."""

    def fetch_aggregated(
        self,
        ecosystem: str,
        tenant_id: str,
        start: date,
        end: date,
        granularity: Literal["daily", "monthly"],
    ) -> list[RowT]: ...


@runtime_checkable
class PipelineEmitterBuilder(Protocol):
    """Constructs an emitter (any variant) from an EmitterSpec."""

    def build(self, spec: EmitterSpec) -> Any: ...

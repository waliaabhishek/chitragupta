"""Provider contracts for ecosystem-specific topic-attribution strategies."""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from core.engine.topic_attribution_models import TopicAttributionOutputConfigProtocol
    from core.models.billing import BillingLineItem
    from core.models.topic_attribution import TopicAttributionRow


@dataclass(frozen=True)
class TopicAttributionClusterOutcome:
    """Terminal rows and retryable billing lines for one logical cluster."""

    rows: tuple[TopicAttributionRow, ...]
    retry_lines: tuple[BillingLineItem, ...] = ()


@runtime_checkable
class TopicAttributionProvider(Protocol):
    """Ecosystem-owned topic evidence and allocation strategy."""

    @property
    def config(self) -> TopicAttributionOutputConfigProtocol: ...

    @property
    def supported_product_types(self) -> frozenset[str]: ...

    @property
    def replace_date_on_completion(self) -> bool: ...

    def attribute_cluster(
        self,
        *,
        tenant_id: str,
        cluster_resource_id: str,
        env_id: str,
        billing_lines: Sequence[BillingLineItem],
        resource_topics: frozenset[str],
        metrics_step: timedelta,
    ) -> TopicAttributionClusterOutcome: ...


@runtime_checkable
class ChunkedTopicEvidenceProvider(Protocol):
    """Optional bounded historical evidence capability."""

    def iter_evidence_chunks(
        self,
        windows: Sequence[tuple[datetime, datetime]],
    ) -> Iterable[tuple[tuple[datetime, datetime], ...]]: ...

    def prepare_evidence_chunk(
        self,
        windows: Sequence[tuple[datetime, datetime]],
        metrics_step: timedelta,
    ) -> None: ...

    def clear_evidence_chunk(self) -> None: ...

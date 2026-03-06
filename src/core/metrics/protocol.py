from __future__ import annotations

import logging
from collections.abc import Sequence
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from core.models.metrics import MetricQuery, MetricRow
logger = logging.getLogger(__name__)


class MetricsQueryError(Exception):
    """Raised when a metrics query fails after retries or with a non-transient error."""

    def __init__(
        self,
        message: str,
        query: str | None = None,
        status_code: int | None = None,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.query = query
        self.status_code = status_code


@runtime_checkable
class MetricsSource(Protocol):
    """Protocol for metrics backends (Prometheus, Datadog, custom SQL, etc.)."""

    def query(
        self,
        queries: Sequence[MetricQuery],
        start: datetime,
        end: datetime,
        step: timedelta = timedelta(hours=1),
        resource_id_filter: str | None = None,
    ) -> dict[str, list[MetricRow]]:
        """Execute multiple metric queries, return results keyed by MetricQuery.key.

        Args:
            queries: metric definitions to execute
            start: range start (inclusive)
            end: range end (inclusive)
            step: resolution step for range queries (default 1h)
            resource_id_filter: optional label filter injected into queries
        """
        ...

    def close(self) -> None: ...

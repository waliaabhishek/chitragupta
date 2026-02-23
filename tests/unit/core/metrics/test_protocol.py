from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime, timedelta
from typing import TYPE_CHECKING

from core.metrics.protocol import MetricsQueryError, MetricsSource

if TYPE_CHECKING:
    from core.models.metrics import MetricQuery, MetricRow


class TestMetricsSourceProtocol:
    def test_is_runtime_checkable(self) -> None:
        assert hasattr(MetricsSource, "__protocol_attrs__") or callable(
            getattr(MetricsSource, "_is_runtime_protocol", None)
        )

        # The real test: isinstance() works
        class _Good:
            def query(
                self,
                queries: Sequence[MetricQuery],
                start: datetime,
                end: datetime,
                step: timedelta = timedelta(hours=1),
                resource_id_filter: str | None = None,
            ) -> dict[str, list[MetricRow]]:
                return {}

        assert isinstance(_Good(), MetricsSource)

    def test_conforming_class_passes_isinstance(self) -> None:
        class Conforming:
            def query(
                self,
                queries: Sequence[MetricQuery],
                start: datetime,
                end: datetime,
                step: timedelta = timedelta(hours=1),
                resource_id_filter: str | None = None,
            ) -> dict[str, list[MetricRow]]:
                return {}

        assert isinstance(Conforming(), MetricsSource)

    def test_non_conforming_class_fails_isinstance(self) -> None:
        class NonConforming:
            pass

        assert not isinstance(NonConforming(), MetricsSource)


class TestMetricsQueryError:
    def test_attributes(self) -> None:
        err = MetricsQueryError("boom", query="up{}", status_code=503)
        assert str(err) == "boom"
        assert err.message == "boom"
        assert err.query == "up{}"
        assert err.status_code == 503

    def test_defaults(self) -> None:
        err = MetricsQueryError("fail")
        assert err.query is None
        assert err.status_code is None

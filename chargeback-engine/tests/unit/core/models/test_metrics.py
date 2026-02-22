from __future__ import annotations

from dataclasses import asdict
from datetime import UTC, datetime

import pytest

from core.models.metrics import MetricQuery, MetricRow

_NOW = datetime(2024, 1, 15, 12, 0, 0, tzinfo=UTC)


class TestMetricQuery:
    def test_construction(self) -> None:
        q = MetricQuery(
            key="kafka_bytes_in",
            query_expression="sum(rate(kafka_bytes_in_total[5m])) by (cluster_id)",
            label_keys=("cluster_id", "topic"),
            resource_label="cluster_id",
            metadata={"source": "prometheus"},
        )
        assert q.key == "kafka_bytes_in"
        assert q.label_keys == ("cluster_id", "topic")
        assert q.resource_label == "cluster_id"
        assert q.metadata == {"source": "prometheus"}

    def test_defaults(self) -> None:
        q = MetricQuery(
            key="test",
            query_expression="up",
            label_keys=(),
            resource_label="instance",
        )
        assert q.metadata == {}

    def test_frozen_enforcement(self) -> None:
        q = MetricQuery(
            key="test",
            query_expression="up",
            label_keys=(),
            resource_label="instance",
        )
        with pytest.raises(AttributeError):
            q.key = "changed"  # type: ignore[misc] -- intentional write to frozen field

    def test_label_keys_is_tuple(self) -> None:
        q = MetricQuery(
            key="test",
            query_expression="up",
            label_keys=("a", "b"),
            resource_label="a",
        )
        assert isinstance(q.label_keys, tuple)

    def test_asdict_round_trip(self) -> None:
        q = MetricQuery(
            key="kafka_bytes_in",
            query_expression="sum(rate(x[5m]))",
            label_keys=("cluster_id",),
            resource_label="cluster_id",
        )
        d = asdict(q)
        # label_keys comes back as list from asdict, convert back to tuple
        d["label_keys"] = tuple(d["label_keys"])
        q2 = MetricQuery(**d)
        assert q == q2


class TestMetricRow:
    def test_construction(self) -> None:
        r = MetricRow(
            timestamp=_NOW,
            metric_key="kafka_bytes_in",
            value=1234.5,
            labels={"cluster_id": "lkc-abc", "topic": "orders"},
        )
        assert r.timestamp == _NOW
        assert r.metric_key == "kafka_bytes_in"
        assert r.value == 1234.5
        assert r.labels == {"cluster_id": "lkc-abc", "topic": "orders"}

    def test_defaults(self) -> None:
        r = MetricRow(
            timestamp=_NOW,
            metric_key="test",
            value=0.0,
        )
        assert r.labels == {}

    def test_frozen_enforcement(self) -> None:
        r = MetricRow(
            timestamp=_NOW,
            metric_key="test",
            value=1.0,
        )
        with pytest.raises(AttributeError):
            r.value = 2.0  # type: ignore[misc] -- intentional write to frozen field

    def test_asdict_round_trip(self) -> None:
        r = MetricRow(
            timestamp=_NOW,
            metric_key="test",
            value=42.0,
            labels={"env": "prod"},
        )
        d = asdict(r)
        r2 = MetricRow(**d)
        assert r == r2

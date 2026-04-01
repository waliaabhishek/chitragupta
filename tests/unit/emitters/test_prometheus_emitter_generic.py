from __future__ import annotations

import contextlib
from collections.abc import Generator
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any, ClassVar
from unittest.mock import Mock

import pytest

TENANT_ID = "t1"
ECOSYSTEM = "aws"
DATE = date(2024, 1, 15)
DATE_TS = datetime(2024, 1, 15, 0, 0, 0, tzinfo=UTC).timestamp()


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def reset_prometheus_emitter_state() -> Generator[None]:
    yield
    try:
        from prometheus_client import REGISTRY

        from emitters.prometheus_emitter import PrometheusEmitter

        for col in list(PrometheusEmitter._collectors.values()):
            with contextlib.suppress(Exception):
                REGISTRY.unregister(col)
        PrometheusEmitter._collectors.clear()
        PrometheusEmitter._server_started = False
    except Exception:
        pass


@pytest.fixture(autouse=True)
def mock_http_server(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("emitters.prometheus_emitter.start_http_server", lambda port: None)


@pytest.fixture()
def mock_registry(monkeypatch: pytest.MonkeyPatch) -> Any:
    """Patch prometheus REGISTRY with a fresh instance to avoid registration conflicts."""
    from prometheus_client import CollectorRegistry

    fresh = CollectorRegistry()
    monkeypatch.setattr("emitters.prometheus_emitter.REGISTRY", fresh)
    return fresh


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_chargeback_row(
    *,
    ecosystem: str = ECOSYSTEM,
    resource_id: str | None = None,
    identity_id: str = "u1",
    product_type: str = "ec2",
    amount: Decimal = Decimal("7.50"),
    allocation_method: str | None = None,
) -> Any:
    from core.models.chargeback import ChargebackRow, CostType

    return ChargebackRow(
        ecosystem=ecosystem,
        tenant_id=TENANT_ID,
        timestamp=datetime(2024, 1, 15, 0, 0, 0, tzinfo=UTC),
        resource_id=resource_id,
        product_category="compute",
        product_type=product_type,
        identity_id=identity_id,
        cost_type=CostType.USAGE,
        amount=amount,
        allocation_method=allocation_method,
        allocation_detail=None,
    )


def _make_ta_row(
    *,
    ecosystem: str = ECOSYSTEM,
    env_id: str = "e1",
    cluster_resource_id: str = "lkc-xyz",
    topic_name: str = "events",
    product_category: str = "kafka",
    product_type: str = "throughput",
    attribution_method: str = "proportional",
    amount: Decimal = Decimal("3.00"),
) -> Any:
    from core.models.topic_attribution import TopicAttributionRow

    return TopicAttributionRow(
        ecosystem=ecosystem,
        tenant_id=TENANT_ID,
        timestamp=datetime(2024, 1, 15, 0, 0, 0, tzinfo=UTC),
        env_id=env_id,
        cluster_resource_id=cluster_resource_id,
        topic_name=topic_name,
        product_category=product_category,
        product_type=product_type,
        attribution_method=attribution_method,
        amount=amount,
    )


# ---------------------------------------------------------------------------
# Constructor signature — no storage_backend
# ---------------------------------------------------------------------------


class TestPrometheusEmitterConstructor:
    def test_constructor_takes_only_port(self) -> None:
        import inspect

        from emitters.prometheus_emitter import PrometheusEmitter

        sig = inspect.signature(PrometheusEmitter.__init__)
        assert "storage_backend" not in sig.parameters

    def test_constructor_with_port_only(self) -> None:
        from emitters.prometheus_emitter import PrometheusEmitter

        emitter = PrometheusEmitter(port=8001)
        assert emitter._port == 8001

    def test_default_port_is_8000(self) -> None:
        import inspect

        from emitters.prometheus_emitter import PrometheusEmitter

        sig = inspect.signature(PrometheusEmitter.__init__)
        assert sig.parameters["port"].default == 8000

    def test_make_prometheus_emitter_no_longer_has_needs_storage_backend(self) -> None:
        from emitters.prometheus_emitter import make_prometheus_emitter

        assert getattr(make_prometheus_emitter, "needs_storage_backend", False) is False

    def test_make_prometheus_emitter_factory_returns_instance(self) -> None:
        from emitters.prometheus_emitter import PrometheusEmitter, make_prometheus_emitter

        instance = make_prometheus_emitter(port=8002)
        assert isinstance(instance, PrometheusEmitter)


# ---------------------------------------------------------------------------
# Verification 5: Prometheus chargeback metric preserved
# ---------------------------------------------------------------------------


class TestPrometheusEmitterChargebackMetric:
    """Verification test 5 from design doc."""

    def test_chargeback_metric_name_and_labels(self, mock_registry: Any) -> None:
        from emitters.prometheus_emitter import PrometheusEmitter

        emitter = PrometheusEmitter.__new__(PrometheusEmitter)
        rows = [
            _make_chargeback_row(
                resource_id=None,
                identity_id="u1",
                product_type="ec2",
                amount=Decimal("7.50"),
            )
        ]
        emitter(TENANT_ID, DATE, rows)

        collector = PrometheusEmitter._collectors["chitragupta_chargeback_amount"]
        samples = collector._samples_by_tenant[TENANT_ID]
        assert samples[0][0] == [TENANT_ID, ECOSYSTEM, "u1", "", "ec2", "usage", ""]
        assert samples[0][1] == 7.50

    def test_chargeback_sample_timestamp_is_midnight_utc(self, mock_registry: Any) -> None:
        from emitters.prometheus_emitter import PrometheusEmitter

        emitter = PrometheusEmitter.__new__(PrometheusEmitter)
        emitter(TENANT_ID, DATE, [_make_chargeback_row()])

        collector = PrometheusEmitter._collectors["chitragupta_chargeback_amount"]
        samples = collector._samples_by_tenant[TENANT_ID]
        assert samples[0][2] == DATE_TS

    def test_chargeback_collector_keyed_by_metric_name(self, mock_registry: Any) -> None:
        from emitters.prometheus_emitter import PrometheusEmitter

        emitter = PrometheusEmitter.__new__(PrometheusEmitter)
        emitter(TENANT_ID, DATE, [_make_chargeback_row()])

        assert "chitragupta_chargeback_amount" in PrometheusEmitter._collectors

    def test_chargeback_resource_id_none_serializes_to_empty_string(self, mock_registry: Any) -> None:
        from emitters.prometheus_emitter import PrometheusEmitter

        emitter = PrometheusEmitter.__new__(PrometheusEmitter)
        emitter(TENANT_ID, DATE, [_make_chargeback_row(resource_id=None)])

        collector = PrometheusEmitter._collectors["chitragupta_chargeback_amount"]
        labels = collector._samples_by_tenant[TENANT_ID][0][0]
        # resource_id is 4th label (index 3)
        assert labels[3] == ""

    def test_chargeback_cost_type_strenum_serializes_to_str(self, mock_registry: Any) -> None:
        from emitters.prometheus_emitter import PrometheusEmitter

        emitter = PrometheusEmitter.__new__(PrometheusEmitter)
        emitter(TENANT_ID, DATE, [_make_chargeback_row()])

        collector = PrometheusEmitter._collectors["chitragupta_chargeback_amount"]
        labels = collector._samples_by_tenant[TENANT_ID][0][0]
        # cost_type is "usage" (str representation of StrEnum)
        assert "usage" in labels


# ---------------------------------------------------------------------------
# Verification 6: Prometheus topic attribution metric preserved
# ---------------------------------------------------------------------------


class TestPrometheusEmitterTopicAttributionMetric:
    """Verification test 6 from design doc."""

    def test_topic_attribution_metric_name_and_labels(self, mock_registry: Any) -> None:
        from emitters.prometheus_emitter import PrometheusEmitter

        emitter = PrometheusEmitter.__new__(PrometheusEmitter)
        rows = [
            _make_ta_row(
                env_id="e1",
                cluster_resource_id="lkc-xyz",
                topic_name="events",
                product_category="kafka",
                product_type="throughput",
                attribution_method="proportional",
                amount=Decimal("3.00"),
            )
        ]
        emitter(TENANT_ID, DATE, rows)

        collector = PrometheusEmitter._collectors["chitragupta_topic_attribution_amount"]
        samples = collector._samples_by_tenant[TENANT_ID]
        assert samples[0][0] == [TENANT_ID, ECOSYSTEM, "e1", "lkc-xyz", "events", "kafka", "throughput", "proportional"]

    def test_topic_attribution_collector_keyed_by_metric_name(self, mock_registry: Any) -> None:
        from emitters.prometheus_emitter import PrometheusEmitter

        emitter = PrometheusEmitter.__new__(PrometheusEmitter)
        emitter(TENANT_ID, DATE, [_make_ta_row()])

        assert "chitragupta_topic_attribution_amount" in PrometheusEmitter._collectors

    def test_topic_attribution_amount_value(self, mock_registry: Any) -> None:
        from emitters.prometheus_emitter import PrometheusEmitter

        emitter = PrometheusEmitter.__new__(PrometheusEmitter)
        emitter(TENANT_ID, DATE, [_make_ta_row(amount=Decimal("3.00"))])

        collector = PrometheusEmitter._collectors["chitragupta_topic_attribution_amount"]
        value = collector._samples_by_tenant[TENANT_ID][0][1]
        assert value == 3.0


# ---------------------------------------------------------------------------
# Generic collectors dict — not per-type class attributes
# ---------------------------------------------------------------------------


class TestPrometheusEmitterCollectorsDict:
    def test_collectors_is_class_level_dict(self) -> None:
        from emitters.prometheus_emitter import PrometheusEmitter

        assert isinstance(PrometheusEmitter._collectors, dict)

    def test_no_hardcoded_chargeback_col_attribute(self) -> None:
        from emitters.prometheus_emitter import PrometheusEmitter

        assert not hasattr(PrometheusEmitter, "_chargeback_col"), (
            "_chargeback_col must not exist — use _collectors dict"
        )

    def test_no_hardcoded_billing_col_attribute(self) -> None:
        from emitters.prometheus_emitter import PrometheusEmitter

        assert not hasattr(PrometheusEmitter, "_billing_col")

    def test_no_emit_topic_attributions_method(self) -> None:
        from emitters.prometheus_emitter import PrometheusEmitter

        assert not hasattr(PrometheusEmitter, "emit_topic_attributions"), (
            "emit_topic_attributions must be deleted — use generic __call__"
        )


# ---------------------------------------------------------------------------
# Edge case: no-op for rows with empty __prometheus_metrics__
# ---------------------------------------------------------------------------


class TestPrometheusEmitterNoOp:
    def test_empty_rows_is_noop(self, mock_registry: Any) -> None:
        from emitters.prometheus_emitter import PrometheusEmitter

        emitter = PrometheusEmitter.__new__(PrometheusEmitter)
        emitter(TENANT_ID, DATE, [])  # must not raise or create collectors
        assert len(PrometheusEmitter._collectors) == 0

    def test_row_type_with_empty_prometheus_metrics_is_noop(self, mock_registry: Any) -> None:
        from core.models.emit_descriptors import MetricDescriptor  # noqa: TC001
        from emitters.prometheus_emitter import PrometheusEmitter

        @dataclass
        class NoMetricRow:
            ecosystem: str
            tenant_id: str
            timestamp: datetime
            amount: Decimal
            __csv_fields__: ClassVar[tuple[str, ...]] = ()
            __prometheus_metrics__: ClassVar[tuple[MetricDescriptor, ...]] = ()

        emitter = PrometheusEmitter.__new__(PrometheusEmitter)
        rows = [
            NoMetricRow(
                ecosystem=ECOSYSTEM,
                tenant_id=TENANT_ID,
                timestamp=datetime(2024, 1, 15, tzinfo=UTC),
                amount=Decimal("1.00"),
            )
        ]
        emitter(TENANT_ID, DATE, rows)
        assert len(PrometheusEmitter._collectors) == 0


# ---------------------------------------------------------------------------
# Zero-code extensibility: new pipeline row type works without emitter changes
# ---------------------------------------------------------------------------


class TestPrometheusEmitterZeroCodeExtensibility:
    """Verification test 11 (Prometheus side) from design doc."""

    def test_new_pipeline_row_type_works_with_prometheus_emitter(self, mock_registry: Any) -> None:
        from core.models.emit_descriptors import MetricDescriptor  # noqa: TC001
        from emitters.prometheus_emitter import PrometheusEmitter

        @dataclass
        class NewPipelineRow:
            ecosystem: str
            tenant_id: str
            timestamp: datetime
            amount: Decimal
            some_field: str
            __csv_fields__: ClassVar[tuple[str, ...]] = ("ecosystem", "tenant_id", "timestamp", "some_field", "amount")
            __prometheus_metrics__: ClassVar[tuple[MetricDescriptor, ...]] = (
                MetricDescriptor(
                    name="chitragupta_new_metric",
                    value_field="amount",
                    label_fields=("tenant_id", "ecosystem", "some_field"),
                ),
            )

        rows = [
            NewPipelineRow(
                ecosystem=ECOSYSTEM,
                tenant_id=TENANT_ID,
                timestamp=datetime(2024, 1, 15, tzinfo=UTC),
                amount=Decimal("42.00"),
                some_field="widget",
            )
        ]
        emitter = PrometheusEmitter.__new__(PrometheusEmitter)
        emitter(TENANT_ID, DATE, rows)

        assert "chitragupta_new_metric" in PrometheusEmitter._collectors
        collector = PrometheusEmitter._collectors["chitragupta_new_metric"]
        samples = collector._samples_by_tenant[TENANT_ID]
        assert samples[0][0] == [TENANT_ID, ECOSYSTEM, "widget"]
        assert samples[0][1] == 42.0


# ---------------------------------------------------------------------------
# Idempotency
# ---------------------------------------------------------------------------


class TestPrometheusEmitterIdempotency:
    def test_second_call_replaces_first(self, mock_registry: Any) -> None:
        from emitters.prometheus_emitter import PrometheusEmitter

        emitter = PrometheusEmitter.__new__(PrometheusEmitter)
        emitter(
            TENANT_ID,
            DATE,
            [_make_chargeback_row(amount=Decimal("10.00")), _make_chargeback_row(amount=Decimal("20.00"))],
        )
        emitter(TENANT_ID, DATE, [_make_chargeback_row(amount=Decimal("5.00"))])

        collector = PrometheusEmitter._collectors["chitragupta_chargeback_amount"]
        assert len(collector._samples_by_tenant[TENANT_ID]) == 1


# ---------------------------------------------------------------------------
# Server singleton
# ---------------------------------------------------------------------------


class TestPrometheusEmitterServerSingleton:
    def test_server_started_once_for_two_instances(self, monkeypatch: pytest.MonkeyPatch) -> None:
        mock_start = Mock()
        monkeypatch.setattr("emitters.prometheus_emitter.start_http_server", mock_start)

        from emitters.prometheus_emitter import PrometheusEmitter

        PrometheusEmitter(port=9090)
        PrometheusEmitter(port=9090)

        mock_start.assert_called_once()

from __future__ import annotations

import contextlib
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, Mock

import pytest

ECOSYSTEM = "test-eco"
TENANT_ID = "tenant-1"
DATE = date(2024, 1, 15)
DATE_TS = datetime(2024, 1, 15, 0, 0, 0, tzinfo=UTC).timestamp()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_row(
    *,
    ecosystem: str = ECOSYSTEM,
    identity_id: str = "user-1",
    resource_id: str | None = "cluster-1",
    product_type: str = "KAFKA_CKU",
    product_category: str = "kafka",
    amount: Decimal = Decimal("10.00"),
    allocation_method: str = "even",
    allocation_detail: str | None = "even_split",
) -> Any:
    from core.models.chargeback import ChargebackRow, CostType

    return ChargebackRow(
        ecosystem=ecosystem,
        tenant_id=TENANT_ID,
        timestamp=datetime(2024, 1, 15, 0, 0, 0, tzinfo=UTC),
        resource_id=resource_id,
        product_category=product_category,
        product_type=product_type,
        identity_id=identity_id,
        cost_type=CostType.USAGE,
        amount=amount,
        allocation_method=allocation_method,
        allocation_detail=allocation_detail,
    )


def _make_billing_line(
    *,
    ecosystem: str = ECOSYSTEM,
    resource_id: str = "cluster-1",
    product_type: str = "KAFKA_CKU",
    product_category: str = "kafka",
    total_cost: Decimal = Decimal("50.00"),
) -> Any:
    from core.models.billing import CoreBillingLineItem

    return CoreBillingLineItem(
        ecosystem=ecosystem,
        tenant_id=TENANT_ID,
        timestamp=datetime(2024, 1, 15, 0, 0, 0, tzinfo=UTC),
        resource_id=resource_id,
        product_category=product_category,
        product_type=product_type,
        quantity=Decimal("1"),
        unit_price=total_cost,
        total_cost=total_cost,
    )


def _make_resource(
    *,
    ecosystem: str = ECOSYSTEM,
    resource_id: str = "cluster-1",
    resource_type: str = "kafka_cluster",
) -> Any:
    from core.models.resource import CoreResource

    return CoreResource(
        ecosystem=ecosystem,
        tenant_id=TENANT_ID,
        resource_id=resource_id,
        resource_type=resource_type,
        last_seen_at=datetime(2024, 1, 15, 0, 0, 0, tzinfo=UTC),
    )


def _make_identity(
    *,
    ecosystem: str = ECOSYSTEM,
    identity_id: str = "user-1",
    identity_type: str = "service_account",
) -> Any:
    from core.models.identity import CoreIdentity

    return CoreIdentity(
        ecosystem=ecosystem,
        tenant_id=TENANT_ID,
        identity_id=identity_id,
        identity_type=identity_type,
        last_seen_at=datetime(2024, 1, 15, 0, 0, 0, tzinfo=UTC),
    )


def _mock_storage_backend(
    billing_lines: list[Any] | None = None,
    resources: list[Any] | None = None,
    identities: list[Any] | None = None,
) -> tuple[Any, Any]:
    uow = MagicMock()
    uow.billing.find_by_date.return_value = billing_lines or []
    uow.resources.find_active_at.return_value = (resources or [], len(resources or []))
    uow.identities.find_active_at.return_value = (identities or [], len(identities or []))
    sb = MagicMock()
    sb.create_unit_of_work.return_value.__enter__ = MagicMock(return_value=uow)
    sb.create_unit_of_work.return_value.__exit__ = MagicMock(return_value=False)
    return sb, uow


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def reset_prometheus_emitter_state() -> Any:
    yield
    from prometheus_client import REGISTRY

    from emitters.prometheus_emitter import PrometheusEmitter

    for attr in ("_chargeback_col", "_billing_col", "_resource_col", "_identity_col"):
        col = getattr(PrometheusEmitter, attr)
        if col is not None:
            with contextlib.suppress(Exception):
                REGISTRY.unregister(col)
        setattr(PrometheusEmitter, attr, None)
    PrometheusEmitter._server_started = False


@pytest.fixture(autouse=True)
def mock_http_server(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("emitters.prometheus_emitter.start_http_server", lambda port: None)


# ---------------------------------------------------------------------------
# AC #1 — Protocol compliance
# ---------------------------------------------------------------------------


class TestPrometheusEmitterProtocol:
    def test_prometheus_emitter_is_instance_of_emitter_protocol(self) -> None:
        from core.plugin.protocols import Emitter
        from emitters.prometheus_emitter import PrometheusEmitter

        sb, _ = _mock_storage_backend()
        instance = PrometheusEmitter(port=9090, storage_backend=sb)
        assert isinstance(instance, Emitter)


# ---------------------------------------------------------------------------
# AC #2 — Chargeback metric labels and value
# ---------------------------------------------------------------------------


class TestChargebackMetric:
    def test_chargeback_sample_labels_and_value(self) -> None:
        from core.models.chargeback import CostType
        from emitters.prometheus_emitter import PrometheusEmitter

        row = _make_row(
            identity_id="user-1",
            resource_id="cluster-1",
            product_type="KAFKA_CKU",
            amount=Decimal("10.00"),
            allocation_method="even",
        )
        sb, _ = _mock_storage_backend()
        emitter = PrometheusEmitter(port=9090, storage_backend=sb)
        emitter(TENANT_ID, DATE, [row])

        samples = PrometheusEmitter._chargeback_col._samples_by_tenant[TENANT_ID]
        assert len(samples) == 1
        labels, value, ts = samples[0]
        assert labels == [
            TENANT_ID,
            ECOSYSTEM,
            "user-1",
            "cluster-1",
            "KAFKA_CKU",
            str(CostType.USAGE),
            "even",
        ]
        assert value == float(Decimal("10.00"))
        assert ts == DATE_TS

    def test_chargeback_sample_null_resource_id(self) -> None:
        from emitters.prometheus_emitter import PrometheusEmitter

        row = _make_row(resource_id=None, amount=Decimal("5.00"))
        sb, _ = _mock_storage_backend()
        emitter = PrometheusEmitter(port=9090, storage_backend=sb)
        emitter(TENANT_ID, DATE, [row])

        samples = PrometheusEmitter._chargeback_col._samples_by_tenant[TENANT_ID]
        assert len(samples) == 1
        labels, value, _ts = samples[0]
        assert labels[3] == ""  # resource_id label empty string for None


# ---------------------------------------------------------------------------
# AC #3 — Billing metric
# ---------------------------------------------------------------------------


class TestBillingMetric:
    def test_billing_samples_count_and_values(self) -> None:
        from emitters.prometheus_emitter import PrometheusEmitter

        line1 = _make_billing_line(resource_id="cluster-1", product_type="KAFKA_CKU", total_cost=Decimal("50.00"))
        line2 = _make_billing_line(resource_id="cluster-2", product_type="KAFKA_STORAGE", total_cost=Decimal("20.00"))
        sb, _ = _mock_storage_backend(billing_lines=[line1, line2])
        emitter = PrometheusEmitter(port=9090, storage_backend=sb)
        emitter(TENANT_ID, DATE, [_make_row()])

        samples = PrometheusEmitter._billing_col._samples_by_tenant[TENANT_ID]
        assert len(samples) == 2

    def test_billing_sample_label_and_value(self) -> None:
        from emitters.prometheus_emitter import PrometheusEmitter

        line = _make_billing_line(
            resource_id="cluster-1",
            product_type="KAFKA_CKU",
            product_category="kafka",
            total_cost=Decimal("50.00"),
        )
        sb, _ = _mock_storage_backend(billing_lines=[line])
        emitter = PrometheusEmitter(port=9090, storage_backend=sb)
        emitter(TENANT_ID, DATE, [_make_row()])

        samples = PrometheusEmitter._billing_col._samples_by_tenant[TENANT_ID]
        assert len(samples) == 1
        labels, value, ts = samples[0]
        assert ECOSYSTEM in labels
        assert "cluster-1" in labels
        assert "KAFKA_CKU" in labels
        assert value == float(Decimal("50.00"))
        assert ts == DATE_TS


# ---------------------------------------------------------------------------
# AC #4 — Resource presence metric
# ---------------------------------------------------------------------------


class TestResourceMetric:
    def test_resource_samples_count_and_value(self) -> None:
        from emitters.prometheus_emitter import PrometheusEmitter

        resources = [
            _make_resource(resource_id="cluster-1", resource_type="kafka_cluster"),
            _make_resource(resource_id="cluster-2", resource_type="kafka_cluster"),
            _make_resource(resource_id="sr-1", resource_type="schema_registry"),
        ]
        sb, _ = _mock_storage_backend(resources=resources)
        emitter = PrometheusEmitter(port=9090, storage_backend=sb)
        emitter(TENANT_ID, DATE, [_make_row()])

        samples = PrometheusEmitter._resource_col._samples_by_tenant[TENANT_ID]
        assert len(samples) == 3
        for _, value, _ in samples:
            assert value == 1.0


# ---------------------------------------------------------------------------
# AC #5 — Identity presence metric
# ---------------------------------------------------------------------------


class TestIdentityMetric:
    def test_identity_samples_count_and_value(self) -> None:
        from emitters.prometheus_emitter import PrometheusEmitter

        identities = [
            _make_identity(identity_id="user-1", identity_type="user"),
            _make_identity(identity_id="sa-1", identity_type="service_account"),
        ]
        sb, _ = _mock_storage_backend(identities=identities)
        emitter = PrometheusEmitter(port=9090, storage_backend=sb)
        emitter(TENANT_ID, DATE, [_make_row()])

        samples = PrometheusEmitter._identity_col._samples_by_tenant[TENANT_ID]
        assert len(samples) == 2
        for _, value, _ in samples:
            assert value == 1.0


# ---------------------------------------------------------------------------
# AC #6 — Timestamped samples
# ---------------------------------------------------------------------------


class TestTimestampedSamples:
    def test_all_collectors_use_midnight_utc_timestamp(self) -> None:
        from emitters.prometheus_emitter import PrometheusEmitter

        billing_lines = [_make_billing_line()]
        resources = [_make_resource()]
        identities = [_make_identity()]
        sb, _ = _mock_storage_backend(billing_lines=billing_lines, resources=resources, identities=identities)
        emitter = PrometheusEmitter(port=9090, storage_backend=sb)
        emitter(TENANT_ID, DATE, [_make_row()])

        for attr in ("_chargeback_col", "_billing_col", "_resource_col", "_identity_col"):
            col = getattr(PrometheusEmitter, attr)
            samples = col._samples_by_tenant[TENANT_ID]
            for _, _, ts in samples:
                assert ts == DATE_TS, f"Expected {DATE_TS} but got {ts} in {attr}"


# ---------------------------------------------------------------------------
# AC #7 — Idempotent re-emit
# ---------------------------------------------------------------------------


class TestIdempotentReEmit:
    def test_second_emit_replaces_first(self) -> None:
        from emitters.prometheus_emitter import PrometheusEmitter

        sb, _ = _mock_storage_backend()
        emitter = PrometheusEmitter(port=9090, storage_backend=sb)

        rows_v1 = [_make_row(identity_id="user-1"), _make_row(identity_id="user-2")]
        rows_v2 = [_make_row(identity_id="user-3")]

        emitter(TENANT_ID, DATE, rows_v1)
        emitter(TENANT_ID, DATE, rows_v2)

        samples = PrometheusEmitter._chargeback_col._samples_by_tenant[TENANT_ID]
        assert len(samples) == len(rows_v2)


# ---------------------------------------------------------------------------
# AC #8 — Server singleton (single instance)
# ---------------------------------------------------------------------------


class TestServerSingleton:
    def test_server_started_exactly_once_for_two_instances(self, monkeypatch: pytest.MonkeyPatch) -> None:
        mock_start = Mock()
        monkeypatch.setattr("emitters.prometheus_emitter.start_http_server", mock_start)

        from emitters.prometheus_emitter import PrometheusEmitter

        sb, _ = _mock_storage_backend()
        PrometheusEmitter(port=9090, storage_backend=sb)
        PrometheusEmitter(port=9090, storage_backend=sb)

        mock_start.assert_called_once()


# ---------------------------------------------------------------------------
# AC #9 — Multi-tenant no double server
# ---------------------------------------------------------------------------


class TestMultiTenantSingleton:
    def test_three_instances_start_server_once(self, monkeypatch: pytest.MonkeyPatch) -> None:
        mock_start = Mock()
        monkeypatch.setattr("emitters.prometheus_emitter.start_http_server", mock_start)

        from emitters.prometheus_emitter import PrometheusEmitter

        sb, _ = _mock_storage_backend()
        PrometheusEmitter(port=9090, storage_backend=sb)
        PrometheusEmitter(port=9090, storage_backend=sb)
        PrometheusEmitter(port=9090, storage_backend=sb)

        mock_start.assert_called_once()


# ---------------------------------------------------------------------------
# AC #10/#11 — Collector script
# ---------------------------------------------------------------------------


class TestCollectorScript:
    _SCRIPT_PATH = Path("examples/shared/scripts/collector.sh")

    def test_collector_script_exists(self) -> None:
        assert self._SCRIPT_PATH.exists(), f"Expected {self._SCRIPT_PATH} to exist"

    def test_collector_script_contains_slow_interval(self) -> None:
        content = self._SCRIPT_PATH.read_text()
        assert "SLOW_INTERVAL" in content

    def test_collector_script_contains_echo_1_catchup(self) -> None:
        content = self._SCRIPT_PATH.read_text()
        assert "echo 1" in content

    def test_collector_script_contains_promtool_tsdb_create_blocks(self) -> None:
        content = self._SCRIPT_PATH.read_text()
        assert "promtool tsdb create-blocks-from openmetrics" in content


# ---------------------------------------------------------------------------
# AC #12 — CSV emitter unaffected
# ---------------------------------------------------------------------------


class TestCsvEmitterUnaffected:
    def test_make_csv_emitter_does_not_have_needs_storage_backend(self) -> None:
        from emitters.csv_emitter import make_csv_emitter

        assert getattr(make_csv_emitter, "needs_storage_backend", False) is False


# ---------------------------------------------------------------------------
# Empty rows guard
# ---------------------------------------------------------------------------


class TestEmptyRowsGuard:
    def test_empty_rows_returns_without_error(self) -> None:
        from emitters.prometheus_emitter import PrometheusEmitter

        sb, uow = _mock_storage_backend()
        emitter = PrometheusEmitter(port=9090, storage_backend=sb)
        emitter(TENANT_ID, DATE, [])  # must not raise

    def test_empty_rows_makes_no_storage_queries(self) -> None:
        from emitters.prometheus_emitter import PrometheusEmitter

        sb, uow = _mock_storage_backend()
        emitter = PrometheusEmitter(port=9090, storage_backend=sb)
        emitter(TENANT_ID, DATE, [])

        uow.billing.find_by_date.assert_not_called()


# ---------------------------------------------------------------------------
# AC #13 — Ecosystem-agnostic metric names
# ---------------------------------------------------------------------------


class TestEcosystemAgnosticMetricNames:
    def test_different_ecosystems_produce_same_metric_name(self) -> None:
        from prometheus_client.metrics_core import GaugeMetricFamily

        from emitters.prometheus_emitter import PrometheusEmitter

        sb, _ = _mock_storage_backend()
        emitter = PrometheusEmitter(port=9090, storage_backend=sb)

        row_ccloud = _make_row(ecosystem="ccloud", identity_id="user-ccloud")
        row_self = _make_row(ecosystem="self-managed", identity_id="user-self")
        emitter(TENANT_ID, DATE, [row_ccloud, row_self])

        families = list(PrometheusEmitter._chargeback_col.collect())
        assert len(families) == 1
        family = families[0]
        assert isinstance(family, GaugeMetricFamily)
        assert family.name == "chitragupt_chargeback_amount"


# ---------------------------------------------------------------------------
# AC #14 — Dependency declared
# ---------------------------------------------------------------------------


class TestDependencyDeclared:
    def test_prometheus_client_in_pyproject_toml(self) -> None:
        content = Path("pyproject.toml").read_text()
        assert "prometheus-client" in content


# ---------------------------------------------------------------------------
# AC #15 — Example config exists
# ---------------------------------------------------------------------------


class TestExampleConfigExists:
    _CONFIG_PATH = Path("examples/self-managed-full/config.yaml")

    def test_example_config_exists(self) -> None:
        assert self._CONFIG_PATH.exists(), f"Expected {self._CONFIG_PATH} to exist"

    def test_example_config_contains_type_prometheus(self) -> None:
        content = self._CONFIG_PATH.read_text()
        assert "type: prometheus" in content

    def test_example_config_contains_prometheus_url(self) -> None:
        content = self._CONFIG_PATH.read_text()
        assert "url:" in content


# ---------------------------------------------------------------------------
# AC #16 — _load_emitters wiring test
# ---------------------------------------------------------------------------


class TestLoadEmittersWiring:
    def test_load_emitters_passes_storage_backend_to_factories_needing_it(self) -> None:
        from core.emitters.registry import register
        from core.engine.orchestrator import _load_emitters

        received_kwargs: dict[str, Any] = {}

        def mock_prometheus_factory(**kwargs: Any) -> Any:
            received_kwargs.update(kwargs)
            emitter = MagicMock()
            emitter.__call__ = MagicMock()
            return emitter

        mock_prometheus_factory.needs_storage_backend = True  # type: ignore[attr-defined]
        register("_test_prometheus_factory", mock_prometheus_factory)

        from core.config.models import EmitterSpec

        sb, _ = _mock_storage_backend()
        spec = EmitterSpec(type="_test_prometheus_factory", params={"port": 9091})
        _load_emitters([spec], "daily", storage_backend=sb)

        assert "storage_backend" in received_kwargs
        assert received_kwargs["storage_backend"] is sb

    def test_load_emitters_does_not_pass_storage_backend_to_csv_factory(self) -> None:
        from core.emitters.registry import register
        from core.engine.orchestrator import _load_emitters

        received_kwargs: dict[str, Any] = {}

        def mock_csv_factory(**kwargs: Any) -> Any:
            received_kwargs.update(kwargs)
            emitter = MagicMock()
            return emitter

        # no needs_storage_backend attribute
        register("_test_csv_factory", mock_csv_factory)

        from core.config.models import EmitterSpec

        sb, _ = _mock_storage_backend()
        spec = EmitterSpec(type="_test_csv_factory", params={"output_dir": "/tmp"})
        _load_emitters([spec], "daily", storage_backend=sb)

        assert "storage_backend" not in received_kwargs


# ---------------------------------------------------------------------------
# collect() output shape
# ---------------------------------------------------------------------------


class TestCollectOutputShape:
    def test_chargeback_collector_collect_returns_single_gauge_family(self) -> None:
        from prometheus_client.metrics_core import GaugeMetricFamily

        from emitters.prometheus_emitter import PrometheusEmitter

        sb, _ = _mock_storage_backend()
        emitter = PrometheusEmitter(port=9090, storage_backend=sb)
        emitter(TENANT_ID, DATE, [_make_row()])

        families = list(PrometheusEmitter._chargeback_col.collect())
        assert len(families) == 1
        family = families[0]
        assert isinstance(family, GaugeMetricFamily)
        assert family.name == "chitragupt_chargeback_amount"


# ---------------------------------------------------------------------------
# GIT-001 — make_prometheus_emitter factory
# ---------------------------------------------------------------------------


class TestMakePrometheusEmitterFactory:
    def test_make_prometheus_emitter_raises_value_error_when_no_storage_backend(self) -> None:
        from emitters.prometheus_emitter import make_prometheus_emitter

        with pytest.raises(ValueError, match="storage_backend"):
            make_prometheus_emitter(storage_backend=None)

    def test_make_prometheus_emitter_returns_instance_with_correct_storage_backend(self) -> None:
        from emitters.prometheus_emitter import PrometheusEmitter, make_prometheus_emitter

        sb, _ = _mock_storage_backend()
        instance = make_prometheus_emitter(port=9090, storage_backend=sb)
        assert isinstance(instance, PrometheusEmitter)
        assert instance._storage_backend is sb


# ---------------------------------------------------------------------------
# GIT-002 — registry.get raises ValueError for unknown emitter
# ---------------------------------------------------------------------------


class TestRegistryGetUnknownEmitter:
    def test_get_unknown_emitter_raises_value_error(self) -> None:
        from core.emitters.registry import get

        with pytest.raises(ValueError, match="nonexistent_emitter"):
            get("nonexistent_emitter", {})

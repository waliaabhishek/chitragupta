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


def _make_billing_emit_row(
    *,
    ecosystem: str = ECOSYSTEM,
    resource_id: str = "cluster-1",
    product_type: str = "KAFKA_CKU",
    product_category: str = "kafka",
    amount: Decimal = Decimal("50.00"),
) -> Any:
    from core.emitters.emit_rows import BillingEmitRow

    return BillingEmitRow(
        tenant_id=TENANT_ID,
        ecosystem=ecosystem,
        resource_id=resource_id,
        product_type=product_type,
        product_category=product_category,
        amount=amount,
        timestamp=datetime(2024, 1, 15, 0, 0, 0, tzinfo=UTC),
    )


def _make_resource_emit_row(
    *,
    ecosystem: str = ECOSYSTEM,
    resource_id: str = "cluster-1",
    resource_type: str = "kafka_cluster",
) -> Any:
    from core.emitters.emit_rows import ResourceEmitRow

    return ResourceEmitRow(
        tenant_id=TENANT_ID,
        ecosystem=ecosystem,
        resource_id=resource_id,
        resource_type=resource_type,
        amount=Decimal(1),
        timestamp=datetime(2024, 1, 15, 0, 0, 0, tzinfo=UTC),
    )


def _make_identity_emit_row(
    *,
    ecosystem: str = ECOSYSTEM,
    identity_id: str = "user-1",
    identity_type: str = "service_account",
) -> Any:
    from core.emitters.emit_rows import IdentityEmitRow

    return IdentityEmitRow(
        tenant_id=TENANT_ID,
        ecosystem=ecosystem,
        identity_id=identity_id,
        identity_type=identity_type,
        amount=Decimal(1),
        timestamp=datetime(2024, 1, 15, 0, 0, 0, tzinfo=UTC),
    )


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def reset_prometheus_emitter_state() -> Any:
    yield
    from prometheus_client import REGISTRY

    from emitters.prometheus_emitter import PrometheusEmitter

    with PrometheusEmitter._collectors_lock:
        for col in list(PrometheusEmitter._collectors.values()):
            with contextlib.suppress(Exception):
                REGISTRY.unregister(col)
        PrometheusEmitter._collectors.clear()
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

        instance = PrometheusEmitter(port=9090)
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
        emitter = PrometheusEmitter(port=9090)
        emitter(TENANT_ID, DATE, [row])

        col = PrometheusEmitter._collectors["chitragupta_chargeback_amount"]
        samples = col._samples_by_tenant[TENANT_ID]
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
        emitter = PrometheusEmitter(port=9090)
        emitter(TENANT_ID, DATE, [row])

        col = PrometheusEmitter._collectors["chitragupta_chargeback_amount"]
        samples = col._samples_by_tenant[TENANT_ID]
        assert len(samples) == 1
        labels, value, _ts = samples[0]
        assert labels[3] == ""  # resource_id label empty string for None


# ---------------------------------------------------------------------------
# AC #3 — Billing metric (via BillingEmitRow)
# ---------------------------------------------------------------------------


class TestBillingMetric:
    def test_billing_samples_count_and_values(self) -> None:
        from emitters.prometheus_emitter import PrometheusEmitter

        row1 = _make_billing_emit_row(resource_id="cluster-1", product_type="KAFKA_CKU", amount=Decimal("50.00"))
        row2 = _make_billing_emit_row(resource_id="cluster-2", product_type="KAFKA_STORAGE", amount=Decimal("20.00"))
        emitter = PrometheusEmitter(port=9090)
        emitter(TENANT_ID, DATE, [row1, row2])

        col = PrometheusEmitter._collectors["chitragupta_billing_amount"]
        samples = col._samples_by_tenant[TENANT_ID]
        assert len(samples) == 2

    def test_billing_sample_label_and_value(self) -> None:
        from emitters.prometheus_emitter import PrometheusEmitter

        row = _make_billing_emit_row(
            resource_id="cluster-1",
            product_type="KAFKA_CKU",
            product_category="kafka",
            amount=Decimal("50.00"),
        )
        emitter = PrometheusEmitter(port=9090)
        emitter(TENANT_ID, DATE, [row])

        col = PrometheusEmitter._collectors["chitragupta_billing_amount"]
        samples = col._samples_by_tenant[TENANT_ID]
        assert len(samples) == 1
        labels, value, ts = samples[0]
        assert ECOSYSTEM in labels
        assert "cluster-1" in labels
        assert "KAFKA_CKU" in labels
        assert value == float(Decimal("50.00"))
        assert ts == DATE_TS


# ---------------------------------------------------------------------------
# AC #4 — Resource presence metric (via ResourceEmitRow)
# ---------------------------------------------------------------------------


class TestResourceMetric:
    def test_resource_samples_count_and_value(self) -> None:
        from emitters.prometheus_emitter import PrometheusEmitter

        rows = [
            _make_resource_emit_row(resource_id="cluster-1", resource_type="kafka_cluster"),
            _make_resource_emit_row(resource_id="cluster-2", resource_type="kafka_cluster"),
            _make_resource_emit_row(resource_id="sr-1", resource_type="schema_registry"),
        ]
        emitter = PrometheusEmitter(port=9090)
        emitter(TENANT_ID, DATE, rows)

        col = PrometheusEmitter._collectors["chitragupta_resource_active"]
        samples = col._samples_by_tenant[TENANT_ID]
        assert len(samples) == 3
        for _, value, _ in samples:
            assert value == 1.0


# ---------------------------------------------------------------------------
# AC #5 — Identity presence metric (via IdentityEmitRow)
# ---------------------------------------------------------------------------


class TestIdentityMetric:
    def test_identity_samples_count_and_value(self) -> None:
        from emitters.prometheus_emitter import PrometheusEmitter

        rows = [
            _make_identity_emit_row(identity_id="user-1", identity_type="user"),
            _make_identity_emit_row(identity_id="sa-1", identity_type="service_account"),
        ]
        emitter = PrometheusEmitter(port=9090)
        emitter(TENANT_ID, DATE, rows)

        col = PrometheusEmitter._collectors["chitragupta_identity_active"]
        samples = col._samples_by_tenant[TENANT_ID]
        assert len(samples) == 2
        for _, value, _ in samples:
            assert value == 1.0


# ---------------------------------------------------------------------------
# AC #6 — Timestamped samples
# ---------------------------------------------------------------------------


class TestTimestampedSamples:
    def test_all_collectors_use_midnight_utc_timestamp(self) -> None:
        from emitters.prometheus_emitter import PrometheusEmitter

        chargeback_emitter = PrometheusEmitter(port=9090)
        chargeback_emitter(TENANT_ID, DATE, [_make_row()])

        billing_emitter = PrometheusEmitter(port=9090)
        billing_emitter(TENANT_ID, DATE, [_make_billing_emit_row()])

        resource_emitter = PrometheusEmitter(port=9090)
        resource_emitter(TENANT_ID, DATE, [_make_resource_emit_row()])

        identity_emitter = PrometheusEmitter(port=9090)
        identity_emitter(TENANT_ID, DATE, [_make_identity_emit_row()])

        for metric_name in (
            "chitragupta_chargeback_amount",
            "chitragupta_billing_amount",
            "chitragupta_resource_active",
            "chitragupta_identity_active",
        ):
            col = PrometheusEmitter._collectors[metric_name]
            samples = col._samples_by_tenant[TENANT_ID]
            for _, _, ts in samples:
                assert ts == DATE_TS, f"Expected {DATE_TS} but got {ts} in {metric_name}"


# ---------------------------------------------------------------------------
# AC #7 — Idempotent re-emit
# ---------------------------------------------------------------------------


class TestIdempotentReEmit:
    def test_second_emit_replaces_first(self) -> None:
        from emitters.prometheus_emitter import PrometheusEmitter

        emitter = PrometheusEmitter(port=9090)

        rows_v1 = [_make_row(identity_id="user-1"), _make_row(identity_id="user-2")]
        rows_v2 = [_make_row(identity_id="user-3")]

        emitter(TENANT_ID, DATE, rows_v1)
        emitter(TENANT_ID, DATE, rows_v2)

        col = PrometheusEmitter._collectors["chitragupta_chargeback_amount"]
        samples = col._samples_by_tenant[TENANT_ID]
        assert len(samples) == len(rows_v2)


# ---------------------------------------------------------------------------
# AC #8 — Server singleton (single instance)
# ---------------------------------------------------------------------------


class TestServerSingleton:
    def test_server_started_exactly_once_for_two_instances(self, monkeypatch: pytest.MonkeyPatch) -> None:
        mock_start = Mock()
        monkeypatch.setattr("emitters.prometheus_emitter.start_http_server", mock_start)

        from emitters.prometheus_emitter import PrometheusEmitter

        PrometheusEmitter(port=9090)
        PrometheusEmitter(port=9090)

        mock_start.assert_called_once()


# ---------------------------------------------------------------------------
# AC #9 — Multi-tenant no double server
# ---------------------------------------------------------------------------


class TestMultiTenantSingleton:
    def test_three_instances_start_server_once(self, monkeypatch: pytest.MonkeyPatch) -> None:
        mock_start = Mock()
        monkeypatch.setattr("emitters.prometheus_emitter.start_http_server", mock_start)

        from emitters.prometheus_emitter import PrometheusEmitter

        PrometheusEmitter(port=9090)
        PrometheusEmitter(port=9090)
        PrometheusEmitter(port=9090)

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

        emitter = PrometheusEmitter(port=9090)
        emitter(TENANT_ID, DATE, [])  # must not raise

    def test_empty_rows_creates_no_collectors(self) -> None:
        from emitters.prometheus_emitter import PrometheusEmitter

        emitter = PrometheusEmitter(port=9090)
        emitter(TENANT_ID, DATE, [])

        assert len(PrometheusEmitter._collectors) == 0


# ---------------------------------------------------------------------------
# AC #13 — Ecosystem-agnostic metric names
# ---------------------------------------------------------------------------


class TestEcosystemAgnosticMetricNames:
    def test_different_ecosystems_produce_same_metric_name(self) -> None:
        from prometheus_client.metrics_core import GaugeMetricFamily

        from emitters.prometheus_emitter import PrometheusEmitter

        emitter = PrometheusEmitter(port=9090)

        row_ccloud = _make_row(ecosystem="ccloud", identity_id="user-ccloud")
        row_self = _make_row(ecosystem="self-managed", identity_id="user-self")
        emitter(TENANT_ID, DATE, [row_ccloud, row_self])

        col = PrometheusEmitter._collectors["chitragupta_chargeback_amount"]
        families = list(col.collect())
        assert len(families) == 1
        family = families[0]
        assert isinstance(family, GaugeMetricFamily)
        assert family.name == "chitragupta_chargeback_amount"


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
# AC #16 — EmitterRunner wiring (no storage_backend needed)
# ---------------------------------------------------------------------------


class TestLoadEmittersWiring:
    def setup_method(self) -> None:
        from core.emitters import registry

        registry._REGISTRY.clear()

    def teardown_method(self) -> None:
        from core.emitters import registry

        registry._REGISTRY.clear()

    def _make_mock_storage(self) -> Any:
        """Minimal mock storage that satisfies EmitterRunner._run_spec UoW calls."""
        from datetime import date

        test_date = date(2025, 1, 1)

        emission_repo = MagicMock()
        emission_repo.get_emitted_dates.return_value = set()
        emission_repo.get_failed_dates.return_value = set()

        chargeback_repo = MagicMock()
        chargeback_repo.get_distinct_dates.return_value = [test_date]
        chargeback_repo.find_by_date.return_value = []  # no rows → emitter not called, but factory is instantiated

        uow = MagicMock()
        uow.__enter__ = MagicMock(return_value=uow)
        uow.__exit__ = MagicMock(return_value=False)
        uow.emissions = emission_repo
        uow.chargebacks = chargeback_repo

        storage = MagicMock()
        storage.create_unit_of_work.return_value = uow
        return storage

    def test_registry_emitter_builder_no_args(self) -> None:
        """RegistryEmitterBuilder no longer takes storage_backend — constructor takes no args."""
        from core.emitters.sources import RegistryEmitterBuilder

        builder = RegistryEmitterBuilder()
        assert builder is not None

    def test_load_emitters_does_not_pass_storage_backend_to_csv_factory(self) -> None:
        from core.config.models import EmitterSpec
        from core.emitters.registry import register
        from core.emitters.runner import EmitterRunner

        received_kwargs: dict[str, Any] = {}

        def mock_csv_factory(**kwargs: Any) -> Any:
            received_kwargs.update(kwargs)
            emitter = MagicMock()
            return emitter

        # no needs_storage_backend attribute
        register("_test_csv_factory", mock_csv_factory)

        storage = self._make_mock_storage()
        spec = EmitterSpec(type="_test_csv_factory", params={"output_dir": "/tmp"})
        from core.emitters.sources import ChargebackDateSource, ChargebackRowFetcher, RegistryEmitterBuilder

        runner = EmitterRunner(
            ecosystem="test-eco",
            storage_backend=storage,
            emitter_specs=[spec],
            date_source=ChargebackDateSource(storage),
            row_fetcher=ChargebackRowFetcher(storage),
            emitter_builder=RegistryEmitterBuilder(),
            pipeline="chargeback",
            chargeback_granularity="daily",
        )
        runner.run("tenant-1")

        assert "storage_backend" not in received_kwargs


# ---------------------------------------------------------------------------
# collect() output shape
# ---------------------------------------------------------------------------


class TestCollectOutputShape:
    def test_chargeback_collector_collect_returns_single_gauge_family(self) -> None:
        from prometheus_client.metrics_core import GaugeMetricFamily

        from emitters.prometheus_emitter import PrometheusEmitter

        emitter = PrometheusEmitter(port=9090)
        emitter(TENANT_ID, DATE, [_make_row()])

        col = PrometheusEmitter._collectors["chitragupta_chargeback_amount"]
        families = list(col.collect())
        assert len(families) == 1
        family = families[0]
        assert isinstance(family, GaugeMetricFamily)
        assert family.name == "chitragupta_chargeback_amount"


# ---------------------------------------------------------------------------
# GIT-001 — make_prometheus_emitter factory (simplified — no storage_backend)
# ---------------------------------------------------------------------------


class TestMakePrometheusEmitterFactory:
    def test_make_prometheus_emitter_returns_instance(self) -> None:
        from emitters.prometheus_emitter import PrometheusEmitter, make_prometheus_emitter

        instance = make_prometheus_emitter(port=9090)
        assert isinstance(instance, PrometheusEmitter)

    def test_make_prometheus_emitter_no_storage_backend_required(self) -> None:
        from emitters.prometheus_emitter import make_prometheus_emitter

        # Must not raise — storage_backend no longer needed
        instance = make_prometheus_emitter()
        assert instance is not None


# ---------------------------------------------------------------------------
# GIT-002 — registry.get raises ValueError for unknown emitter
# ---------------------------------------------------------------------------


class TestRegistryGetUnknownEmitter:
    def test_get_unknown_emitter_raises_value_error(self) -> None:
        from core.emitters.registry import get

        with pytest.raises(ValueError, match="nonexistent_emitter"):
            get("nonexistent_emitter", {})


# ---------------------------------------------------------------------------
# TASK-172 — Topic attribution metric: attribution_method label
# ---------------------------------------------------------------------------


def _make_topic_attribution_row(
    *,
    ecosystem: str = ECOSYSTEM,
    env_id: str = "env-abc",
    cluster_resource_id: str = "lkc-xyz",
    topic_name: str = "orders-events",
    product_category: str = "KAFKA",
    product_type: str = "KAFKA_NETWORK_WRITE",
    attribution_method: str = "bytes_ratio",
    amount: Decimal = Decimal("10.00"),
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


class TestTopicAttributionMetric:
    def test_attribution_method_in_label_names(self) -> None:
        from emitters.prometheus_emitter import PrometheusEmitter

        emitter = PrometheusEmitter(port=9090)
        emitter(TENANT_ID, DATE, [_make_topic_attribution_row()])

        col = PrometheusEmitter._collectors["chitragupta_topic_attribution_amount"]
        assert "attribution_method" in col._label_names

    def test_attribution_method_in_sample_values(self) -> None:
        from emitters.prometheus_emitter import PrometheusEmitter

        emitter = PrometheusEmitter(port=9090)
        emitter(TENANT_ID, DATE, [_make_topic_attribution_row(attribution_method="bytes_ratio")])

        col = PrometheusEmitter._collectors["chitragupta_topic_attribution_amount"]
        samples = col._samples_by_tenant[TENANT_ID]
        assert len(samples) == 1
        labels, value, ts = samples[0]
        assert "bytes_ratio" in labels
        assert value == float(Decimal("10.00"))
        assert ts == DATE_TS

    def test_different_attribution_methods_produce_distinct_samples(self) -> None:
        from emitters.prometheus_emitter import PrometheusEmitter

        emitter = PrometheusEmitter(port=9090)
        rows = [
            _make_topic_attribution_row(attribution_method="bytes_ratio", amount=Decimal("10.00")),
            _make_topic_attribution_row(attribution_method="even_split", amount=Decimal("5.00")),
        ]
        emitter(TENANT_ID, DATE, rows)

        col = PrometheusEmitter._collectors["chitragupta_topic_attribution_amount"]
        samples = col._samples_by_tenant[TENANT_ID]
        assert len(samples) == 2
        label_sets = [tuple(s[0]) for s in samples]
        assert len(set(label_sets)) == 2, "Label sets must be distinct — no silent overwrite"

    def test_empty_rows_does_not_raise(self) -> None:
        from emitters.prometheus_emitter import PrometheusEmitter

        emitter = PrometheusEmitter(port=9090)
        emitter(TENANT_ID, DATE, [])  # must not raise

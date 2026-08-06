from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from sqlmodel import Session, select

from core.engine.allocation import AllocatorRegistry
from core.engine.orchestrator import CalculatePhase
from core.models.pipeline import PipelineState
from core.plugin.registry import EcosystemBundle
from core.storage.backends.sqlmodel.engine import _engine_lock, _engines, get_or_create_engine
from core.storage.backends.sqlmodel.tables import ChargebackFactTable
from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend
from plugins.confluent_cloud.allocators.default_allocators import default_allocator
from plugins.confluent_cloud.models.billing import CCloudBillingLineItem
from plugins.confluent_cloud.plugin import ConfluentCloudPlugin
from plugins.confluent_cloud.storage.module import CCloudStorageModule
from tests.unit.core.engine.test_batch_chargeback_write import MockBillingRepo, MockUnitOfWork, _make_billing_line

TRACKING_DATE = date(2026, 7, 1)
COMPLETED_AT = datetime(2026, 7, 3, 2, tzinfo=UTC)
CURRENT_NATIVE_LINE_TYPES = (
    "AUDIT_LOG_READ",
    "CLUSTER_LINKING_PER_LINK",
    "CLUSTER_LINKING_READ",
    "CLUSTER_LINKING_WRITE",
    "CONNECT_CAPACITY",
    "CONNECT_NUM_RECORDS",
    "CONNECT_NUM_TASKS",
    "CONNECT_THROUGHPUT",
    "CUSTOM_CONNECT_NUM_TASKS",
    "CUSTOM_CONNECT_THROUGHPUT",
    "FLINK_NUM_CFUS",
    "GOVERNANCE_BASE",
    "KAFKA_BASE",
    "KAFKA_NETWORK_READ",
    "KAFKA_NETWORK_WRITE",
    "KAFKA_NUM_CKUS",
    "KAFKA_PARTITION",
    "KAFKA_REST_PRODUCE",
    "KAFKA_STORAGE",
    "KAFKA_STREAMS",
    "KSQL_NUM_CSUS",
    "NUM_RULES",
    "PROMO_CREDIT",
    "SCHEMA_REGISTRY",
    "SUPPORT",
    "TABLEFLOW_DATA_PROCESSED",
    "TABLEFLOW_NUM_TOPICS",
    "TABLEFLOW_STORAGE",
    "USM_CONNECTED_NODE",
)


@pytest.fixture(autouse=True)
def clean_engine_cache() -> Any:
    with _engine_lock:
        for engine in _engines.values():
            engine.dispose()
        _engines.clear()
    yield
    with _engine_lock:
        for engine in _engines.values():
            engine.dispose()
        _engines.clear()


def _phase(*, ecosystem: str = "confluent_cloud", tenant_id: str = "org-1") -> CalculatePhase:
    return CalculatePhase(
        ecosystem=ecosystem,
        tenant_id=tenant_id,
        bundle=EcosystemBundle(
            plugin=MagicMock(),
            handlers={},
            product_type_to_handler={},
            fallback_allocator=default_allocator,
        ),
        retry_checker=MagicMock(),
        metrics_source=None,
        allocator_registry=AllocatorRegistry(),
        identity_overrides={},
        allocator_params={},
        metrics_step=timedelta(hours=1),
        calculation_id_factory=MagicMock(return_value="calculation-1"),
        calculation_clock=MagicMock(return_value=COMPLETED_AT),
    )


def _origin() -> CCloudBillingLineItem:
    return CCloudBillingLineItem(
        ecosystem="confluent_cloud",
        tenant_id="org-1",
        timestamp=datetime(2026, 7, 1, tzinfo=UTC),
        env_id="env-1",
        resource_id="lkc-1",
        product_category="KAFKA",
        product_type="KAFKA_STORAGE",
        quantity=Decimal("5.000"),
        unit_price=Decimal("2"),
        total_cost=Decimal("8"),
    )


def _backend(tmp_path: Any, name: str) -> tuple[SQLModelBackend, str]:
    connection_string = f"sqlite:///{tmp_path / name}"
    backend = SQLModelBackend(connection_string, CCloudStorageModule(), use_migrations=False)
    backend.create_tables()
    return backend, connection_string


def _opt_in_backend(tmp_path: Any, name: str, *, enabled: bool) -> tuple[SQLModelBackend, str]:
    connection_string = f"sqlite:///{tmp_path / name}"
    backend = SQLModelBackend(
        connection_string,
        CCloudStorageModule(),
        use_migrations=False,
        focus_preview_enabled=enabled,
    )
    backend.create_tables()
    return backend, connection_string


def _seed_pipeline_state(uow: Any) -> None:
    uow.pipeline_state.upsert(
        PipelineState(
            ecosystem="confluent_cloud",
            tenant_id="org-1",
            tracking_date=TRACKING_DATE,
            billing_gathered=True,
            resources_gathered=True,
        )
    )


def test_disabled_ccloud_calculation_commits_without_constructing_lineage(tmp_path: Any) -> None:
    backend, connection_string = _opt_in_backend(tmp_path, "disabled-lineage.db", enabled=False)
    with backend.create_unit_of_work() as uow:
        _seed_pipeline_state(uow)
        uow.billing.upsert(_origin())
        uow.commit()

    with (
        patch(
            "core.engine.orchestrator.build_allocation_lineage_capture",
            side_effect=AssertionError("disabled calculation constructed Preview lineage"),
        ) as builder,
        backend.create_unit_of_work() as uow,
    ):
        assert _phase().run(uow, TRACKING_DATE) == 1
        uow.commit()

    builder.assert_not_called()
    engine = get_or_create_engine(connection_string)
    with Session(engine) as session:
        assert len(session.exec(select(ChargebackFactTable)).all()) == 1
    with backend.create_read_only_unit_of_work() as uow:
        state = uow.pipeline_state.get("confluent_cloud", "org-1", TRACKING_DATE)
    assert state is not None and state.has_usable_calculation is True
    backend.dispose()


def test_enabled_calculation_returns_lineage_capture_without_persisting_it_in_generic_uow(
    tmp_path: Any,
) -> None:
    backend, connection_string = _opt_in_backend(tmp_path, "enabled-lineage.db", enabled=True)
    with backend.create_unit_of_work() as uow:
        _seed_pipeline_state(uow)
        uow.billing.upsert(_origin())
        uow.commit()

    with backend.create_unit_of_work() as uow:
        result = _phase().run_with_lineage_capture(uow, TRACKING_DATE)
        uow.commit()

    assert result.ecosystem == "confluent_cloud"
    assert result.tenant_id == "org-1"
    assert result.tracking_date == TRACKING_DATE
    assert result.rows_written == 1
    assert result.calculation_id == "calculation-1"
    assert result.calculation_completed_at == COMPLETED_AT
    assert result.lineage_capture is not None
    assert result.lineage_failure is None
    engine = get_or_create_engine(connection_string)
    preview_tables = __import__(
        "plugins.confluent_cloud.storage.preview_tables",
        fromlist=["CCloudAllocationLineageRunTable"],
    )
    with Session(engine) as session:
        assert session.exec(select(preview_tables.CCloudAllocationLineageRunTable)).all() == []
    backend.dispose()


def test_lineage_construction_failure_does_not_rollback_generic_calculation(tmp_path: Any) -> None:
    backend, connection_string = _opt_in_backend(tmp_path, "capture-failure.db", enabled=True)
    with backend.create_unit_of_work() as uow:
        _seed_pipeline_state(uow)
        uow.billing.upsert(_origin())
        uow.commit()

    with (
        patch("core.engine.orchestrator.build_allocation_lineage_capture", side_effect=ValueError("bad capture")),
        backend.create_unit_of_work() as uow,
    ):
        result = _phase().run_with_lineage_capture(uow, TRACKING_DATE)
        uow.commit()

    assert result.rows_written == 1
    assert result.lineage_capture is None
    assert result.lineage_failure.value == "construction_failed"
    engine = get_or_create_engine(connection_string)
    with Session(engine) as session:
        assert len(session.exec(select(ChargebackFactTable)).all()) == 1
    backend.dispose()


def test_supported_zero_billing_returns_complete_empty_capture_with_shared_success_identity_and_time(
    tmp_path: Any,
) -> None:
    backend, _connection_string = _backend(tmp_path, "zero.db")
    phase = _phase()
    with backend.create_unit_of_work() as uow:
        _seed_pipeline_state(uow)
        result = phase.run_with_lineage_capture(uow, TRACKING_DATE, calculation_run_id=None)
        uow.commit()

    with backend.create_read_only_unit_of_work() as uow:
        pipeline_state = uow.pipeline_state.get("confluent_cloud", "org-1", TRACKING_DATE)

    assert result.rows_written == 0
    assert result.lineage_failure is None
    assert result.lineage_capture is not None
    assert result.lineage_capture.calculation_id == "calculation-1"
    assert result.lineage_capture.captures == ()
    assert result.calculation_completed_at == COMPLETED_AT
    assert pipeline_state is not None
    assert pipeline_state.calculation_id == result.lineage_capture.calculation_id
    assert pipeline_state.calculation_completed_at == COMPLETED_AT
    backend.dispose()


def test_supported_rows_build_once_per_origin_and_persist_actual_output_after_chargebacks(tmp_path: Any) -> None:
    backend, connection_string = _backend(tmp_path, "rows.db")
    with backend.create_unit_of_work() as uow:
        _seed_pipeline_state(uow)
        uow.billing.upsert(_origin())
        uow.commit()

    from core.engine.allocation_lineage import build_allocation_lineage_capture

    with (
        patch(
            "core.engine.orchestrator.build_allocation_lineage_capture",
            wraps=build_allocation_lineage_capture,
        ) as builder,
        backend.create_unit_of_work() as uow,
    ):
        result = _phase().run_with_lineage_capture(uow, TRACKING_DATE)
        uow.commit()

    builder.assert_called_once()
    assert result.rows_written == 1
    assert result.lineage_capture is not None
    assert len(result.lineage_capture.captures) == 1
    origin_arg = builder.call_args.kwargs["origin"]
    actual_rows = builder.call_args.kwargs["rows"]
    assert origin_arg.resource_id == "lkc-1"
    assert len(actual_rows) == 1
    assert actual_rows[0].identity_id == "lkc-1"
    assert actual_rows[0].amount == Decimal("8")
    engine = get_or_create_engine(connection_string)
    with Session(engine) as session:
        assert len(session.exec(select(ChargebackFactTable)).all()) == 1
    backend.dispose()


def test_supported_capability_builds_each_origin_and_returns_one_run_envelope(tmp_path: Any) -> None:
    backend, _connection_string = _backend(tmp_path, "one-envelope.db")
    second = CCloudBillingLineItem(**{**_origin().__dict__, "resource_id": "lkc-2"})
    with backend.create_unit_of_work() as uow:
        _seed_pipeline_state(uow)
        uow.billing.upsert(_origin())
        uow.billing.upsert(second)
        uow.commit()

    from core.engine.allocation_lineage import build_allocation_lineage_capture

    with (
        backend.create_unit_of_work() as uow,
        patch(
            "core.engine.orchestrator.build_allocation_lineage_capture",
            wraps=build_allocation_lineage_capture,
        ) as builder,
    ):
        result = _phase().run_with_lineage_capture(uow, TRACKING_DATE)
        uow.commit()

    assert builder.call_count == 2
    assert result.rows_written == 2
    assert result.lineage_capture is not None
    run = result.lineage_capture
    assert run.calculation_id == "calculation-1"
    assert len(run.captures) == 2
    backend.dispose()


def test_unsupported_generic_repository_does_not_call_builder_codec_or_lineage_persistence() -> None:
    phase = _phase(ecosystem="test-eco", tenant_id="tenant-1")
    uow = MockUnitOfWork(
        billing=MockBillingRepo(
            lines=[
                _make_billing_line(
                    product_type="UNKNOWN",
                    timestamp=datetime(2026, 7, 1, tzinfo=UTC),
                )
            ]
        )
    )

    with patch(
        "core.engine.orchestrator.build_allocation_lineage_capture",
        side_effect=AssertionError("unsupported repository must not build lineage"),
    ) as builder:
        assert phase.run(uow, TRACKING_DATE) == 1

    builder.assert_not_called()
    assert len(uow.chargebacks._upsert_batch_calls) == 1
    assert not hasattr(uow.chargebacks, "replace_calculation_lineage")


def test_unsupported_zero_billing_retains_direct_success_without_lineage_work() -> None:
    phase = _phase(ecosystem="test-eco", tenant_id="tenant-1")
    uow = MockUnitOfWork(billing=MockBillingRepo(lines=[]))

    with patch(
        "core.engine.orchestrator.build_allocation_lineage_capture",
        side_effect=AssertionError("unsupported empty run must not build lineage"),
    ) as builder:
        assert phase.run(uow, TRACKING_DATE) == 0

    builder.assert_not_called()
    assert uow.chargebacks._upsert_batch_calls == []
    assert uow.pipeline_state.marked == [("test-eco", "tenant-1", TRACKING_DATE, "calculation-1", COMPLETED_AT, None)]


def test_invalid_capture_is_returned_without_changing_ordinary_calculation_success(tmp_path: Any) -> None:
    backend, _connection_string = _backend(tmp_path, "invalid.db")
    with backend.create_unit_of_work() as uow:
        _seed_pipeline_state(uow)
        uow.billing.upsert(_origin())
        uow.commit()

    from core.storage.interface import (
        AllocationLineageCapture,
        LineageCaptureReason,
        LineageCaptureStatus,
    )

    invalid = AllocationLineageCapture(
        origin_timestamp=_origin().timestamp,
        origin_env_id="env-1",
        origin_resource_id="lkc-1",
        origin_product_type="KAFKA_STORAGE",
        origin_product_category="KAFKA",
        status=LineageCaptureStatus.INVALID,
        reason=LineageCaptureReason.INVALID_METADATA,
        facts=(),
    )
    with (
        patch("core.engine.orchestrator.build_allocation_lineage_capture", return_value=invalid),
        backend.create_unit_of_work() as uow,
    ):
        result = _phase().run_with_lineage_capture(uow, TRACKING_DATE)
        uow.commit()

    with backend.create_read_only_unit_of_work() as uow:
        state = uow.pipeline_state.get("confluent_cloud", "org-1", TRACKING_DATE)
    assert result.rows_written == 1
    assert result.lineage_failure is None
    assert result.lineage_capture is not None
    assert result.lineage_capture.captures == (invalid,)
    assert state is not None and state.chargeback_calculated is True
    backend.dispose()


def test_generic_calculation_never_calls_preview_lineage_repository(tmp_path: Any) -> None:
    backend, connection_string = _backend(tmp_path, "no-preview-write.db")
    with backend.create_unit_of_work() as uow:
        _seed_pipeline_state(uow)
        uow.billing.upsert(_origin())
        uow.commit()

    with (
        backend.create_unit_of_work() as uow,
        patch.object(
            uow.chargebacks,
            "replace_calculation_lineage",
            side_effect=AssertionError("generic calculation persisted Preview evidence"),
        ) as writer,
    ):
        result = _phase().run_with_lineage_capture(uow, TRACKING_DATE)
        uow.commit()

    engine = get_or_create_engine(connection_string)
    with Session(engine) as session:
        assert len(session.exec(select(ChargebackFactTable)).all()) == 1
    with backend.create_read_only_unit_of_work() as uow:
        state = uow.pipeline_state.get("confluent_cloud", "org-1", TRACKING_DATE)
    assert result.lineage_capture is not None
    writer.assert_not_called()
    assert state is not None and state.chargeback_calculated is True
    backend.dispose()


@pytest.mark.parametrize("line_type", CURRENT_NATIVE_LINE_TYPES)
def test_real_production_bundle_captures_every_current_native_line_type_without_registry_policy_change(
    tmp_path: Any,
    line_type: str,
) -> None:
    connection_string = f"sqlite:///{tmp_path / f'{line_type}.db'}"
    plugin = ConfluentCloudPlugin()
    plugin.initialize({"ccloud_api": {"key": "key", "secret": "secret"}})  # pragma: allowlist secret
    bundle = EcosystemBundle.build(plugin)
    retry_checker = MagicMock()
    retry_checker.increment_and_check.return_value = (3, True)
    phase = CalculatePhase(
        ecosystem="confluent_cloud",
        tenant_id="org-1",
        bundle=bundle,
        retry_checker=retry_checker,
        metrics_source=None,
        allocator_registry=AllocatorRegistry(),
        identity_overrides={},
        allocator_params={},
        metrics_step=timedelta(hours=1),
        calculation_id_factory=lambda: "calculation-1",
        calculation_clock=lambda: COMPLETED_AT,
    )
    backend = SQLModelBackend(connection_string, plugin.get_storage_module(), use_migrations=False)
    backend.create_tables()
    origin = _origin()
    origin = CCloudBillingLineItem(
        **{
            **origin.__dict__,
            "product_type": line_type,
            "product_category": "SUPPORT_CLOUD_BASIC" if line_type == "SUPPORT" else "KAFKA",
        }
    )
    with backend.create_unit_of_work() as uow:
        _seed_pipeline_state(uow)
        uow.billing.upsert(origin)
        uow.commit()
    with backend.create_unit_of_work() as uow:
        result = phase.run_with_lineage_capture(uow, TRACKING_DATE)
        uow.commit()

    assert result.lineage_failure is None
    assert result.lineage_capture is not None
    assert result.lineage_capture.calculation_id == "calculation-1"
    capture = result.lineage_capture.captures[0]
    assert capture.status.value == "complete"
    portions = capture.facts
    if line_type == "KAFKA_NUM_CKUS":
        assert result.rows_written == 1
        assert len(portions) == 2
    else:
        assert result.rows_written == len(portions)
    assert len(portions) >= 1
    assert capture.origin_product_type == line_type
    assert {portion.method_version for portion in portions} == {"v1"}
    backend.dispose()
    plugin.close()

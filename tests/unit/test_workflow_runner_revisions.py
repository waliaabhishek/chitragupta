from __future__ import annotations

import threading
from datetime import UTC, date, datetime
from typing import Any
from unittest.mock import MagicMock

from core.config.models import (
    AppSettings,
    FeaturesConfig,
    FocusPreviewTenantConfig,
    TenantConfig,
)
from core.engine.orchestrator import PipelineRunResult
from workflow_runner import TenantRuntime, WorkflowRunner


class _PreviewBackend:
    def __init__(self) -> None:
        self.dispose_calls = 0

    def create_unit_of_work(self) -> Any:
        raise AssertionError("pipeline owns backend use")

    def create_read_only_unit_of_work(self) -> Any:
        raise AssertionError("pipeline owns backend use")

    def create_preview_read_unit_of_work(self) -> Any:
        raise AssertionError("publisher owns backend use")

    def create_preview_write_unit_of_work(self) -> Any:
        raise AssertionError("publisher owns backend use")

    def create_tables(self) -> None:
        return None

    def dispose(self) -> None:
        self.dispose_calls += 1


def _tenant(*, ecosystem: str = "confluent_cloud", enabled: bool = True) -> TenantConfig:
    return TenantConfig(
        ecosystem=ecosystem,
        tenant_id="tenant-1",
        lookback_days=40,
        cutoff_days=5,
        focus_preview=(
            FocusPreviewTenantConfig(
                commercial_profile="direct_payg",
                effective_start_date=date(2026, 7, 1),
                effective_end_date=date(2026, 8, 1),
            )
            if enabled
            else None
        ),
    )


def _result(**overrides: object) -> PipelineRunResult:
    values: dict[str, object] = {
        "tenant_name": "production",
        "tenant_id": "tenant-1",
        "dates_gathered": 1,
        "dates_calculated": 1,
        "chargeback_rows_written": 1,
        "errors": [],
    }
    values.update(overrides)
    return PipelineRunResult(**values)


def _runner(
    *,
    periodic: bool = True,
    publisher: Any | None = None,
    owned_store: Any | None = None,
    tenant: TenantConfig | None = None,
) -> WorkflowRunner:
    settings = AppSettings(
        features=FeaturesConfig(enable_periodic_refresh=periodic, refresh_interval=1),
        tenants={"production": tenant or _tenant()},
    )
    runner = WorkflowRunner(
        settings,
        MagicMock(),
        revision_publisher=publisher,
        owned_preview_artifact_store=owned_store,
    )
    runner._tenant_runtimes["production"] = TenantRuntime(  # noqa: SLF001
        tenant_name="production",
        plugin=MagicMock(),
        storage=_PreviewBackend(),
        orchestrator=MagicMock(),
        config_hash="hash",
        created_at=datetime(2026, 8, 4, tzinfo=UTC),
    )
    return runner


def test_only_periodic_run_loop_publishes_after_successful_calculation() -> None:
    publisher = MagicMock()
    runner = _runner(publisher=publisher)
    shutdown = threading.Event()

    def run_once() -> dict[str, PipelineRunResult]:
        shutdown.set()
        return {"production": _result()}

    runner.run_once = run_once  # type: ignore[method-assign]
    runner.run_loop(shutdown)

    publisher.publish_eligible_months.assert_called_once()
    call = publisher.publish_eligible_months.call_args.kwargs
    assert call["tenant_name"] == "production"
    assert call["tenant_config"].tenant_id == "tenant-1"
    assert isinstance(call["backend"], _PreviewBackend)
    assert call["now"].tzinfo is UTC


def test_direct_run_once_and_run_tenant_do_not_publish() -> None:
    publisher = MagicMock()
    runner = _runner(publisher=publisher)
    runner._bootstrapped = True  # noqa: SLF001
    runner._run_tenant = MagicMock(return_value=_result())  # type: ignore[method-assign]

    runner.run_once()
    runner.run_tenant("production")

    publisher.publish_eligible_months.assert_not_called()


def test_nonperiodic_single_cycle_does_not_publish() -> None:
    publisher = MagicMock()
    runner = _runner(periodic=False, publisher=publisher)
    runner.run_once = MagicMock(return_value={"production": _result()})  # type: ignore[method-assign]

    runner.run_loop(threading.Event())

    publisher.publish_eligible_months.assert_not_called()


def test_periodic_publisher_skips_failed_already_running_and_fatal_results() -> None:
    publisher = MagicMock()
    runner = _runner(publisher=publisher)

    for result in (
        _result(errors=["failed"]),
        _result(already_running=True),
        _result(fatal=True),
    ):
        runner._publish_scheduled_revisions({"production": result})  # noqa: SLF001

    publisher.publish_eligible_months.assert_not_called()


def test_periodic_publisher_skips_missing_runtime_unsupported_profile_and_wrong_backend() -> None:
    publisher = MagicMock()
    runner = _runner(publisher=publisher)
    runner._tenant_runtimes.clear()  # noqa: SLF001
    runner._publish_scheduled_revisions({"production": _result()})  # noqa: SLF001

    unsupported = _runner(publisher=publisher, tenant=_tenant(ecosystem="other"))
    unsupported._publish_scheduled_revisions({"production": _result()})  # noqa: SLF001

    no_profile = _runner(publisher=publisher, tenant=_tenant(enabled=False))
    no_profile._publish_scheduled_revisions({"production": _result()})  # noqa: SLF001

    wrong_backend = _runner(publisher=publisher)
    wrong_backend._tenant_runtimes["production"].storage = object()  # type: ignore[assignment]  # noqa: SLF001
    wrong_backend._publish_scheduled_revisions({"production": _result()})  # noqa: SLF001

    publisher.publish_eligible_months.assert_not_called()


def test_runner_closes_owned_store_and_cached_runtimes_exactly_once() -> None:
    store = MagicMock()
    runner = _runner(publisher=MagicMock(), owned_store=store)
    runtime = runner._tenant_runtimes["production"]  # noqa: SLF001
    runtime.close = MagicMock()  # type: ignore[method-assign]

    runner.close()
    runner.close()
    runner.drain(0)

    runtime.close.assert_called_once()
    store.close.assert_called_once()


def test_injected_publisher_without_owned_store_is_borrowed() -> None:
    publisher = MagicMock()
    runner = _runner(publisher=publisher)

    runner.close()

    assert not hasattr(publisher, "close") or publisher.close.call_count == 0


def test_drain_waits_for_scheduled_publication_before_closing_backend_and_artifact_store() -> None:
    entered = threading.Event()
    release = threading.Event()
    drained = threading.Event()
    store = MagicMock()

    class BlockingPublisher:
        def publish_eligible_months(self, **kwargs: Any) -> tuple[()]:
            backend = kwargs["backend"]
            assert isinstance(backend, _PreviewBackend)
            entered.set()
            assert backend.dispose_calls == 0
            assert store.close.call_count == 0
            assert release.wait(5)
            assert backend.dispose_calls == 0
            assert store.close.call_count == 0
            return ()

    runner = _runner(publisher=BlockingPublisher(), owned_store=store)
    runtime = runner._tenant_runtimes["production"]  # noqa: SLF001
    backend = runtime.storage
    assert isinstance(backend, _PreviewBackend)
    runner.run_once = MagicMock(return_value={"production": _result()})  # type: ignore[method-assign]
    shutdown = threading.Event()
    runner.set_shutdown_event(shutdown)
    run_thread = threading.Thread(target=runner.run_loop, args=(shutdown,))

    def drain() -> None:
        runner.drain(5)
        drained.set()

    run_thread.start()
    assert entered.wait(5)
    drain_thread = threading.Thread(target=drain)
    drain_thread.start()
    assert not drained.wait(0.2)
    assert backend.dispose_calls == 0
    runtime.plugin.close.assert_not_called()
    store.close.assert_not_called()

    release.set()
    run_thread.join(timeout=10)
    drain_thread.join(timeout=10)
    assert not run_thread.is_alive()
    assert not drain_thread.is_alive()
    assert drained.is_set()
    assert backend.dispose_calls == 1
    runtime.plugin.close.assert_called_once()
    store.close.assert_called_once()

    runner.close()
    assert backend.dispose_calls == 1
    runtime.plugin.close.assert_called_once()
    store.close.assert_called_once()

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
        revision_manager=publisher,
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
    publisher.cleanup_retention.assert_called_once()
    assert publisher.cleanup_retention.call_args.kwargs["now"] is call["now"]


def test_revision_cleanup_is_independent_of_result_and_preview_enablement() -> None:
    manager = MagicMock()
    runner = _runner(publisher=manager, tenant=_tenant(enabled=False))
    now = datetime(2026, 8, 4, 12, 0, 0, 123456, tzinfo=UTC)

    runner._cleanup_preview_revision_retention(now=now)  # noqa: SLF001

    manager.cleanup_retention.assert_called_once()
    call = manager.cleanup_retention.call_args.kwargs
    assert call["tenant_name"] == "production"
    assert call["tenant_config"].focus_preview is None
    assert isinstance(call["backend"], _PreviewBackend)
    assert call["now"] is now


def test_periodic_cleanup_covers_every_cached_owner_once_despite_publication_and_owner_failures() -> None:
    enabled_base = _tenant()
    disabled_base = _tenant(enabled=False)
    enabled = enabled_base.model_copy(
        update={
            "tenant_id": "tenant-enabled",
            "storage": type(enabled_base.storage)(connection_string="sqlite:///enabled.db"),
        }
    )
    disabled = disabled_base.model_copy(
        update={
            "tenant_id": "tenant-disabled",
            "storage": type(disabled_base.storage)(connection_string="sqlite:///disabled.db"),
        }
    )
    settings = AppSettings(
        features=FeaturesConfig(enable_periodic_refresh=True, refresh_interval=1),
        tenants={"enabled": enabled, "disabled": disabled},
    )
    manager = MagicMock()
    manager.publish_eligible_months.side_effect = RuntimeError("synthetic publication failure")
    manager.cleanup_retention.side_effect = [
        RuntimeError("synthetic first-owner cleanup failure"),
        MagicMock(),
    ]
    runner = WorkflowRunner(settings, MagicMock(), revision_manager=manager)
    for tenant_name in settings.tenants:
        runner._tenant_runtimes[tenant_name] = TenantRuntime(  # noqa: SLF001
            tenant_name=tenant_name,
            plugin=MagicMock(),
            storage=_PreviewBackend(),
            orchestrator=MagicMock(),
            config_hash=f"hash-{tenant_name}",
            created_at=datetime(2026, 8, 4, tzinfo=UTC),
        )
    shutdown = threading.Event()

    def run_once() -> dict[str, PipelineRunResult]:
        shutdown.set()
        return {
            "enabled": _result(tenant_name="enabled", tenant_id="tenant-enabled"),
            "disabled": _result(tenant_name="disabled", tenant_id="tenant-disabled"),
        }

    runner.run_once = run_once  # type: ignore[method-assign]
    runner.run_loop(shutdown)

    manager.publish_eligible_months.assert_called_once()
    cleanup_calls = manager.cleanup_retention.call_args_list
    assert [call.kwargs["tenant_name"] for call in cleanup_calls] == ["enabled", "disabled"]
    assert [call.kwargs["tenant_config"].focus_preview is not None for call in cleanup_calls] == [True, False]
    assert all(isinstance(call.kwargs["backend"], _PreviewBackend) for call in cleanup_calls)
    assert cleanup_calls[0].kwargs["now"] is cleanup_calls[1].kwargs["now"]
    assert cleanup_calls[0].kwargs["now"] is manager.publish_eligible_months.call_args.kwargs["now"]


def test_revision_cleanup_skips_unsupported_uncached_and_wrong_backends() -> None:
    manager = MagicMock()
    now = datetime(2026, 8, 4, tzinfo=UTC)

    unsupported = _runner(publisher=manager, tenant=_tenant(ecosystem="other"))
    unsupported._cleanup_preview_revision_retention(now=now)  # noqa: SLF001

    uncached = _runner(publisher=manager)
    uncached._tenant_runtimes.clear()  # noqa: SLF001
    uncached._cleanup_preview_revision_retention(now=now)  # noqa: SLF001

    wrong = _runner(publisher=manager)
    wrong._tenant_runtimes["production"].storage = object()  # type: ignore[assignment]  # noqa: SLF001
    wrong._cleanup_preview_revision_retention(now=now)  # noqa: SLF001

    manager.cleanup_retention.assert_not_called()


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
        runner._publish_scheduled_revisions(  # noqa: SLF001
            {"production": result}, now=datetime(2026, 8, 4, tzinfo=UTC)
        )

    publisher.publish_eligible_months.assert_not_called()


def test_periodic_publisher_skips_missing_runtime_unsupported_profile_and_wrong_backend() -> None:
    publisher = MagicMock()
    runner = _runner(publisher=publisher)
    runner._tenant_runtimes.clear()  # noqa: SLF001
    runner._publish_scheduled_revisions(  # noqa: SLF001
        {"production": _result()}, now=datetime(2026, 8, 4, tzinfo=UTC)
    )

    unsupported = _runner(publisher=publisher, tenant=_tenant(ecosystem="other"))
    unsupported._publish_scheduled_revisions(  # noqa: SLF001
        {"production": _result()}, now=datetime(2026, 8, 4, tzinfo=UTC)
    )

    no_profile = _runner(publisher=publisher, tenant=_tenant(enabled=False))
    no_profile._publish_scheduled_revisions(  # noqa: SLF001
        {"production": _result()}, now=datetime(2026, 8, 4, tzinfo=UTC)
    )

    wrong_backend = _runner(publisher=publisher)
    wrong_backend._tenant_runtimes["production"].storage = object()  # type: ignore[assignment]  # noqa: SLF001
    wrong_backend._publish_scheduled_revisions(  # noqa: SLF001
        {"production": _result()}, now=datetime(2026, 8, 4, tzinfo=UTC)
    )

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

        def cleanup_retention(self, **kwargs: Any) -> Any:
            del kwargs
            revisions = __import__("core.preview.revisions", fromlist=["PreviewRevisionCleanupResult"])
            return revisions.PreviewRevisionCleanupResult(0, 0, 0)

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

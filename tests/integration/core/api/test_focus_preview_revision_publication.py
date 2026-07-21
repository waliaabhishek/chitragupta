from __future__ import annotations

import hashlib
import json
import logging
import threading
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, text

from core.api.app import create_app
from core.config.models import AppSettings, FeaturesConfig, PreviewConfig
from core.engine.orchestrator import PipelineRunResult
from core.models.pipeline import PipelineState
from core.models.resource import CoreResource, ResourceStatus
from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend
from core.storage.interface import AllocationLineageRunCapture
from plugins.confluent_cloud.storage.module import CCloudStorageModule
from tests.unit.core.preview.test_revisions import _tenant_config
from tests.unit.core.preview.test_service import _aggregate, _allocation, _seed, _source
from workflow_runner import TenantRuntime, WorkflowRunner


class _BarrierArtifactStore:
    def __init__(self, delegate: Any, barrier: threading.Barrier) -> None:
        self.delegate = delegate
        self.barrier = barrier

    def stage_data_files(self, *, request_id: str, data_files: tuple[Any, ...]) -> Any:
        self.barrier.wait(timeout=10)
        return self.delegate.stage_data_files(request_id=request_id, data_files=data_files)

    def __getattr__(self, name: str) -> Any:
        return getattr(self.delegate, name)


def _run_periodic_cycle_without_clock_patch(runner: WorkflowRunner, result: PipelineRunResult) -> None:
    shutdown = threading.Event()

    def run_once() -> dict[str, PipelineRunResult]:
        shutdown.set()
        return {"production": result}

    runner.run_once = run_once  # type: ignore[method-assign]
    runner.run_loop(shutdown)


def _run_periodic_cycle(runner: WorkflowRunner, result: PipelineRunResult, *, now: datetime) -> None:
    class FrozenDateTime(datetime):
        @classmethod
        def now(cls, tz: object = None) -> datetime:
            del tz
            return now

    with patch("workflow_runner.datetime", FrozenDateTime):
        _run_periodic_cycle_without_clock_patch(runner, result)


def _result(*, errors: list[str] | None = None) -> PipelineRunResult:
    return PipelineRunResult(
        tenant_name="production",
        tenant_id="tenant-1",
        dates_gathered=1,
        dates_calculated=1,
        chargeback_rows_written=1,
        errors=[] if errors is None else errors,
    )


def _seed_month(
    backend: SQLModelBackend,
    *,
    billed_cost: Decimal,
    billing_account_name: str = "Provider billing organization",
) -> None:
    from core.engine.allocation_lineage import build_allocation_lineage_capture

    month_start = date(2026, 7, 1)
    _seed(
        backend,
        state=PipelineState(
            ecosystem="confluent_cloud",
            tenant_id="tenant-1",
            tracking_date=month_start,
            billing_gathered=True,
            resources_gathered=True,
            chargeback_calculated=True,
            calculation_id="calculation-2026-07-01",
            calculation_completed_at=datetime(2026, 7, 3, 2, tzinfo=UTC),
            calculation_run_id=None,
        ),
    )
    sources = []
    aggregates = []
    allocations = []
    states = []
    for offset in range(31):
        tracking_date = month_start + timedelta(days=offset)
        start = datetime.combine(tracking_date, datetime.min.time(), tzinfo=UTC)
        end = start + timedelta(days=1)
        calculation_id = f"calculation-{tracking_date.isoformat()}"
        completed_at = end + timedelta(days=1, hours=2)
        sources.append(
            _source(
                source_record_id=f"provider:cost-{tracking_date.isoformat()}",
                provider_cost_id=f"cost-{tracking_date.isoformat()}",
                source_period_start=start,
                source_period_end=end,
                collection_window_start=datetime(2026, 6, 30, tzinfo=UTC),
                collection_window_end=datetime(2026, 8, 3, tzinfo=UTC),
                evidence_scope_start=start,
                evidence_scope_end=end,
                allocation_timestamp=start,
                retention_timestamp=start,
                billing_timestamp=start,
                amount=billed_cost,
                original_amount=billed_cost + Decimal("2"),
                discount_amount=Decimal("2"),
                price=(billed_cost + Decimal("2")) / Decimal("5"),
            )
        )
        aggregates.append(_aggregate(timestamp=start, total_cost=billed_cost))
        allocations.append(_allocation(timestamp=start, amount=billed_cost))
        states.append(
            PipelineState(
                ecosystem="confluent_cloud",
                tenant_id="tenant-1",
                tracking_date=tracking_date,
                billing_gathered=True,
                resources_gathered=True,
                chargeback_calculated=True,
                calculation_id=calculation_id,
                calculation_completed_at=completed_at,
                calculation_run_id=None,
            )
        )
    with backend.create_unit_of_work() as uow:
        uow.resources.upsert(
            CoreResource(
                ecosystem="confluent_cloud",
                tenant_id="tenant-1",
                resource_id="11111111-2222-4333-8444-555555555555",
                resource_type="organization",
                display_name=billing_account_name,
                status=ResourceStatus.ACTIVE,
                metadata={"organization_binding_state": "bound"},
            )
        )
        uow.billing.replace_source_window(
            "confluent_cloud",
            "tenant-1",
            datetime(2026, 6, 30, tzinfo=UTC),
            datetime(2026, 8, 3, tzinfo=UTC),
            sources,
        )
        for aggregate in aggregates:
            uow.billing.upsert(aggregate)
        for state in states:
            uow.chargebacks.delete_by_date("confluent_cloud", "tenant-1", state.tracking_date)
        uow.chargebacks.upsert_batch(allocations)
        for aggregate, allocation, state in zip(aggregates, allocations, states, strict=True):
            assert state.calculation_id is not None and state.calculation_completed_at is not None
            uow.chargebacks.replace_calculation_lineage(  # type: ignore[attr-defined]
                AllocationLineageRunCapture(
                    ecosystem="confluent_cloud",
                    tenant_id="tenant-1",
                    tracking_date=state.tracking_date,
                    calculation_id=state.calculation_id,
                    captures=(build_allocation_lineage_capture(origin=aggregate, rows=(allocation,)),),
                ),
                calculation_completed_at=state.calculation_completed_at,
            )
            uow.pipeline_state.upsert(state)
        uow.commit()


def _seed_calculation_days(
    backend: SQLModelBackend,
    *,
    start: date,
    end: date,
    tenant_id: str = "tenant-1",
) -> None:
    with backend.create_unit_of_work() as uow:
        current = start
        while current < end:
            calculation_id = f"calculation-{current.isoformat()}"
            completed_at = datetime.combine(current + timedelta(days=2), datetime.min.time(), tzinfo=UTC)
            uow.chargebacks.replace_calculation_lineage(  # type: ignore[attr-defined]
                AllocationLineageRunCapture(
                    ecosystem="confluent_cloud",
                    tenant_id=tenant_id,
                    tracking_date=current,
                    calculation_id=calculation_id,
                    captures=(),
                ),
                calculation_completed_at=completed_at,
            )
            uow.pipeline_state.upsert(
                PipelineState(
                    ecosystem="confluent_cloud",
                    tenant_id=tenant_id,
                    tracking_date=current,
                    billing_gathered=True,
                    resources_gathered=True,
                    chargeback_calculated=True,
                    calculation_id=calculation_id,
                    calculation_completed_at=completed_at,
                    calculation_run_id=None,
                )
            )
            current += timedelta(days=1)
        uow.commit()


def test_periodic_publication_lifecycle_is_visible_through_real_current_api(
    tmp_path: Path,
) -> None:
    from core.preview.artifacts import LocalPreviewArtifactStore
    from core.preview.generator import PreviewPackageGenerator
    from core.preview.revisions import PreviewRevisionService

    connection_string = f"sqlite:///{tmp_path / 'tenant.db'}"
    artifact_root = tmp_path / "artifacts"
    tenant = _tenant_config(connection_string)
    settings = AppSettings(
        features=FeaturesConfig(enable_periodic_refresh=True, refresh_interval=1),
        preview=PreviewConfig(artifact_root=artifact_root, max_workers=1),
        tenants={"production": tenant},
    )
    backend = SQLModelBackend(connection_string, CCloudStorageModule(), use_migrations=False)
    backend.create_tables()
    _seed_month(backend, billed_cost=Decimal("8"))
    worker_store = LocalPreviewArtifactStore(artifact_root)
    generator = PreviewPackageGenerator(
        max_csv_file_bytes=None,
        clock=lambda: datetime(2026, 8, 8, tzinfo=UTC),
    )
    identifiers = iter(f"revision-{index}" for index in range(1, 10))
    publisher = PreviewRevisionService(
        artifact_store=worker_store,
        package_generator=generator,
        clock=lambda: datetime(2026, 8, 4, tzinfo=UTC),
        revision_id_factory=lambda: next(identifiers),
    )
    runner = WorkflowRunner(
        settings,
        MagicMock(),
        revision_publisher=publisher,
        owned_preview_artifact_store=worker_store,
    )
    runner._tenant_runtimes["production"] = TenantRuntime(  # noqa: SLF001
        tenant_name="production",
        plugin=MagicMock(),
        storage=backend,
        orchestrator=MagicMock(),
        config_hash="stable",
        created_at=datetime(2026, 8, 4, tzinfo=UTC),
    )

    _run_periodic_cycle(runner, _result(), now=datetime(2026, 8, 4, tzinfo=UTC))
    _run_periodic_cycle(runner, _result(), now=datetime(2026, 8, 4, tzinfo=UTC))
    _seed_month(
        backend,
        billed_cost=Decimal("8"),
        billing_account_name="Revised billing organization",
    )
    _run_periodic_cycle(runner, _result(), now=datetime(2026, 8, 4, tzinfo=UTC))
    _run_periodic_cycle(runner, _result(), now=datetime(2026, 8, 7, tzinfo=UTC))
    _seed_month(
        backend,
        billed_cost=Decimal("8"),
        billing_account_name="Final billing organization",
    )
    _run_periodic_cycle(runner, _result(), now=datetime(2026, 8, 7, tzinfo=UTC))
    _run_periodic_cycle(runner, _result(errors=["failed"]), now=datetime(2026, 8, 7, tzinfo=UTC))

    engine = create_engine(connection_string)
    with engine.connect() as connection:
        revisions = (
            connection.execute(
                text(
                    "SELECT revision_id, monthly_status, is_current, supersedes_revision_id, "
                    "superseded_by_revision_id FROM preview_revisions ORDER BY published_at, revision_id"
                )
            )
            .mappings()
            .all()
        )
        request_count = connection.execute(text("SELECT COUNT(*) FROM preview_requests")).scalar_one()
    assert [row["revision_id"] for row in revisions] == [
        "revision-1",
        "revision-2",
        "revision-3",
        "revision-4",
    ]
    assert [row["monthly_status"] for row in revisions] == [
        "provisional",
        "provisional",
        "settled",
        "settled",
    ]
    assert [row["is_current"] for row in revisions] == [False, False, False, True]
    assert [row["supersedes_revision_id"] for row in revisions] == [
        None,
        "revision-1",
        "revision-2",
        "revision-3",
    ]
    assert [row["superseded_by_revision_id"] for row in revisions] == [
        "revision-2",
        "revision-3",
        "revision-4",
        None,
    ]
    assert request_count == 0

    app = create_app(settings)
    first_manifest_body = b""
    first_file_body = b""
    first_archive_body = b""
    with patch("workflow_runner.cleanup_orphaned_runs_for_all_tenants"), TestClient(app) as client:
        app.state.backends["production"] = backend
        metadata = client.get("/api/v1/tenants/production/focus-preview/revisions/current?month=2026-07")
        assert metadata.status_code == 200
        body = metadata.json()
        assert body["revision_id"] == "revision-4"
        assert body["monthly_status"] == "settled"
        manifest_response = client.get(body["package"]["manifest"]["download_url"])
        assert manifest_response.status_code == 200
        first_manifest_body = manifest_response.content
        file_response = client.get(body["package"]["files"][0]["download_url"])
        assert file_response.status_code == 200
        first_file_body = file_response.content
        archive = client.get(body["package"]["download_all_url"])
        assert archive.status_code == 200
        assert archive.headers["content-type"].startswith("application/zip")
        first_archive_body = archive.content

        stale = client.get(
            "/api/v1/tenants/production/focus-preview/revisions/current/manifest?month=2026-07&revision_id=revision-3"
        )
        assert stale.status_code == 409
        assert stale.json()["detail"]["retryable"] is True

    renamed_settings = settings.model_copy(update={"tenants": {"renamed": tenant}})
    renamed_app = create_app(renamed_settings)
    with patch("workflow_runner.cleanup_orphaned_runs_for_all_tenants"), TestClient(renamed_app) as client:
        renamed_app.state.backends["renamed"] = backend
        renamed = client.get("/api/v1/tenants/renamed/focus-preview/revisions/current?month=2026-07")
        assert renamed.status_code == 200
        renamed_body = renamed.json()
        assert renamed_body["tenant_name"] == "renamed"
        assert "/renamed/" in renamed_body["self_url"]
        renamed_manifest = client.get(renamed_body["package"]["manifest"]["download_url"])
        assert renamed_manifest.content == first_manifest_body
        assert renamed_manifest.json()["tenant_name"] == "production"
        assert client.get(renamed_body["package"]["files"][0]["download_url"]).content == first_file_body
        assert client.get(renamed_body["package"]["download_all_url"]).content == first_archive_body
        assert client.get("/api/v1/tenants/production/focus-preview/revisions/current?month=2026-07").status_code == 404

    engine.dispose()
    runner.close()


def test_concurrent_real_publication_deletes_loser_package_and_keeps_winner_readable(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    from core.preview.artifacts import LocalPreviewArtifactStore
    from core.preview.generator import PreviewPackageGenerator
    from core.preview.revisions import PreviewRevisionReadService, PreviewRevisionService

    connection_string = f"sqlite:///{tmp_path / 'race.db'}"
    backend = SQLModelBackend(connection_string, CCloudStorageModule(), use_migrations=False)
    backend.create_tables()
    _seed_month(backend, billed_cost=Decimal("8"))
    root = tmp_path / "race-artifacts"
    local_store = LocalPreviewArtifactStore(root)
    tenant = _tenant_config(connection_string)
    settings = AppSettings(
        features=FeaturesConfig(enable_periodic_refresh=True, refresh_interval=1),
        preview=PreviewConfig(artifact_root=root, max_workers=1),
        tenants={"production": tenant},
    )

    def scheduled_runner(publisher: Any, *, owned_store: Any) -> WorkflowRunner:
        runner = WorkflowRunner(
            settings,
            MagicMock(),
            revision_publisher=publisher,
            owned_preview_artifact_store=owned_store,
        )
        runner._tenant_runtimes["production"] = TenantRuntime(  # noqa: SLF001
            tenant_name="production",
            plugin=MagicMock(),
            storage=backend,
            orchestrator=MagicMock(),
            config_hash="stable",
            created_at=datetime(2026, 8, 7, tzinfo=UTC),
        )
        return runner

    initial_service = PreviewRevisionService(
        artifact_store=local_store,
        package_generator=PreviewPackageGenerator(max_csv_file_bytes=None),
        revision_id_factory=lambda: "revision-race-initial",
    )
    initial_runner = scheduled_runner(initial_service, owned_store=local_store)
    _run_periodic_cycle(initial_runner, _result(), now=datetime(2026, 8, 7, tzinfo=UTC))
    reader = PreviewRevisionReadService(artifact_store=local_store)
    initial = reader.get_current(
        backend=backend,
        ecosystem="confluent_cloud",
        tenant_id="tenant-1",
        month_start=date(2026, 7, 1),
    )
    assert initial is not None and initial.revision_id == "revision-race-initial"
    initial_manifest = local_store.read_manifest(initial.package.storage_key, initial.package.manifest)
    _seed_month(
        backend,
        billed_cost=Decimal("8"),
        billing_account_name="CAS replacement billing organization",
    )
    store = _BarrierArtifactStore(local_store, threading.Barrier(2))
    services = (
        PreviewRevisionService(
            artifact_store=store,
            package_generator=PreviewPackageGenerator(max_csv_file_bytes=None),
            revision_id_factory=lambda: "revision-race-a",
        ),
        PreviewRevisionService(
            artifact_store=store,
            package_generator=PreviewPackageGenerator(max_csv_file_bytes=None),
            revision_id_factory=lambda: "revision-race-b",
        ),
    )
    runners = tuple(scheduled_runner(service, owned_store=None) for service in services)

    def publish(runner: WorkflowRunner) -> None:
        _run_periodic_cycle_without_clock_patch(runner, _result())

    class FrozenDateTime(datetime):
        @classmethod
        def now(cls, tz: object = None) -> datetime:
            del tz
            return datetime(2026, 8, 7, tzinfo=UTC)

    threads = [threading.Thread(target=publish, args=(runner,)) for runner in runners]
    caplog.clear()
    with (
        patch("workflow_runner.datetime", FrozenDateTime),
        caplog.at_level(logging.ERROR, logger="core.preview.revisions"),
    ):
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=15)
    assert all(not thread.is_alive() for thread in threads)
    import workflow_runner as workflow_module

    assert workflow_module.datetime is datetime
    probe_publisher = MagicMock()
    probe_runner = scheduled_runner(probe_publisher, owned_store=None)
    before = datetime.now(UTC)
    probe_runner._publish_scheduled_revisions({"production": _result()})  # noqa: SLF001
    after = datetime.now(UTC)
    observed_now = probe_publisher.publish_eligible_months.call_args.kwargs["now"]
    assert before <= observed_now <= after
    assert "revision publication failed tenant=production month=2026-07" in caplog.text
    assert "error_type=PreviewRevisionConflictError" in caplog.text

    current = reader.get_current(
        backend=backend,
        ecosystem="confluent_cloud",
        tenant_id="tenant-1",
        month_start=date(2026, 7, 1),
    )
    assert current is not None
    assert current.revision_id in {"revision-race-a", "revision-race-b"}
    assert current.supersedes_revision_id == "revision-race-initial"
    assert local_store.read_manifest(initial.package.storage_key, initial.package.manifest) == initial_manifest
    assert reader.read_manifest(current)
    assert reader.read_file(current, current.package.files[0].name)[1]
    archive = reader.open_archive(current)
    try:
        assert b"".join(archive.iter_chunks())
    finally:
        archive.close()
    assert sorted(path.name for path in root.iterdir() if not path.name.startswith(".")) == sorted(
        [initial.package.storage_key, current.package.storage_key]
    )
    engine = create_engine(connection_string)
    with engine.connect() as connection:
        assert connection.execute(text("SELECT COUNT(*) FROM preview_revisions")).scalar_one() == 2
    engine.dispose()
    app = create_app(settings)
    with patch("workflow_runner.cleanup_orphaned_runs_for_all_tenants"), TestClient(app) as client:
        app.state.backends["production"] = backend
        response = client.get("/api/v1/tenants/production/focus-preview/revisions/current?month=2026-07")
        assert response.status_code == 200
        assert response.json()["revision_id"] == current.revision_id
        assert client.get(response.json()["package"]["manifest"]["download_url"]).status_code == 200
    assert local_store.cleanup_staging() == 0
    initial_runner.close()
    for runner in runners:
        runner.close()
    probe_runner.close()
    backend.dispose()


def test_periodic_real_generator_seeds_every_header_only_month_without_request_rows(
    tmp_path: Path,
) -> None:
    from core.preview.artifacts import LocalPreviewArtifactStore
    from core.preview.generator import PreviewPackageGenerator
    from core.preview.revisions import PreviewRevisionService

    connection_string = f"sqlite:///{tmp_path / 'header-only.db'}"
    backend = SQLModelBackend(connection_string, CCloudStorageModule(), use_migrations=False)
    backend.create_tables()
    _seed_calculation_days(backend, start=date(2026, 4, 1), end=date(2026, 7, 1))
    base_tenant = _tenant_config(connection_string)
    assert base_tenant.focus_preview is not None
    tenant = base_tenant.model_copy(
        update={
            "lookback_days": 130,
            "focus_preview": base_tenant.focus_preview.model_copy(
                update={
                    "effective_start_date": date(2026, 4, 1),
                    "effective_end_date": date(2026, 7, 1),
                }
            ),
        }
    )
    settings = AppSettings(
        features=FeaturesConfig(enable_periodic_refresh=True, refresh_interval=1),
        preview=PreviewConfig(artifact_root=tmp_path / "header-only-artifacts", max_workers=1),
        tenants={"production": tenant},
    )
    store = LocalPreviewArtifactStore(settings.preview.artifact_root)
    identifiers = iter(("revision-april", "revision-may", "revision-june"))
    publisher = PreviewRevisionService(
        artifact_store=store,
        package_generator=PreviewPackageGenerator(max_csv_file_bytes=None),
        revision_id_factory=lambda: next(identifiers),
    )
    runner = WorkflowRunner(
        settings,
        MagicMock(),
        revision_publisher=publisher,
        owned_preview_artifact_store=store,
    )
    runner._tenant_runtimes["production"] = TenantRuntime(  # noqa: SLF001
        tenant_name="production",
        plugin=MagicMock(),
        storage=backend,
        orchestrator=MagicMock(),
        config_hash="stable",
        created_at=datetime(2026, 8, 4, tzinfo=UTC),
    )

    _run_periodic_cycle(runner, _result(), now=datetime(2026, 8, 4, tzinfo=UTC))

    engine = create_engine(connection_string)
    with engine.connect() as connection:
        rows = (
            connection.execute(
                text("SELECT revision_id, month_start, is_current FROM preview_revisions ORDER BY month_start")
            )
            .mappings()
            .all()
        )
        assert connection.execute(text("SELECT COUNT(*) FROM preview_requests")).scalar_one() == 0
    engine.dispose()
    assert [row["revision_id"] for row in rows] == ["revision-april", "revision-may", "revision-june"]
    assert all(row["is_current"] for row in rows)

    app = create_app(settings)
    with patch("workflow_runner.cleanup_orphaned_runs_for_all_tenants"), TestClient(app) as client:
        app.state.backends["production"] = backend
        for month in ("2026-04", "2026-05", "2026-06"):
            response = client.get(f"/api/v1/tenants/production/focus-preview/revisions/current?month={month}")
            assert response.status_code == 200
            file_response = client.get(response.json()["package"]["files"][0]["download_url"])
            assert file_response.status_code == 200
            assert file_response.content.count(b"\n") == 1
    runner.close()


def test_real_header_only_publication_keeps_two_storage_owners_isolated(
    tmp_path: Path,
) -> None:
    from core.preview.artifacts import LocalPreviewArtifactStore
    from core.preview.generator import PreviewPackageGenerator
    from core.preview.revisions import PreviewRevisionReadService, PreviewRevisionService

    connection_string = f"sqlite:///{tmp_path / 'owners.db'}"
    backend = SQLModelBackend(connection_string, CCloudStorageModule(), use_migrations=False)
    backend.create_tables()
    for tenant_id in ("tenant-1", "tenant-2"):
        _seed_calculation_days(
            backend,
            start=date(2026, 7, 1),
            end=date(2026, 8, 1),
            tenant_id=tenant_id,
        )
    tenant_a = _tenant_config(connection_string)
    tenant_b = tenant_a.model_copy(update={"tenant_id": "tenant-2"})
    store = LocalPreviewArtifactStore(tmp_path / "owner-artifacts")
    identifiers = iter(("revision-owner-a", "revision-owner-b"))
    publisher = PreviewRevisionService(
        artifact_store=store,
        package_generator=PreviewPackageGenerator(max_csv_file_bytes=None),
        revision_id_factory=lambda: next(identifiers),
    )

    owner_a = publisher.publish_eligible_months(
        tenant_name="owner-a",
        tenant_config=tenant_a,
        backend=backend,
        now=datetime(2026, 8, 7, tzinfo=UTC),
    )
    owner_b = publisher.publish_eligible_months(
        tenant_name="owner-b",
        tenant_config=tenant_b,
        backend=backend,
        now=datetime(2026, 8, 7, tzinfo=UTC),
    )
    assert [row.revision_id for row in owner_a] == ["revision-owner-a"]
    assert [row.revision_id for row in owner_b] == ["revision-owner-b"]

    reader = PreviewRevisionReadService(artifact_store=store)
    a = reader.get_current(
        backend=backend,
        ecosystem="confluent_cloud",
        tenant_id="tenant-1",
        month_start=date(2026, 7, 1),
    )
    b = reader.get_current(
        backend=backend,
        ecosystem="confluent_cloud",
        tenant_id="tenant-2",
        month_start=date(2026, 7, 1),
    )
    assert a is not None and a.revision_id == "revision-owner-a"
    assert b is not None and b.revision_id == "revision-owner-b"
    assert b.revision_id not in reader.read_manifest(a).decode()
    assert a.revision_id not in reader.read_manifest(b).decode()
    backend.dispose()


@pytest.mark.parametrize("artifact", ["manifest", "file"])
@pytest.mark.parametrize("damage", ["delete", "corrupt"])
def test_real_physical_artifact_damage_is_redacted_across_http_delivery(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
    artifact: str,
    damage: str,
) -> None:
    from core.preview.artifacts import LocalPreviewArtifactStore
    from core.preview.generator import PreviewPackageGenerator
    from core.preview.revisions import PreviewRevisionService

    connection_string = f"sqlite:///{tmp_path / f'{artifact}-{damage}.db'}"
    backend = SQLModelBackend(connection_string, CCloudStorageModule(), use_migrations=False)
    backend.create_tables()
    _seed_calculation_days(backend, start=date(2026, 7, 1), end=date(2026, 8, 1))
    tenant = _tenant_config(connection_string)
    root = tmp_path / f"{artifact}-{damage}-artifacts"
    store = LocalPreviewArtifactStore(root)
    revision = PreviewRevisionService(
        artifact_store=store,
        package_generator=PreviewPackageGenerator(max_csv_file_bytes=None),
        revision_id_factory=lambda: "revision-damaged",
    ).publish_eligible_months(
        tenant_name="production",
        tenant_config=tenant,
        backend=backend,
        now=datetime(2026, 8, 7, tzinfo=UTC),
    )[0]
    metadata = revision.package.manifest if artifact == "manifest" else revision.package.files[0]
    path = root / revision.package.storage_key / metadata.name
    if damage == "delete":
        path.unlink()
    else:
        path.write_bytes(b"private /tmp/private tenant-1 corrupt bytes")
    settings = AppSettings(
        preview=PreviewConfig(artifact_root=root, max_workers=1),
        tenants={"production": tenant},
    )
    app = create_app(settings)
    with (
        caplog.at_level(logging.ERROR, logger="core.api.routes.focus_preview"),
        patch("workflow_runner.cleanup_orphaned_runs_for_all_tenants"),
        TestClient(app) as client,
    ):
        app.state.backends["production"] = backend
        current = client.get("/api/v1/tenants/production/focus-preview/revisions/current?month=2026-07").json()
        responses = {
            "manifest": client.get(current["package"]["manifest"]["download_url"]),
            "file": client.get(current["package"]["files"][0]["download_url"]),
            "archive": client.get(current["package"]["download_all_url"]),
        }
    expected_failures = {"manifest", "file", "archive"} if artifact == "manifest" else {"file", "archive"}
    for operation, response in responses.items():
        assert response.status_code == (500 if operation in expected_failures else 200)
        if operation in expected_failures:
            assert response.json() == {"detail": "Stored FOCUS Mapping Preview revision artifact is unavailable"}
            assert str(path) not in response.text
            assert "tenant-1" not in response.text
    assert str(path) not in caplog.text
    assert "tenant-1" not in caplog.text
    backend.dispose()


@pytest.mark.parametrize(
    ("corruption", "rewrite_material"),
    [
        ("identity", False),
        ("snapshot", False),
        ("files", False),
        *(
            (field, False)
            for field in (
                "mapping_profile_version",
                "target_focus_version",
                "column_profile",
                "effective_columns",
                "logical_data_sha256",
            )
        ),
        *(
            (field, True)
            for field in (
                "mapping_profile_version",
                "target_focus_version",
                "column_profile",
                "effective_columns",
                "logical_data_sha256",
            )
        ),
    ],
)
def test_canonical_manifest_corruption_is_redacted_across_real_http_delivery(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
    corruption: str,
    rewrite_material: bool,
) -> None:
    from core.preview.artifacts import LocalPreviewArtifactStore
    from core.preview.generator import PreviewPackageGenerator
    from core.preview.mapping import preview_revision_content_sha256
    from core.preview.revisions import PreviewRevisionReadService, PreviewRevisionService

    connection_string = f"sqlite:///{tmp_path / f'canonical-{corruption}.db'}"
    backend = SQLModelBackend(connection_string, CCloudStorageModule(), use_migrations=False)
    backend.create_tables()
    _seed_calculation_days(backend, start=date(2026, 7, 1), end=date(2026, 8, 1))
    tenant = _tenant_config(connection_string)
    root = tmp_path / f"canonical-{corruption}-artifacts"
    store = LocalPreviewArtifactStore(root)
    service = PreviewRevisionService(
        artifact_store=store,
        package_generator=PreviewPackageGenerator(max_csv_file_bytes=None),
        revision_id_factory=lambda: "revision-canonical-corrupt",
    )
    settings = AppSettings(
        features=FeaturesConfig(enable_periodic_refresh=True, refresh_interval=1),
        preview=PreviewConfig(artifact_root=root, max_workers=1),
        tenants={"production": tenant},
    )
    runner = WorkflowRunner(
        settings,
        MagicMock(),
        revision_publisher=service,
        owned_preview_artifact_store=store,
    )
    runner._tenant_runtimes["production"] = TenantRuntime(  # noqa: SLF001
        tenant_name="production",
        plugin=MagicMock(),
        storage=backend,
        orchestrator=MagicMock(),
        config_hash="stable",
        created_at=datetime(2026, 8, 7, tzinfo=UTC),
    )
    _run_periodic_cycle(runner, _result(), now=datetime(2026, 8, 7, tzinfo=UTC))
    revision = PreviewRevisionReadService(artifact_store=store).get_current(
        backend=backend,
        ecosystem="confluent_cloud",
        tenant_id="tenant-1",
        month_start=date(2026, 7, 1),
    )
    assert revision is not None
    path = root / revision.package.storage_key / revision.package.manifest.name
    manifest = json.loads(path.read_bytes())
    if corruption == "identity":
        manifest["tenant_name"] = "private-tenant-name"
    elif corruption == "snapshot":
        manifest["source_snapshot"]["calculation_coverage"][0]["calculation_id"] = "private-calculation"
    elif corruption == "files":
        manifest["files"][0]["size_bytes"] += 1
    elif corruption == "effective_columns":
        manifest[corruption] = list(reversed(manifest[corruption]))
    elif corruption == "logical_data_sha256":
        manifest[corruption] = "e" * 64
    else:
        manifest[corruption] = f"{manifest[corruption]}-changed"
    if rewrite_material and corruption in {
        "mapping_profile_version",
        "target_focus_version",
        "column_profile",
        "effective_columns",
        "logical_data_sha256",
    }:
        manifest["material_sha256"] = preview_revision_content_sha256(
            mapping_profile_version=manifest["mapping_profile_version"],
            target_focus_version=manifest["target_focus_version"],
            column_profile=manifest["column_profile"],
            effective_columns=tuple(manifest["effective_columns"]),
            logical_data_sha256=manifest["logical_data_sha256"],
        )
    body = json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode()
    path.write_bytes(body)
    manifest_metadata = {
        "name": revision.package.manifest.name,
        "media_type": revision.package.manifest.media_type,
        "size_bytes": len(body),
        "sha256": hashlib.sha256(body).hexdigest(),
        "order": None,
    }
    engine = create_engine(connection_string)
    with engine.begin() as connection:
        connection.execute(
            text("UPDATE preview_revisions SET manifest_metadata_json = :metadata WHERE revision_id = :revision_id"),
            {
                "metadata": json.dumps(manifest_metadata, sort_keys=True, separators=(",", ":")),
                "revision_id": revision.revision_id,
            },
        )
    engine.dispose()
    app = create_app(settings)
    with (
        caplog.at_level(logging.ERROR, logger="core.api.routes.focus_preview"),
        patch("workflow_runner.cleanup_orphaned_runs_for_all_tenants"),
        TestClient(app) as client,
    ):
        app.state.backends["production"] = backend
        current = client.get("/api/v1/tenants/production/focus-preview/revisions/current?month=2026-07").json()
        responses = (
            client.get(current["package"]["manifest"]["download_url"]),
            client.get(current["package"]["files"][0]["download_url"]),
            client.get(current["package"]["download_all_url"]),
        )
    for response in responses:
        assert response.status_code == 500
        assert response.json() == {"detail": "Stored FOCUS Mapping Preview revision artifact is unavailable"}
        assert "private" not in response.text
        assert revision.package.storage_key not in response.text
    assert "private" not in caplog.text
    assert revision.package.storage_key not in caplog.text
    runner.close()
    backend.dispose()


def test_builder_supplied_material_mismatch_is_rejected_through_periodic_publish_and_http(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from core.preview import revisions as revision_module
    from core.preview.artifacts import LocalPreviewArtifactStore
    from core.preview.generator import PreviewPackageGenerator

    connection_string = f"sqlite:///{tmp_path / 'builder-material.db'}"
    backend = SQLModelBackend(connection_string, CCloudStorageModule(), use_migrations=False)
    backend.create_tables()
    _seed_month(backend, billed_cost=Decimal("8"))
    tenant = _tenant_config(connection_string)
    root = tmp_path / "builder-material-artifacts"
    store = LocalPreviewArtifactStore(root)
    service = revision_module.PreviewRevisionService(
        artifact_store=store,
        package_generator=PreviewPackageGenerator(max_csv_file_bytes=None),
        revision_id_factory=iter(("revision-builder-current", "revision-builder-mismatch")).__next__,
    )
    settings = AppSettings(
        features=FeaturesConfig(enable_periodic_refresh=True, refresh_interval=1),
        preview=PreviewConfig(artifact_root=root, max_workers=1),
        tenants={"production": tenant},
    )
    runner = WorkflowRunner(
        settings,
        MagicMock(),
        revision_publisher=service,
        owned_preview_artifact_store=store,
    )
    runner._tenant_runtimes["production"] = TenantRuntime(  # noqa: SLF001
        tenant_name="production",
        plugin=MagicMock(),
        storage=backend,
        orchestrator=MagicMock(),
        config_hash="stable",
        created_at=datetime(2026, 8, 7, tzinfo=UTC),
    )
    original_builder = revision_module.build_preview_revision_manifest

    def mismatched_builder(**kwargs: Any) -> bytes:
        kwargs["material_sha256"] = "f" * 64
        return original_builder(**kwargs)

    _run_periodic_cycle(runner, _result(), now=datetime(2026, 8, 7, tzinfo=UTC))
    engine = create_engine(connection_string)
    with engine.begin() as connection:
        connection.execute(
            text(
                "UPDATE resources SET display_name = 'builder replacement material' "
                "WHERE tenant_id = 'tenant-1' AND resource_type = 'organization'"
            )
        )
    monkeypatch.setattr(revision_module, "build_preview_revision_manifest", mismatched_builder)
    _run_periodic_cycle(runner, _result(), now=datetime(2026, 8, 7, tzinfo=UTC))

    app = create_app(settings)
    with patch("workflow_runner.cleanup_orphaned_runs_for_all_tenants"), TestClient(app) as client:
        app.state.backends["production"] = backend
        current = client.get("/api/v1/tenants/production/focus-preview/revisions/current?month=2026-07")
        assert current.status_code == 200
        body = current.json()
        assert body["revision_id"] == "revision-builder-current"
        responses = (
            client.get(body["package"]["manifest"]["download_url"]),
            client.get(body["package"]["files"][0]["download_url"]),
            client.get(body["package"]["download_all_url"]),
        )
    assert all(response.status_code == 200 for response in responses)
    with engine.connect() as connection:
        assert connection.execute(text("SELECT COUNT(*) FROM preview_revisions")).scalar_one() == 1
    engine.dispose()
    runner.close()
    backend.dispose()


def test_real_api_masks_missing_current_owner_and_unknown_current_file(tmp_path: Path) -> None:
    from core.preview.artifacts import LocalPreviewArtifactStore
    from core.preview.generator import PreviewPackageGenerator
    from core.preview.revisions import PreviewRevisionService

    connection_string = f"sqlite:///{tmp_path / 'masked-missing.db'}"
    backend = SQLModelBackend(connection_string, CCloudStorageModule(), use_migrations=False)
    backend.create_tables()
    _seed_calculation_days(backend, start=date(2026, 7, 1), end=date(2026, 8, 1))
    tenant = _tenant_config(connection_string)
    other_connection_string = f"sqlite:///{tmp_path / 'masked-other-owner.db'}"
    other_backend = SQLModelBackend(other_connection_string, CCloudStorageModule(), use_migrations=False)
    other_backend.create_tables()
    other_tenant = _tenant_config(other_connection_string).model_copy(update={"tenant_id": "tenant-2"})
    root = tmp_path / "masked-missing-artifacts"
    store = LocalPreviewArtifactStore(root)
    PreviewRevisionService(
        artifact_store=store,
        package_generator=PreviewPackageGenerator(max_csv_file_bytes=None),
        revision_id_factory=lambda: "revision-masked",
    ).publish_eligible_months(
        tenant_name="production",
        tenant_config=tenant,
        backend=backend,
        now=datetime(2026, 8, 7, tzinfo=UTC),
    )
    app = create_app(
        AppSettings(
            preview=PreviewConfig(artifact_root=root, max_workers=1),
            tenants={"production": tenant, "other-owner": other_tenant},
        )
    )
    with patch("workflow_runner.cleanup_orphaned_runs_for_all_tenants"), TestClient(app) as client:
        app.state.backends["production"] = backend
        app.state.backends["other-owner"] = other_backend
        missing = client.get("/api/v1/tenants/other-owner/focus-preview/revisions/current?month=2026-07")
        unknown_file = client.get(
            "/api/v1/tenants/production/focus-preview/revisions/current/files/private.csv"
            "?month=2026-07&revision_id=revision-masked"
        )
    assert missing.status_code == 404
    assert missing.json() == {"detail": "Current FOCUS Mapping Preview revision not found"}
    assert unknown_file.status_code == 404
    assert unknown_file.json() == {"detail": "FOCUS Mapping Preview file not found for current revision"}
    assert "tenant-1" not in missing.text
    assert "tenant-1" not in unknown_file.text
    assert "tenant-2" not in missing.text
    backend.dispose()
    other_backend.dispose()


@pytest.mark.parametrize(
    "failure_layer",
    ["eligibility", "source", "allocation", "row-validation", "staging"],
)
def test_real_layered_publication_failures_preserve_current_row_and_artifact(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
    failure_layer: str,
) -> None:
    from core.preview.artifacts import LocalPreviewArtifactStore
    from core.preview.generator import PreviewPackageGenerator
    from core.preview.revisions import PreviewRevisionReadService, PreviewRevisionService

    connection_string = f"sqlite:///{tmp_path / f'failure-{failure_layer}.db'}"
    backend = SQLModelBackend(connection_string, CCloudStorageModule(), use_migrations=False)
    backend.create_tables()
    _seed_month(backend, billed_cost=Decimal("8"))
    tenant = _tenant_config(connection_string)
    root = tmp_path / f"failure-{failure_layer}-artifacts"
    store = LocalPreviewArtifactStore(root)
    identifiers = iter(("revision-current", "revision-must-not-publish"))
    service = PreviewRevisionService(
        artifact_store=store,
        package_generator=PreviewPackageGenerator(max_csv_file_bytes=None),
        revision_id_factory=lambda: next(identifiers),
    )
    settings = AppSettings(
        features=FeaturesConfig(enable_periodic_refresh=True, refresh_interval=1),
        preview=PreviewConfig(artifact_root=root, max_workers=1),
        tenants={"production": tenant},
    )
    runner = WorkflowRunner(
        settings,
        MagicMock(),
        revision_publisher=service,
        owned_preview_artifact_store=store,
    )
    runner._tenant_runtimes["production"] = TenantRuntime(  # noqa: SLF001
        tenant_name="production",
        plugin=MagicMock(),
        storage=backend,
        orchestrator=MagicMock(),
        config_hash="stable",
        created_at=datetime(2026, 8, 7, tzinfo=UTC),
    )
    _run_periodic_cycle(runner, _result(), now=datetime(2026, 8, 7, tzinfo=UTC))
    reader = PreviewRevisionReadService(artifact_store=store)
    current = reader.get_current(
        backend=backend,
        ecosystem="confluent_cloud",
        tenant_id="tenant-1",
        month_start=date(2026, 7, 1),
    )
    assert current is not None and current.revision_id == "revision-current"
    current_manifest = store.read_manifest(current.package.storage_key, current.package.manifest)
    engine = create_engine(connection_string)
    configured_tenant = tenant
    blocked_root: Path | None = None
    if failure_layer == "eligibility":
        assert tenant.focus_preview is not None
        configured_tenant = tenant.model_copy(
            update={"focus_preview": tenant.focus_preview.model_copy(update={"billing_currency": "EUR"})}
        )
        runner._settings = settings.model_copy(  # noqa: SLF001
            update={"tenants": {"production": configured_tenant}}
        )
    elif failure_layer == "source":
        with engine.begin() as connection:
            connection.execute(
                text(
                    "UPDATE ccloud_cost_source_records SET malformed = 1, "
                    "diagnostics_json = '[\"persisted malformed source\"]' "
                    "WHERE source_record_id = 'provider:cost-2026-07-01'"
                )
            )
    elif failure_layer == "allocation":
        with engine.begin() as connection:
            connection.execute(
                text(
                    "DELETE FROM ccloud_allocation_lineage_runs "
                    "WHERE tenant_id = 'tenant-1' AND tracking_date = '2026-07-01'"
                )
            )
    elif failure_layer == "row-validation":
        with engine.begin() as connection:
            connection.execute(
                text(
                    "UPDATE resources SET display_name = '' "
                    "WHERE tenant_id = 'tenant-1' AND resource_type = 'organization'"
                )
            )
    else:
        with engine.begin() as connection:
            connection.execute(
                text(
                    "UPDATE resources SET display_name = 'material replacement' "
                    "WHERE tenant_id = 'tenant-1' AND resource_type = 'organization'"
                )
            )
        blocked_root = tmp_path / "preserved-artifact-root"
        root.rename(blocked_root)
        root.write_bytes(b"configured staging path is not a directory")

    caplog.clear()
    try:
        with caplog.at_level(logging.WARNING):
            _run_periodic_cycle(runner, _result(), now=datetime(2026, 8, 7, tzinfo=UTC))
    finally:
        if blocked_root is not None:
            root.unlink()
            blocked_root.rename(root)

    expected_log = {
        "eligibility": "diagnostic_code=preview_billing_currency_unsupported",
        "source": "diagnostic_code=preview_source_record_malformed",
        "allocation": "diagnostic_code=preview_allocation_lineage_incomplete",
        "row-validation": "diagnostic_code=preview_mapping_validation_failed",
        "staging": "revision publication failed tenant=production month=2026-07 error_type=NotADirectoryError",
    }[failure_layer]
    assert expected_log in caplog.text

    with engine.connect() as connection:
        rows = (
            connection.execute(
                text(
                    "SELECT revision_id, is_current, superseded_by_revision_id "
                    "FROM preview_revisions ORDER BY revision_id"
                )
            )
            .mappings()
            .all()
        )
    engine.dispose()
    assert rows == [{"revision_id": "revision-current", "is_current": True, "superseded_by_revision_id": None}]
    assert [path.name for path in root.iterdir() if not path.name.startswith(".")] == [current.package.storage_key]
    app = create_app(settings)
    with patch("workflow_runner.cleanup_orphaned_runs_for_all_tenants"), TestClient(app) as client:
        app.state.backends["production"] = backend
        retained = client.get("/api/v1/tenants/production/focus-preview/revisions/current?month=2026-07")
        assert retained.status_code == 200
        assert retained.json()["revision_id"] == "revision-current"
        delivered_manifest = client.get(retained.json()["package"]["manifest"]["download_url"])
        assert delivered_manifest.status_code == 200
        assert delivered_manifest.content == current_manifest
    runner.close()
    backend.dispose()

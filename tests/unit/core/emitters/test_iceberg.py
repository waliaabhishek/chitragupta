from __future__ import annotations

import builtins
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack, closing, contextmanager
from datetime import UTC, date, datetime, timedelta
from sqlite3 import connect
from threading import Event
from time import monotonic
from types import SimpleNamespace
from typing import TYPE_CHECKING, cast
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from core import emitters
from core.api.app import create_app
from core.config.models import AppSettings, StorageConfig, TenantConfig
from core.emitters import iceberg
from core.emitters.iceberg import IcebergExportError, IcebergSchedule, publish_iceberg
from core.models.pipeline import PipelineState
from core.storage.backends.sqlmodel.module import CoreStorageModule
from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend
from core.storage.iceberg import IcebergExportKey, IcebergStorageDeadlineError
from workflow_runner import WorkflowRunner

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path

    from core.storage.interface import StorageBackend

DAY = date(2026, 11, 18)
NOW = datetime(2026, 11, 21, tzinfo=UTC)
DISCOVERY_SECONDS = (
    iceberg._SCHEDULED_PASS_SECONDS - iceberg._ATTEMPT_SECONDS - iceberg._HANDOFF_SECONDS - iceberg._CLEANUP_SECONDS
)


def test_catalog_import_time_counts_toward_pass_deadline_and_utc_date(monkeypatch: pytest.MonkeyPatch) -> None:
    clock = monotonic()
    original_import = builtins.__import__

    def delayed_import(
        name: str,
        global_namespace: dict[str, object] | None = None,
        local_namespace: dict[str, object] | None = None,
        fromlist: tuple[str, ...] = (),
        level: int = 0,
    ) -> object:
        nonlocal clock
        if name == "core.emitters" and fromlist == ("iceberg_process",):
            clock += 2
        return original_import(name, global_namespace, local_namespace, fromlist, level)

    monkeypatch.setattr(iceberg, "monotonic", lambda: clock)
    monkeypatch.setattr(emitters, "iceberg_process", SimpleNamespace(), raising=False)
    with monkeypatch.context() as import_patch:
        import_patch.setattr(builtins, "__import__", delayed_import)
        catalogs = iceberg._Catalogs(NOW - timedelta(seconds=1), lambda: False, 2)

    assert catalogs.stopped()
    assert catalogs.utc_now() == NOW + timedelta(seconds=1)


@pytest.mark.parametrize("startup_during_lease", [False, True], ids=["acquire", "release"])
def test_prepared_iceberg_lease_does_not_wait_for_api_startup(startup_during_lease: bool) -> None:
    settings = AppSettings(
        tenants={
            name: TenantConfig(
                ecosystem="generic_metrics_only",
                tenant_id=name,
                storage=StorageConfig(connection_string=f"sqlite:///{name}.db"),
            )
            for name in ("export", "cold")
        }
    )
    startup_entered, startup_release, export_entered = Event(), Event(), Event()

    def block_startup(_settings: object) -> None:
        startup_entered.set()
        assert startup_release.wait(5)

    export_plugin, cold_plugin = MagicMock(), MagicMock()
    cold_plugin.initialize.side_effect = block_startup
    registry = MagicMock()
    registry.create.side_effect = [export_plugin, cold_plugin]
    export_storage, cold_storage = MagicMock(), MagicMock()
    cold_uow = cold_storage.create_read_only_unit_of_work.return_value.__enter__.return_value
    cold_uow.pipeline_runs.get_latest_run.return_value = None
    runner = WorkflowRunner(settings, registry)

    def run_export() -> None:
        with runner._acquire_iceberg_backend("export") as backend:
            assert backend is export_storage
            export_entered.set()
            if startup_during_lease:
                assert startup_entered.wait(5)

    with (
        patch("core.storage.registry.create_storage_backend", side_effect=[export_storage, cold_storage]),
        TestClient(create_app(settings, workflow_runner=runner, mode="both")) as client,
        ThreadPoolExecutor(max_workers=2) as pool,
    ):
        with runner.acquire_backend("export", settings.tenants["export"]):
            pass
        try:
            if startup_during_lease:
                export = pool.submit(run_export)
                assert export_entered.wait(5)
            status = pool.submit(client.get, "/api/v1/tenants/cold/pipeline/status")
            assert startup_entered.wait(5)
            if not startup_during_lease:
                export = pool.submit(run_export)
            export.result(timeout=1)
            assert not status.done()
        finally:
            startup_release.set()
        assert status.result(timeout=5).status_code == 200


def _tenant(name: str, *, source: str | None = None) -> TenantConfig:
    return TenantConfig.model_validate(
        {
            "ecosystem": "generic_metrics_only",
            "tenant_id": source or name,
            "plugin_settings": {
                "emitters": [
                    {
                        "type": "iceberg",
                        "params": {
                            "table": f"finance.{name}",
                            "catalog": {"type": "rest", "uri": "https://catalog.example.test", "token": "test"},
                        },
                    },
                ]
            },
        }
    )


@pytest.mark.parametrize("blocked", ["claim", "checkpoint", "source"])
def test_blocked_tenant_defers_remaining_dates_without_spending_the_shared_slot(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, blocked: str
) -> None:
    tenants = {name: _tenant(name) for name in ("busy", "healthy")}
    ecosystem = tenants["healthy"].ecosystem
    loads: Counter[str] = Counter()
    claims: Counter[str] = Counter()
    written: list[str] = []

    with ExitStack() as stack:
        backends: dict[str, SQLModelBackend] = {}
        for name, days in (("busy", (DAY, DAY + timedelta(days=1))), ("healthy", (NOW.date(),))):
            backend = SQLModelBackend(f"sqlite:///{tmp_path / f'{name}.db'}", CoreStorageModule(), use_migrations=False)
            stack.callback(backend.dispose)
            backend.create_tables()
            backends[name] = backend
            with backend.create_unit_of_work() as uow:
                for day in days:
                    uow.pipeline_state.upsert(PipelineState(ecosystem, name, day, chargeback_calculated=True))
                uow.commit()

        if blocked != "claim":
            monkeypatch.setattr(iceberg, "_ATTEMPT_SECONDS", 0.5)
            monkeypatch.setattr(iceberg, "_LOOKUP_SECONDS", 0.5)
            if blocked == "source":
                backends["busy"].dispose()
            writer = stack.enter_context(closing(connect(tmp_path / "busy.db")))
            if blocked == "source":
                assert writer.execute("PRAGMA journal_mode=DELETE").fetchone() == ("delete",)
                writer.execute("BEGIN EXCLUSIVE")
            else:
                writer.execute("BEGIN IMMEDIATE")

        @contextmanager
        def acquire_backend(name: str) -> Iterator[SQLModelBackend | None]:
            claims[name] += 1
            yield None if blocked == "claim" and name == "busy" and claims[name] > 1 else backends[name]

        def overwrite_batches(batches: Iterator[list[dict[str, object]]], *, tenant_id: str, **_: object) -> int:
            assert list(batches) == []
            written.append(tenant_id)
            return 1

        @contextmanager
        def open_table(_settings: object, identifier: str, **_: object) -> Iterator[SimpleNamespace]:
            loads[identifier] += 1
            yield SimpleNamespace(uuid=identifier, overwrite_batches=overwrite_batches)

        monkeypatch.setattr(
            emitters,
            "iceberg_process",
            SimpleNamespace(open_iceberg_table=open_table, IcebergDestinationError=IcebergExportError),
            raising=False,
        )
        started = monotonic()
        summary = publish_iceberg(tenants, acquire_backend=acquire_backend, now=NOW, limit=1)
        elapsed = monotonic() - started
        if blocked != "claim":
            assert elapsed < 3.0  # Ordinary SQLite UoWs use a five-second busy timeout.
            writer.rollback()
            with backends["busy"].create_consistent_read_unit_of_work() as uow:
                assert uow.iceberg_exports.get(IcebergExportKey("finance.busy", ecosystem, "busy", DAY)) is None

    assert (summary.attempted, summary.confirmed, summary.errors) == (1, 1, int(blocked != "claim"))
    assert written == ["healthy"]
    assert loads["finance.busy"] == (1 if blocked == "source" else 2)


class _ConflictError(Exception):
    pass


class _UnknownCommitError(Exception):
    pass


class _Pass:
    """Use a controlled clock for catalog and SQL stalls. Exercise the real scheduler."""

    def __init__(self, monkeypatch: pytest.MonkeyPatch, tenants: dict[str, TenantConfig]) -> None:
        self.tenants = tenants
        self.schedule = IcebergSchedule()
        self.clock = monotonic()
        self.catalog_delays: dict[str, float] = {}
        self.source_delays: dict[str, float] = {}
        self.checkpoint_delays: dict[str, float] = {}
        self.unknown_checkpoint_commits: set[str] = set()
        self.failed_catalogs: set[str] = set()
        self.uuids: dict[str, str] = {}
        self.states = {
            name: [PipelineState(tenant.ecosystem, tenant.tenant_id, DAY, chargeback_calculated=True)]
            for name, tenant in tenants.items()
        }
        self.capture_delays: dict[tuple[str, date], float] = {}
        self.capture_states: dict[tuple[str, date], PipelineState] = {}
        self.events: list[tuple[str, str]] = []
        self.attempt_times: dict[tuple[str, date], float] = {}
        self.outcomes: dict[tuple[str, date], str] = {}
        self.settlement_slack: list[float] = []
        self.shutdown = False
        self.interrupt_capture = False
        monkeypatch.setattr(iceberg, "monotonic", lambda: self.clock)
        monkeypatch.setattr(
            emitters,
            "iceberg_process",
            SimpleNamespace(
                open_iceberg_table=self.open_table,
                IcebergDestinationError=IcebergExportError,
                IcebergConflict=_ConflictError,
                IcebergUnconfirmed=_UnknownCommitError,
            ),
            raising=False,
        )

    def wait(self, seconds: float, deadline: float) -> None:
        next_time = self.clock + seconds
        self.clock = min(next_time, deadline)
        if next_time > deadline:
            raise IcebergStorageDeadlineError

    @contextmanager
    def open_table(
        self, _settings: object, identifier: str, *, deadline: float, **_: object
    ) -> Iterator[SimpleNamespace]:
        name = identifier.rsplit(".", 1)[1]
        self.events.append(("catalog", name))
        self.wait(self.catalog_delays.get(name, 0), deadline)
        if name in self.failed_catalogs:
            raise IcebergExportError("table_unavailable")

        def overwrite_batches(rows: Iterator[list[dict[str, object]]], **_: object) -> int:
            assert list(rows) == []
            self.events.append(("publish", name))
            return 7

        yield SimpleNamespace(uuid=self.uuids.get(name, identifier), overwrite_batches=overwrite_batches)

    @contextmanager
    def acquire(self, name: str) -> Iterator[StorageBackend]:
        def state_for(day: date) -> PipelineState | None:
            return next((state for state in self.states[name] if state.tracking_date == day), None)

        @contextmanager
        def read(*, deadline: float) -> Iterator[SimpleNamespace]:
            def captured_state(*args: object) -> PipelineState | None:
                day = cast("date", args[2])
                return self.capture_states.get((name, day), state_for(day))

            def find_by_range(*_: object) -> list[PipelineState]:
                self.events.append(("discover", name))
                self.wait(self.source_delays.get(name, 0), deadline)
                return self.states[name]

            def iter_iceberg_chargebacks(*args: object, **_: object) -> Iterator[object]:
                day = cast("datetime", args[2]).date()
                assert self.outcomes[name, day] == "unconfirmed"
                if self.interrupt_capture:
                    self.shutdown = True
                    raise IcebergExportError("export_interrupted")
                self.wait(self.capture_delays.get((name, day), 0), deadline)
                return iter(())

            yield SimpleNamespace(
                pipeline_state=SimpleNamespace(find_by_range=find_by_range, get=captured_state),
                iceberg_exports=SimpleNamespace(find_by_range=lambda *_: [], get=lambda _: None),
                iter_iceberg_chargebacks=iter_iceberg_chargebacks,
            )

        @contextmanager
        def write(*, deadline: float) -> Iterator[SimpleNamespace]:
            admitted = False

            def record(key: IcebergExportKey, revision: str | None, _: datetime) -> None:
                nonlocal admitted
                state = state_for(key.date)
                assert state is not None and revision == state.calculation_id
                self.events.append(("checkpoint", name))
                self.wait(self.checkpoint_delays.get(name, 0), deadline)
                self.attempt_times[name, key.date] = self.clock
                self.outcomes[name, key.date] = "unconfirmed"
                admitted = True

            def finish(key: IcebergExportKey, outcome: str, *_: object, **__: object) -> None:
                self.settlement_slack.append(deadline - self.clock)
                self.outcomes[name, key.date] = outcome

            def commit() -> None:
                if admitted and name in self.unknown_checkpoint_commits:
                    raise OSError

            yield SimpleNamespace(
                pipeline_state=SimpleNamespace(get=lambda *_args: state_for(_args[2])),
                iceberg_exports=SimpleNamespace(
                    get=lambda _: None,
                    record_attempt=record,
                    fail_attempt=lambda key, code, **kwargs: finish(key, code, **kwargs),
                    confirm_attempt=lambda key, *args: finish(key, "confirmed", *args),
                ),
                commit=commit,
            )

        yield cast(
            "StorageBackend",
            SimpleNamespace(create_iceberg_read_unit_of_work=read, create_iceberg_write_unit_of_work=write),
        )

    def run(self, *, limit: int | None = 10) -> iceberg.IcebergPassSummary:
        self.events.clear()
        return publish_iceberg(
            self.tenants,
            acquire_backend=self.acquire,
            now=NOW,
            schedule=self.schedule,
            shutdown_check=lambda: self.shutdown,
            limit=limit,
        )


@pytest.mark.parametrize("stall", ["catalog", "source"])
@pytest.mark.parametrize("healthy_first", [False, True], ids=["healthy-last", "healthy-first"])
def test_discovery_stalls_preserve_publication_time(
    monkeypatch: pytest.MonkeyPatch, stall: str, healthy_first: bool
) -> None:
    blocked = tuple(f"blocked_{index:02}" for index in range(21))
    names = ("healthy", *blocked) if healthy_first else (*blocked, "healthy")
    cycle = _Pass(monkeypatch, {name: _tenant(name) for name in names})
    delays = cycle.catalog_delays if stall == "catalog" else cycle.source_delays
    delays.update(dict.fromkeys(blocked, 1))
    for name in blocked:
        cycle.states[name].clear()
    settlement_seconds = iceberg._ATTEMPT_SECONDS / 10 + 0.25
    cycle.capture_delays["healthy", DAY] = iceberg._ATTEMPT_SECONDS - settlement_seconds
    if healthy_first:
        started = cycle.clock
        assert cycle.run(limit=1).confirmed == 1
        assert cycle.outcomes["healthy", DAY] == "confirmed"
        assert cycle.events.index(("discover", "healthy")) < cycle.events.index(("catalog", blocked[0]))
    else:
        # The last stalled lookup of each full turn gets the first lookup next time.
        for _ in range((len(blocked) - 1) // (DISCOVERY_SECONDS - 1)):
            assert cycle.run(limit=1).confirmed == 0
            assert ("healthy", DAY) not in cycle.outcomes
        started = cycle.clock
        assert cycle.run(limit=1).confirmed == 1
        assert ("discover", "healthy") in cycle.events
        assert cycle.outcomes["healthy", DAY] == "confirmed"
    assert cycle.clock - started < iceberg._SCHEDULED_PASS_SECONDS
    assert cycle.settlement_slack == [pytest.approx(settlement_seconds)]


def test_discovery_prioritizes_the_partial_group_then_rotates(monkeypatch: pytest.MonkeyPatch) -> None:
    pass_seconds = 15 + iceberg._ATTEMPT_SECONDS + iceberg._HANDOFF_SECONDS + iceberg._CLEANUP_SECONDS
    monkeypatch.setattr(iceberg, "_SCHEDULED_PASS_SECONDS", pass_seconds)
    names = ("slow_one", "slow_two", "slow_three", "healthy")
    cycle = _Pass(monkeypatch, {name: _tenant(name) for name in names})
    cycle.catalog_delays.update(zip(names, (5, 5, 4, 2), strict=True))
    for name in names[:-1]:
        cycle.states[name].clear()
    assert cycle.run().confirmed == 0
    assert cycle.schedule.discovery == (cycle.tenants["healthy"].ecosystem, cycle.tenants["healthy"].tenant_id)
    assert cycle.run(limit=1).confirmed == 1
    assert cycle.events[0] == ("catalog", "healthy")
    assert cycle.outcomes["healthy", DAY] == "confirmed"


def test_discovery_rotates_when_first_group_spends_its_turn(monkeypatch: pytest.MonkeyPatch) -> None:
    names = ("blocked", "healthy")
    pass_seconds = 4 + iceberg._ATTEMPT_SECONDS + iceberg._HANDOFF_SECONDS + iceberg._CLEANUP_SECONDS
    monkeypatch.setattr(iceberg, "_SCHEDULED_PASS_SECONDS", pass_seconds)
    cycle = _Pass(monkeypatch, {name: _tenant(name) for name in names})
    cycle.source_delays["blocked"] = 10
    assert cycle.run().confirmed == 0
    assert cycle.run(limit=1).confirmed == 1
    assert cycle.events[0] == ("catalog", "healthy")


@pytest.mark.parametrize("collision", [False, True], ids=["unresolved-peer", "duplicate-owner"])
def test_discovery_checks_peer_group_before_source_sql(monkeypatch: pytest.MonkeyPatch, collision: bool) -> None:
    cycle = _Pass(
        monkeypatch, {"owner": _tenant("owner"), "healthy": _tenant("healthy"), "peer": _tenant("peer", source="owner")}
    )
    if collision:
        cycle.uuids["peer"] = "finance.owner"
    else:
        cycle.failed_catalogs.add("peer")
    result = cycle.run()
    assert (result.confirmed, result.errors) == (1, 2)
    assert cycle.events[:3] == [("catalog", "owner"), ("catalog", "peer"), ("catalog", "healthy")]
    assert [name for stage, name in cycle.events if stage == "discover"] == ["healthy"]


@pytest.mark.parametrize("discovery_time", [0.0, 1.0, 2.5])
def test_publication_requires_full_attempt_and_continues_past_blocked_dates(
    monkeypatch: pytest.MonkeyPatch, discovery_time: float
) -> None:
    names = ("a", "b", "healthy")
    cycle = _Pass(monkeypatch, {name: _tenant(name) for name in names})
    cycle.source_delays.update(a=2.5, b=2.5, healthy=discovery_time)
    cycle.checkpoint_delays.update(a=iceberg._ATTEMPT_SECONDS + 1, b=iceberg._ATTEMPT_SECONDS + 1)
    for blocked, next_name in zip(names[:-1], names[1:], strict=True):
        result = cycle.run(limit=1)
        assert (result.attempted, result.errors) == (0, 1)
        assert [name for stage, name in cycle.events if stage == "checkpoint"] == [blocked]
        assert [name for stage, name in cycle.events if stage == "catalog"].count(next_name) == 1
        assert cycle.schedule.publication is not None and cycle.schedule.publication[2] == next_name
    assert cycle.run(limit=1).confirmed == 1
    assert cycle.outcomes == {("healthy", DAY): "confirmed"}
    assert cycle.schedule.publication is None


def test_publication_preserves_position_for_a_deferred_binding(monkeypatch: pytest.MonkeyPatch) -> None:
    cycle = _Pass(monkeypatch, {name: _tenant(name) for name in ("a", "waiting", "z")})
    position = (datetime.combine(DAY, datetime.min.time(), tzinfo=UTC), DAY, "waiting")
    cycle.schedule.publication = position
    cycle.failed_catalogs.add("waiting")
    assert cycle.run(limit=1).confirmed == 1
    assert cycle.outcomes == {("z", DAY): "confirmed"}
    assert cycle.schedule.publication == position
    cycle.failed_catalogs.clear()
    assert cycle.run(limit=1).confirmed == 1
    assert cycle.outcomes["waiting", DAY] == "confirmed"
    assert cycle.schedule.publication is None


def test_publication_rotates_past_stalled_fallbacks_while_primary_is_deferred(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    names = ("a_deferred", "b_stalled", "c_stalled", "healthy")
    cycle = _Pass(monkeypatch, {name: _tenant(name) for name in names})
    primary = (datetime.combine(DAY, datetime.min.time(), tzinfo=UTC), DAY, "a_deferred")
    cycle.schedule.publication = primary
    cycle.failed_catalogs.add("a_deferred")
    cycle.source_delays.update(b_stalled=2.5, c_stalled=2.5)
    cycle.checkpoint_delays.update(b_stalled=iceberg._ATTEMPT_SECONDS + 1, c_stalled=iceberg._ATTEMPT_SECONDS + 1)

    for expected in ("b_stalled", "c_stalled"):
        result = cycle.run(limit=1)
        assert result.attempted == 0
        assert [name for stage, name in cycle.events if stage == "checkpoint"] == [expected]
        assert cycle.schedule.publication == primary

    assert cycle.run(limit=1).confirmed == 1
    assert cycle.outcomes == {("healthy", DAY): "confirmed"}
    assert cycle.schedule.publication == primary
    assert cycle.schedule.publication_fallback is None

    cycle.failed_catalogs.clear()
    assert cycle.run(limit=1).confirmed == 1
    assert cycle.outcomes["a_deferred", DAY] == "confirmed"
    assert cycle.schedule.publication is None


def test_publication_skips_iceberg_writes_if_sql_admission_is_unknown(monkeypatch: pytest.MonkeyPatch) -> None:
    cycle = _Pass(monkeypatch, {name: _tenant(name) for name in ("a", "healthy")})
    cycle.unknown_checkpoint_commits.add("a")
    result = cycle.run(limit=1)
    assert (result.attempted, result.confirmed, result.errors) == (1, 1, 1)
    assert ("publish", "a") not in cycle.events
    assert cycle.outcomes == {("a", DAY): "unconfirmed", ("healthy", DAY): "confirmed"}


@pytest.mark.parametrize("changed_to", ["invalid", "superseded", "incomplete"])
def test_durable_attempt_rechecks_source_revision_before_capture(
    monkeypatch: pytest.MonkeyPatch, changed_to: str
) -> None:
    cycle = _Pass(monkeypatch, {"a": _tenant("a")})
    revision = PipelineState("generic_metrics_only", "a", DAY, chargeback_calculated=changed_to != "incomplete")
    if changed_to in {"invalid", "superseded"}:
        revision.calculation_id = "new"
    if changed_to == "superseded":
        revision.calculation_completed_at = NOW
    cycle.capture_states["a", DAY] = revision
    result = cycle.run()
    assert (result.attempted, result.failed) == (1, 1)
    assert cycle.outcomes["a", DAY] == (
        "invalid_calculation_revision" if changed_to == "invalid" else "source_revision_changed"
    )
    assert ("publish", "a") not in cycle.events


@pytest.mark.parametrize("interrupt", [False, True], ids=["deadline", "shutdown"])
def test_durable_attempt_keeps_time_for_outcome(monkeypatch: pytest.MonkeyPatch, interrupt: bool) -> None:
    cycle = _Pass(monkeypatch, {"a": _tenant("a")})
    cycle.states["a"].append(PipelineState("generic_metrics_only", "a", NOW.date(), chargeback_calculated=True))
    cycle.capture_delays["a", DAY] = iceberg._ATTEMPT_SECONDS + 1
    cycle.interrupt_capture = interrupt
    result = cycle.run()
    assert cycle.settlement_slack[0] == pytest.approx(iceberg._ATTEMPT_SECONDS / (1 if interrupt else 10))
    assert cycle.outcomes["a", DAY] == ("export_interrupted" if interrupt else "source_deadline")
    assert result.failed == 1
    if interrupt:
        assert result.attempted == 1
    else:
        assert result.attempted == 1 and result.confirmed == 0
        assert cycle.schedule.publication is not None
        assert cycle.schedule.publication[1] == NOW.date()
        assert cycle.run().confirmed == 1
        assert cycle.outcomes["a", NOW.date()] == "confirmed"


@pytest.mark.parametrize("failed_lookup", [1, 2], ids=["discovery", "attempt"])
def test_unreaped_destination_stops_the_pass(monkeypatch: pytest.MonkeyPatch, failed_lookup: int) -> None:
    from core.emitters.iceberg_process import IcebergDestinationError

    cycle = _Pass(monkeypatch, {name: _tenant(name) for name in ("a", "healthy")})
    lookups = 0

    @contextmanager
    def open_table(
        settings: object, identifier: str, *, deadline: float, **kwargs: object
    ) -> Iterator[SimpleNamespace]:
        nonlocal lookups
        if identifier.endswith(".a"):
            lookups += 1
            if lookups == failed_lookup:
                raise IcebergDestinationError("destination_reap")
        with cycle.open_table(settings, identifier, deadline=deadline, **kwargs) as table:
            yield table

    monkeypatch.setattr(emitters.iceberg_process, "open_iceberg_table", open_table)
    monkeypatch.setattr(emitters.iceberg_process, "IcebergDestinationError", IcebergDestinationError)
    with pytest.raises(IcebergDestinationError, match="^destination_reap$"):
        cycle.run()
    assert ("publish", "healthy") not in cycle.events

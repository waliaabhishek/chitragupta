from __future__ import annotations

import hashlib
import sqlite3
import tomllib
import tracemalloc
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, get_type_hints

import pytest

from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend
from plugins.confluent_cloud.storage.module import CCloudStorageModule
from tests.integration.core.api.backend_provider import FixedTenantBackendProvider
from tests.unit.core.preview.conftest import preview_module
from tests.unit.core.preview.test_artifacts import _data_files, _owner, _publish
from tests.unit.core.preview.test_monthly_v5 import _lineage, _row
from tests.unit.core.preview.test_revision_mapping import _monthly_request, _settled_snapshot
from tests.unit.core.preview.test_service import (
    ControlledExecutor,
    _aggregate,
    _allocation,
    _seed,
    _source,
    _tenant_config,
)


class _TrackedSqliteConnection:
    def __init__(self, connection: sqlite3.Connection, tracker: _DirectSqliteConnectionTracker) -> None:
        self._connection = connection
        self._tracker = tracker
        self._closed = False

    def execute(self, sql: str, parameters: Any = ()) -> sqlite3.Cursor:
        self._tracker.executions.append((sql, parameters))
        if self._tracker.fail_query is not None and self._tracker.fail_query in sql:
            raise sqlite3.OperationalError("synthetic tracked query failure")
        return self._connection.execute(sql, parameters)

    def executemany(self, sql: str, parameters: Any) -> sqlite3.Cursor:
        return self._connection.executemany(sql, parameters)

    def executescript(self, sql_script: str) -> sqlite3.Cursor:
        return self._connection.executescript(sql_script)

    def cursor(self, *args: object, **kwargs: object) -> sqlite3.Cursor:
        return self._connection.cursor(*args, **kwargs)

    def commit(self) -> None:
        self._connection.commit()

    def rollback(self) -> None:
        self._connection.rollback()

    def close(self) -> None:
        if self._closed:
            return
        self._connection.close()
        self._closed = True
        self._tracker.explicit_closes += 1
        self._tracker.live -= 1

    def __enter__(self) -> _TrackedSqliteConnection:
        self._connection.__enter__()
        return self

    def __exit__(self, *args: object) -> bool | None:
        if self._closed:
            return None
        return self._connection.__exit__(*args)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._connection, name)


class _DirectSqliteConnectionTracker:
    def __init__(self, real_connect: Any, target_path: Path | None = None) -> None:
        self._real_connect = real_connect
        self._target_path = target_path
        self._connections: list[_TrackedSqliteConnection] = []
        self.opens = 0
        self.explicit_closes = 0
        self.live = 0
        self.max_live = 0
        self.executions: list[tuple[str, Any]] = []
        self.fail_query: str | None = None

    def connect(self, *args: object, **kwargs: object) -> Any:
        database = args[0] if args else kwargs.get("database")
        tracks = self._tracks(database)
        if tracks and len(args) < 5 and "check_same_thread" not in kwargs:
            kwargs = {**kwargs, "check_same_thread": False}
        connection = self._real_connect(*args, **kwargs)
        if not tracks:
            return connection
        tracked = _TrackedSqliteConnection(connection, self)
        self._connections.append(tracked)
        self.opens += 1
        self.live += 1
        self.max_live = max(self.max_live, self.live)
        return tracked

    def _tracks(self, database: object) -> bool:
        if database is None:
            return False
        path = Path(str(database))
        if self._target_path is not None:
            return path == self._target_path
        return path.name == "projection.sqlite" and path.parent.name.endswith(".workspace")

    def cleanup(self) -> None:
        for connection in self._connections:
            if not connection._closed:
                connection._connection.close()
                connection._closed = True
                self.live -= 1


def _install_direct_sqlite_tracker(
    spooling: Any,
    monkeypatch: pytest.MonkeyPatch,
    *,
    target_path: Path | None = None,
) -> _DirectSqliteConnectionTracker:
    tracker = _DirectSqliteConnectionTracker(spooling.sqlite3.connect, target_path)
    monkeypatch.setattr(spooling.sqlite3, "connect", tracker.connect)
    return tracker


def _artifact_catalog(tmp_path: Path, *, count: int = 257) -> tuple[Path, tuple[tuple[Any, ...], ...]]:
    root = tmp_path / "catalog.workspace"
    root.mkdir()
    catalog_path = root / "projection.sqlite"
    expected: list[tuple[Any, ...]] = []
    connection = sqlite3.connect(catalog_path)
    try:
        connection.execute(
            """
            CREATE TABLE artifacts (
                file_order INTEGER PRIMARY KEY,
                name TEXT NOT NULL UNIQUE,
                media_type TEXT NOT NULL,
                path TEXT NOT NULL,
                size_bytes INTEGER NOT NULL,
                sha256 TEXT NOT NULL
            )
            """
        )
        for order in range(1, count + 1):
            body = f"row-{order:04d}\n".encode()
            path = root / f"part-{order:04d}.csv"
            path.write_bytes(body)
            name = path.name
            media_type = "text/csv"
            sha256 = hashlib.sha256(body).hexdigest()
            expected.append((name, media_type, order, path, len(body), sha256, body))
        connection.executemany(
            """
            INSERT INTO artifacts (file_order, name, media_type, path, size_bytes, sha256)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                (order, name, media_type, str(path), size_bytes, sha256)
                for name, media_type, order, path, size_bytes, sha256, _body in expected
            ),
        )
        connection.commit()
    finally:
        connection.close()
    return catalog_path, tuple(expected)


def _payload_value(payload: Any) -> tuple[Any, ...]:
    body = b"".join(payload.body.iter_chunks(chunk_size=3))
    return (
        payload.name,
        payload.media_type,
        payload.order,
        payload.body.path,
        payload.body.size_bytes,
        payload.body.sha256,
        body,
    )


def _metadata_value(metadata: Any) -> tuple[Any, ...]:
    return (
        metadata.name,
        metadata.media_type,
        metadata.order,
        metadata.size_bytes,
        metadata.sha256,
    )


def _expected_metadata(expected: tuple[tuple[Any, ...], ...]) -> tuple[tuple[Any, ...], ...]:
    return tuple(
        (name, media_type, order, size_bytes, sha256)
        for name, media_type, order, _path, size_bytes, sha256, _body in expected
    )


def _collection_case(
    spooling: Any,
    catalog_path: Path,
    expected: tuple[tuple[Any, ...], ...],
    collection_name: str,
) -> tuple[Any, tuple[tuple[Any, ...], ...], Any]:
    payloads = spooling.PreviewSpooledArtifactCollection(catalog_path)
    if collection_name == "payload":
        return payloads, expected, _payload_value
    return payloads.metadata, _expected_metadata(expected), _metadata_value


def test_verified_artifact_stream_hashes_before_return_and_yields_exact_stored_chunks(
    preview_artifact_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    artifacts = preview_module("artifacts")
    store = artifacts.LocalPreviewArtifactStore(preview_artifact_root)
    stored, _manifest = _publish(store)

    def forbid_read_bytes(path: Path) -> bytes:
        raise AssertionError(f"whole-file read forbidden: {path}")

    monkeypatch.setattr(Path, "read_bytes", forbid_read_bytes)
    stream = store.open_verified(stored.storage_key, stored.files[0])
    assert isinstance(stream, artifacts.PreviewVerifiedArtifactStream)
    assert stream.size_bytes == len(_data_files()[0].body)
    with stream:
        assert b"".join(stream.iter_chunks(chunk_size=3)) == _data_files()[0].body
    stream.close()
    stream.close()


def test_runtime_and_revision_reader_require_the_explicit_streaming_store_contract() -> None:
    artifacts = preview_module("artifacts")
    service = preview_module("service")
    revisions = preview_module("revisions")

    assert get_type_hints(service.PreviewRuntime.__init__)["artifact_store"] is artifacts.PreviewRuntimeArtifactStore
    assert issubclass(
        artifacts.PreviewRuntimeArtifactStore,
        artifacts.PreviewStreamingArtifactStore,
    )
    assert (
        get_type_hints(revisions.PreviewRevisionReadService.__init__)["artifact_store"]
        is artifacts.PreviewStreamingArtifactStore
    )


def test_verified_artifact_stream_rejects_corruption_before_returning_a_stream(
    preview_artifact_root: Path,
) -> None:
    artifacts = preview_module("artifacts")
    store = artifacts.LocalPreviewArtifactStore(preview_artifact_root)
    stored, _manifest = _publish(store)
    target = preview_artifact_root / stored.storage_key / stored.files[0].name
    target.write_bytes(b"corrupt")

    with pytest.raises(artifacts.PreviewArtifactIntegrityError):
        store.open_verified(stored.storage_key, stored.files[0])


def test_startup_cleanup_removes_stale_sibling_workspace_and_unpublished_staging(
    preview_artifact_root: Path,
) -> None:
    artifacts = preview_module("artifacts")
    store = artifacts.LocalPreviewArtifactStore(preview_artifact_root)
    owner = _owner()
    owner_token = artifacts.preview_storage_owner_token(owner)
    package_id = "a" * 32
    owner_root = preview_artifact_root / ".staging" / "v1" / owner_token
    lock_root = preview_artifact_root / ".locks" / "v1" / owner_token
    staging = owner_root / f"{package_id}.staging"
    workspace = owner_root / f"{package_id}.workspace"
    lock = lock_root / f"{package_id}.lock"
    staging.mkdir(parents=True)
    workspace.mkdir()
    (workspace / "projection.sqlite").write_bytes(b"workspace")
    lock.parent.mkdir(parents=True)
    lock.touch()

    assert store.cleanup_staging(owner) == 1
    assert not staging.exists()
    assert not workspace.exists()
    assert not lock.exists()


def test_production_generation_session_allocates_sibling_workspace_and_stable_lock_before_generation(
    preview_artifact_root: Path,
) -> None:
    artifacts = preview_module("artifacts")
    store = artifacts.LocalPreviewArtifactStore(preview_artifact_root)
    generation = store.begin_generation(
        owner=_owner(),
        request_id="request-before-generation",
        max_spool_bytes=1024 * 1024,
    )
    workspace = generation.workspace.root
    package_id = workspace.name.removesuffix(".workspace")
    staging = workspace.with_name(f"{package_id}.staging")
    lock = (
        preview_artifact_root / ".locks" / "v1" / artifacts.preview_storage_owner_token(_owner()) / f"{package_id}.lock"
    )
    try:
        assert workspace.is_dir()
        assert staging.is_dir()
        assert lock.is_file()
        with lock.open("a+b") as competing, pytest.raises(BlockingIOError):
            artifacts.fcntl.flock(
                competing.fileno(),
                artifacts.fcntl.LOCK_EX | artifacts.fcntl.LOCK_NB,
            )
    finally:
        generation.close()
    assert not workspace.exists()
    assert not staging.exists()
    assert not lock.exists()


def test_production_generation_session_rejects_oversized_bytes_before_staging_write(
    preview_artifact_root: Path,
) -> None:
    artifacts = preview_module("artifacts")
    models = preview_module("models")
    spooling = preview_module("spooling")
    store = artifacts.LocalPreviewArtifactStore(preview_artifact_root)
    generation = store.begin_generation(
        owner=_owner(),
        request_id="request-hard-ceiling",
        max_spool_bytes=8,
    )
    workspace = generation.workspace.root
    staging = workspace.with_name(f"{workspace.name.removesuffix('.workspace')}.staging")
    try:
        with pytest.raises(
            spooling.PreviewGenerationSpoolLimitError,
            match="FOCUS Mapping Preview package exceeds the configured generation spool limit",
        ):
            generation.stage_data_files(
                (
                    models.PreviewArtifactPayload(
                        name="cost-and-usage.csv",
                        media_type="text/csv",
                        order=1,
                        body=b"123456789",
                    ),
                )
            )
        assert not tuple(staging.iterdir())
        assert generation.workspace.used_bytes == 0
    finally:
        generation.close()
    assert not workspace.exists()
    assert not staging.exists()


def test_workspace_accounting_never_drops_or_omits_an_accounting_root(
    tmp_path: Path,
) -> None:
    spooling = preview_module("spooling")
    staging = tmp_path / "package.staging"
    staging.mkdir()
    workspace = spooling.PreviewGenerationWorkspace(
        16 * 1024,
        root=tmp_path / "package.workspace",
        accounting_roots=(staging,),
    )
    try:
        (workspace.root / "evidence.sqlite").write_bytes(b"e" * 4096)
        (staging / "already-staged.csv").write_bytes(b"s" * 2048)
        workspace.enforce_limit()
        accounted = workspace.used_bytes

        workspace.enforce_used_bytes(1)

        assert accounted == 6144
        assert workspace.used_bytes == accounted
        with pytest.raises(spooling.PreviewGenerationSpoolLimitError):
            workspace.preflight_write(workspace.limit_bytes - accounted + 1)
        assert workspace._disk_usage() <= workspace.limit_bytes  # noqa: SLF001
    finally:
        workspace.close()


def test_authoritative_workspace_reconciliation_reclaims_deleted_bytes(
    tmp_path: Path,
) -> None:
    spooling = preview_module("spooling")
    workspace = spooling.PreviewGenerationWorkspace(
        8 * 1024,
        root=tmp_path / "reclaim.workspace",
    )
    try:
        transient = workspace.root / "transient.sqlite"
        transient.write_bytes(b"x" * 6144)
        workspace.enforce_limit()
        assert workspace.used_bytes == 6144

        transient.unlink()
        workspace.enforce_limit()
        assert workspace.used_bytes == 0

        workspace.record_write(7000)
        final = workspace.root / "final.csv"
        final.write_bytes(b"y" * 7000)
        workspace.enforce_limit()
        workspace.enforce_used_bytes(1)

        assert workspace.used_bytes == 7000
        with pytest.raises(spooling.PreviewGenerationSpoolLimitError):
            workspace.preflight_write(1193)
    finally:
        workspace.close()


def test_concurrent_sqlite_workspaces_do_not_mutate_process_global_temp_directory(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spooling = preview_module("spooling")
    real_connect = spooling.sqlite3.connect
    traced_statements: list[str] = []

    def traced_connect(*args: object, **kwargs: object) -> Any:
        connection = real_connect(*args, **kwargs)
        connection.set_trace_callback(traced_statements.append)
        return connection

    monkeypatch.setattr(spooling.sqlite3, "connect", traced_connect)
    workspaces = (
        spooling.PreviewGenerationWorkspace(8 * 1024 * 1024, root=tmp_path / "a.workspace"),
        spooling.PreviewGenerationWorkspace(8 * 1024 * 1024, root=tmp_path / "b.workspace"),
    )

    def spill(workspace: Any) -> tuple[int, int]:
        with workspace.sqlite_connection(workspace.root / "projection.sqlite") as connection:
            connection.execute(
                """
                CREATE TABLE ordered_rows (
                    sort_key TEXT NOT NULL,
                    encounter_ordinal INTEGER NOT NULL,
                    record BLOB NOT NULL,
                    PRIMARY KEY (sort_key, encounter_ordinal)
                ) WITHOUT ROWID
                """
            )
            connection.executemany(
                "INSERT INTO ordered_rows VALUES (?, ?, ?)",
                ((f"{2048 - index:05d}", index, b"x" * 512) for index in range(2048)),
            )
            connection.commit()
            workspace.enforce_limit()
            first = connection.execute(
                "SELECT encounter_ordinal FROM ordered_rows ORDER BY sort_key, encounter_ordinal LIMIT 1"
            ).fetchone()
            assert first is not None
            return int(first[0]), workspace.used_bytes

    try:
        with ThreadPoolExecutor(max_workers=2) as executor:
            results = tuple(executor.map(spill, workspaces))
        assert results[0][0] == results[1][0]
        assert all(0 < used <= workspace.limit_bytes for (_, used), workspace in zip(results, workspaces, strict=True))
        assert all("temp_store_directory" not in statement.lower() for statement in traced_statements)
        assert all(
            tuple(path.name for path in workspace.root.iterdir()) == ("projection.sqlite",) for workspace in workspaces
        )
    finally:
        for workspace in workspaces:
            workspace.close()


def test_workspace_sqlite_connection_explicitly_closes_after_success(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spooling = preview_module("spooling")
    workspace = spooling.PreviewGenerationWorkspace(
        8 * 1024 * 1024,
        root=tmp_path / "success.workspace",
    )
    database_path = workspace.root / "projection.sqlite"
    tracker = _install_direct_sqlite_tracker(spooling, monkeypatch, target_path=database_path)
    try:
        with workspace.sqlite_connection(database_path) as connection:
            connection.execute("CREATE TABLE control (value INTEGER NOT NULL)")
        assert tracker.opens == tracker.explicit_closes == 1
        assert tracker.live == 0
    finally:
        tracker.cleanup()
        workspace.close()


def test_workspace_sqlite_connection_explicitly_closes_before_exception_propagates(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spooling = preview_module("spooling")
    workspace = spooling.PreviewGenerationWorkspace(
        8 * 1024 * 1024,
        root=tmp_path / "exception.workspace",
    )
    database_path = workspace.root / "projection.sqlite"
    tracker = _install_direct_sqlite_tracker(spooling, monkeypatch, target_path=database_path)
    try:
        with (
            pytest.raises(RuntimeError, match="consumer failure"),
            workspace.sqlite_connection(database_path),
        ):
            raise RuntimeError("consumer failure")
        assert tracker.opens == tracker.explicit_closes == 1
        assert tracker.live == 0
    finally:
        tracker.cleanup()
        workspace.close()


@pytest.mark.parametrize("collection_name", ["payload", "metadata"])
def test_spooled_collection_scalar_and_index_operations_close_before_returning_or_raising(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    collection_name: str,
) -> None:
    spooling = preview_module("spooling")
    catalog_path, expected = _artifact_catalog(tmp_path, count=3)
    collection, expected_values, value_of = _collection_case(spooling, catalog_path, expected, collection_name)
    tracker = _install_direct_sqlite_tracker(spooling, monkeypatch, target_path=catalog_path)
    try:
        assert len(collection) == 3
        assert tracker.live == 0
        assert value_of(collection[1]) == expected_values[1]
        assert tracker.live == 0
        assert value_of(collection[-1]) == expected_values[-1]
        assert tracker.live == 0
        with pytest.raises(IndexError):
            collection[3]
        assert tracker.live == 0
        with pytest.raises(IndexError):
            collection[-4]
        assert tracker.live == 0
        assert tracker.opens == tracker.explicit_closes
    finally:
        tracker.cleanup()


@pytest.mark.parametrize("collection_name", ["payload", "metadata"])
def test_spooled_collection_complete_iteration_preserves_exact_values_and_closes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    collection_name: str,
) -> None:
    spooling = preview_module("spooling")
    catalog_path, expected = _artifact_catalog(tmp_path, count=3)
    collection, expected_values, value_of = _collection_case(spooling, catalog_path, expected, collection_name)
    tracker = _install_direct_sqlite_tracker(spooling, monkeypatch, target_path=catalog_path)
    try:
        assert tuple(value_of(item) for item in collection) == expected_values
        assert tracker.live == 0
        assert tracker.opens == tracker.explicit_closes
    finally:
        tracker.cleanup()


@pytest.mark.parametrize("collection_name", ["payload", "metadata"])
def test_spooled_collection_partial_and_consumer_aborted_iteration_never_yield_with_live_connection(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    collection_name: str,
) -> None:
    spooling = preview_module("spooling")
    catalog_path, expected = _artifact_catalog(tmp_path, count=3)
    collection, expected_values, value_of = _collection_case(spooling, catalog_path, expected, collection_name)
    tracker = _install_direct_sqlite_tracker(spooling, monkeypatch, target_path=catalog_path)
    try:
        retained_iterator = iter(collection)
        assert value_of(next(retained_iterator)) == expected_values[0]
        assert tracker.live == 0

        for item in collection:
            assert value_of(item) == expected_values[0]
            assert tracker.live == 0
            break

        with pytest.raises(RuntimeError, match="consumer failure"):
            for item in collection:
                assert value_of(item) == expected_values[0]
                assert tracker.live == 0
                raise RuntimeError("consumer failure")
        assert tracker.live == 0
        assert tracker.opens == tracker.explicit_closes
    finally:
        tracker.cleanup()


@pytest.mark.parametrize("collection_name", ["payload", "metadata"])
def test_spooled_collection_query_exception_closes_before_propagating(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    collection_name: str,
) -> None:
    spooling = preview_module("spooling")
    catalog_path, expected = _artifact_catalog(tmp_path, count=3)
    collection, _expected_values, _value_of = _collection_case(spooling, catalog_path, expected, collection_name)
    tracker = _install_direct_sqlite_tracker(spooling, monkeypatch, target_path=catalog_path)
    tracker.fail_query = "SELECT COUNT(*)"
    try:
        with pytest.raises(sqlite3.OperationalError, match="synthetic tracked query failure"):
            len(collection)
        assert tracker.live == 0
        assert tracker.opens == tracker.explicit_closes == 1
    finally:
        tracker.cleanup()


@pytest.mark.parametrize("collection_name", ["payload", "metadata"])
def test_spooled_collection_multi_batch_iteration_is_bounded_ordered_and_connection_free(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    collection_name: str,
) -> None:
    spooling = preview_module("spooling")
    catalog_path, expected = _artifact_catalog(tmp_path)
    collection, expected_values, value_of = _collection_case(spooling, catalog_path, expected, collection_name)
    tracker = _install_direct_sqlite_tracker(spooling, monkeypatch, target_path=catalog_path)
    try:
        assert tuple(value_of(item) for item in collection) == expected_values
        iteration_queries = [
            (sql, parameters)
            for sql, parameters in tracker.executions
            if "FROM artifacts" in sql and "file_order >" in sql
        ]
        assert len(iteration_queries) >= 2
        assert all("LIMIT ?" in sql for sql, _parameters in iteration_queries)
        assert all(tuple(parameters)[-1] == spooling.SQLITE_BATCH_SIZE for _sql, parameters in iteration_queries)
        assert tracker.max_live == 1
        assert tracker.live == 0
        assert tracker.opens == tracker.explicit_closes
    finally:
        tracker.cleanup()


@pytest.mark.parametrize("collection_name", ["payload", "metadata"])
def test_repeated_spooled_catalog_access_returns_live_count_to_zero_after_every_operation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    collection_name: str,
) -> None:
    spooling = preview_module("spooling")
    catalog_path, _expected = _artifact_catalog(tmp_path, count=3)
    payloads = spooling.PreviewSpooledArtifactCollection(catalog_path)
    collection = payloads if collection_name == "payload" else payloads.metadata
    tracker = _install_direct_sqlite_tracker(spooling, monkeypatch, target_path=catalog_path)
    try:
        for _repeat in range(3):
            assert len(collection) == 3
            assert tracker.live == 0
            assert collection[1].order == 2
            assert tracker.live == 0
            assert tuple(item.order for item in collection) == (1, 2, 3)
            assert tracker.live == 0
            retained_iterator = iter(collection)
            assert next(retained_iterator).order == 1
            assert tracker.live == 0
        assert tracker.opens == tracker.explicit_closes
    finally:
        tracker.cleanup()


def test_repeated_real_bounded_package_construction_and_staging_close_all_direct_connections(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    artifacts = preview_module("artifacts")
    mapping = preview_module("mapping")
    spooling = preview_module("spooling")
    store = artifacts.LocalPreviewArtifactStore(tmp_path / "artifacts")
    request = _monthly_request()
    snapshot = _settled_snapshot()
    rows = (
        _row(day=2, AllocatedResourceId="sa-b"),
        _row(day=1, AllocatedResourceId="sa-a"),
    )
    reconciliation = mapping.PreviewPackageReconciliation(
        source_records=2,
        source_cost=Decimal("16"),
        allocated_cost=Decimal("16"),
        source_quantity=Decimal("10"),
        allocated_quantity=Decimal("10"),
    )
    baseline = mapping.build_preview_data_package(
        request=request,
        snapshot=snapshot,
        full_rows=rows,
        reconciliation=reconciliation,
        max_csv_file_bytes=None,
    )
    lines = baseline.data_files[0].body.splitlines(keepends=True)
    part_limit = len(lines[0]) + max(len(line) for line in lines[1:])
    tracker = _install_direct_sqlite_tracker(spooling, monkeypatch)
    try:
        for repeat in range(3):
            generation = store.begin_generation(
                owner=_owner(),
                request_id=f"repeated-direct-sqlite-{repeat}",
                max_spool_bytes=16 * 1024 * 1024,
            )
            try:
                draft = mapping.build_bounded_preview_data_package(
                    request=request,
                    snapshot=snapshot,
                    full_rows=iter(rows),
                    reconciliation=reconciliation,
                    max_csv_file_bytes=part_limit,
                    max_generation_spool_bytes=generation.workspace.limit_bytes,
                    workspace=generation.workspace,
                )
                assert tracker.live == 0
                generation.stage_data_files(draft.data_files)
                assert tracker.live == 0
            finally:
                generation.close()
            assert tracker.live == 0
        assert tracker.opens > 0
        assert tracker.opens == tracker.explicit_closes
    finally:
        tracker.cleanup()


def test_pytest_configuration_does_not_hide_unclosed_database_resource_warnings() -> None:
    project = tomllib.loads(Path("pyproject.toml").read_text(encoding="utf-8"))
    warning_filters = project["tool"]["pytest"]["ini_options"]["filterwarnings"]

    assert not any("unclosed database" in item and "ResourceWarning" in item for item in warning_filters)


def test_explicit_workspace_close_surfaces_and_logs_cleanup_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    spooling = preview_module("spooling")
    workspace = spooling.PreviewGenerationWorkspace(1024, root=tmp_path / "failure.workspace")

    def fail_cleanup(_path: Path) -> None:
        raise OSError("synthetic cleanup failure")

    monkeypatch.setattr(spooling.shutil, "rmtree", fail_cleanup)
    with caplog.at_level("ERROR"), pytest.raises(OSError, match="synthetic cleanup failure"):
        workspace.close()
    assert "generation workspace cleanup failed" in caplog.text
    monkeypatch.undo()
    workspace.close()


def test_recovery_preserves_referenced_final_artifact_and_removes_its_crash_workspace(
    preview_artifact_root: Path,
) -> None:
    artifacts = preview_module("artifacts")
    store = artifacts.LocalPreviewArtifactStore(preview_artifact_root)
    owner = _owner()
    stored, _manifest = _publish(store)
    package_id = stored.storage_key.rsplit("-", 1)[1]
    owner_token = artifacts.preview_storage_owner_token(owner)
    workspace = preview_artifact_root / ".staging" / "v1" / owner_token / f"{package_id}.workspace"
    workspace.mkdir(parents=True)
    (workspace / "catalog.sqlite").write_bytes(b"workspace")

    removed = store.reconcile_finalized(
        owner=owner,
        referenced_storage_keys=frozenset({stored.storage_key}),
        is_referenced=lambda storage_key: storage_key == stored.storage_key,
    )

    assert removed == 0
    assert (preview_artifact_root / stored.storage_key).is_dir()
    assert not workspace.exists()


def _spool_limited_runtime(
    tmp_path: Path,
    backend: SQLModelBackend,
    executor: ControlledExecutor,
) -> Any:
    artifacts = preview_module("artifacts")
    service = preview_module("service")
    request_ids = iter(("spool-request-1", "spool-request-2"))
    return service.PreviewRuntime(
        artifact_store=artifacts.LocalPreviewArtifactStore(tmp_path / "artifacts"),
        backend_provider=FixedTenantBackendProvider({"production": backend}),
        max_workers=1,
        max_queued_generations=0,
        max_running_generations_per_tenant=1,
        max_queued_generations_per_tenant=0,
        max_generation_spool_bytes=64,
        clock=lambda: datetime(2026, 7, 4, tzinfo=UTC),
        request_id_factory=lambda: next(request_ids),
        executor=executor,
    )


def test_spool_ceiling_fails_request_nonretryably_and_removes_all_package_paths(
    tmp_path: Path,
) -> None:
    backend = SQLModelBackend(
        f"sqlite:///{tmp_path / 'preview.db'}",
        CCloudStorageModule(),
        use_migrations=False,
        focus_preview_enabled=True,
    )
    backend.create_tables()
    _seed(backend, source=_source(), aggregate=_aggregate(), allocation=_allocation())
    executor = ControlledExecutor()
    runtime = _spool_limited_runtime(tmp_path, backend, executor)
    try:
        queued = runtime.submit(
            tenant_name="production",
            tenant_config=_tenant_config(backend._connection_string),
            backend=backend,
            start_date=date(2026, 7, 1),
            end_date=date(2026, 7, 2),
            grain="daily",
            column_profile="full",
            effective_columns=preview_module("mapping").FOCUS_1_4_FULL_PROFILE_COLUMNS,
        )
        executor.run_all()
        failed = runtime.get_request(
            backend=backend,
            request_id=queued.request_id,
            ecosystem="confluent_cloud",
            tenant_id="tenant-1",
        )
        retried = runtime.submit(
            tenant_name="production",
            tenant_config=_tenant_config(backend._connection_string),
            backend=backend,
            start_date=date(2026, 7, 1),
            end_date=date(2026, 7, 2),
            grain="daily",
            column_profile="full",
            effective_columns=preview_module("mapping").FOCUS_1_4_FULL_PROFILE_COLUMNS,
        )
        executor.run_all()
    finally:
        runtime.close()
        backend.dispose()

    assert failed.status.value == "failed"
    assert failed.diagnostic.code == "preview_generation_spool_limit_exceeded"
    assert failed.diagnostic.message == ("FOCUS Mapping Preview package exceeds the configured generation spool limit.")
    assert failed.diagnostic.retryable is False
    assert failed.storage_key is None
    assert failed.package is None
    assert retried.request_id == "spool-request-2"
    assert retried.status.value == "queued"
    artifact_root = tmp_path / "artifacts"
    assert not tuple(artifact_root.rglob("*.workspace"))
    assert not tuple(artifact_root.rglob("*.staging"))
    assert not tuple(artifact_root.rglob("*.lock"))
    assert not tuple(path for path in artifact_root.iterdir() if path.name.startswith("v1-"))


def test_bounded_package_externalizes_ordered_rows_parts_and_manifest(
    tmp_path: Path,
) -> None:
    del tmp_path
    mapping = preview_module("mapping")
    spooling = preview_module("spooling")
    rows = (
        _row(day=2, AllocatedResourceId="sa-b"),
        _row(day=1, AllocatedResourceId="sa-a"),
    )
    request = _monthly_request()
    snapshot = _settled_snapshot()
    reconciliation = mapping.PreviewPackageReconciliation(
        source_records=2,
        source_cost=Decimal("16"),
        allocated_cost=Decimal("16"),
        source_quantity=Decimal("10"),
        allocated_quantity=Decimal("10"),
    )
    baseline = mapping.build_preview_data_package(
        request=request,
        snapshot=snapshot,
        full_rows=rows,
        reconciliation=reconciliation,
        max_csv_file_bytes=None,
    )
    lines = baseline.data_files[0].body.splitlines(keepends=True)
    part_limit = len(lines[0]) + max(len(line) for line in lines[1:])
    bounded = mapping.build_bounded_preview_data_package(
        request=request,
        snapshot=snapshot,
        full_rows=iter(rows),
        reconciliation=reconciliation,
        max_csv_file_bytes=part_limit,
        max_generation_spool_bytes=16 * 1024 * 1024,
    )
    workspace = bounded._workspace
    assert workspace is not None
    workspace_root = workspace.root
    try:
        assert isinstance(bounded.data_files, spooling.PreviewSpooledArtifactCollection)
        assert len(bounded.data_files) == 2
        rendered = b"".join(chunk for item in bounded.data_files for chunk in item.body.iter_chunks())
        assert rendered == lines[0] + lines[1] + lines[0] + lines[2]
        metadata = bounded.data_files.metadata
        manifest = mapping.build_preview_revision_manifest(
            revision_id="revision-bounded",
            tenant_name_at_publication="production",
            month="2026-07",
            start_date=date(2026, 7, 1),
            end_date=date(2026, 8, 1),
            monthly_status="settled",
            material_sha256=mapping.preview_revision_content_sha256(logical_data_sha256=bounded.logical_data_sha256),
            supersedes_revision_id=None,
            snapshot=snapshot,
            draft=bounded,
            files=metadata,
            published_at=datetime(2026, 8, 4, tzinfo=UTC),
        )
        assert isinstance(manifest, spooling.PreviewSpooledBody)
    finally:
        bounded.close()
    assert not workspace_root.exists()


def test_staging_spooled_parts_uses_one_final_workspace_reconciliation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    artifacts = preview_module("artifacts")
    mapping = preview_module("mapping")
    store = artifacts.LocalPreviewArtifactStore(tmp_path / "artifacts")
    generation = store.begin_generation(
        owner=_owner(),
        request_id="incremental-staging",
        max_spool_bytes=32 * 1024 * 1024,
    )
    request = _monthly_request()
    snapshot = _settled_snapshot()
    rows = tuple(_row(day=1, AllocatedResourceId=f"sa-{index:04d}") for index in range(12))
    baseline = mapping.build_preview_data_package(
        request=request,
        snapshot=snapshot,
        full_rows=rows,
        reconciliation=mapping.PreviewPackageReconciliation(
            source_records=len(rows),
            source_cost=Decimal("96"),
            allocated_cost=Decimal("96"),
            source_quantity=Decimal("60"),
            allocated_quantity=Decimal("60"),
        ),
        max_csv_file_bytes=None,
    )
    lines = baseline.data_files[0].body.splitlines(keepends=True)
    part_limit = len(lines[0]) + max(len(line) for line in lines[1:])
    draft = mapping.build_bounded_preview_data_package(
        request=request,
        snapshot=snapshot,
        full_rows=iter(rows),
        reconciliation=baseline.reconciliation,
        max_csv_file_bytes=part_limit,
        max_generation_spool_bytes=generation.workspace.limit_bytes,
        workspace=generation.workspace,
    )
    scans = 0
    real_disk_usage = generation.workspace._disk_usage  # noqa: SLF001

    def counted_disk_usage() -> int:
        nonlocal scans
        scans += 1
        return real_disk_usage()

    monkeypatch.setattr(generation.workspace, "_disk_usage", counted_disk_usage)
    try:
        assert len(draft.data_files) == len(rows)
        generation.stage_data_files(draft.data_files)
        assert scans == 1
        assert generation.workspace.used_bytes <= generation.workspace.limit_bytes
    finally:
        generation.close()


def test_multipart_accounting_retains_nonprojection_bytes_without_crossing_limit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mapping = preview_module("mapping")
    spooling = preview_module("spooling")
    request = _monthly_request()
    snapshot = _settled_snapshot()
    rows = tuple(_row(day=1, AllocatedResourceId=f"sa-{index:04d}") for index in range(24))
    reconciliation = mapping.PreviewPackageReconciliation(
        source_records=len(rows),
        source_cost=Decimal(len(rows) * 8),
        allocated_cost=Decimal(len(rows) * 8),
        source_quantity=Decimal(len(rows) * 5),
        allocated_quantity=Decimal(len(rows) * 5),
    )
    baseline = mapping.build_preview_data_package(
        request=request,
        snapshot=snapshot,
        full_rows=rows,
        reconciliation=reconciliation,
        max_csv_file_bytes=None,
    )
    lines = baseline.data_files[0].body.splitlines(keepends=True)
    part_limit = len(lines[0]) + max(len(line) for line in lines[1:])

    probe = spooling.PreviewGenerationWorkspace(
        32 * 1024 * 1024,
        root=tmp_path / "probe.workspace",
    )
    try:
        (probe.root / "evidence.sqlite").write_bytes(b"e" * 4096)
        probe.enforce_limit()
        probe_draft = mapping.build_bounded_preview_data_package(
            request=request,
            snapshot=snapshot,
            full_rows=iter(rows),
            reconciliation=reconciliation,
            max_csv_file_bytes=part_limit,
            max_generation_spool_bytes=probe.limit_bytes,
            workspace=probe,
        )
        calibrated_limit = probe.used_bytes
        assert len(probe_draft.data_files) == len(rows)
    finally:
        probe.close()

    workspace = spooling.PreviewGenerationWorkspace(
        calibrated_limit,
        root=tmp_path / "near-limit.workspace",
    )
    observed_usage: list[int] = []
    real_disk_usage = workspace._disk_usage  # noqa: SLF001

    def observed_disk_usage() -> int:
        used = real_disk_usage()
        observed_usage.append(used)
        return used

    monkeypatch.setattr(workspace, "_disk_usage", observed_disk_usage)
    try:
        evidence = workspace.root / "evidence.sqlite"
        evidence.write_bytes(b"e" * 4096)
        workspace.enforce_limit()
        draft = mapping.build_bounded_preview_data_package(
            request=request,
            snapshot=snapshot,
            full_rows=iter(rows),
            reconciliation=reconciliation,
            max_csv_file_bytes=part_limit,
            max_generation_spool_bytes=workspace.limit_bytes,
            workspace=workspace,
        )
        workspace.enforce_limit()

        assert len(draft.data_files) == len(rows)
        assert evidence.read_bytes() == b"e" * 4096
        assert observed_usage
        assert max(observed_usage) <= calibrated_limit
        assert workspace.used_bytes <= calibrated_limit
    finally:
        workspace.close()


def test_production_multipart_generation_has_byte_parity_integrity_and_measured_bounds(
    tmp_path: Path,
) -> None:
    artifacts = preview_module("artifacts")
    mapping = preview_module("mapping")
    row_count = 256
    request = _monthly_request()
    snapshot = _settled_snapshot()
    reconciliation = mapping.PreviewPackageReconciliation(
        source_records=row_count,
        source_cost=Decimal(row_count * 8),
        allocated_cost=Decimal(row_count * 8),
        source_quantity=Decimal(row_count * 5),
        allocated_quantity=Decimal(row_count * 5),
    )

    def rows() -> Any:
        for index in range(row_count):
            yield _row(day=1, AllocatedResourceId=f"sa-{index:06d}")

    baseline = mapping.build_preview_data_package(
        request=request,
        snapshot=snapshot,
        full_rows=rows(),
        reconciliation=reconciliation,
        max_csv_file_bytes=None,
    )
    lines = baseline.data_files[0].body.splitlines(keepends=True)
    part_limit = len(lines[0]) + max(len(line) for line in lines[1:])
    expected = mapping.build_preview_data_package(
        request=request,
        snapshot=snapshot,
        full_rows=rows(),
        reconciliation=reconciliation,
        max_csv_file_bytes=part_limit,
    )
    store = artifacts.LocalPreviewArtifactStore(tmp_path / "artifacts")
    generation = store.begin_generation(
        owner=_owner(),
        request_id="large-multipart",
        max_spool_bytes=32 * 1024 * 1024,
    )
    workspace = generation.workspace
    try:
        tracemalloc.start()
        draft = mapping.build_bounded_preview_data_package(
            request=request,
            snapshot=snapshot,
            full_rows=rows(),
            reconciliation=reconciliation,
            max_csv_file_bytes=part_limit,
            max_generation_spool_bytes=workspace.limit_bytes,
            workspace=workspace,
        )
        generation.stage_data_files(draft.data_files)
        staged_usage = workspace._disk_usage()  # noqa: SLF001
        manifest = mapping.build_preview_revision_manifest(
            revision_id="revision-large-multipart",
            tenant_name_at_publication="production",
            month="2026-07",
            start_date=date(2026, 7, 1),
            end_date=date(2026, 8, 1),
            monthly_status="settled",
            material_sha256=mapping.preview_revision_content_sha256(logical_data_sha256=draft.logical_data_sha256),
            supersedes_revision_id=None,
            snapshot=snapshot,
            draft=draft,
            files=draft.data_files.metadata,
            published_at=datetime(2026, 8, 4, tzinfo=UTC),
        )
        stored = generation.publish(manifest_body=manifest)
        _current, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()

        assert len(draft.data_files) == row_count
        assert len(expected.data_files) == row_count
        assert draft.logical_data_sha256 == expected.logical_data_sha256
        assert staged_usage <= workspace.limit_bytes
        assert workspace.used_bytes <= workspace.limit_bytes
        assert peak < 16 * 1024 * 1024
        for expected_file, stored_file in zip(expected.data_files, stored.files, strict=True):
            with store.open_verified(stored.storage_key, stored_file) as stream:
                assert b"".join(stream.iter_chunks()) == expected_file.body
    finally:
        if tracemalloc.is_tracing():
            tracemalloc.stop()
        generation.close()


def test_bounded_monthly_grouping_preserves_cells_with_sublinear_python_peak(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monthly = preview_module("monthly")
    spooling = preview_module("spooling")
    monkeypatch.setattr(monthly, "validate_preview_row", lambda **_kwargs: None)
    month_start = datetime(2026, 7, 1, tzinfo=UTC)
    month_end = datetime(2026, 8, 1, tzinfo=UTC)
    parity_rows = tuple(
        _row(
            day=1,
            billed="0.125",
            contracted="0.25",
            effective="0.125",
            list_cost="0.25",
            pricing_cost="0.125",
            pricing_quantity="0.5",
            consumed_quantity="0.5",
            discount="0.125",
            lineage=(_lineage(source_id=f"cost-{index:05d}", ordinal=index),),
        )
        for index in range(8)
    )
    baseline = monthly.aggregate_monthly_full_rows(
        rows=parity_rows,
        month_start=month_start,
        month_end=month_end,
    )
    parity_workspace = spooling.PreviewGenerationWorkspace(32 * 1024 * 1024)
    try:
        bounded = tuple(
            monthly.aggregate_monthly_full_rows_bounded(
                rows=iter(parity_rows),
                month_start=month_start,
                month_end=month_end,
                workspace=parity_workspace,
            )
        )
        assert [(row.target_values, row.custom_values, row.financials) for row in bounded] == [
            (row.target_values, row.custom_values, row.financials) for row in baseline
        ]
        consumed_index = preview_module("mapping").FOCUS_1_4_FULL_COLUMNS.index("ConsumedQuantity")
        unit_index = preview_module("mapping").FOCUS_1_4_FULL_COLUMNS.index("ConsumedUnit")
        assert bounded[0].target_values[consumed_index] == Decimal("4")
        assert bounded[0].target_values[unit_index] == "GB"
    finally:
        parity_workspace.close()

    large_workspace = spooling.PreviewGenerationWorkspace(128 * 1024 * 1024)

    def large_rows() -> Any:
        for index in range(4_000):
            yield _row(
                day=1,
                lineage=(_lineage(source_id=f"large-{index:05d}", ordinal=index),),
            )

    try:
        tracemalloc.start()
        result = tuple(
            monthly.aggregate_monthly_full_rows_bounded(
                rows=large_rows(),
                month_start=month_start,
                month_end=month_end,
                workspace=large_workspace,
            )
        )
        _current, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        assert len(result) == 1
        consumed_index = preview_module("mapping").FOCUS_1_4_FULL_COLUMNS.index("ConsumedQuantity")
        assert result[0].target_values[consumed_index] == Decimal("20000")
        assert peak < 8 * 1024 * 1024
        assert large_workspace.used_bytes < large_workspace.limit_bytes
    finally:
        if tracemalloc.is_tracing():
            tracemalloc.stop()
        large_workspace.close()

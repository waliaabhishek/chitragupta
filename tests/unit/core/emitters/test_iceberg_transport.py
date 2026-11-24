from __future__ import annotations

import asyncio
import json
from contextlib import ExitStack, nullcontext
from datetime import UTC, date, datetime
from decimal import Decimal
from importlib import import_module
from threading import Event
from time import monotonic
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock

import pytest

from core.config.models import IcebergCatalogConfig

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator
    from pathlib import Path
    from types import ModuleType

_CATALOG = IcebergCatalogConfig.model_validate(
    {"type": "rest", "uri": "https://catalog.example", "token": "test-bearer"}
)
_CLOSE_BUDGET = 0.5
_RETURN_LIMIT = 1.5


class _StalledClient:
    def __init__(self) -> None:
        self.cancelled = Event()

    async def aclose(self) -> None:
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            self.cancelled.set()
            raise


@pytest.fixture
def transport() -> ModuleType:
    for dependency in ("pyiceberg", "pyarrow", "s3fs"):
        pytest.importorskip(dependency)
    return import_module("core.emitters.iceberg_transport")


@pytest.fixture
def stalled_clients(monkeypatch: pytest.MonkeyPatch, transport: ModuleType) -> list[_StalledClient]:
    clients: list[_StalledClient] = []

    def make_client(**_options: object) -> _StalledClient:
        client = _StalledClient()
        clients.append(client)
        return client

    # Separate the catalog's own timeout from the shared close limit far enough
    # to allow for scheduling delays on a loaded Docker host.
    monkeypatch.setattr(transport, "_IO_TIMEOUT_SECONDS", 2.0)
    monkeypatch.setattr(transport.httpx, "AsyncClient", make_client)
    transport.get_loop()
    return clients


def test_catalog_clients_share_a_close_deadline(
    monkeypatch: pytest.MonkeyPatch, transport: ModuleType, stalled_clients: list[_StalledClient]
) -> None:
    monkeypatch.setattr(transport.RestCatalog, "_fetch_config", lambda _catalog: None)
    with ExitStack() as stack:
        for _ in range(4):
            stack.enter_context(
                transport.open_iceberg_catalog(_CATALOG, deadline=monotonic() + 5, close_deadline=lambda: cutoff)
            )
        start = monotonic()
        cutoff = start + _CLOSE_BUDGET

    assert monotonic() - start < _RETURN_LIMIT
    assert len(stalled_clients) == 4
    for client in stalled_clients:
        assert client.cancelled.wait(5)


def test_failed_catalog_open_stays_within_discovery_deadline(
    monkeypatch: pytest.MonkeyPatch, transport: ModuleType, stalled_clients: list[_StalledClient]
) -> None:
    def fail_discovery(_catalog: object) -> None:
        raise RuntimeError("catalog configuration could not be loaded")

    monkeypatch.setattr(transport.RestCatalog, "_fetch_config", fail_discovery)
    start = monotonic()
    with (
        pytest.raises(transport.IcebergDestinationError, match="^catalog_connection$"),
        transport.open_iceberg_catalog(_CATALOG, deadline=start + _CLOSE_BUDGET),
    ):
        pytest.fail("failed catalog discovery yielded a client")

    assert monotonic() - start < _RETURN_LIMIT
    assert len(stalled_clients) == 1
    assert stalled_clients[0].cancelled.wait(5)


def _table(
    transport: ModuleType,
    properties: dict[str, str],
    partition_fields: tuple[tuple[str, str], ...],
) -> Any:
    location = "s3://warehouse/table"
    sdk_arrow = import_module("pyiceberg.io.pyarrow")
    sdk_partition = import_module("pyiceberg.partitioning")
    sdk_schema = import_module("pyiceberg.schema")
    sdk_metadata = import_module("pyiceberg.table.metadata")
    sdk_sorting = import_module("pyiceberg.table.sorting")
    schema = sdk_schema.assign_fresh_schema_ids(
        sdk_arrow._pyarrow_to_schema_without_ids(transport.ICEBERG_ARROW_SCHEMA)
    )
    spec = sdk_partition.PartitionSpec(
        *(
            sdk_partition.PartitionField(
                source_id=schema.find_field(field).field_id,
                field_id=1000 + index,
                transform=transport.IdentityTransform(),
                name=name,
            )
            for index, (field, name) in enumerate(partition_fields)
        )
    )
    table = transport.IcebergTable(
        identifier=("billing", "chargebacks"),
        metadata_location=f"{location}/metadata/table.json",
        metadata=sdk_metadata.new_table_metadata(schema, spec, sdk_sorting.UNSORTED_SORT_ORDER, location, properties),
        io=transport.IcebergFileIO(transport.load_location_provider(location, {}), {}),
        catalog=object.__new__(transport.IcebergCatalog),
    )
    table.validate_contract()
    return table


@pytest.mark.parametrize(
    ("target_bytes", "batch_sizes", "max_files", "planned", "error", "wide_bytes"),
    [
        (None, (), 0, (), None, 0),
        ("1", (512,), 128, (), "destination_file_limit", 0),
        ("1", (1, 2), 2, (1,), "destination_file_limit", 0),
        (None, (64, 64), 2, (64, 64), None, 0),
        ("4096", (512,), 28, (512,), None, 2 * 1024 * 1024),
        ("4096", (512,), 27, (), "destination_file_limit", 2 * 1024 * 1024),
        ("0", (1,), 1, (), "table_contract", 0),
        ("invalid", (1,), 1, (), "table_contract", 0),
    ],
    ids=[
        "empty-overwrite",
        "oversized-plan",
        "remaining-files",
        "grouped-overwrite",
        "uneven-rows-at-cap",
        "uneven-rows-over-cap",
        "zero-target",
        "malformed-target",
    ],
)
def test_parquet_plan_is_bounded_before_the_sdk_writer(
    monkeypatch: pytest.MonkeyPatch,
    transport: ModuleType,
    target_bytes: str | None,
    batch_sizes: tuple[int, ...],
    max_files: int,
    planned: tuple[int, ...],
    error: str | None,
    wide_bytes: int,
) -> None:
    key = transport.TableProperties.WRITE_TARGET_FILE_SIZE_BYTES
    properties = {**transport.ICEBERG_TABLE_PROPERTIES}
    if target_bytes is not None:
        properties[key] = target_bytes

    sdk_arrow = import_module("pyiceberg.io.pyarrow")
    table = _table(transport, properties, tuple((field, field) for field in transport.ICEBERG_SCOPE_FIELDS))
    transaction = MagicMock()
    transaction.table_metadata = table.metadata
    producer = MagicMock()
    overwrite = producer.return_value.__enter__.return_value
    monkeypatch.setattr(transport, "_PartitionOverwrite", producer)
    snapshot_id = 123

    def confirm() -> None:
        table.confirmed_snapshot_id = snapshot_id

    transaction.commit_transaction.side_effect = confirm
    monkeypatch.setattr(table, "transaction", lambda: transaction)
    calls: list[int] = []
    files: list[tuple[object, int, int]] = []
    plan_files = transport._dataframe_to_data_files

    def plan_batch(*, df: Any, **kwargs: Any) -> Iterable[object]:
        calls.append(df.num_rows)
        partitions = list(sdk_arrow._determine_partitions(table.spec(), table.schema(), df))
        assert len(partitions) == 1
        partition = partitions[0].arrow_table_partition
        assert partition.num_rows == df.num_rows
        assert partition.nbytes <= 2 * df.nbytes
        return plan_files(df=df, **kwargs)

    def write_tasks(*, tasks: Iterable[Any], **_kwargs: object) -> Iterator[object]:
        for task in tasks:
            files.append((task.write_uuid, task.task_id, sum(batch.num_rows for batch in task.record_batches)))
            yield object()

    monkeypatch.setattr(transport, "_dataframe_to_data_files", plan_batch)
    monkeypatch.setattr(sdk_arrow, "write_file", write_tasks)
    timestamp = datetime(2026, 1, 1, tzinfo=UTC)
    scope = {"ecosystem": "generic_metrics_only", "tenant_id": "example", "chargeback_date": timestamp.date()}
    row = {
        **scope,
        "timestamp": timestamp,
        "identity_id": "example",
        "product_category": "compute",
        "product_type": "usage",
        "cost_type": "hourly",
        "amount": Decimal("1.23456"),
    }

    def batches() -> Iterator[list[dict[str, object]]]:
        for size in batch_sizes:
            batch = [row] * size
            if wide_bytes:
                batch[0] = {**row, "allocation_detail": "x" * wide_bytes, "tags": {"source": "test"}}
            yield batch

    result = None
    with pytest.raises(transport.IcebergDestinationError, match=f"^{error}$") if error else nullcontext():
        result = table.overwrite_batches(
            batches(),
            **scope,
            deadline=monotonic() + 5,
            max_bytes=4 * 1024 * 1024,
            max_files=max_files,
        )

    assert calls == list(planned)
    producer.assert_called_once_with(operation=transport.Operation.APPEND, transaction=transaction, io=table.io)
    assert sum(row_count for _, _, row_count in files) == sum(planned)
    assert len(files) <= max_files
    assert [(write_uuid, task_id) for write_uuid, task_id, _ in files] == [
        (overwrite.commit_uuid, index) for index in range(len(files))
    ]
    if error:
        transaction.commit_transaction.assert_not_called()
    else:
        assert result == snapshot_id
        assert len(files) == max_files
        transaction.commit_transaction.assert_called_once()


@pytest.mark.parametrize("defect", [None, "date-type", "arity", "duplicate-path", "old-spec", "delete-manifest"])
def test_partition_selection_uses_typed_identity_values(
    monkeypatch: pytest.MonkeyPatch, transport: ModuleType, defect: str | None
) -> None:
    table = _table(
        transport,
        transport.ICEBERG_TABLE_PROPERTIES,
        (("chargeback_date", "bill_day"), ("tenant_id", "customer"), ("ecosystem", "system")),
    )
    sdk_manifest = import_module("pyiceberg.manifest")
    day = date(1970, 1, 2)
    ecosystem = "generic_metrics_only"
    scope = (ecosystem, "example", day)

    target = sdk_manifest.DataFile.from_args(
        content=sdk_manifest.DataFileContent.DATA,
        file_path="s3://warehouse/table/data/target.parquet",
        file_format=sdk_manifest.FileFormat.PARQUET,
        partition=transport.Record(
            *{"date-type": (True, "example", ecosystem), "arity": (1, "example")}.get(defect, (1, "example", ecosystem))
        ),
        lower_bounds={table.schema().find_field("ecosystem").field_id: b"generic_metrics_"},
    )
    neighbor = sdk_manifest.DataFile.from_args(
        content=sdk_manifest.DataFileContent.DATA,
        file_path=target.file_path if defect == "duplicate-path" else "s3://warehouse/table/data/neighbor.parquet",
        file_format=sdk_manifest.FileFormat.PARQUET,
        partition=transport.Record(1, "neighbor", ecosystem),
    )
    manifest = SimpleNamespace(
        partition_spec_id=table.spec().spec_id + int(defect == "old-spec"),
        content=sdk_manifest.ManifestContent.DELETES
        if defect == "delete-manifest"
        else sdk_manifest.ManifestContent.DATA,
        partitions=None,
        fetch_manifest_entry=MagicMock(return_value=[SimpleNamespace(data_file=file) for file in (target, neighbor)]),
    )
    table.metadata = table.metadata.model_copy(update={"current_snapshot_id": 123})
    snapshot = SimpleNamespace(snapshot_id=123, manifests=lambda _io: [manifest])
    monkeypatch.setattr(table, "snapshot_by_name", lambda _name: snapshot)

    if defect:
        with pytest.raises(transport.IcebergDestinationError, match="^table_layout$"):
            table._partition_files(scope, lambda: None)
    else:
        assert table._partition_files(scope, lambda: None) == {target}
    if defect in {"old-spec", "delete-manifest"}:
        manifest.fetch_manifest_entry.assert_not_called()
    else:
        manifest.fetch_manifest_entry.assert_called_once_with(table.io, discard_deleted=True)


def test_overwrite_omits_only_inherited_manifests_known_to_have_no_live_files(
    monkeypatch: pytest.MonkeyPatch, transport: ModuleType
) -> None:
    manifest = import_module("pyiceberg.manifest").ManifestFile
    deleted = manifest.from_args(added_files_count=0, existing_files_count=0, deleted_files_count=1)
    kept = [
        manifest.from_args(added_files_count=added, existing_files_count=existing)
        for added, existing in [(None, 0), (0, None), (1, 0), (0, 1)]
    ]
    monkeypatch.setattr(transport._OverwriteFiles, "_existing_manifests", lambda _self: [deleted, *kept])

    overwrite = object.__new__(transport._PartitionOverwrite)
    assert all(a is b for a, b in zip(overwrite._existing_manifests(), kept, strict=True))


@pytest.mark.parametrize("reordered", [False, True], ids=["spec-order", "physical-order-differs"])
def test_partition_selection_checks_the_physical_avro_order(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, transport: ModuleType, reordered: bool
) -> None:
    table = _table(
        transport,
        transport.ICEBERG_TABLE_PROPERTIES,
        (("tenant_id", "customer"), ("ecosystem", "system"), ("chargeback_date", "bill_day")),
    )
    sdk_manifest = import_module("pyiceberg.manifest")
    sdk_avro = import_module("pyiceberg.avro.file")
    sdk_snapshots = import_module("pyiceberg.table.snapshots")
    sdk_file_io = import_module("pyiceberg.io.pyarrow").PyArrowFileIO()
    file_io_module = import_module("core.emitters.iceberg_file_io")

    scope = ("generic_metrics_only", "example", date(1970, 1, 2))
    spec = table.spec()
    partition_type = spec.partition_type(table.schema())
    fields = partition_type.fields
    struct_type = import_module("pyiceberg.types").StructType
    physical_type = struct_type(fields[1], fields[0], fields[2]) if reordered else partition_type
    manifest_path = tmp_path / "manifest.avro"
    list_path = tmp_path / "manifest-list.avro"
    manifest_uri = f"s3://warehouse/table/metadata/{tmp_path.name}/manifest.avro"
    list_uri = f"s3://warehouse/table/metadata/{tmp_path.name}/manifest-list.avro"
    file = sdk_manifest.DataFile.from_args(
        content=sdk_manifest.DataFileContent.DATA,
        file_path="s3://warehouse/table/data/original.parquet",
        file_format=sdk_manifest.FileFormat.PARQUET,
        partition=transport.Record(scope[1], scope[0], 1),
        record_count=1,
        file_size_in_bytes=32,
    )
    entry = sdk_manifest.ManifestEntry.from_args(status=sdk_manifest.ManifestEntryStatus.ADDED, data_file=file)
    with sdk_avro.AvroOutputFile(
        output_file=sdk_file_io.new_output(str(manifest_path)),
        file_schema=sdk_manifest.manifest_entry_schema_with_data_file(
            2, sdk_manifest.data_file_with_partition(physical_type, 2)
        ),
        record_schema=sdk_manifest.manifest_entry_schema_with_data_file(
            2, sdk_manifest.data_file_with_partition(partition_type, 2)
        ),
        schema_name="manifest_entry",
        metadata={"avro.codec": "null"},
    ) as writer:
        writer.write_block([entry])
    manifest = sdk_manifest.ManifestFile.from_args(
        manifest_path=manifest_uri,
        manifest_length=manifest_path.stat().st_size,
        partition_spec_id=spec.spec_id,
        content=sdk_manifest.ManifestContent.DATA,
        sequence_number=1,
        min_sequence_number=1,
        added_snapshot_id=123,
        added_files_count=1,
        existing_files_count=0,
        deleted_files_count=0,
        added_rows_count=1,
        existing_rows_count=0,
        deleted_rows_count=0,
    )
    with sdk_manifest.write_manifest_list(2, sdk_file_io.new_output(str(list_path)), 123, None, 1, "null") as writer:
        writer.add_manifests([manifest])
    paths = {manifest_uri: manifest_path, list_uri: list_path}
    table.io.deadline = monotonic() + 5
    table.io.partition_fields = fields
    monkeypatch.setattr(table.io, "_filesystem", lambda: SimpleNamespace(loop=None, read_metadata=None))
    monkeypatch.setattr(file_io_module, "sync", lambda _loop, _reader, uri, **_kwargs: paths[uri].read_bytes())

    snapshot = sdk_snapshots.Snapshot(snapshot_id=123, manifest_list=list_uri, sequence_number=1)
    table.metadata = table.metadata.model_copy(update={"current_snapshot_id": 123})
    monkeypatch.setattr(table, "snapshot_by_name", lambda _name: snapshot)
    if reordered:
        with pytest.raises(transport.IcebergDestinationError, match="^table_layout$"):
            table._partition_files(scope, lambda: None)
        with pytest.raises(transport.IcebergDestinationError, match="^table_layout$"):
            manifest.fetch_manifest_entry(table.io)
    else:
        assert table._partition_files(scope, lambda: None) == {file}


@pytest.mark.parametrize("existing_main", [False, True], ids=["first-snapshot", "keep-retention"])
def test_main_commit_preserves_the_loaded_retention_policy(
    monkeypatch: pytest.MonkeyPatch, transport: ModuleType, existing_main: bool
) -> None:
    table = _table(
        transport, transport.ICEBERG_TABLE_PROPERTIES, tuple((name, name) for name in transport.ICEBERG_SCOPE_FIELDS)
    )
    sdk_refs = import_module("pyiceberg.table.refs")
    sdk_update = import_module("pyiceberg.table.update")
    policy = {"min-snapshots-to-keep": 12, "max-snapshot-age-ms": 5_000, "max-ref-age-ms": 6_000}
    if existing_main:
        ref = sdk_refs.SnapshotRef.model_validate({"snapshot-id": 123, "type": "branch", **policy})
        table.metadata = table.metadata.model_copy(update={"refs": {"main": ref}, "current_snapshot_id": 123})
    else:
        policy = {}
    update = sdk_update.SetSnapshotRefUpdate.model_validate({"snapshot-id": 456, "ref-name": "main", "type": "branch"})
    catalog = table.catalog
    monkeypatch.setattr(catalog, "_check_endpoint", lambda _endpoint: None)
    monkeypatch.setattr(catalog, "_split_identifier_for_path", lambda _identifier: {})
    monkeypatch.setattr(catalog, "url", lambda *_args, **_kwargs: "https://catalog.example/tables/chargebacks")
    session = MagicMock()
    session.post.return_value.status_code = 409
    monkeypatch.setattr(catalog, "_session", session, raising=False)
    catalog.deadline = monotonic() + 5

    with pytest.raises(transport.CommitFailedException):
        catalog.commit_table(table, (), (update,))
    wire = json.loads(session.post.call_args.kwargs["data"])
    assert wire["updates"] == [
        {"action": "set-snapshot-ref", "snapshot-id": 456, "ref-name": "main", "type": "branch", **policy}
    ]

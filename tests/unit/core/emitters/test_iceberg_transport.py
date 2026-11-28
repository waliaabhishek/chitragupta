"""Test boundaries of the public SDK transport."""

from __future__ import annotations

import subprocess
import sys
from datetime import date
from importlib import import_module
from io import BytesIO
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any

import pytest

if TYPE_CHECKING:
    from pathlib import Path
    from types import ModuleType


@pytest.fixture
def transport() -> ModuleType:
    pytest.importorskip("pyiceberg")
    pytest.importorskip("pyarrow")
    return import_module("core.emitters.iceberg_transport")


def _table(transport: ModuleType, tmp_path: Path) -> Any:
    sdk = import_module("pyiceberg.catalog.memory")
    catalog = sdk.InMemoryCatalog("test", warehouse=tmp_path.as_uri())
    catalog.create_namespace("billing")
    return catalog.create_table(
        "billing.chargebacks", schema=transport.ICEBERG_ARROW_SCHEMA, properties=transport.ICEBERG_TABLE_PROPERTIES
    )


def _locations() -> Any:
    return import_module("pyiceberg.table.locations").load_location_provider("s3://warehouse/table", {})


@pytest.mark.parametrize("physical_swap", [False, True], ids=["spec-order", "physical-order-differs"])
def test_manifest_header_preserves_physical_partition_order(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, transport: ModuleType, physical_swap: bool
) -> None:
    table = _table(transport, tmp_path)
    sdk = import_module("pyiceberg.partitioning")
    spec = sdk.PartitionSpec(
        *(
            sdk.PartitionField(
                source_id=table.schema().find_field(source).field_id,
                field_id=1000 + index,
                transform=transport.IdentityTransform(),
                name=name,
            )
            for index, (source, name) in enumerate(
                (("tenant_id", "customer"), ("ecosystem", "system"), ("chargeback_date", "bill_day"))
            )
        )
    )
    manifest_sdk = import_module("pyiceberg.manifest")
    avro = import_module("pyiceberg.avro.file")
    arrow_file_io = import_module("pyiceberg.io.pyarrow").PyArrowFileIO
    structure = import_module("pyiceberg.types").StructType
    record = import_module("pyiceberg.typedef").Record
    partition_type = spec.partition_type(table.schema())
    fields = partition_type.fields
    physical = structure(fields[1], fields[0], fields[2]) if physical_swap else partition_type
    local = str(tmp_path / "manifest.avro")
    path = "s3://warehouse/table/metadata/manifest.avro"
    target = manifest_sdk.DataFile.from_args(
        content=manifest_sdk.DataFileContent.DATA,
        file_path="s3://warehouse/table/data/original.parquet",
        file_format=manifest_sdk.FileFormat.PARQUET,
        partition=record("example", "generic_metrics_only", 1),
        record_count=1,
        file_size_in_bytes=32,
    )
    entry = manifest_sdk.ManifestEntry.from_args(status=manifest_sdk.ManifestEntryStatus.ADDED, data_file=target)
    native = arrow_file_io()
    with avro.AvroOutputFile(
        output_file=native.new_output(local),
        file_schema=manifest_sdk.manifest_entry_schema_with_data_file(
            2, manifest_sdk.data_file_with_partition(physical, 2)
        ),
        record_schema=manifest_sdk.manifest_entry_schema_with_data_file(
            2, manifest_sdk.data_file_with_partition(partition_type, 2)
        ),
        schema_name="manifest_entry",
        metadata={"avro.codec": "null"},
    ) as writer:
        writer.write_block([entry])
    manifest = manifest_sdk.ManifestFile.from_args(
        manifest_path=path,
        manifest_length=(tmp_path / "manifest.avro").stat().st_size,
        partition_spec_id=spec.spec_id,
        content=manifest_sdk.ManifestContent.DATA,
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
    original_input = arrow_file_io.new_input
    monkeypatch.setattr(arrow_file_io, "new_input", lambda self, uri: original_input(self, local))
    guarded = transport.IcebergFileIO(native, _locations(), fields, "s3://warehouse/table/metadata/table.json")
    if physical_swap:
        with pytest.raises(transport.IcebergDestinationError, match="^table_layout$"):
            manifest.fetch_manifest_entry(guarded)
    else:
        assert [item.data_file for item in manifest.fetch_manifest_entry(guarded)] == [target]


@pytest.mark.parametrize("bad_head", [False, True])
def test_commit_binds_all_guards_and_preserves_retention_on_every_head(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, transport: ModuleType, bad_head: bool
) -> None:
    table = _table(transport, tmp_path)
    refs = import_module("pyiceberg.table.refs")
    update = import_module("pyiceberg.table.update")
    retention = {"min-snapshots-to-keep": 12, "max-snapshot-age-ms": 5_000, "max-ref-age-ms": 6_000}
    base = refs.SnapshotRef.model_validate({"snapshot-id": 123, "type": "branch", **retention})
    table.metadata = table.metadata.model_copy(update={"refs": {"main": base}, "current_snapshot_id": 123})
    updates = tuple(
        update.SetSnapshotRefUpdate(snapshot_id=head, ref_name="main", type="branch") for head in (456, 789)
    )
    observed: dict[str, Any] = {}

    def native_commit(_catalog: Any, _table: Any, requirements: Any, requested: Any) -> Any:
        observed.update(requirements=requirements, updates=requested)
        return SimpleNamespace(
            metadata=table.metadata.model_copy(update={"current_snapshot_id": 456 if bad_head else 789})
        )

    monkeypatch.setattr(transport.RestCatalog, "commit_table", native_commit)
    guarded = object.__new__(transport.IcebergCatalog)
    if bad_head:
        with pytest.raises(transport.CommitStateUnknownException):
            guarded.commit_table(table, (), updates)
    else:
        assert guarded.commit_table(table, (), updates).metadata.current_snapshot_id == 789
    assert [item.model_dump(by_alias=True) for item in observed["requirements"]] == [
        {"type": "assert-table-uuid", "uuid": table.metadata.table_uuid},
        {"type": "assert-ref-snapshot-id", "ref": "main", "snapshot-id": 123},
        {"type": "assert-current-schema-id", "current-schema-id": table.metadata.current_schema_id},
        {"type": "assert-default-spec-id", "default-spec-id": table.metadata.default_spec_id},
    ]
    assert [item.model_dump(by_alias=True) for item in observed["updates"]] == [
        {"action": "set-snapshot-ref", "snapshot-id": head, "ref-name": "main", "type": "branch", **retention}
        for head in (456, 789)
    ]


def test_file_io_rejects_outside_location_and_excess_output(
    monkeypatch: pytest.MonkeyPatch, transport: ModuleType
) -> None:
    arrow_file_io = import_module("pyiceberg.io.pyarrow").PyArrowFileIO
    monkeypatch.setattr(arrow_file_io, "new_output", lambda _io, path: SimpleNamespace(location=path))
    native = transport.IcebergFileIO(arrow_file_io(), _locations(), (), "s3://warehouse/table/metadata/table.json")
    for index in range(128):
        native.new_output(f"s3://warehouse/table/data/{index}.parquet")
    with pytest.raises(transport.IcebergDestinationError, match="^destination_file_limit$"):
        native.new_output("s3://warehouse/table/data/overflow.parquet")
    with pytest.raises(transport.IcebergDestinationError, match="^storage_location$"):
        native.new_output("s3://warehouse/table-sibling/data/bad.parquet")


def test_incomplete_day_never_commits(transport: ModuleType, recording_table: Any) -> None:
    with pytest.raises(transport.IcebergDestinationError, match="^incomplete_day$"):
        transport.publish(
            recording_table,
            [recording_table.row],
            ("generic_metrics_only", "tenant", date(2026, 11, 18)),
            2,
            1024,
            permit_commit=lambda: pytest.fail("Incomplete day requested commit permission"),
        )
    assert recording_table.appended == 1
    assert not recording_table.committed


@pytest.mark.parametrize("defect", ["old_spec", "delete_manifest"])
def test_publication_rejects_unsupported_manifests(transport: ModuleType, recording_table: Any, defect: str) -> None:
    content = transport.ManifestContent.DELETES if defect == "delete_manifest" else transport.ManifestContent.DATA
    manifest = SimpleNamespace(partition_spec_id=1 if defect == "old_spec" else 0, content=content)
    recording_table.io = None
    recording_table.spec = lambda: SimpleNamespace(spec_id=0)
    recording_table.current_snapshot = lambda: SimpleNamespace(manifests=lambda _: [manifest])
    with pytest.raises(transport.IcebergDestinationError, match="^table_layout$"):
        transport.publish(recording_table, [], ("generic_metrics_only", "tenant", date(2026, 11, 18)), 0, 1024)
    assert not recording_table.committed and recording_table.appended == 0


@pytest.mark.skipif(sys.platform != "linux", reason="Destination process isolation requires Linux")
def test_destination_rejects_oversized_mapping_without_harming_parent() -> None:
    child = subprocess.run(
        [
            sys.executable,
            "-c",
            """
import errno
import mmap
import os
import resource
from core.emitters.iceberg_process import _limit_destination

_limit_destination(os.getppid())
ceiling = 2 * 1024 ** 3
assert resource.getrlimit(resource.RLIMIT_AS) == (ceiling, ceiling)
try:
    mmap.mmap(-1, ceiling)
except OSError as exc:
    assert exc.errno == errno.ENOMEM, exc
else:
    raise AssertionError('Oversized destination mapping succeeded')
with mmap.mmap(-1, 1024) as small:
    small[0] = 1
""",
        ],
        capture_output=True,
        timeout=10,
        check=False,
    )
    assert child.returncode == 0, child.stderr.decode()


def test_memory_failure_after_commit_is_unconfirmed(monkeypatch: pytest.MonkeyPatch, transport: ModuleType) -> None:
    from core.emitters import iceberg_process

    class Catalog:
        commit_started = False

        def __enter__(self) -> Catalog:
            return self

        def __exit__(self, *_: object) -> None:
            raise MemoryError

    def publish(*_: object, permit_commit: Any) -> int:
        permit_commit()
        return 456

    events = []
    messages = iter([(None, "billing.chargebacks"), (("ecosystem", "tenant", date(2026, 11, 18)), 1, 1024), "commit"])
    channel = SimpleNamespace(receive=lambda: next(messages), send=events.append)
    table = SimpleNamespace(metadata=SimpleNamespace(table_uuid="base-table"))
    monkeypatch.setattr(iceberg_process, "_limit_destination", lambda _parent: None)
    monkeypatch.setattr(transport, "IcebergCatalog", lambda _settings: Catalog())
    monkeypatch.setattr(transport, "load_table", lambda _catalog, _name: table)
    monkeypatch.setattr(transport, "publish", publish)
    iceberg_process._destination(123, channel, BytesIO())
    assert events == [("loaded", "base-table"), ("commit_ready", None), ("unconfirmed", None)]


@pytest.mark.parametrize(
    "receipt",
    [("error", "destination_memory"), MemoryError(), ValueError(), ("conflict", None)],
    ids=["child-error", "receipt-memory", "receipt-decode", "explicit-conflict"],
)
def test_post_permit_reply_preserves_commit_uncertainty(receipt: tuple[str, object] | Exception) -> None:
    from time import monotonic

    from core.emitters.iceberg_process import IcebergConflict, IcebergDestination, IcebergUnconfirmed

    replies = iter([("commit_ready", None), receipt])
    sent = []

    def receive() -> object:
        reply = next(replies)
        if isinstance(reply, Exception):
            raise reply
        return reply

    destination = IcebergDestination(SimpleNamespace(), BytesIO(), monotonic() + 10, lambda: False)
    destination._channel = SimpleNamespace(send=sent.append, receive=receive)
    expected = IcebergConflict if receipt == ("conflict", None) else IcebergUnconfirmed
    with pytest.raises(expected):
        destination.overwrite_batches(
            [],
            ecosystem="generic_metrics_only",
            tenant_id="tenant",
            chargeback_date=date(2026, 11, 18),
            row_count=0,
            max_bytes=1024,
        )
    assert sent[-1] == "commit"


@pytest.fixture
def recording_table(transport: ModuleType) -> Any:
    class RecordingTable:
        row = [{"ecosystem": "generic_metrics_only", "tenant_id": "tenant", "chargeback_date": date(2026, 11, 18)}]
        metadata = SimpleNamespace(current_snapshot_id=123)
        appended = 0
        committed = False

        @staticmethod
        def schema() -> Any:
            arrow = import_module("pyarrow")
            return SimpleNamespace(
                as_arrow=lambda: arrow.schema(
                    [("ecosystem", arrow.string()), ("tenant_id", arrow.string()), ("chargeback_date", arrow.date32())]
                )
            )

        def transaction(self) -> Any:
            return self

        @staticmethod
        def current_snapshot() -> None:
            return None

        def __enter__(self) -> Any:
            return self

        def __exit__(self, error: Any, *_: object) -> None:
            self.committed = error is None

        @staticmethod
        def delete(_scope: object) -> None:
            pass

        def append(self, batch: Any) -> None:
            self.appended += batch.num_rows

    return RecordingTable()

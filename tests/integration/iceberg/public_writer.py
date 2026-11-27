"""Run one isolated publication using stock public PyIceberg APIs."""

from __future__ import annotations

import json
import os
import sys
import time
from datetime import date, datetime, timedelta
from itertools import islice
from pathlib import Path

import pyarrow as pa
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.expressions import And, EqualTo
from pyiceberg.io.pyarrow import PyArrowFileIO

from core.config.models import StorageConfig
from core.emitters.iceberg import _project
from core.emitters.iceberg_spool import IcebergDaySpool
from core.storage.iceberg import IcebergExportKey, IcebergExportStorageBackend
from core.storage.registry import create_storage_backend
from plugins.storage_modules import get_storage_module_for_ecosystem

MIB = 1024 * 1024


class IncompleteDayError(RuntimeError):
    """The batch iterator ended before staging all prepared rows."""


class ObservedFileIO(PyArrowFileIO):
    """Record old data reads and optionally reject them at the public FileIO boundary."""

    def __init__(self, properties):
        super().__init__(properties)
        self.data_files: set[str] = set()
        self.old_data_reads: list[str] = []
        self.metadata_reads = 0
        self.metadata_lengths: dict[str, list[int]] = {}

    def new_input(self, location):
        if location.endswith(".avro"):
            self.metadata_reads += 1
        if location.endswith(".parquet") and location not in self.data_files:
            self.old_data_reads.append(location)
            if self.properties.get("proof.deny-old-parquet") == "true":
                raise OSError("proof rejected reading old Parquet")
        source = super().new_input(location)
        if location.endswith(".avro") and self.properties.get("proof.observe-metadata") == "true":
            self.metadata_lengths.setdefault(location, []).append(len(source))
        return source

    def new_output(self, location):
        if location.endswith(".parquet"):
            assert len(self.data_files) < 128 or location in self.data_files, "proof output file limit"
            self.data_files.add(location)
        return super().new_output(location)


def require_time(deadline: float) -> None:
    if time.monotonic() >= deadline:
        raise TimeoutError("proof attempt deadline")


def scratch_bytes(root: Path) -> int:
    total = 0
    for fd in Path("/proc/self/fd").iterdir():
        try:
            if str(fd.readlink()).startswith(f"{root}/") and fd.readlink().name.endswith(" (deleted)"):
                total += fd.stat().st_size
        except FileNotFoundError:
            continue
    return total


def peak_rss_mib() -> float:
    # Linux reports the current process image's high-water RSS here. The
    # rusage value can also include the harness's memory before exec.
    sample = next(line for line in Path("/proc/self/status").read_text().splitlines() if line.startswith("VmHWM:"))
    return int(sample.split()[1]) / 1024


def public_catalog(settings: dict) -> RestCatalog:
    properties = {
        "uri": settings["catalog_uri"],
        "warehouse": settings["warehouse"],
        "ssl": {"cabundle": settings["ca_file"]},
        "s3.endpoint": settings["s3_endpoint"],
        "s3.region": settings["region"],
        "s3.force-virtual-addressing": "false",
        "py-io-impl": f"{__name__}.ObservedFileIO",
        "proof.deny-old-parquet": str(settings.get("deny_old_parquet", False)).lower(),
        "proof.observe-metadata": str(settings.get("observe_metadata", False)).lower(),
        "header.X-Iceberg-Access-Delegation": "vended-credentials",
    }
    if settings.get("legacy_token"):
        properties["token"] = settings["token"]
    else:
        # Stock legacy auth overrides a request-local table credential.
        # Noop preserves the documented header and the table response's token.
        properties["auth"] = {"type": "noop"}
        properties["header.Authorization"] = f"Bearer {settings['token']}"
    return RestCatalog("chargeback-proof", **properties)


def file_ids(schema) -> dict[str, int]:
    expected = {field.name: field.field_id for field in schema.fields}
    tags = schema.find_field("tags").field_type
    return {**expected, "key": tags.key_id, "value": tags.value_id}


def wait_for_reader(scratch: Path, index: int, deadline: float, **evidence) -> float:
    started = time.monotonic()
    pending = scratch / f"stage-{index}.tmp"
    pending.write_text(json.dumps(evidence))
    pending.replace(scratch / f"stage-{index}.json")
    resume = scratch / f"resume-{index}"
    while not resume.is_file():
        require_time(deadline)
        time.sleep(0.01)
    return time.monotonic() - started


def stage_day(table, transaction, file_io: ObservedFileIO, settings: dict, deadline: float, result: dict) -> None:
    from datetime import UTC
    from datetime import time as day_time

    ecosystem, tenant = settings["ecosystem"], settings["tenant_id"]
    day = date.fromisoformat(settings["day"])
    key = IcebergExportKey(str(table.metadata.table_uuid), ecosystem, tenant, day)
    backend = create_storage_backend(
        StorageConfig(connection_string=settings["connection"]),
        storage_module=get_storage_module_for_ecosystem(ecosystem),
    )
    assert isinstance(backend, IcebergExportStorageBackend)
    output_schema = table.schema().as_arrow()
    start = datetime.combine(day, day_time.min, tzinfo=UTC)
    scratch_root = Path(os.environ["SQLITE_TMPDIR"])
    try:
        with IcebergDaySpool(
            deadline, limit_bytes=settings.get("scratch_limit", 224 * MIB), batch_bytes=16 * MIB
        ) as spool:
            capture = time.monotonic()
            with backend.create_iceberg_read_unit_of_work(deadline=deadline) as uow:
                state = uow.pipeline_state.get(ecosystem, tenant, day)
                assert state is not None and state.chargeback_calculated
                assert state.calculation_id == settings["revision"]
                for fact in uow.iter_iceberg_chargebacks(
                    ecosystem,
                    tenant,
                    start,
                    start + timedelta(days=1),
                    max_row_bytes=8 * MIB,
                    max_batch_bytes=8 * MIB,
                ):
                    assert fact.dimension_id is not None
                    spool.add(_project(fact, key, state), dimension_id=fact.dimension_id)
            spool.finish()
            result.update(
                capture_seconds=time.monotonic() - capture,
                source_rows=spool.row_count,
                rows=0,
                scratch_bytes=scratch_bytes(scratch_root),
                batches=0,
                max_arrow_bytes=0,
            )
            write = time.monotonic()
            paused = 0.0
            barriers = 0
            batches = spool.batches(16 * MIB, max_rows=16_384)
            if settings.get("short_batch_iterator"):
                batches = islice(batches, 1)
            for batch in batches:
                require_time(deadline)
                arrow = pa.Table.from_pylist(batch, schema=output_schema)
                assert len(batch) <= 16_384 and arrow.nbytes <= 16 * MIB, (len(batch), arrow.nbytes)
                transaction.append(arrow)
                result["rows"] += arrow.num_rows
                result["batches"] += 1
                result["max_arrow_bytes"] = max(result["max_arrow_bytes"], arrow.nbytes)
                result["scratch_bytes"] = max(result["scratch_bytes"], scratch_bytes(scratch_root))
                assert result["scratch_bytes"] <= 224 * MIB
                assert arrow.nbytes + result["scratch_bytes"] <= 256 * MIB
                staged_rows = result["rows"]
                del arrow
                first = barriers == 0 and result["batches"] == 1
                middle = barriers == 1 and staged_rows * 2 >= spool.row_count
                if settings.get("hold_batches") and (first or (middle and staged_rows < spool.row_count)):
                    barriers += 1
                    paused += wait_for_reader(
                        scratch_root,
                        barriers,
                        deadline,
                        staged_rows=staged_rows,
                        total_rows=spool.row_count,
                        batches=result["batches"],
                        files=sorted(file_io.data_files),
                    )
            if result["rows"] != spool.row_count:
                raise IncompleteDayError
            result["write_seconds"] = time.monotonic() - write - paused
            result["pause_seconds"] = paused
    finally:
        backend.dispose()


def attempt(settings: dict) -> dict:
    deadline = settings["deadline"]
    started = time.monotonic()
    file_io = None
    result: dict = {"status": "ok"}
    try:
        require_time(deadline)
        with public_catalog(settings) as catalog:
            table = catalog.load_table(settings["table"])
            file_io = table.io
            assert isinstance(file_io, ObservedFileIO)
            assert table.format_version == 2
            assert table.properties.get("commit.retry.num-retries") == "0"
            result["field_ids"] = file_ids(table.schema())
            if not settings.get("load_only"):
                scope = And(
                    EqualTo("ecosystem", settings["ecosystem"]),
                    EqualTo("tenant_id", settings["tenant_id"]),
                    EqualTo("chargeback_date", date.fromisoformat(settings["day"])),
                )
                result["intended_uuid"] = str(table.metadata.table_uuid)
                with table.transaction() as transaction:
                    transaction.delete(scope)
                    stage_day(table, transaction, file_io, settings, deadline, result)
                    result["intended_snapshot"] = transaction.table_metadata.current_snapshot_id
                    require_time(deadline)
                    commit = time.monotonic()
                result.update(
                    transaction_seconds=time.monotonic() - commit,
                    snapshot=table.metadata.current_snapshot_id,
                    table_uuid=str(table.metadata.table_uuid),
                )
    except Exception as exc:
        result.update(status="error", error_type=type(exc).__name__)
    finally:
        if file_io is not None:
            result.update(
                old_data_reads=len(file_io.old_data_reads),
                metadata_reads=file_io.metadata_reads,
                metadata_lengths=file_io.metadata_lengths,
                files=len(file_io.data_files),
            )
        result["peak_rss_mib"] = peak_rss_mib()
        result["elapsed"] = time.monotonic() - started
        assert "core.emitters.iceberg_transport" not in sys.modules
        assert "core.emitters.iceberg_file_io" not in sys.modules
    return result


if __name__ == "__main__":
    print(json.dumps(attempt(json.load(sys.stdin))))

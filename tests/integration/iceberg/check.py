from __future__ import annotations

import io
import json
import logging
import os
import subprocess
import sys
import threading
import time
from collections.abc import Callable
from contextlib import ExitStack, chdir, contextmanager, redirect_stderr
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING
from unittest.mock import patch
from urllib.parse import quote, urlsplit

import duckdb
import duckdb_core_ext_avro
import duckdb_core_ext_httpfs
import duckdb_core_ext_iceberg
import pyarrow as pa
import requests
import yaml
from botocore.auth import S3SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.credentials import Credentials
from fsspec.asyn import sync
from fsspec.config import conf as filesystem_config
from gateway import (
    ADMIN_KEY,
    ADMIN_SECRET,
    BUCKET,
    CATALOG_TOKEN,
    NAMESPACE,
    REGION,
    S3_HOST,
    S3_URL,
    TABLES,
    catalog_gateway,
    storage_gateway,
    table_key,
    table_prefix,
    table_secret,
    table_token,
)
from pyiceberg.catalog.rest import RestCatalog, StorageCredential, TableResponse
from pyiceberg.io.pyarrow import _pyarrow_to_schema_without_ids
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import assign_fresh_schema_ids
from pyiceberg.table import TableProperties
from pyiceberg.table.locations import load_location_provider
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import StringType

from core.config.loader import load_config
from core.config.models import IcebergCatalogConfig, StorageConfig
from core.emitters.iceberg_file_io import IcebergFileIO, storage_file_io
from core.emitters.iceberg_transport import (
    ICEBERG_ARROW_SCHEMA,
    ICEBERG_SCOPE_FIELDS,
    ICEBERG_TABLE_PROPERTIES,
    IcebergConflict,
    IcebergDestinationError,
    open_iceberg_catalog,
)
from core.models.chargeback import ChargebackRow, CostType
from core.models.pipeline import PipelineState
from core.storage.iceberg import IcebergExportCheckpoint, IcebergExportKey
from core.storage.registry import create_storage_backend
from plugins.storage_modules import get_storage_module_for_ecosystem

if TYPE_CHECKING:
    from gateway import CatalogGateway
    from pyiceberg.table import Table

    from core.storage.interface import StorageBackend

ECOSYSTEM = "generic_metrics_only"
DAY = datetime.now(UTC).date() - timedelta(days=3)
NEIGHBOR = DAY - timedelta(days=1)
WAREHOUSE = f"s3://{BUCKET}/"
REST_URL = "http://catalog:8181"
ICEBERG_EPOCH = date(1970, 1, 1)


def mc(*args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["mc", *args],
        env={
            **os.environ,
            "MC_HOST_admin": f"http://{ADMIN_KEY}:{ADMIN_SECRET}@{S3_HOST}",
        },
        check=check,
        capture_output=True,
        text=True,
        timeout=10,
    )


def provision_storage(root: Path) -> None:
    deadline = time.monotonic() + 30
    while mc("ready", "admin", check=False).returncode != 0:
        assert time.monotonic() < deadline, "MinIO did not become ready"
        time.sleep(0.25)
    mc("mb", f"admin/{BUCKET}")
    for table in TABLES:
        policy = root / f"{table}.json"
        policy.write_text(
            json.dumps(
                {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
                            "Resource": [
                                f"arn:aws:s3:::{table_prefix(table)}{folder}/*" for folder in ("data", "metadata")
                            ],
                        }
                    ],
                }
            )
        )
        mc("admin", "user", "add", "admin", table_key(table), table_secret(table))
        mc("admin", "policy", "create", "admin", table, str(policy))
        mc("admin", "policy", "attach", "admin", table, "--user", table_key(table))


def qualified(table: str) -> str:
    return f"{NAMESPACE}.{table}"


def table_schema_and_spec(
    partition_fields: tuple[tuple[str, str], ...] | None = None,
):
    schema = assign_fresh_schema_ids(_pyarrow_to_schema_without_ids(ICEBERG_ARROW_SCHEMA))
    fields = partition_fields or tuple((name, name) for name in ICEBERG_SCOPE_FIELDS)
    partitions = PartitionSpec(
        *[
            PartitionField(
                source_id=schema.find_field(source_name).field_id,
                field_id=1000 + index,
                transform=IdentityTransform(),
                name=partition_name,
            )
            for index, (source_name, partition_name) in enumerate(fields)
        ]
    )
    return schema, partitions


def table_properties(*, full_metrics: bool = False, metrics_default: str | None = None) -> dict[str, str]:
    assert not (full_metrics and metrics_default is not None)
    metrics = "full" if full_metrics else metrics_default
    return {
        **ICEBERG_TABLE_PROPERTIES,
        **({"write.metadata.metrics.default": metrics} if metrics is not None else {}),
    }


def provision_tables() -> None:
    schema, partitions = table_schema_and_spec()
    with admin_catalog() as catalog:
        catalog.create_namespace(NAMESPACE)
        for table in TABLES:
            catalog.create_table(
                qualified(table),
                schema=schema,
                partition_spec=partitions,
                location=f"s3://{table_prefix(table)}",
                properties=table_properties(),
            )


def reset_table(
    table: str,
    *,
    full_metrics: bool = False,
    partition_fields: tuple[tuple[str, str], ...] | None = None,
    metrics_default: str | None = None,
) -> None:
    """Recreate one fixture table to isolate independent worker proofs."""
    schema, partitions = table_schema_and_spec(partition_fields)
    with admin_catalog() as catalog:
        identifier = qualified(table)
        catalog.drop_table(identifier)
        catalog.create_table(
            identifier,
            schema=schema,
            partition_spec=partitions,
            location=f"s3://{table_prefix(table)}",
            properties=table_properties(full_metrics=full_metrics, metrics_default=metrics_default),
        )


def admin_catalog() -> RestCatalog:
    return RestCatalog(
        "local",
        **{
            "uri": REST_URL,
            "warehouse": WAREHOUSE,
            "s3.endpoint": S3_URL,
            "s3.region": REGION,
            "s3.access-key-id": ADMIN_KEY,
            "s3.secret-access-key": ADMIN_SECRET,
        },
    )


def catalog_settings(gateway: CatalogGateway, s3_endpoint: str = S3_URL) -> dict[str, object]:
    return {
        "type": "rest",
        "uri": gateway.uri,
        "token": CATALOG_TOKEN,
        "warehouse": WAREHOUSE,
        "ca_file": str(gateway.ca),
        "s3": {"region": REGION, "endpoint": s3_endpoint},
    }


def chargeback(tenant_id: str, day: date, amount: str) -> ChargebackRow:
    return ChargebackRow(
        ecosystem=ECOSYSTEM,
        tenant_id=tenant_id,
        timestamp=datetime(day.year, day.month, day.day, 12, tzinfo=UTC),
        identity_id="identity",
        product_category="compute",
        product_type="storage",
        cost_type=CostType.USAGE,
        amount=Decimal(amount),
        resource_id=f"cluster-{tenant_id}",
    )


def projected_row(tenant_id: str, day: date, amount: str) -> dict[str, object]:
    row = chargeback(tenant_id, day, amount)
    return {
        **{field: getattr(row, field) for field in row.__csv_fields__},
        "ecosystem": ECOSYSTEM,
        "tenant_id": tenant_id,
        "chargeback_date": day,
        "tags": {"owner": tenant_id},
        "team": None,
        "env_id": None,
        "calculation_id": None,
        "calculation_completed_at": None,
    }


def read_rows(gateway: CatalogGateway, table: str) -> list[tuple]:
    with duckdb_connection(gateway, table) as db:
        return db.execute(f"""
            SELECT tenant_id, chargeback_date, amount, tags['owner'], typeof(amount),
                   ecosystem, env_id, team, timestamp
            FROM warehouse.{qualified(table)} ORDER BY tenant_id, chargeback_date, timestamp
        """).fetchall()


@contextmanager
def duckdb_connection(gateway: CatalogGateway, table: str):
    with duckdb.connect(config={"autoinstall_known_extensions": "false", "autoload_known_extensions": "false"}) as db:
        for extension in (duckdb_core_ext_httpfs, duckdb_core_ext_avro, duckdb_core_ext_iceberg):
            db.load_extension(str(extension.get_extension_load_path()))
        db.execute(f"SET ca_cert_file='{gateway.ca}'; SET enable_server_cert_verification=true")
        db.execute(f"CREATE SECRET catalog_token (TYPE iceberg, TOKEN '{CATALOG_TOKEN}')")
        db.execute(f"""
            CREATE SECRET table_storage (
                TYPE s3, KEY_ID '{table_key(table)}', SECRET '{table_secret(table)}', REGION '{REGION}',
                ENDPOINT '{S3_HOST}', URL_STYLE 'path', USE_SSL false
            )
        """)
        db.execute(f"""
            ATTACH '{WAREHOUSE}' AS warehouse (
                TYPE iceberg, URI '{gateway.uri}', SECRET catalog_token, ACCESS_DELEGATION_MODE 'none'
            )
        """)
        yield db


def expected(
    tenant_id: str,
    day: date,
    amount: str,
    *,
    owner: str | None = None,
    ecosystem: str = ECOSYSTEM,
    env_id: str | None = None,
    team: str | None = None,
    hour: int = 12,
) -> tuple:
    return (
        tenant_id,
        day,
        Decimal(amount),
        owner or tenant_id,
        "DECIMAL(38,5)",
        ecosystem,
        env_id,
        team,
        datetime(day.year, day.month, day.day, hour, tzinfo=UTC),
    )


def verify_object_access(tables: dict[str, Table]) -> None:
    for table, loaded in tables.items():
        data_path = str(loaded.inspect.files()["file_path"][0])
        assert data_path.startswith(f"s3://{table_prefix(table)}data/"), data_path
        for uri in (loaded.metadata_location, data_path):
            url = f"{S3_URL}/{quote(uri.removeprefix('s3://'))}"
            for reader in TABLES:
                request = AWSRequest(method="HEAD", url=url)
                credentials = Credentials(table_key(reader), table_secret(reader))
                S3SigV4Auth(credentials, "s3", REGION).add_auth(request)
                response = requests.head(url, headers=dict(request.headers), timeout=5)
                assert response.status_code == (200 if reader == table else 403), (uri, reader, response.status_code)


def verify_shared_transport(gateway: CatalogGateway) -> None:
    catalog = IcebergCatalogConfig.model_validate(catalog_settings(gateway))

    def write(table: str, tenant: str, day: date, amount: str) -> int:
        deadline = time.monotonic() + 12
        loaded = client.load_table(qualified(table), deadline=deadline)
        return loaded.overwrite(
            [projected_row(tenant, day, amount)],
            ecosystem=ECOSYSTEM,
            tenant_id=tenant,
            chargeback_date=day,
            max_bytes=32 * 1024 * 1024,
            deadline=deadline,
        )

    with open_iceberg_catalog(catalog, deadline=time.monotonic() + 12) as client:
        write("alpha", "alpha", NEIGHBOR, "2.00000")
        write("alpha", "other", DAY, "4.00000")
        write("alpha", "alpha", DAY, "1.23456")
        gateway.requests.clear()
        gateway.next_commit_fault = "reject"
        try:
            write("alpha", "alpha", DAY, "8.88888")
        except IcebergConflict:
            pass
        else:
            raise AssertionError("Injected alpha commit rejection did not reach the caller")
        write("bravo", "bravo", DAY, "6.00000")
        write("alpha", "alpha", DAY, "-1.23456")

    expected_requests = [
        request
        for table in ("alpha", "bravo", "alpha")
        for request in [("GET", table, f"Bearer {CATALOG_TOKEN}"), ("POST", table, f"Bearer {table_token(table)}")]
    ]
    assert gateway.requests == expected_requests, gateway.requests
    with admin_catalog() as catalog_admin:
        verify_object_access({table: catalog_admin.load_table(qualified(table)) for table in TABLES})
    assert read_rows(gateway, "alpha") == [
        expected("alpha", NEIGHBOR, "2.00000"),
        expected("alpha", DAY, "-1.23456"),
        expected("other", DAY, "4.00000"),
    ]
    assert read_rows(gateway, "bravo") == [expected("bravo", DAY, "6.00000")]
    print("PASS shared catalog bearer isolation and table-specific MinIO metadata and data credentials")


def seed(
    backend: StorageBackend,
    tenant: str,
    revision: str,
    *rows: ChargebackRow,
    ecosystem: str = ECOSYSTEM,
    day: date = DAY,
) -> None:
    with backend.create_unit_of_work() as uow:
        uow.chargebacks.delete_by_date(ecosystem, tenant, day)
        for row in rows:
            uow.chargebacks.upsert(row)
            if not uow.tags.get_tags(tenant, "resource", row.resource_id):
                uow.tags.add_tag(tenant, "resource", row.resource_id, "owner", tenant, "fixture")
        uow.pipeline_state.upsert(
            PipelineState(
                ecosystem,
                tenant,
                day,
                billing_gathered=True,
                resources_gathered=True,
                chargeback_calculated=True,
                calculation_id=revision,
                calculation_completed_at=datetime.now(UTC),
            )
        )
        uow.commit()


_LARGE_DAY_DIMENSIONS = 128
_LARGE_DAY_TAGS = 8
_LARGE_DAY_BATCH = 10_000
_POSITIVE_AMOUNT = Decimal("1.234565")
_NEGATIVE_AMOUNT = Decimal("-0.234565")


def large_day_row(tenant_id: str, day: date, index: int, *, ecosystem: str = ECOSYSTEM) -> ChargebackRow:
    dimension = index % _LARGE_DAY_DIMENSIONS
    metadata = {}
    if ecosystem == "confluent_cloud":
        metadata["env_id"] = f"env-{dimension % 8:02d}"
    elif ecosystem == "self_managed_kafka":
        metadata["team"] = f"team-{dimension % 8:02d}"
    return ChargebackRow(
        ecosystem=ecosystem,
        tenant_id=tenant_id,
        timestamp=datetime(day.year, day.month, day.day, 12, tzinfo=UTC)
        + timedelta(seconds=index // _LARGE_DAY_DIMENSIONS),
        identity_id=f"identity-{dimension:03d}",
        product_category="compute",
        product_type="storage",
        cost_type=CostType.USAGE,
        amount=_POSITIVE_AMOUNT if index % 2 == 0 else _NEGATIVE_AMOUNT,
        resource_id=f"resource-{dimension:03d}",
        metadata=metadata,
    )


def large_day_total(row_count: int) -> Decimal:
    return ((row_count + 1) // 2) * _POSITIVE_AMOUNT + (row_count // 2) * _NEGATIVE_AMOUNT


def seed_large_day(
    backend: StorageBackend,
    tenant_id: str,
    revision: str,
    row_count: int,
    day: date = DAY,
    *,
    ecosystem: str = ECOSYSTEM,
    previous_count: int = 0,
) -> Decimal:
    """Seed generated source facts in bounded SQL transactions with wide resource tags."""
    if previous_count == 0:
        with backend.create_unit_of_work() as uow:
            uow.chargebacks.delete_by_date(ecosystem, tenant_id, day)
            uow.commit()

    for start in range(previous_count, row_count, _LARGE_DAY_BATCH):
        stop = min(start + _LARGE_DAY_BATCH, row_count)
        with backend.create_unit_of_work() as uow:
            rows = [large_day_row(tenant_id, day, index, ecosystem=ecosystem) for index in range(start, stop)]
            assert uow.chargebacks.upsert_batch(rows) == len(rows)
            uow.commit()

    tags = [
        {
            "entity_type": "resource",
            "entity_id": f"resource-{dimension:03d}",
            "tag_key": "owner" if tag_index == 0 else f"wide_{tag_index:02d}",
            "tag_value": f"{tenant_id}-resource-{dimension:03d}-tag-{tag_index:02d}-{'x' * 8}",
        }
        for dimension in range(_LARGE_DAY_DIMENSIONS)
        for tag_index in range(_LARGE_DAY_TAGS)
    ]
    with backend.create_unit_of_work() as uow:
        uow.tags.bulk_add_tags(tenant_id, tags, override_existing=True, created_by="iceberg-docker-fixture")
        uow.pipeline_state.upsert(
            PipelineState(
                ecosystem,
                tenant_id,
                day,
                billing_gathered=True,
                resources_gathered=True,
                chargeback_calculated=True,
                calculation_id=revision,
                calculation_completed_at=datetime.now(UTC),
            )
        )
        uow.commit()
    return large_day_total(row_count)


def worker_config(root: Path, gateway: CatalogGateway, tables: tuple[str, ...], *, s3_endpoint: str = S3_URL) -> Path:
    catalog = catalog_settings(gateway, s3_endpoint)
    catalog["token"] = "${FIXTURE_CATALOG_TOKEN}"
    config = root / "worker.yaml"
    config.write_text(
        yaml.safe_dump(
            {
                "features": {"refresh_interval": 60},
                "tenants": {
                    table: {
                        "ecosystem": ECOSYSTEM,
                        "tenant_id": table,
                        "lookback_days": 2,
                        "cutoff_days": 1,
                        "storage": {"connection_string": f"sqlite:///{root / f'{table}.sqlite'}"},
                        "plugin_settings": {
                            "cluster_id": f"cluster-{table}",
                            "metrics": {"url": gateway.uri},
                            "identity_source": {
                                "source": "static",
                                "static_identities": [
                                    {"identity_id": "identity", "identity_type": "user"},
                                ],
                            },
                            "cost_types": [
                                {
                                    "name": "storage",
                                    "product_category": "compute",
                                    "rate": 1,
                                    "cost_quantity": {"type": "storage_gib", "query": "fixture_usage"},
                                    "allocation_strategy": "even_split",
                                }
                            ],
                            "emitters": [
                                {"type": "iceberg", "params": {"catalog": catalog, "table": qualified(table)}}
                            ],
                        },
                    }
                    for table in tables
                },
            }
        )
    )
    return config


def worker_process(
    config: Path,
    gateway: CatalogGateway,
    *args: str,
    stderr: int = subprocess.STDOUT,
    env_overrides: dict[str, str] | None = None,
) -> subprocess.Popen[str]:
    environment = {**os.environ, "SSL_CERT_FILE": str(gateway.ca), "FIXTURE_CATALOG_TOKEN": CATALOG_TOKEN}
    if env_overrides:
        environment.update(env_overrides)
    return subprocess.Popen(
        [sys.executable, "-m", "main", "--config-file", str(config), *args],
        env=environment,
        stdout=subprocess.PIPE,
        stderr=stderr,
        text=True,
    )


def finish_worker(process: subprocess.Popen[str], seconds: float) -> str:
    try:
        output, errors = process.communicate(timeout=seconds)
    except subprocess.TimeoutExpired:
        process.kill()
        output, errors = process.communicate()
        raise AssertionError(f"Worker did not exit in {seconds}s\n{output}{errors or ''}") from None
    captured = output + (errors or "")
    assert process.returncode == 0, captured
    assert CATALOG_TOKEN not in captured, "Worker exposed the catalog bearer"
    for table in TABLES:
        assert table_token(table) not in captured, "Worker exposed a table bearer"
        assert table_secret(table) not in captured, "Worker exposed a table storage secret"
    return output


def sample_worker_process(process: subprocess.Popen[str], samples: list[tuple[int, int]]) -> None:
    """Sample the worker's RSS high-water mark and unlinked SQLite scratch size."""
    proc = Path(f"/proc/{process.pid}")
    while process.poll() is None:
        try:
            status = (proc / "status").read_text()
            rss_kib = next(int(line.split()[1]) for line in status.splitlines() if line.startswith("VmHWM:"))
        except FileNotFoundError, StopIteration:
            return
        scratch_bytes = 0
        for fd in (proc / "fd").glob("*"):
            try:
                target = os.readlink(fd)
                if "etilqs_" in target and target.endswith(" (deleted)"):
                    scratch_bytes += fd.stat().st_size
            except FileNotFoundError:
                continue
        samples.append((rss_kib, scratch_bytes))
        time.sleep(0.05)


def _scratch_fd_open(process: subprocess.Popen[str]) -> bool:
    for fd in Path(f"/proc/{process.pid}/fd").glob("*"):
        try:
            if "etilqs_" in os.readlink(fd):
                return True
        except FileNotFoundError:
            continue
    return False


def _checkpoint(
    backend: StorageBackend, table: str, *, table_uuid: str | None = None
) -> IcebergExportCheckpoint | None:
    if table_uuid is None:
        table_uuid, _, _ = snapshot_metadata(table)
    with backend.create_unit_of_work() as uow:
        return uow.iceberg_exports.get(IcebergExportKey(table_uuid, ECOSYSTEM, table, DAY))


def _change_revision(backend: StorageBackend, tenant_id: str, revision: str) -> None:
    with backend.create_unit_of_work() as uow:
        state = uow.pipeline_state.get(ECOSYSTEM, tenant_id, DAY)
        assert state is not None, (tenant_id, "missing pipeline state")
        uow.pipeline_state.upsert(replace(state, calculation_id=revision, calculation_completed_at=datetime.now(UTC)))
        uow.commit()


def _fill_scratch(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    filler = path / "fixture-fill"
    available = os.statvfs(path).f_bavail * os.statvfs(path).f_frsize
    target = max(0, available - 8 * 1024 * 1024)
    chunk = b"x" * (1024 * 1024)
    with filler.open("wb") as stream:
        for _ in range(target // len(chunk)):
            stream.write(chunk)
        stream.write(b"x" * (target % len(chunk)))
        stream.flush()
        os.fsync(stream.fileno())
    return filler


def cli_report(config: Path, gateway: CatalogGateway, *args: str) -> list[dict[str, object]]:
    process = worker_process(config, gateway, *args, stderr=subprocess.PIPE)
    return [json.loads(line) for line in finish_worker(process, 25).splitlines()]


def snapshot_metadata(table: str) -> tuple[str, int, str]:
    with admin_catalog() as catalog:
        loaded = catalog.load_table(qualified(table))
        return (str(loaded.metadata.table_uuid), loaded.current_snapshot().snapshot_id, loaded.metadata_location)


def source_backend(stack: ExitStack, root: Path, tenant: str, ecosystem: str) -> StorageBackend:
    backend = create_storage_backend(
        StorageConfig(connection_string=f"sqlite:///{root / f'{tenant}.sqlite'}"),
        storage_module=get_storage_module_for_ecosystem(ecosystem),
    )
    stack.callback(backend.dispose)
    backend.create_tables()
    return backend


def verify_worker(root: Path, gateway: CatalogGateway) -> None:
    with ExitStack() as stack:
        backends = {table: source_backend(stack, root, table, ECOSYSTEM) for table in TABLES}

        alpha = backends["alpha"]
        seed(alpha, "alpha", "alpha-first", chargeback("alpha", DAY, "3.45678"))
        config = worker_config(root, gateway, ("alpha",))
        table_uuid, former_snapshot, _ = snapshot_metadata("alpha")
        key = IcebergExportKey(table_uuid, ECOSYSTEM, "alpha", DAY)
        gateway.next_commit_fault = "lose-response"
        output = finish_worker(worker_process(config, gateway, "--run-once"), 45)
        _, uncertain_snapshot, uncertain_metadata = snapshot_metadata("alpha")
        assert former_snapshot != uncertain_snapshot and gateway.lost_commit_metadata == uncertain_metadata, output
        with alpha.create_unit_of_work() as uow:
            uncertain = uow.iceberg_exports.get(key)
            assert uncertain is not None and uncertain.outcome == "unconfirmed", (uncertain, output)
            assert uncertain.attempted_revision == "alpha-first"
            assert uncertain.confirmed_revision is None and uncertain.snapshot_id is None
        rows = [
            expected("alpha", NEIGHBOR, "2.00000"),
            expected("alpha", DAY, "3.45678"),
            expected("other", DAY, "4.00000"),
        ]
        assert read_rows(gateway, "alpha") == rows
        output = finish_worker(worker_process(config, gateway, "--run-once"), 45)
        _, recovered_snapshot, _ = snapshot_metadata("alpha")
        assert recovered_snapshot != uncertain_snapshot, output
        with alpha.create_unit_of_work() as uow:
            confirmed = uow.iceberg_exports.get(key)
            assert confirmed is not None and confirmed.outcome == "succeeded", (confirmed, output)
            assert confirmed.confirmed_revision == "alpha-first" and confirmed.snapshot_id == recovered_snapshot
            assert confirmed.row_count == 1
        assert read_rows(gateway, "alpha") == rows
        print("PASS remote commit with lost response left SQL unconfirmed and restart replayed without duplicates")

        seed(alpha, "alpha", "alpha-empty")
        seed(backends["bravo"], "bravo", "bravo-first", chargeback("bravo", DAY, "9.87654"))
        config = worker_config(root, gateway, TABLES)
        gateway.requests.clear()
        gateway.stall_lookup = True
        with worker_process(config, gateway, "--mode", "worker") as process:
            try:
                assert gateway.lookup_received.wait(timeout=35), "Worker did not reach the stalled catalog lookup"
                start = time.monotonic()
                process.terminate()
                output = finish_worker(process, 8)
                elapsed = time.monotonic() - start
            finally:
                if process.poll() is None:
                    process.kill()
                gateway.stall_lookup = False
                gateway.release_lookup.set()
        assert "Received signal 15" in output and "Chargeback engine worker stopped" in output, output
        assert [request for request in gateway.requests if request[1]] == [
            ("GET", "alpha", f"Bearer {CATALOG_TOKEN}"),
        ], (gateway.requests, output)
        assert snapshot_metadata("alpha")[1] == recovered_snapshot
        with alpha.create_unit_of_work() as uow:
            assert uow.iceberg_exports.get(key) == confirmed
        print(f"PASS worker SIGTERM during catalog stall in {elapsed:.2f}s")

        with storage_gateway() as s3:
            config = worker_config(root, gateway, TABLES, s3_endpoint=s3.endpoint)
            start = time.monotonic()
            output = finish_worker(worker_process(config, gateway, "--run-once"), 12)
            stall_elapsed = time.monotonic() - start
            assert s3.stalled.is_set(), output
            assert snapshot_metadata("alpha")[1] == recovered_snapshot
            with alpha.create_unit_of_work() as uow:
                stalled = uow.iceberg_exports.get(key)
                assert stalled is not None and stalled.outcome == "failed", (stalled, output)
                assert stalled.attempted_revision == "alpha-empty" and stalled.confirmed_revision == "alpha-first"
            assert read_rows(gateway, "alpha") == rows
            assert read_rows(gateway, "bravo") == [expected("bravo", DAY, "9.87654")]
            bravo_snapshot = snapshot_metadata("bravo")[1]

            s3.release.set()
            gateway.requests.clear()
            output = finish_worker(worker_process(config, gateway, "--run-once"), 45)
        assert [request for request in gateway.requests if not request[1]] == [
            ("GET", "", f"Bearer {CATALOG_TOKEN}"),
        ], (gateway.requests, output)
        _, cleared_snapshot, _ = snapshot_metadata("alpha")
        assert cleared_snapshot != recovered_snapshot, output
        with alpha.create_unit_of_work() as uow:
            cleared = uow.iceberg_exports.get(key)
            assert cleared is not None and cleared.confirmed_revision == "alpha-empty", (cleared, output)
            assert cleared.row_count == 0 and cleared.snapshot_id == cleared_snapshot
        assert read_rows(gateway, "alpha") == [rows[0], rows[2]]
        assert snapshot_metadata("bravo")[1] == bravo_snapshot
        print(f"PASS MinIO stall pass ended in {stall_elapsed:.2f}s, preserved alpha and published bravo")

        config = worker_config(root, gateway, TABLES)
        day = DAY.isoformat()
        (empty,) = cli_report(config, gateway, "--iceberg-status", "alpha", day, day)
        assert empty["current"] and empty["row_count"] == 0 and empty["outcome"] == "succeeded", empty
        assert empty["snapshot_id"] == cleared_snapshot
        (replayed,) = cli_report(config, gateway, "--emit-once", "--iceberg-replay", "alpha", day, day)
        assert replayed["current"] and replayed["row_count"] == 0 and replayed["outcome"] == "succeeded", replayed
        assert replayed["replay_outcome"] == "confirmed", replayed
        assert replayed["snapshot_id"] == snapshot_metadata("alpha")[1] != cleared_snapshot
        assert read_rows(gateway, "alpha") == [rows[0], rows[2]]
        assert snapshot_metadata("bravo")[1] == bravo_snapshot
        print("PASS scoped replay of a confirmed empty day advanced its snapshot and preserved neighbors")


def verify_plugin_cli(root: Path, gateway: CatalogGateway) -> None:
    cloud_ecosystem = "confluent_cloud"
    kafka_ecosystem = "self_managed_kafka"
    bindings = (("cloud", cloud_ecosystem, "alpha"), ("kafka", kafka_ecosystem, "bravo"))
    catalog = catalog_settings(gateway)
    catalog["token"] = "${FIXTURE_CATALOG_TOKEN}"
    config = root / "standalone.yaml"
    config.write_text(
        yaml.safe_dump(
            {
                "tenants": {
                    tenant: {
                        "ecosystem": ecosystem,
                        "tenant_id": tenant,
                        "storage": {"connection_string": f"sqlite:///{root / f'{tenant}.sqlite'}"},
                        "plugin_settings": {
                            "emitters": [
                                {"type": "iceberg", "params": {"catalog": catalog, "table": qualified(table)}},
                            ]
                        },
                    }
                    for tenant, ecosystem, table in bindings
                },
            }
        )
    )
    target_date, neighbor_date = DAY.isoformat(), NEIGHBOR.isoformat()

    with ExitStack() as stack:
        cloud = source_backend(stack, root, "cloud", cloud_ecosystem)
        kafka = source_backend(stack, root, "kafka", kafka_ecosystem)
        hourly = replace(chargeback("cloud", DAY, "1.2345"), ecosystem=cloud_ecosystem, metadata={"env_id": "env-01"})
        later = replace(hourly, timestamp=hourly.timestamp.replace(hour=14), amount=Decimal("-0.1234"))
        neighbor = replace(hourly, timestamp=hourly.timestamp - timedelta(days=1), amount=Decimal("2.3456"))
        daily = chargeback("kafka", DAY, "7.65432")
        daily = replace(
            daily, timestamp=daily.timestamp.replace(hour=0), ecosystem=kafka_ecosystem, metadata={"team": "payments"}
        )
        seed(cloud, "cloud", "cloud-neighbor", neighbor, ecosystem=cloud_ecosystem, day=NEIGHBOR)
        seed(cloud, "cloud", "cloud-target", hourly, later, ecosystem=cloud_ecosystem)
        seed(kafka, "kafka", "kafka-target", daily, ecosystem=kafka_ecosystem)

        pending = cli_report(config, gateway, "--iceberg-status", "cloud", neighbor_date, target_date)
        assert [(item["date"], item["current"], item["row_count"]) for item in pending] == [
            (neighbor_date, False, None),
            (target_date, False, None),
        ], pending
        finish_worker(worker_process(config, gateway, "--emit-once"), 40)
        cloud_rows = [
            expected("cloud", NEIGHBOR, "2.3456", ecosystem=cloud_ecosystem, env_id="env-01"),
            expected("cloud", DAY, "1.2345", ecosystem=cloud_ecosystem, env_id="env-01"),
            expected("cloud", DAY, "-0.1234", ecosystem=cloud_ecosystem, env_id="env-01", hour=14),
        ]
        alpha_neighbors = [expected("alpha", NEIGHBOR, "2.00000"), expected("other", DAY, "4.00000")]
        bravo_rows = [
            expected("bravo", DAY, "9.87654"),
            expected("kafka", DAY, "7.65432", ecosystem=kafka_ecosystem, team="payments", hour=0),
        ]
        assert read_rows(gateway, "alpha") == [alpha_neighbors[0], *cloud_rows, alpha_neighbors[1]]
        assert read_rows(gateway, "bravo") == bravo_rows
        print("PASS common publisher exported Generic Metrics, Confluent Cloud environment, and Kafka historical team")

        before = cli_report(config, gateway, "--iceberg-status", "cloud", neighbor_date, target_date)
        assert [
            (item["date"], item["current"], item["confirmed_revision"], item["row_count"], item["outcome"])
            for item in before
        ] == [
            (neighbor_date, True, "cloud-neighbor", 1, "succeeded"),
            (target_date, True, "cloud-target", 2, "succeeded"),
        ], before

        with cloud.create_unit_of_work() as uow:
            (tag,) = uow.tags.get_tags("cloud", "resource", hourly.resource_id)
            uow.tags.update_tag(tag.tag_id, "replayed")
            uow.commit()
        bravo_snapshot = snapshot_metadata("bravo")[1]
        (replayed,) = cli_report(config, gateway, "--emit-once", "--iceberg-replay", "cloud", target_date, target_date)
        assert replayed["date"] == target_date and replayed["current"] and not replayed["replay_requested"], replayed
        assert replayed["row_count"] == 2 and replayed["confirmed_revision"] == "cloud-target", replayed
        assert replayed["snapshot_id"] == snapshot_metadata("alpha")[1] != before[1]["snapshot_id"], replayed
        assert replayed.pop("replay_outcome") == "confirmed" and replayed.pop("replay_error_code") is None
        after = cli_report(config, gateway, "--iceberg-status", "cloud", neighbor_date, target_date)
        assert after == [before[0], replayed], (after, before[0], replayed)
        assert read_rows(gateway, "alpha") == [
            alpha_neighbors[0],
            cloud_rows[0],
            expected("cloud", DAY, "1.2345", owner="replayed", ecosystem=cloud_ecosystem, env_id="env-01"),
            expected("cloud", DAY, "-0.1234", owner="replayed", ecosystem=cloud_ecosystem, env_id="env-01", hour=14),
            alpha_neighbors[1],
        ]
        assert snapshot_metadata("bravo")[1] == bravo_snapshot
        assert read_rows(gateway, "bravo") == bravo_rows
        print("PASS documented JSONL status and scoped replay refreshed only the selected tenant and date")


def active_day_file_count(table: str, tenant_id: str, target_day: date, *, ecosystem: str = ECOSYSTEM) -> int:
    with admin_catalog() as catalog:
        files = catalog.load_table(qualified(table)).inspect.files().to_pylist()
    selected = 0
    for item in files:
        partition = item.get("partition")
        value = (
            partition.get("chargeback_date")
            if isinstance(partition, dict)
            else getattr(partition, "chargeback_date", None)
        )
        partition_ecosystem = (
            partition.get("ecosystem") if isinstance(partition, dict) else getattr(partition, "ecosystem", None)
        )
        partition_tenant = (
            partition.get("tenant_id") if isinstance(partition, dict) else getattr(partition, "tenant_id", None)
        )
        if (
            (value == target_day or str(value) == target_day.isoformat())
            and partition_ecosystem == ecosystem
            and partition_tenant == tenant_id
        ):
            selected += 1
    assert selected > 0, f"No active data files for {table}/{tenant_id} {target_day}: {files[:2]}"
    return selected


def read_day_metrics(gateway: CatalogGateway, table: str, tenant_id: str, day: date) -> tuple:
    with duckdb_connection(gateway, table) as db:
        return db.execute(
            f"""
            SELECT count(*), CAST(sum(amount) AS DECIMAL(38,5)), min(amount), max(amount),
                   count(DISTINCT tags['owner']), max(length(tags['wide_01'])), any_value(typeof(amount))
            FROM warehouse.{qualified(table)}
            WHERE tenant_id = ? AND chargeback_date = ?
        """,
            [tenant_id, day],
        ).fetchone()


def read_large_day_rows(gateway: CatalogGateway, table: str, tenant_id: str, day: date) -> list[tuple]:
    with duckdb_connection(gateway, table) as db:
        return db.execute(
            f"""
            SELECT timestamp, resource_id, identity_id, amount, tags['owner'], tags['wide_01']
            FROM warehouse.{qualified(table)}
            WHERE tenant_id = ? AND chargeback_date = ?
            ORDER BY timestamp, resource_id
        """,
            [tenant_id, day],
        ).fetchall()


def verify_scratch_recovery(root: Path, gateway: CatalogGateway, backend: StorageBackend, table: str) -> None:
    """Exercise a real worker scratch-full failure and kill/restart on the same state volume."""
    scratch = Path("/state/scratch")
    config = worker_config(root, gateway, (table,))
    previous_revision = "bravo-large-100000"
    failed_revision = "bravo-large-100000-scratch-full"
    killed_revision = "bravo-large-100000-killed"
    table_uuid, previous_snapshot, _ = snapshot_metadata(table)
    previous = _checkpoint(backend, table, table_uuid=table_uuid)
    assert previous is not None and previous.outcome == "succeeded", previous
    assert previous.confirmed_revision == previous_revision and previous.snapshot_id == previous_snapshot, previous

    scratch_env = {"SQLITE_TMPDIR": str(scratch)}
    _change_revision(backend, table, failed_revision)
    gateway.requests.clear()
    filler = _fill_scratch(scratch)
    started = time.monotonic()
    try:
        output = finish_worker(worker_process(config, gateway, "--run-once", env_overrides=scratch_env), 45)
    finally:
        filler.unlink(missing_ok=True)
    failed = _checkpoint(backend, table, table_uuid=table_uuid)
    assert "scratch_byte_limit" in output, output
    assert failed is not None and failed.outcome == "failed", failed
    assert failed.error_code == "scratch_byte_limit" and failed.confirmed_revision == previous_revision, failed
    assert failed.snapshot_id == previous_snapshot and failed.row_count == 100_000, failed
    assert snapshot_metadata(table)[1] == previous_snapshot
    assert not list(scratch.iterdir()), list(scratch.iterdir())
    assert not [request for request in gateway.requests if request[0] == "POST"], gateway.requests

    _change_revision(backend, table, killed_revision)
    gateway.requests.clear()
    filler = _fill_scratch(scratch)
    process = worker_process(config, gateway, "--run-once", env_overrides=scratch_env)
    poll_deadline = time.monotonic() + 15
    try:
        while True:
            checkpoint = _checkpoint(backend, table, table_uuid=table_uuid)
            if (
                checkpoint is not None
                and checkpoint.attempted_revision == killed_revision
                and checkpoint.outcome == "unconfirmed"
                and _scratch_fd_open(process)
            ):
                break
            if process.poll() is not None:
                raise AssertionError(f"Worker exited before scratch kill: {process.returncode}")
            if time.monotonic() >= poll_deadline:
                raise AssertionError("Worker did not enter scratch before kill")
            time.sleep(0.02)
        process.kill()
        killed_output, killed_errors = process.communicate(timeout=5)
    finally:
        filler.unlink(missing_ok=True)
        if process.poll() is None:
            process.kill()
            process.communicate(timeout=5)
    assert CATALOG_TOKEN not in killed_output + (killed_errors or "")
    killed = _checkpoint(backend, table, table_uuid=table_uuid)
    assert killed is not None and killed.outcome == "unconfirmed", killed
    assert killed.attempted_revision == killed_revision and killed.confirmed_revision == previous_revision, killed
    assert killed.snapshot_id == previous_snapshot and snapshot_metadata(table)[1] == previous_snapshot, killed
    assert not list(scratch.iterdir()), list(scratch.iterdir())

    gateway.requests.clear()
    restart_started = time.monotonic()
    output = finish_worker(worker_process(config, gateway, "--run-once", env_overrides=scratch_env), 45)
    restart_elapsed = time.monotonic() - restart_started
    current = _checkpoint(backend, table, table_uuid=table_uuid)
    current_snapshot = snapshot_metadata(table)[1]
    assert current is not None and current.outcome == "succeeded", current
    assert current.confirmed_revision == killed_revision and current.row_count == 100_000, current
    assert current.snapshot_id == current_snapshot != previous_snapshot, current
    assert read_day_metrics(gateway, table, table, DAY)[:2] == (100_000, Decimal("50000.00000"))
    assert len([request for request in gateway.requests if request[0] == "POST" and request[1] == table]) == 1
    assert not list(scratch.iterdir()), list(scratch.iterdir())
    print(
        f"PASS scratch-full failure preserved snapshot and SIGKILL restart confirmed in "
        f"{time.monotonic() - started:.2f}s + {restart_elapsed:.2f}s"
    )


def verify_large_days(
    root: Path,
    gateway: CatalogGateway,
    cases: tuple[tuple[str, int], ...] = (("alpha", 60_000), ("bravo", 100_000)),
) -> None:
    with ExitStack() as stack:
        backends = {table: source_backend(stack, root, table, ECOSYSTEM) for table, _ in cases}
        for table, row_count in cases:
            tenant_total = seed_large_day(backends[table], table, f"{table}-large-{row_count}", row_count)
            config = worker_config(root, gateway, (table,))
            gateway.requests.clear()
            started = time.monotonic()
            process = worker_process(config, gateway, "--run-once")
            process_samples: list[tuple[int, int]] = []
            sampler = threading.Thread(target=sample_worker_process, args=(process, process_samples))
            sampler.start()
            try:
                output = finish_worker(process, 45)
            except AssertionError as exc:
                print(f"LARGE_DAY_WORKER_FAILURE table={table} rows={row_count}\n{exc}", flush=True)
                raise
            finally:
                sampler.join(timeout=2)
            elapsed = time.monotonic() - started
            metrics = read_day_metrics(gateway, table, table, DAY)
            exported = read_large_day_rows(gateway, table, table, DAY)
            expected_total = tenant_total.quantize(Decimal("0.00001"), rounding=ROUND_HALF_UP)
            assert metrics[0] == row_count, (table, metrics, output)
            assert len(exported) == row_count, (table, len(exported), output)
            for expected_index, (timestamp, resource_id, identity_id, amount, owner, wide_tag) in enumerate(exported):
                dimension = int(str(resource_id).rsplit("-", 1)[1])
                index = (
                    int((timestamp - datetime(DAY.year, DAY.month, DAY.day, 12, tzinfo=UTC)).total_seconds())
                    * _LARGE_DAY_DIMENSIONS
                    + dimension
                )
                assert index == expected_index, (table, expected_index, index)
                base_amount = Decimal("1.23456") if index % 2 == 0 else Decimal("-0.23457")
                expected_amount = base_amount + (Decimal("0.00001") if index < row_count // 2 else Decimal(0))
                assert identity_id == f"identity-{dimension:03d}"
                assert timestamp.date() == DAY
                assert amount == expected_amount
                assert owner == f"{table}-resource-{dimension:03d}-tag-00-{'x' * 8}"
                assert wide_tag == f"{table}-resource-{dimension:03d}-tag-01-{'x' * 8}"
            assert metrics[1] == expected_total, (table, metrics, expected_total, output)
            assert metrics[2:4] == (Decimal("-0.23457"), Decimal("1.23457")), metrics
            assert metrics[4] == _LARGE_DAY_DIMENSIONS and metrics[5] >= 24, metrics
            assert metrics[6] == "DECIMAL(38,5)", metrics
            files = active_day_file_count(table, table, DAY)
            assert files > 1, f"{table} {row_count} rows were not split into multiple files"
            table_uuid, snapshot_id, _ = snapshot_metadata(table)
            with backends[table].create_unit_of_work() as uow:
                checkpoint = uow.iceberg_exports.get(IcebergExportKey(table_uuid, ECOSYSTEM, table, DAY))
            assert checkpoint is not None, (table, "missing checkpoint")
            assert checkpoint.outcome == "succeeded", checkpoint
            assert checkpoint.confirmed_revision == f"{table}-large-{row_count}", checkpoint
            assert checkpoint.row_count == row_count, checkpoint
            assert checkpoint.snapshot_id == snapshot_id, (checkpoint, snapshot_id)
            posts = [request for request in gateway.requests if request[0] == "POST" and request[1] == table]
            assert len(posts) == 1, (table, posts, output)
            assert process_samples, (table, "missing process samples")
            peak_rss_mib = max(rss for rss, _ in process_samples) / 1024
            scratch_mib = max(scratch for _, scratch in process_samples) / (1024 * 1024)
            assert peak_rss_mib > 0 and scratch_mib > 0, (table, process_samples)
            print(
                f"PASS worker {row_count:,}-row day rows={metrics[0]} files={files} "
                f"total={metrics[1]} elapsed={elapsed:.2f}s posts={len(posts)} "
                f"peak_rss_mib={peak_rss_mib:.1f} sampled_scratch_mib={scratch_mib:.1f}"
            )
        if ("bravo", 100_000) in cases:
            verify_scratch_recovery(root, gateway, backends["bravo"], "bravo")


def trino_query(statement: str) -> list[list[object]]:
    """Run one SQL statement against the disposable Trino catalog."""
    url = "http://trino:8080/v1/statement"
    headers = {"X-Trino-User": "iceberg-export-check"}
    response = requests.post(url, data=statement, headers=headers, timeout=5)
    response.raise_for_status()
    result = response.json()
    rows: list[list[object]] = []
    deadline = time.monotonic() + 60
    while True:
        if result.get("error"):
            raise AssertionError(f"Trino query failed: {statement}: {result['error']}")
        rows.extend(result.get("data", []))
        next_uri = result.get("nextUri")
        if not next_uri:
            return rows
        assert time.monotonic() < deadline, f"Trino query timed out: {statement}"
        response = requests.get(next_uri, headers=headers, timeout=5)
        response.raise_for_status()
        result = response.json()


def file_partition(data_file: object, partition_fields: int) -> tuple[object, ...]:
    partition = data_file.partition  # type: ignore[attr-defined]
    return tuple(partition[index] for index in range(partition_fields))


def partition_key(tenant_id: str, day: date) -> tuple[str, str, int]:
    return (ECOSYSTEM, tenant_id, (day - ICEBERG_EPOCH).days)


def active_data_file_identity(
    table: Table,
) -> dict[str, tuple[int | None, int | None, tuple[object, ...]]]:
    """Return live paths with their data sequence and typed partition."""
    snapshot = table.current_snapshot()
    assert snapshot is not None
    files: dict[str, tuple[int | None, int | None, tuple[object, ...]]] = {}
    for manifest in snapshot.manifests(table.io):
        if manifest.content.name != "DATA":
            continue
        for entry in manifest.fetch_manifest_entry(table.io):
            if entry.status.name == "DELETED":
                continue
            data_file = entry.data_file
            partition = file_partition(data_file, len(table.spec().fields))
            files[str(data_file.file_path)] = (entry.sequence_number, entry.file_sequence_number, partition)
    return files


def main_retention_settings(table: Table) -> tuple[int | None, int | None, int | None]:
    main = table.metadata.refs.get("main")
    assert main is not None, table.metadata.refs
    return (main.min_snapshots_to_keep, main.max_snapshot_age_ms, main.max_ref_age_ms)


def append_fixture_rows(table: Table, rows: list[dict[str, object]]) -> None:
    for row in rows:
        table.append(pa.Table.from_pylist([row], schema=ICEBERG_ARROW_SCHEMA))


def wait_for_trino() -> None:
    deadline = time.monotonic() + 60
    while True:
        try:
            trino_query("SELECT 1")
            return
        except requests.RequestException, AssertionError:
            assert time.monotonic() < deadline, "Trino did not become ready"
            time.sleep(0.5)


def verify_trino_compaction_worker(root: Path, gateway: CatalogGateway) -> None:
    """Exercise compacted, truncated metrics through the production worker."""
    wait_for_trino()
    reset_table("alpha", full_metrics=True)
    table_name = qualified("alpha")
    relation = f"chargeback.{table_name}"
    with admin_catalog() as catalog:
        table = catalog.load_table(table_name)
        append_fixture_rows(
            table,
            [
                projected_row("alpha", DAY, "1.00001"),
                projected_row("alpha", DAY, "2.00002"),
                projected_row("alpha", DAY, "3.00003"),
                projected_row("alpha", NEIGHBOR, "4.00004"),
                projected_row("other", DAY, "5.00005"),
            ],
        )
    trino_query(f"ALTER TABLE {relation} EXECUTE optimize WHERE chargeback_date = DATE '{DAY.isoformat()}'")
    trino_query(f"ALTER TABLE {relation} EXECUTE optimize_manifests")

    with admin_catalog() as catalog:
        compacted = catalog.load_table(table_name)
        retained_main = compacted.current_snapshot().snapshot_id
        compacted.manage_snapshots().create_branch(
            retained_main,
            "main",
            max_ref_age_ms=2_592_000_000,
            max_snapshot_age_ms=604_800_000,
            min_snapshots_to_keep=30,
        ).commit()
        compacted = catalog.load_table(table_name)
        expected_retention = (30, 604_800_000, 2_592_000_000)
        assert main_retention_settings(compacted) == expected_retention
        live_files = active_data_file_identity(compacted)
        target = [path for path, (_, _, partition) in live_files.items() if partition == partition_key("alpha", DAY)]
        assert len(target) == 1, (target, live_files)
        ecosystem_id = compacted.schema().find_field("ecosystem").field_id
        compacted_file = next(
            entry.data_file
            for manifest in compacted.current_snapshot().manifests(compacted.io)
            for entry in manifest.fetch_manifest_entry(compacted.io)
            if str(entry.data_file.file_path) == target[0] and entry.status.name != "DELETED"
        )
        lower = (compacted_file.lower_bounds or {}).get(ecosystem_id)
        upper = (compacted_file.upper_bounds or {}).get(ecosystem_id)
        assert lower != ECOSYSTEM.encode() or upper != ECOSYSTEM.encode(), (lower, upper)
        neighbor_identity = {
            path: identity
            for path, identity in live_files.items()
            if identity[2] in (partition_key("alpha", NEIGHBOR), partition_key("other", DAY))
        }
        assert len(neighbor_identity) == 2, neighbor_identity
        retained_snapshot = compacted.current_snapshot().snapshot_id
        before_snapshot_count = len(compacted.metadata.snapshots)
        scope_manifests = [
            manifest
            for manifest in compacted.current_snapshot().manifests(compacted.io)
            if manifest.partition_spec_id == compacted.spec().spec_id and manifest.content.name == "DATA"
        ]
        mixed = any(
            {
                file_partition(entry.data_file, len(compacted.spec().fields))
                for entry in manifest.fetch_manifest_entry(compacted.io)
                if entry.status.name != "DELETED"
            }
            >= {partition_key("alpha", DAY), partition_key("alpha", NEIGHBOR)}
            for manifest in scope_manifests
        )
        assert mixed, "Trino did not produce a mixed target/neighbor manifest"

    with ExitStack() as stack:
        backend = source_backend(stack, root, "alpha", ECOSYSTEM)
        seed(backend, "alpha", "compacted-replacement", chargeback("alpha", DAY, "8.76543"))
        config = worker_config(root, gateway, ("alpha",))
        gateway.requests.clear()
        gateway.commit_payloads.clear()
        gateway.commit_responses.clear()
        output = finish_worker(worker_process(config, gateway, "--run-once"), 45)

        with admin_catalog() as catalog:
            after = catalog.load_table(table_name)
            current_snapshot = after.current_snapshot().snapshot_id
            current_files = active_data_file_identity(after)
            assert main_retention_settings(after) == expected_retention
            assert len(after.metadata.snapshots) == before_snapshot_count + 1
            assert current_snapshot != retained_snapshot
            assert {path: current_files[path] for path in neighbor_identity} == neighbor_identity
            assert set(current_files).isdisjoint(target), (current_files, target)
            previous = after.snapshot_by_id(retained_snapshot)
            assert previous is not None
        assert read_rows(gateway, "alpha") == [
            expected("alpha", NEIGHBOR, "4.00004"),
            expected("alpha", DAY, "8.76543"),
            expected("other", DAY, "5.00005"),
        ]
        assert trino_query(
            f"SELECT amount FROM {relation} FOR VERSION AS OF {retained_snapshot} "
            f"WHERE tenant_id = 'alpha' AND chargeback_date = DATE '{DAY.isoformat()}' ORDER BY amount"
        ) == [["1.00001"], ["2.00002"], ["3.00003"]]
        posts = [event for event in gateway.requests if event[0] == "POST" and event[1] == "alpha"]
        assert len(posts) == 1, (posts, output)
        assert gateway.commit_responses == [("alpha", 200)], gateway.commit_responses
        payload = gateway.commit_payloads[-1][1]
        requirements = {item["type"] for item in payload["requirements"]}
        assert {
            "assert-table-uuid",
            "assert-ref-snapshot-id",
            "assert-current-schema-id",
            "assert-default-spec-id",
        } <= requirements, payload["requirements"]
        with backend.create_unit_of_work() as uow:
            checkpoint = uow.iceberg_exports.get(
                IcebergExportKey(str(after.metadata.table_uuid), ECOSYSTEM, "alpha", DAY)
            )
        assert checkpoint is not None and checkpoint.outcome == "succeeded", checkpoint
        assert checkpoint.confirmed_revision == "compacted-replacement"
        assert checkpoint.snapshot_id == current_snapshot and checkpoint.row_count == 1
        assert trino_query(
            f"SELECT typeof(amount), amount, tags['owner'] FROM {relation} "
            f"WHERE tenant_id = 'alpha' AND chargeback_date = DATE '{DAY.isoformat()}'"
        ) == [["decimal(38,5)", "8.76543", "alpha"]]

        for revision in ("compacted-clear", "compacted-empty-clear"):
            seed(backend, "alpha", revision)
            prior = snapshot_metadata("alpha")[1]
            gateway.requests.clear()
            gateway.commit_payloads.clear()
            gateway.commit_responses.clear()
            output = finish_worker(worker_process(config, gateway, "--run-once"), 45)
            now = snapshot_metadata("alpha")[1]
            assert now != prior, (revision, output)
            current_uuid, _, _ = snapshot_metadata("alpha")
            checkpoint = _checkpoint(backend, "alpha", table_uuid=current_uuid)
            assert checkpoint is not None and checkpoint.outcome == "succeeded", (revision, checkpoint)
            assert checkpoint.confirmed_revision == revision
            assert checkpoint.snapshot_id == now and checkpoint.row_count == 0
            assert read_rows(gateway, "alpha") == [
                expected("alpha", NEIGHBOR, "4.00004"),
                expected("other", DAY, "5.00005"),
            ]
            with admin_catalog() as catalog:
                assert main_retention_settings(catalog.load_table(table_name)) == expected_retention
            assert trino_query(
                f"SELECT tenant_id, amount, tags['owner'] FROM {relation} "
                f"WHERE chargeback_date = DATE '{NEIGHBOR.isoformat()}' ORDER BY tenant_id"
            ) == [["alpha", "4.00004", "alpha"]]
            assert len([event for event in gateway.requests if event[0] == "POST" and event[1] == "alpha"]) == 1
            assert gateway.commit_responses == [("alpha", 200)], gateway.commit_responses
        print(
            "PASS worker replaced and cleared a Trino-compacted tenant-day, preserved mixed-manifest neighbors "
            "and sequences, retained time travel, and sent the required REST guards"
        )


def verify_commit_races(root: Path, gateway: CatalogGateway) -> None:
    """Exercise stale worker commits against actual REST catalog mutations."""
    reset_table("alpha")
    with ExitStack() as stack:
        alpha = source_backend(stack, root, "alpha", ECOSYSTEM)
        config = worker_config(root, gateway, ("alpha",))
        table_name = qualified("alpha")
        relation = f"chargeback.{table_name}"
        seed(alpha, "alpha", "race-compaction-base", chargeback("alpha", DAY, "3.00003"))
        finish_worker(worker_process(config, gateway, "--run-once"), 45)
        with admin_catalog() as catalog:
            table = catalog.load_table(table_name)
            append_fixture_rows(
                table,
                [
                    projected_row("alpha", DAY, "1.00001"),
                    projected_row("alpha", DAY, "2.00002"),
                    projected_row("alpha", DAY, "3.00003"),
                    projected_row("alpha", NEIGHBOR, "4.00004"),
                ],
            )
        seed(alpha, "alpha", "race-compaction-replacement", chargeback("alpha", DAY, "9.00009"))

        prepared = threading.Event()
        release = threading.Event()

        def compact_before_commit(table: str, _: dict[str, object]) -> None:
            assert table == "alpha"
            prepared.set()
            assert release.wait(5), "test did not release the held commit"

        gateway.before_next_commit = compact_before_commit
        gateway.requests.clear()
        gateway.commit_payloads.clear()
        gateway.commit_responses.clear()
        process = worker_process(config, gateway, "--run-once")
        compacted_snapshot = None
        try:
            assert prepared.wait(30), "worker did not reach its commit"
            trino_query(f"ALTER TABLE {relation} EXECUTE optimize WHERE chargeback_date = DATE '{DAY.isoformat()}'")
            compacted_snapshot = snapshot_metadata("alpha")[1]
        finally:
            release.set()
        output = finish_worker(process, 45)
        assert gateway.commit_responses[:1] == [("alpha", 409)], gateway.commit_responses
        assert gateway.commit_responses[-1:] == [("alpha", 200)], gateway.commit_responses
        assert gateway.commit_payloads[0][1]["requirements"] != gateway.commit_payloads[-1][1]["requirements"]
        assert compacted_snapshot is not None
        assert snapshot_metadata("alpha")[1] != compacted_snapshot
        with alpha.create_unit_of_work() as uow:
            checkpoint = uow.iceberg_exports.get(
                IcebergExportKey(snapshot_metadata("alpha")[0], ECOSYSTEM, "alpha", DAY)
            )
        assert checkpoint is not None and checkpoint.outcome == "succeeded", (checkpoint, output)
        assert checkpoint.confirmed_revision == "race-compaction-replacement"
        assert read_rows(gateway, "alpha") == [
            expected("alpha", NEIGHBOR, "4.00004"),
            expected("alpha", DAY, "9.00009"),
        ]

    reset_table("bravo")
    with ExitStack() as mutation_stack:
        for table_name, mutation in (
            (qualified("alpha"), "schema"),
            (qualified("bravo"), "spec"),
        ):
            tenant = table_name.rsplit(".", 1)[-1]
            backend = source_backend(mutation_stack, root, tenant, ECOSYSTEM)
            config = worker_config(root, gateway, (tenant,))
            if mutation == "spec":
                seed(backend, tenant, "race-spec-base", chargeback(tenant, DAY, "6.00006"))
                finish_worker(worker_process(config, gateway, "--run-once"), 45)
            table_uuid, before_snapshot, _ = snapshot_metadata(tenant)
            with admin_catalog() as catalog:
                baseline_table = catalog.load_table(table_name)
                before_schema_id = baseline_table.metadata.current_schema_id
                before_spec_id = baseline_table.metadata.default_spec_id
            with backend.create_unit_of_work() as uow:
                prior = uow.iceberg_exports.get(IcebergExportKey(table_uuid, ECOSYSTEM, tenant, DAY))
            assert prior is not None and prior.outcome == "succeeded", prior
            seed(backend, tenant, f"race-{mutation}-replacement", chargeback(tenant, DAY, "7.00007"))

            def mutation_hook(
                expected_tenant: str,
                identifier: str,
                kind: str,
                snapshot_id: int,
                schema_id: int,
                spec_id: int,
            ) -> Callable[[str, dict[str, object]], None]:
                def mutate_before_commit(actual_table: str, _: dict[str, object]) -> None:
                    assert actual_table == expected_tenant
                    with admin_catalog() as catalog:
                        loaded = catalog.load_table(identifier)
                        if kind == "schema":
                            loaded.update_schema().add_column("race_marker", StringType()).commit()
                        else:
                            loaded.update_spec().add_field("resource_id", IdentityTransform()).commit()
                        assert loaded.current_snapshot().snapshot_id == snapshot_id
                        current_table = catalog.load_table(identifier)
                        if kind == "schema":
                            assert current_table.metadata.current_schema_id != schema_id
                        else:
                            assert current_table.metadata.default_spec_id != spec_id

                return mutate_before_commit

            gateway.before_next_commit = mutation_hook(
                tenant, table_name, mutation, before_snapshot, before_schema_id, before_spec_id
            )
            gateway.requests.clear()
            gateway.commit_payloads.clear()
            gateway.commit_responses.clear()
            output = finish_worker(worker_process(config, gateway, "--run-once"), 45)
            assert gateway.commit_responses == [(tenant, 409)], (mutation, gateway.commit_responses, output)
            payload = gateway.commit_payloads[0][1]
            requirement_types = {item["type"] for item in payload["requirements"]}
            assert {"assert-current-schema-id", "assert-default-spec-id"} <= requirement_types, payload
            current_uuid, current_snapshot, _ = snapshot_metadata(tenant)
            assert current_uuid == table_uuid and current_snapshot == before_snapshot
            with backend.create_unit_of_work() as uow:
                current = uow.iceberg_exports.get(IcebergExportKey(table_uuid, ECOSYSTEM, tenant, DAY))
            assert current is not None and current.outcome == "failed", (mutation, current, output)
            assert current.confirmed_revision == prior.confirmed_revision
            assert current.snapshot_id == prior.snapshot_id
            if tenant == "alpha":
                assert read_rows(gateway, tenant) == [
                    expected("alpha", NEIGHBOR, "4.00004"),
                    expected("alpha", DAY, "9.00009"),
                ]
    print(
        "PASS live REST commit races: compaction retried from new main; "
        "schema/spec changes with unchanged main were rejected"
    )


def verify_custom_identity_worker(root: Path, gateway: CatalogGateway) -> None:
    """Prove custom partition order and absent scope metrics through the worker."""
    table_name = qualified("bravo")
    relation = f"chargeback.{table_name}"
    reset_table(
        "bravo",
        partition_fields=(
            ("chargeback_date", "billing_day"),
            ("tenant_id", "customer_key"),
            ("ecosystem", "source_name"),
        ),
        metrics_default="none",
    )
    with admin_catalog() as catalog:
        table = catalog.load_table(table_name)
        append_fixture_rows(
            table,
            [
                projected_row("bravo", DAY, "1.00001"),
                projected_row("bravo", NEIGHBOR, "2.00002"),
            ],
        )
        scope_ids = {table.schema().find_field(name).field_id for name in ICEBERG_SCOPE_FIELDS}
        assert [field.name for field in table.spec().fields] == ["billing_day", "customer_key", "source_name"]
        live_files = active_data_file_identity(table)
        assert len(live_files) == 2, live_files
        for manifest in table.current_snapshot().manifests(table.io):
            for entry in manifest.fetch_manifest_entry(table.io):
                if entry.status.name == "DELETED":
                    continue
                data_file = entry.data_file
                for metric in (
                    data_file.value_counts,
                    data_file.null_value_counts,
                    data_file.nan_value_counts,
                    data_file.lower_bounds,
                    data_file.upper_bounds,
                ):
                    assert not scope_ids.intersection(metric or {})

    with ExitStack() as stack:
        backend = source_backend(stack, root, "bravo", ECOSYSTEM)
        config = worker_config(root, gateway, ("bravo",))
        seed(backend, "bravo", "custom-layout-replacement", chargeback("bravo", DAY, "8.76543"))
        finish_worker(worker_process(config, gateway, "--run-once"), 45)
        assert read_rows(gateway, "bravo") == [
            expected("bravo", NEIGHBOR, "2.00002"),
            expected("bravo", DAY, "8.76543"),
        ]
        assert trino_query(
            f"SELECT typeof(amount), amount, tags['owner'] FROM {relation} "
            f"WHERE tenant_id = 'bravo' AND chargeback_date = DATE '{DAY.isoformat()}'"
        ) == [["decimal(38,5)", "8.76543", "bravo"]]

        seed(backend, "bravo", "custom-layout-clear")
        finish_worker(worker_process(config, gateway, "--run-once"), 45)
        assert trino_query(
            f"SELECT count(*) FROM {relation} WHERE tenant_id = 'bravo' AND chargeback_date = DATE '{DAY.isoformat()}'"
        ) == [[0]]
        assert trino_query(
            f"SELECT tenant_id, amount, tags['owner'] FROM {relation} "
            f"WHERE chargeback_date = DATE '{NEIGHBOR.isoformat()}'"
        ) == [["bravo", "2.00002", "bravo"]]
        table_uuid, snapshot_id, _ = snapshot_metadata("bravo")
        checkpoint = _checkpoint(backend, "bravo", table_uuid=table_uuid)
        assert checkpoint is not None and checkpoint.outcome == "succeeded", checkpoint
        assert checkpoint.confirmed_revision == "custom-layout-clear"
        assert checkpoint.snapshot_id == snapshot_id and checkpoint.row_count == 0
    print("PASS worker replaced and cleared a custom ordered identity partition without scope metrics")


def verify_publisher_regressions() -> None:
    process = subprocess.run(
        [
            sys.executable,
            "-m",
            "pytest",
            "tests/unit/core/emitters/test_iceberg.py",
            "tests/unit/core/emitters/test_iceberg_transport.py",
            "tests/unit/core/emitters/test_iceberg_spool.py",
            "tests/unit/core/storage/test_iceberg_exports.py",
            "-q",
        ],
        cwd="/app",
        check=False,
        capture_output=True,
        text=True,
        timeout=180,
    )
    captured = process.stdout + process.stderr
    assert process.returncode == 0, captured
    print(f"PASS publisher starvation and scheduling regressions in Docker: {captured.strip()}")


def verify_storage_client_binding() -> None:
    locations = load_location_provider(f"s3://{table_prefix('alpha')}", {})
    ambient = {
        "AWS_PROFILE": "missing-fixture-profile",
        "AWS_DEFAULT_PROFILE": "missing-fixture-default-profile",
        "AWS_ENDPOINT_URL_S3": "https://ambient-aws.invalid",
        "AWS_SESSION_TOKEN": "ambient-fixture-token",
        "AWS_CA_BUNDLE": "/missing-fixture-ca",
    }
    defaults = {"anon": True, "endpoint_url": "https://ambient-fsspec.invalid"}
    with patch.dict(os.environ, ambient), patch.dict(filesystem_config, {"s3": defaults, "s3a": defaults}):
        for endpoint in (None, S3_URL):
            options = {
                "aws_access_key_id": "fixture-key",
                "aws_secret_access_key": "fixture-secret",  # pragma: allowlist secret
                "region_name": REGION,
            }
            if endpoint is not None:
                options["endpoint_url"] = endpoint
            table_io = IcebergFileIO(locations, options)
            table_io.deadline = time.monotonic() + 3
            try:
                fs = table_io._filesystem()
                assert not fs.anon and fs.client_kwargs["verify"] is True
                client = fs.connect()
                actual = client.meta.endpoint_url
                if endpoint is None:
                    assert urlsplit(actual).hostname in {"s3.amazonaws.com", "s3.us-east-1.amazonaws.com"}, actual
                else:
                    assert actual == endpoint, actual
                signer = client._request_signer
                credentials = sync(fs.loop, signer._credentials.get_frozen_credentials)
                assert signer._signature_version == "s3v4"
                assert (credentials.access_key, credentials.secret_key, credentials.token) == (
                    "fixture-key",
                    "fixture-secret",
                    None,
                )
            finally:
                table_io.close()


def verify_vended_storage_binding() -> None:
    path = f"s3://{table_prefix('alpha').rstrip('/')}"
    locations = load_location_provider(path, {})
    metadata_location = f"{path}/metadata/0001.json"

    def grant(prefix: str, key: str) -> StorageCredential:
        return StorageCredential(
            prefix=prefix, config={"s3.access-key-id": key, "s3.secret-access-key": "fixture-secret"}
        )

    ancestor = grant(WAREHOUSE, "ancestor")
    selected = grant(path, "alpha")
    valid = TableResponse.model_construct(
        metadata_location=metadata_location,
        storage_credentials=[ancestor, selected, grant(f"{path}-neighbor/data", "neighbor")],
    )
    assert storage_file_io(None, valid, locations)._client_options["aws_access_key_id"] == "alpha"
    valid.storage_credentials.extend([grant(f"{path}/metadata", "alpha"), grant(f"{path}/data", "alpha")])
    assert storage_file_io(None, valid, locations)._client_options["aws_access_key_id"] == "alpha"
    for grants in [
        [ancestor, grant(f"{path}/metadata", "metadata"), grant(f"{path}/data", "data")],
        [ancestor, selected, grant(f"{path}/data/tenant=child", "child")],
        [ancestor, selected, grant(metadata_location, "object")],
        [ancestor, selected, grant(f"{path}/", "conflict")],
    ]:
        invalid = TableResponse.model_construct(metadata_location=metadata_location, storage_credentials=grants)
        try:
            storage_file_io(None, invalid, locations)
        except IcebergDestinationError as exc:
            assert exc.code == "storage_credentials"
        else:
            raise AssertionError("Incompatible credential grants were accepted")


def verify_manifest_codec(gateway: CatalogGateway) -> None:
    property_name = TableProperties.WRITE_AVRO_COMPRESSION
    with admin_catalog() as admin:
        table = admin.load_table(qualified("alpha"))
        with table.transaction() as transaction:
            transaction.set_properties({property_name: "bzip2"})
        try:
            settings = IcebergCatalogConfig.model_validate(catalog_settings(gateway))
            deadline = time.monotonic() + 3
            with open_iceberg_catalog(settings, deadline=deadline) as catalog:
                try:
                    catalog.load_table(qualified("alpha"), deadline=deadline)
                except IcebergDestinationError as exc:
                    assert exc.code == "table_contract"
                else:
                    raise AssertionError("The table accepted an unsupported manifest write codec")
        finally:
            with table.transaction() as transaction:
                transaction.remove_properties(property_name)


def verify_cleanup_diagnostics(gateway: CatalogGateway) -> None:
    settings = IcebergCatalogConfig.model_validate(catalog_settings(gateway))
    trace = io.StringIO()
    logger = logging.getLogger()
    handler = logging.StreamHandler(trace)
    logger.addHandler(handler)
    marker = "fixture-cleanup-secret-marker"
    scope = dict(ecosystem=ECOSYSTEM, tenant_id="cleanup_probe", chargeback_date=DAY, max_bytes=1024)
    try:
        with redirect_stderr(trace), open_iceberg_catalog(settings, deadline=time.monotonic() + 3) as catalog:
            deadline = time.monotonic() + 8
            table = catalog.load_table(qualified("alpha"), deadline=deadline)
            with patch.object(catalog, "_delete_old_metadata", side_effect=RuntimeError(marker)):
                snapshot = table.overwrite([], deadline=deadline, **scope)

            deadline = time.monotonic() + 8
            table = catalog.load_table(qualified("alpha"), deadline=deadline)
            gateway.next_commit_fault = "reject"
            with patch.object(table.io, "delete", side_effect=RuntimeError(marker)):
                try:
                    table.overwrite([], deadline=deadline, **scope)
                except IcebergConflict:
                    pass
                else:
                    raise AssertionError("Expected a conflict while testing manifest cleanup")
    finally:
        logger.removeHandler(handler)
    assert trace.getvalue().count("Iceberg cleanup reported a warning") == 2, trace.getvalue()
    assert marker not in trace.getvalue()
    assert snapshot_metadata("alpha")[1] == snapshot


def verify_example_config(root: Path) -> None:
    tables = {
        "confluent_cloud": "ccloud_chargebacks",
        "self_managed_kafka": "kafka_chargebacks",
        ECOSYSTEM: "postgres_chargebacks",
    }
    table_names = tuple(tables.values())
    example = Path("/example")
    source_config = example / "config.yaml"
    config = root / "example.yaml"
    env_file = root / "example.env"
    cli_env = ("--env-file", str(env_file))
    (root / "data").mkdir()
    text = source_config.read_text()
    for setting in ('# ca_file: "${ICEBERG_CATALOG_CA_FILE}"', "# s3:", '#   endpoint: "${ICEBERG_S3_ENDPOINT}"'):
        assert text.count(setting) == 1, setting
        text = text.replace(setting, setting[2:], 1)
    config.write_text(text)

    with (
        ExitStack() as stack,
        chdir(root),
        patch(f"{__name__}.TABLES", table_names),
        patch("gateway.TABLES", table_names),
    ):
        provision_storage(root)
        provision_tables()
        gateway = stack.enter_context(catalog_gateway(root / "example-ca.pem"))
        overrides = {
            "ICEBERG_CATALOG_URI": gateway.uri,
            "ICEBERG_CATALOG_TOKEN": CATALOG_TOKEN,
            "CCLOUD_API_KEY": "fixture-cloud-key",
            "CCLOUD_API_SECRET": "fixture-cloud-secret",
        }
        env_lines = (example / ".env.example").read_text().splitlines()
        for key, value in overrides.items():
            assert sum(line.startswith(f"{key}=") for line in env_lines) == 1, key
            env_lines = [f"{key}={value}" if line.startswith(f"{key}=") else line for line in env_lines]
        env_lines.extend((f"ICEBERG_CATALOG_CA_FILE={gateway.ca}", f"ICEBERG_S3_ENDPOINT={S3_URL}"))
        env_file.write_text("\n".join(env_lines) + "\n")
        with patch.dict(os.environ):
            original = load_config(source_config, env_file)
            local = load_config(config, env_file)
        assert local.features == original.features
        assert local.tenants.keys() == original.tenants.keys()
        assert {tenant.ecosystem for tenant in local.tenants.values()} == set(tables)

        backends = {}
        for name, tenant in local.tenants.items():
            base = original.tenants[name]
            emitter = tenant.plugin_settings.iceberg_emitter
            original_emitter = base.plugin_settings.iceberg_emitter
            assert emitter is not None and original_emitter is not None
            assert emitter.params.table == original_emitter.params.table == qualified(tables[tenant.ecosystem])
            catalog = emitter.params.catalog
            assert catalog.ca_file == str(gateway.ca)
            assert catalog.s3 is not None
            assert catalog.s3.endpoint == S3_URL
            assert catalog.s3.access_key_id is None
            assert catalog.model_copy(update={"ca_file": None, "s3": original_emitter.params.catalog.s3}) == (
                original_emitter.params.catalog
            )
            assert tenant.storage == base.storage
            assert tenant.plugin_settings.plugin_payload() == base.plugin_settings.plugin_payload()
            backend = create_storage_backend(
                tenant.storage, storage_module=get_storage_module_for_ecosystem(tenant.ecosystem)
            )
            stack.callback(backend.dispose)
            backend.create_tables()
            backends[name] = backend
        assert "Config is valid." in finish_worker(worker_process(config, gateway, *cli_env, "--validate"), 25)

        previous_snapshots: dict[str, tuple[str, int, str]] = {}
        for previous_count, row_count in ((0, 60_000), (60_000, 100_000)):
            for name, tenant in local.tenants.items():
                seed_large_day(
                    backends[name],
                    tenant.tenant_id,
                    f"{tenant.tenant_id}-{row_count}",
                    row_count,
                    ecosystem=tenant.ecosystem,
                    previous_count=previous_count,
                )
            gateway.requests.clear()
            gateway.commit_responses.clear()
            started = time.monotonic()
            finish_worker(worker_process(config, gateway, *cli_env, "--emit-once"), 65)
            elapsed = time.monotonic() - started
            assert sorted(gateway.commit_responses) == sorted((table, 200) for table in table_names)
            assert sorted(target for method, target, _ in gateway.requests if method == "POST") == sorted(table_names)

            for name, tenant in local.tenants.items():
                table = tables[tenant.ecosystem]
                revision = f"{tenant.tenant_id}-{row_count}"
                with duckdb_connection(gateway, table) as db:
                    metrics = db.execute(
                        f"""
                        SELECT ecosystem, tenant_id, chargeback_date, count(*),
                               count(DISTINCT (timestamp, resource_id, identity_id)),
                               count(*) FILTER (WHERE calculation_id = ?),
                               CAST(sum(amount) AS DECIMAL(38,5)), any_value(typeof(amount)),
                               min(amount), max(amount), count(DISTINCT tags['owner']), min(cardinality(tags)),
                               count(env_id), count(team),
                               count(*) FILTER (WHERE env_id =
                                   'env-' || lpad(CAST(CAST(right(identity_id, 3) AS INT) % 8 AS VARCHAR), 2, '0')),
                               count(*) FILTER (WHERE team =
                                   'team-' || lpad(CAST(CAST(right(identity_id, 3) AS INT) % 8 AS VARCHAR), 2, '0'))
                        FROM warehouse.{qualified(table)}
                        GROUP BY 1, 2, 3
                    """,
                        [revision],
                    ).fetchall()
                env_rows = row_count if tenant.ecosystem == "confluent_cloud" else 0
                team_rows = row_count if tenant.ecosystem == "self_managed_kafka" else 0
                amount = large_day_total(row_count).quantize(Decimal("0.00001"))
                assert metrics == [
                    (
                        tenant.ecosystem,
                        tenant.tenant_id,
                        DAY,
                        row_count,
                        row_count,
                        row_count,
                        amount,
                        "DECIMAL(38,5)",
                        Decimal("-0.23457"),
                        Decimal("1.23457"),
                        _LARGE_DAY_DIMENSIONS,
                        _LARGE_DAY_TAGS,
                        env_rows,
                        team_rows,
                        env_rows,
                        team_rows,
                    )
                ], (table, metrics)

                current = snapshot_metadata(table)
                table_uuid, snapshot_id, _ = current
                with admin_catalog() as admin:
                    snapshot_ids = {snapshot.snapshot_id for snapshot in admin.load_table(qualified(table)).snapshots()}
                if previous_count:
                    old_uuid, old_snapshot_id, _ = previous_snapshots[table]
                    assert table_uuid == old_uuid and snapshot_id != old_snapshot_id
                    assert snapshot_ids == {old_snapshot_id, snapshot_id}
                else:
                    assert snapshot_ids == {snapshot_id}
                previous_snapshots[table] = current
                files = active_day_file_count(table, tenant.tenant_id, DAY, ecosystem=tenant.ecosystem)
                assert files > 1, (table, row_count, files)
                target_date = DAY.isoformat()
                (status,) = cli_report(config, gateway, *cli_env, "--iceberg-status", name, target_date, target_date)
                assert status["current"] and status["outcome"] == "succeeded", status
                assert status["confirmed_revision"] == status["source_revision"] == revision, status
                assert status["snapshot_id"] == snapshot_id and status["row_count"] == row_count, status
                print(f"PASS example {qualified(table)} rows={row_count:,} total={amount} files={files} commits=1")
            assert len({metadata[0] for metadata in previous_snapshots.values()}) == len(tables)
            print(f"PASS example worker published all three {row_count:,}-row days in {elapsed:.2f}s")

        gateway.requests.clear()
        gateway.commit_responses.clear()
        finish_worker(worker_process(config, gateway, *cli_env, "--emit-once"), 65)
        assert not gateway.commit_responses
        assert all(method != "POST" for method, _, _ in gateway.requests)
        assert {table: snapshot_metadata(table) for table in table_names} == previous_snapshots
        print("PASS repeat example invocation left all three tables current without a commit")


def main() -> None:
    if os.environ.get("ICEBERG_EXAMPLE") == "1":
        with TemporaryDirectory(dir="/state") as directory:
            verify_example_config(Path(directory))
        return
    focused_rows = os.environ.get("ICEBERG_FOCUSED_LARGE")
    if focused_rows:
        with TemporaryDirectory(dir="/state") as directory:
            root = Path(directory)
            provision_storage(root)
            provision_tables()
            with catalog_gateway(root / "ca.pem") as gateway:
                verify_large_days(root, gateway, (("bravo", int(focused_rows)),))
        return
    verify_storage_client_binding()
    verify_vended_storage_binding()
    with TemporaryDirectory(dir="/state") as directory:
        root = Path(directory)
        provision_storage(root)
        provision_tables()
        with catalog_gateway(root / "ca.pem") as gateway:
            unauthenticated = requests.get(f"{gateway.uri}/v1/config", verify=str(gateway.ca), timeout=3)
            assert unauthenticated.status_code == 401
            verify_manifest_codec(gateway)
            verify_shared_transport(gateway)
            verify_worker(root, gateway)
            verify_plugin_cli(root, gateway)
            verify_large_days(root, gateway)
            verify_publisher_regressions()
            verify_cleanup_diagnostics(gateway)
            if os.environ.get("ICEBERG_TRINO") == "1":
                verify_trino_compaction_worker(root, gateway)
                verify_commit_races(root, gateway)
                verify_custom_identity_worker(root, gateway)


if __name__ == "__main__":
    main()

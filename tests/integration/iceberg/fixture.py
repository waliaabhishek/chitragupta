"""Share synthetic source data and disposable catalog operations across Iceberg checks."""

from __future__ import annotations

import json
import os
import subprocess
import time
from contextlib import contextmanager
from dataclasses import asdict
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING
from urllib.parse import quote

import duckdb
import duckdb_core_ext_avro
import duckdb_core_ext_httpfs
import duckdb_core_ext_iceberg
import pyarrow as pa
import requests
from botocore.auth import S3SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.credentials import Credentials
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
    table_key,
    table_prefix,
    table_secret,
)
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import DateType, DecimalType, MapType, NestedField, StringType, TimestamptzType

from core.config.models import StorageConfig
from core.models.chargeback import ChargebackRow, CostType
from core.models.pipeline import PipelineState
from core.storage.registry import create_storage_backend
from plugins.storage_modules import get_storage_module_for_ecosystem

if TYPE_CHECKING:
    from contextlib import ExitStack
    from pathlib import Path

    from gateway import CatalogGateway
    from pyiceberg.table import Table

    from core.storage.interface import StorageBackend

ECOSYSTEM = "generic_metrics_only"
DAY = datetime.now(UTC).date() - timedelta(days=3)
NEIGHBOR = DAY - timedelta(days=1)
WAREHOUSE = f"s3://{BUCKET}/"
REST_URL = "http://catalog:8181"
ICEBERG_EPOCH = date(1970, 1, 1)
_LARGE_DAY_DIMENSIONS = 128
_LARGE_DAY_TAGS = 8
_LARGE_DAY_BATCH = 10_000
_POSITIVE_AMOUNT = Decimal("1.234565")
_NEGATIVE_AMOUNT = Decimal("-0.234565")


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


def provision_storage(root: Path, tables: tuple[str, ...] = TABLES) -> None:
    deadline = time.monotonic() + 30
    while mc("ready", "admin", check=False).returncode != 0:
        assert time.monotonic() < deadline, "MinIO did not become ready"
        time.sleep(0.25)
    mc("mb", f"admin/{BUCKET}")
    for table in tables:
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
    schema = Schema(
        NestedField(1, "ecosystem", StringType(), required=True),
        NestedField(2, "tenant_id", StringType(), required=True),
        NestedField(3, "chargeback_date", DateType(), required=True),
        NestedField(4, "timestamp", TimestamptzType(), required=True),
        NestedField(5, "identity_id", StringType(), required=True),
        NestedField(6, "product_category", StringType(), required=True),
        NestedField(7, "product_type", StringType(), required=True),
        NestedField(8, "cost_type", StringType(), required=True),
        NestedField(9, "resource_id", StringType()),
        NestedField(10, "allocation_method", StringType()),
        NestedField(11, "allocation_detail", StringType()),
        NestedField(12, "amount", DecimalType(38, 5), required=True),
        NestedField(13, "tags", MapType(18, StringType(), 19, StringType(), value_required=False)),
        NestedField(14, "env_id", StringType()),
        NestedField(15, "team", StringType()),
        NestedField(16, "calculation_id", StringType()),
        NestedField(17, "calculation_completed_at", TimestamptzType()),
    )
    fields = partition_fields or tuple((name, name) for name in ("ecosystem", "tenant_id", "chargeback_date"))
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
        "format-version": "2",
        "commit.retry.num-retries": "0",
        **({"write.metadata.metrics.default": metrics} if metrics is not None else {}),
    }


def provision_tables(tables: tuple[str, ...] = TABLES, *, full_metrics: bool = False) -> None:
    schema, partitions = table_schema_and_spec()
    with admin_catalog() as catalog:
        catalog.create_namespace(NAMESPACE)
        for table in tables:
            catalog.create_table(
                qualified(table),
                schema=schema,
                partition_spec=partitions,
                location=f"s3://{table_prefix(table)}",
                properties=table_properties(full_metrics=full_metrics),
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
    values = asdict(row)
    return {
        **{field: values[field] for field in row.__csv_fields__},
        "ecosystem": ECOSYSTEM,
        "tenant_id": tenant_id,
        "chargeback_date": day,
        "tags": {"owner": tenant_id},
        "team": None,
        "env_id": None,
        "calculation_id": None,
        "calculation_completed_at": None,
    }


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


def verify_object_access(tables: dict[str, Table]) -> None:
    for table, loaded in tables.items():
        data_path = str(loaded.inspect.files()["file_path"][0])
        assert data_path.startswith(f"s3://{table_prefix(table)}data/"), data_path
        for uri in (loaded.metadata_location, data_path):
            url = f"{S3_URL}/{quote(uri.removeprefix('s3://'))}"
            for reader in tables:
                request = AWSRequest(method="HEAD", url=url)
                credentials = Credentials(table_key(reader), table_secret(reader))
                S3SigV4Auth(credentials, "s3", REGION).add_auth(request)
                response = requests.head(url, headers=dict(request.headers), timeout=5)
                assert response.status_code == (200 if reader == table else 403), (uri, reader, response.status_code)


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
        table.append(pa.Table.from_pylist([row], schema=table.schema().as_arrow()))


def wait_for_trino() -> None:
    deadline = time.monotonic() + 60
    while True:
        try:
            trino_query("SELECT 1")
            return
        except requests.RequestException, AssertionError:
            assert time.monotonic() < deadline, "Trino did not become ready"
            time.sleep(0.5)

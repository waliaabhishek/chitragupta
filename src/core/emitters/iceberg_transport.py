"""Publish one chargeback day with the documented PyIceberg APIs."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any
from urllib.parse import urlsplit

import pyarrow as pa  # type: ignore[import-untyped]
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.exceptions import CommitFailedException, CommitStateUnknownException, NoSuchTableError
from pyiceberg.expressions import And, EqualTo, Reference
from pyiceberg.expressions.literals import literal
from pyiceberg.manifest import ManifestContent
from pyiceberg.table import CommitTableResponse, Table, TableProperties
from pyiceberg.table.update import (
    AssertCurrentSchemaId,
    AssertDefaultSpecId,
    AssertRefSnapshotId,
    AssertTableUUID,
    SetSnapshotRefUpdate,
)
from pyiceberg.transforms import IdentityTransform

from core.emitters.iceberg_file_io import IcebergFileIO
from core.emitters.iceberg_process import IcebergDestinationError

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable
    from datetime import date

    from pyiceberg.table.update import TableRequirement, TableUpdate

    from core.config.models import IcebergCatalogConfig

ICEBERG_SCOPE_FIELDS = ("ecosystem", "tenant_id", "chargeback_date")
ICEBERG_ARROW_SCHEMA = pa.schema(
    [
        pa.field("ecosystem", pa.large_string(), nullable=False),
        pa.field("tenant_id", pa.large_string(), nullable=False),
        pa.field("chargeback_date", pa.date32(), nullable=False),
        pa.field("timestamp", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("identity_id", pa.large_string(), nullable=False),
        pa.field("product_category", pa.large_string(), nullable=False),
        pa.field("product_type", pa.large_string(), nullable=False),
        pa.field("cost_type", pa.large_string(), nullable=False),
        pa.field("resource_id", pa.large_string()),
        pa.field("allocation_method", pa.large_string()),
        pa.field("allocation_detail", pa.large_string()),
        pa.field("amount", pa.decimal128(38, 5), nullable=False),
        pa.field("tags", pa.map_(pa.large_string(), pa.large_string())),
        pa.field("env_id", pa.large_string()),
        pa.field("team", pa.large_string()),
        pa.field("calculation_id", pa.large_string()),
        pa.field("calculation_completed_at", pa.timestamp("us", tz="UTC")),
    ]
)
ICEBERG_TABLE_PROPERTIES = {
    TableProperties.FORMAT_VERSION: "2",
    TableProperties.COMMIT_NUM_RETRIES: "0",
}


def _origin(url: str) -> tuple[str, str | None, int]:
    parsed = urlsplit(url)
    return parsed.scheme, parsed.hostname, parsed.port or 443


class IcebergCatalog(RestCatalog):
    """Guard the native REST commit against the table read for this attempt."""

    def __init__(self, settings: IcebergCatalogConfig) -> None:
        self.commit_started = False
        properties: dict[str, Any] = {
            "uri": settings.uri,
            "auth": {"type": "noop"},
            "header.Authorization": f"Bearer {settings.token.get_secret_value()}",
            "header.X-Iceberg-Access-Delegation": "vended-credentials",
        }
        if settings.warehouse is not None:
            properties["warehouse"] = settings.warehouse
        if settings.ca_file is not None:
            properties["ssl"] = {"cabundle": settings.ca_file}
        if settings.s3 is not None:
            s3 = settings.s3
            if s3.endpoint is not None:
                properties.update({"s3.endpoint": s3.endpoint, "s3.force-virtual-addressing": "false"})
            if s3.region is not None:
                properties["s3.region"] = s3.region
            for name, value in (
                ("s3.access-key-id", s3.access_key_id),
                ("s3.secret-access-key", s3.secret_access_key),
                ("s3.session-token", s3.session_token),
            ):
                if value is not None:
                    properties[name] = value.get_secret_value()
        super().__init__("chargebacks", **properties)
        if _origin(self.uri) != _origin(settings.uri):
            self.close()
            raise IcebergDestinationError("catalog_origin")

    def commit_table(
        self, table: Table, requirements: tuple[TableRequirement, ...], updates: tuple[TableUpdate, ...]
    ) -> CommitTableResponse:
        base = table.metadata
        main = base.refs.get("main")
        current = base.current_snapshot_id
        head = None
        guarded_updates = []
        for update in updates:
            if isinstance(update, SetSnapshotRefUpdate) and update.ref_name == "main":
                head = update.snapshot_id
                if main is not None:
                    update = update.model_copy(
                        update={
                            "min_snapshots_to_keep": main.min_snapshots_to_keep,
                            "max_snapshot_age_ms": main.max_snapshot_age_ms,
                            "max_ref_age_ms": main.max_ref_age_ms,
                        }
                    )
            guarded_updates.append(update)
        if head is None or head == current:
            raise IcebergDestinationError("table_commit")

        guarded_requirements: tuple[TableRequirement, ...] = (
            *(
                requirement
                for requirement in requirements
                if not isinstance(requirement, (AssertTableUUID, AssertCurrentSchemaId, AssertDefaultSpecId))
                and not (isinstance(requirement, AssertRefSnapshotId) and requirement.ref == "main")
            ),
            AssertTableUUID(uuid=base.table_uuid),
            AssertRefSnapshotId.model_validate({"ref": "main", "snapshot-id": current}),
            AssertCurrentSchemaId.model_validate({"current-schema-id": base.current_schema_id}),
            AssertDefaultSpecId.model_validate({"default-spec-id": base.default_spec_id}),
        )
        self.commit_started = True
        try:
            result: CommitTableResponse = super().commit_table(table, guarded_requirements, tuple(guarded_updates))
        except CommitFailedException, NoSuchTableError:
            raise
        except Exception:
            # The SDK must not clean up files after an ambiguous result.
            raise CommitStateUnknownException from None
        if result.metadata.table_uuid != base.table_uuid or result.metadata.current_snapshot_id != head:
            raise CommitStateUnknownException
        return result


def load_table(catalog: IcebergCatalog, identifier: str) -> Table:
    try:
        table: Table = catalog.load_table(identifier)
    except NoSuchTableError:
        raise IcebergDestinationError("table_missing") from None

    properties = table.properties
    if (
        not table.metadata_location
        or table.format_version != 2
        or properties.get(TableProperties.WRITE_PY_LOCATION_PROVIDER_IMPL)
        or properties.get(TableProperties.WRITE_FILE_FORMAT, "parquet").lower() != "parquet"
        or properties.get(TableProperties.COMMIT_NUM_RETRIES) != "0"
    ):
        raise IcebergDestinationError("table_contract")
    token = table.config.get("token")
    if token is not None and (not token or any(char.isspace() or ord(char) < 32 for char in token)):
        raise IcebergDestinationError("table_authorization")

    schema = table.schema()
    arrow_schema = schema.as_arrow()
    spec = table.spec()
    try:
        scope_ids = {schema.find_field(name).field_id for name in ICEBERG_SCOPE_FIELDS}
    except ValueError:
        raise IcebergDestinationError("table_contract") from None
    if (
        len(arrow_schema) != len(ICEBERG_ARROW_SCHEMA)
        or any(
            arrow_schema.get_field_index(field.name) == -1
            or not arrow_schema.field(field.name).equals(field, check_metadata=False)
            for field in ICEBERG_ARROW_SCHEMA
        )
        or len(spec.fields) != len(scope_ids)
        or {field.source_id for field in spec.fields} != scope_ids
        or any(not isinstance(field.transform, IdentityTransform) for field in spec.fields)
    ):
        raise IcebergDestinationError("table_contract")
    table.io = IcebergFileIO(
        table.io, table.location_provider(), spec.partition_type(schema).fields, table.metadata_location
    )
    return table


def publish(
    table: Table,
    batches: Iterable[list[dict[str, object]]],
    scope: tuple[str, str, date],
    row_count: int,
    max_bytes: int,
    *,
    permit_commit: Callable[[], None] | None = None,
) -> int:
    """Stage the day and commit once after every prepared row arrives."""
    fields: tuple[tuple[str, str | date], ...] = tuple(zip(ICEBERG_SCOPE_FIELDS, scope, strict=True))
    schema = table.schema().as_arrow()
    base = table.metadata.current_snapshot_id
    current = table.current_snapshot()
    if current is not None and any(
        manifest.partition_spec_id != table.spec().spec_id or manifest.content != ManifestContent.DATA
        for manifest in current.manifests(table.io)
    ):
        raise IcebergDestinationError("table_layout")
    staged = 0
    with table.transaction() as transaction:
        transaction.delete(And(*(EqualTo(term=Reference(name), value=literal(value)) for name, value in fields)))
        for rows in batches:
            if any(row.get(name) != value for row in rows for name, value in fields):
                raise IcebergDestinationError("row_scope")
            arrow = pa.Table.from_pylist(rows, schema=schema)
            if arrow.nbytes > max_bytes:
                raise IcebergDestinationError("source_size")
            transaction.append(arrow)
            staged += arrow.num_rows
        if staged != row_count:
            raise IcebergDestinationError("incomplete_day")
        if transaction.table_metadata.current_snapshot_id == base:
            transaction.append(pa.Table.from_batches([], schema=schema))
        intended = transaction.table_metadata.current_snapshot_id
        if intended is None or intended == base:
            raise IcebergDestinationError("table_commit")
        if permit_commit is not None:
            permit_commit()
    if table.metadata.current_snapshot_id != intended:
        raise CommitStateUnknownException
    return intended

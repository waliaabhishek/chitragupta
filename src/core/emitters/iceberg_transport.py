"""Bounded REST bearer transport and table contract for chargeback publication."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import ssl
import warnings
from contextvars import ContextVar
from datetime import date
from itertools import count
from sys import maxsize
from time import monotonic
from typing import TYPE_CHECKING
from urllib.parse import urlsplit

import httpx
import pyarrow as pa  # type: ignore[import-untyped]
import requests
from fsspec.asyn import get_loop, sync  # type: ignore[import-untyped]
from pyiceberg import table as sdk_table
from pyiceberg.catalog.rest import (
    Capability,
    Endpoints,
    RestCatalog,
    TableResponse,
)
from pyiceberg.exceptions import CommitFailedException
from pyiceberg.expressions import And, EqualTo, Reference
from pyiceberg.expressions.literals import literal
from pyiceberg.expressions.visitors import inclusive_projection, manifest_evaluator
from pyiceberg.io.pyarrow import _dataframe_to_data_files, _determine_partitions, schema_to_pyarrow
from pyiceberg.manifest import DataFileContent, FileFormat, ManifestContent, clear_manifest_cache
from pyiceberg.table import CommitTableRequest, CommitTableResponse, Namespace, Table, TableIdentifier, TableProperties
from pyiceberg.table.locations import load_location_provider
from pyiceberg.table.refs import MAIN_BRANCH
from pyiceberg.table.snapshots import Operation
from pyiceberg.table.update import AssertCurrentSchemaId, AssertDefaultSpecId, SetSnapshotRefUpdate
from pyiceberg.table.update.snapshot import _OverwriteFiles
from pyiceberg.transforms import IdentityTransform
from pyiceberg.typedef import Record
from pyiceberg.utils.bin_packing import PackingIterator
from pyiceberg.utils.properties import property_as_int

from core.emitters.iceberg_file_io import (
    IcebergDestinationError as IcebergDestinationError,
)
from core.emitters.iceberg_file_io import (
    IcebergFileIO,
    close_before_deadline,
    remaining,
    storage_file_io,
    validate_avro_write_codec,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Iterator, Mapping
    from typing import Any, TextIO

    from pyiceberg.manifest import DataFile, ManifestFile
    from pyiceberg.table.update import TableRequirement, TableUpdate
    from pyiceberg.typedef import Identifier

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
_REST_BODY_BYTES = 8 * 1024 * 1024
_IO_TIMEOUT_SECONDS = 1.0
_SDK_LOGGERS = ("pyiceberg", "s3fs", "fsspec", "aiobotocore", "botocore")
_METADATA_WARNING_PREFIX = "Failed to delete old metadata after commit:"
_CATALOG_REQUEST = ContextVar("iceberg_catalog_request", default=False)
_logger = logging.getLogger(__name__)


class IcebergConflict(Exception):  # noqa: N818
    """The catalog declined this commit because a requirement no longer held."""


class IcebergUnconfirmed(Exception):  # noqa: N818
    """The commit may have landed. Keep the prior SQL confirmation."""


@contextlib.contextmanager
def _quiet_sdk() -> Iterator[None]:
    # The SDK can put table URLs and raw exceptions in logs and cleanup warnings.
    handler = logging.NullHandler()
    loggers = [logging.getLogger(name) for name in _SDK_LOGGERS]
    propagation = [logger.propagate for logger in loggers]
    sdk_prefixes = tuple(f"{name}." for name in _SDK_LOGGERS)
    existing = [
        (name, logger)
        for name, logger in list(logging.Logger.manager.loggerDict.items())
        if isinstance(logger, logging.Logger)
    ]
    sdk_loggers = [logger for name, logger in existing if name in _SDK_LOGGERS or name.startswith(sdk_prefixes)]
    cleanup_warning = False
    show_warning = warnings.showwarning

    def suppress(record: logging.LogRecord) -> bool:
        nonlocal cleanup_warning
        if (
            record.name == "pyiceberg.table.update.snapshot"
            and record.levelno == logging.WARNING
            and isinstance(record.msg, str)
            and record.msg.startswith("Failed to delete uncommitted manifest")
        ):
            cleanup_warning = True
        return False

    def sanitize_warning(
        message: Warning | str,
        category: type[Warning],
        filename: str,
        lineno: int,
        file: TextIO | None = None,
        line: str | None = None,
    ) -> None:
        nonlocal cleanup_warning
        if filename == sdk_table.__file__ and str(message).startswith(_METADATA_WARNING_PREFIX):
            cleanup_warning = True
        else:
            show_warning(message, category, filename, lineno, file, line)

    for logger in sdk_loggers:
        logger.addFilter(suppress)
    for logger in loggers:
        logger.addHandler(handler)
        logger.propagate = False
    try:
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", module=rf"^(?:{'|'.join(_SDK_LOGGERS)})(?:\.|$)")
            warnings.filterwarnings("always", message=f"^{_METADATA_WARNING_PREFIX}", module=r"^pyiceberg\.table$")
            warnings.showwarning = sanitize_warning
            yield
    finally:
        for logger, propagate in zip(loggers, propagation, strict=True):
            logger.propagate = propagate
            logger.removeHandler(handler)
        for logger in sdk_loggers:
            logger.removeFilter(suppress)
        if cleanup_warning:
            _logger.warning("Iceberg cleanup reported a warning. Details suppressed.")


@contextlib.contextmanager
def _quiet_catalog_request() -> Iterator[None]:
    # HTTPX loads its transport loggers when the client is created. Those
    # loggers are shared with other clients and can print response headers.
    loggers = [
        logger
        for name, logger in list(logging.Logger.manager.loggerDict.items())
        if isinstance(logger, logging.Logger)
        and (name in ("httpx", "httpcore") or name.startswith(("httpx.", "httpcore.")))
    ]

    def exclude_catalog_request(record: logging.LogRecord) -> bool:
        return not _CATALOG_REQUEST.get()

    for logger in loggers:
        logger.addFilter(exclude_catalog_request)
    token = _CATALOG_REQUEST.set(True)
    try:
        yield
    finally:
        _CATALOG_REQUEST.reset(token)
        for logger in loggers:
            logger.removeFilter(exclude_catalog_request)


def _origin(url: str) -> tuple[str, int]:
    uri = urlsplit(url)
    if uri.scheme != "https" or not uri.hostname or uri.username is not None or uri.fragment or uri.port == 0:
        raise IcebergDestinationError("catalog_origin")
    return uri.hostname, uri.port or 443


class _CatalogSession(requests.Session):
    """Apply request bounds before the pinned SDK parses the response."""

    def __init__(self, catalog: IcebergCatalog) -> None:
        super().__init__()
        self._catalog = catalog
        self.trust_env = False
        self.headers.update(
            {
                "Authorization": f"Bearer {catalog.settings.token.get_secret_value()}",
                "Accept-Encoding": "identity",
                "Content-Type": "application/json",
                "X-Iceberg-Access-Delegation": "vended-credentials",
            }
        )

    def send(self, request: requests.PreparedRequest, **kwargs: Any) -> requests.Response:
        if request.url is None or _origin(request.url) != self._catalog.origin:
            raise IcebergDestinationError("catalog_origin")
        deadline = min(self._catalog.deadline, monotonic() + _IO_TIMEOUT_SECONDS)
        response: requests.Response = sync(get_loop(), self._send_before_deadline, request, deadline)
        return response

    async def _send_before_deadline(self, request: requests.PreparedRequest, deadline: float) -> requests.Response:
        assert request.url is not None and request.method is not None
        assert request.body is None or isinstance(request.body, bytes)
        # Keep the SDK's prepared headers, including the request-local table bearer.
        # A single async deadline covers DNS, TLS, headers, and the bounded body.
        with _quiet_catalog_request():
            async with asyncio.timeout(remaining(deadline)):
                return await self._send_prepared(request.method, request.url, request.headers, request.body)

    async def _send_prepared(
        self, method: str, url: str, headers: Mapping[str, str | bytes], body: bytes | None
    ) -> requests.Response:
        wire_headers = [
            (name.encode("ascii"), value if isinstance(value, bytes) else value.encode("latin-1"))
            for name, value in headers.items()
        ]
        request = httpx.Request(method, url, headers=wire_headers, content=body)
        incoming = await self._catalog._http.send(request, stream=True)
        try:
            if incoming.is_redirect:
                raise IcebergDestinationError("catalog_redirect")
            content = bytearray()
            if incoming.is_success:
                if incoming.headers.get("Content-Encoding", "identity").lower() != "identity":
                    raise IcebergDestinationError("catalog_response")
                async for chunk in incoming.aiter_raw():
                    if len(chunk) > _REST_BODY_BYTES - len(content):
                        raise IcebergDestinationError("destination_size")
                    content.extend(chunk)
            # The SDK reads JSON from the Requests response after this bridge has closed the stream.
            response = requests.Response()
            response.status_code = incoming.status_code
            response.reason = incoming.reason_phrase
            response.url = url
            response.encoding = "utf-8"
            response._content = bytes(content)
            response._content_consumed = True
            return response
        finally:
            await incoming.aclose()


class IcebergCatalog(RestCatalog):
    """Keep one catalog bearer and session for a publisher pass."""

    def __init__(self, settings: IcebergCatalogConfig, *, deadline: float) -> None:
        self.settings = settings
        self.deadline = deadline
        self.origin = _origin(settings.uri)
        properties = {"uri": settings.uri}
        if settings.warehouse is not None:
            properties["warehouse"] = settings.warehouse
        verify = ssl.create_default_context(cafile=settings.ca_file) if settings.ca_file is not None else True
        self._http = httpx.AsyncClient(verify=verify, trust_env=False, follow_redirects=False, timeout=None)
        try:
            super().__init__("chargebacks", **properties)
        except BaseException:
            self._close_http(deadline)
            raise

    def _create_session(self) -> requests.Session:
        if _origin(self.uri) != self.origin:
            raise IcebergDestinationError("catalog_origin")
        return _CatalogSession(self)

    def _close_http(self, deadline: float) -> None:
        deadline = min(deadline, monotonic() + _IO_TIMEOUT_SECONDS)
        close_before_deadline(get_loop(), self._http.aclose, deadline, "catalog connection")

    def close(self, *, deadline: float = float("inf")) -> None:
        try:
            super().close()
        finally:
            self._close_http(deadline)

    def load_table(self, identifier: str | Identifier, *, deadline: float | None = None) -> IcebergTable:
        if deadline is not None:
            self.deadline = deadline
        with _quiet_sdk():
            try:
                self._check_endpoint(Capability.V1_LOAD_TABLE)
                url = self.url(Endpoints.load_table, **self._split_identifier_for_path(identifier))
                response = self._session.get(url)
                if response.status_code == 404:
                    raise IcebergDestinationError("table_missing")
                response.raise_for_status()
                body = TableResponse.model_validate_json(response.content)
                return self._response_to_table(self.identifier_to_tuple(identifier), body)
            except IcebergDestinationError:
                raise
            except Exception:
                raise IcebergDestinationError("table_load") from None

    def _response_to_table(self, identifier_tuple: tuple[str, ...], table_response: TableResponse) -> IcebergTable:
        metadata = table_response.metadata
        properties = metadata.properties
        if (
            metadata.format_version != 2
            or properties.get(TableProperties.WRITE_PY_LOCATION_PROVIDER_IMPL)
            or properties.get(TableProperties.WRITE_FILE_FORMAT, "parquet").lower() != "parquet"
            or any(
                properties.get(key) != value
                for key, value in ICEBERG_TABLE_PROPERTIES.items()
                if key != TableProperties.FORMAT_VERSION
            )
        ):
            raise IcebergDestinationError("table_contract")
        validate_avro_write_codec(
            properties.get(TableProperties.WRITE_AVRO_COMPRESSION, TableProperties.WRITE_AVRO_COMPRESSION_DEFAULT)
        )
        locations = load_location_provider(metadata.location, properties)
        if not table_response.metadata_location:
            raise IcebergDestinationError("table_contract")
        io = storage_file_io(self.settings.s3, table_response, locations)
        token = table_response.config.get("token")
        if token is not None and (
            not isinstance(token, str) or not token or any(char.isspace() or ord(char) < 32 for char in token)
        ):
            raise IcebergDestinationError("table_authorization")
        table = IcebergTable(
            identifier=identifier_tuple,
            metadata_location=table_response.metadata_location,
            metadata=metadata,
            io=io,
            catalog=self,
            config={"token": token} if token is not None else {},
        )
        table.validate_contract()
        return table

    def commit_table(
        self, table: Table, requirements: tuple[TableRequirement, ...], updates: tuple[TableUpdate, ...]
    ) -> CommitTableResponse:
        self._check_endpoint(Capability.V1_UPDATE_TABLE)
        main_updates = [
            update for update in updates if isinstance(update, SetSnapshotRefUpdate) and update.ref_name == MAIN_BRANCH
        ]
        if (
            not isinstance(table, IcebergTable)
            or len(main_updates) != 1
            or main_updates[0].snapshot_id == table.metadata.current_snapshot_id
        ):
            raise IcebergDestinationError("table_commit")
        main_update = main_updates[0]
        main_ref = table.metadata.refs.get(MAIN_BRANCH)
        if main_ref is not None:
            # The snapshot producer moves main without carrying its retention settings.
            preserved_update = main_update.model_copy(
                update={
                    "min_snapshots_to_keep": main_ref.min_snapshots_to_keep,
                    "max_snapshot_age_ms": main_ref.max_snapshot_age_ms,
                    "max_ref_age_ms": main_ref.max_ref_age_ms,
                }
            )
            updates = tuple(preserved_update if update is main_update else update for update in updates)
        identifier = table.name()
        request = CommitTableRequest(
            identifier=TableIdentifier(namespace=Namespace(root=list(identifier[:-1])), name=identifier[-1]),
            requirements=(
                *requirements,
                AssertCurrentSchemaId.model_validate({"current-schema-id": table.metadata.current_schema_id}),
                AssertDefaultSpecId.model_validate({"default-spec-id": table.metadata.default_spec_id}),
            ),
            updates=updates,
        )
        url = self.url(Endpoints.update_table, **self._split_identifier_for_path(request.identifier))
        token = table.config.get("token")
        headers = {"Authorization": f"Bearer {token}"} if token is not None else {}
        data = request.model_dump_json().encode()
        if len(data) > _REST_BODY_BYTES:
            raise IcebergDestinationError("destination_size")
        remaining(self.deadline)
        try:
            response = self._session.post(url, data=data, headers=headers)
        except IcebergDestinationError as exc:
            if exc.code in {"catalog_origin", "catalog_redirect"}:
                raise
            raise IcebergUnconfirmed from None
        except Exception:
            raise IcebergUnconfirmed from None
        if response.status_code == 409:
            raise CommitFailedException()
        if response.status_code >= 500 or response.status_code == 408:
            raise IcebergUnconfirmed
        if not response.ok:
            raise IcebergDestinationError("table_commit")
        try:
            commit = CommitTableResponse.model_validate_json(response.content)
        except Exception:
            raise IcebergUnconfirmed from None
        if (
            commit.metadata.table_uuid != table.metadata.table_uuid
            or commit.metadata.current_snapshot_id != main_update.snapshot_id
        ):
            raise IcebergUnconfirmed
        table.confirmed_snapshot_id = main_update.snapshot_id
        return commit


class _PartitionOverwrite(_OverwriteFiles):
    def _existing_manifests(self) -> list[ManifestFile]:
        # Like the SDK's fast append, keep inherited manifests that may contain live files.
        return [
            manifest
            for manifest in super()._existing_manifests()
            if manifest.has_added_files() or manifest.has_existing_files()
        ]


class IcebergTable(Table):
    """Validate and replace a single chargeback scope against a loaded base."""

    confirmed_snapshot_id: int | None = None

    @property
    def uuid(self) -> str:
        return str(self.metadata.table_uuid)

    def validate_contract(self) -> None:
        schema = self.schema()
        arrow_schema = schema_to_pyarrow(schema, include_field_ids=False)
        field_ids = {schema.find_field(name).field_id for name in ICEBERG_SCOPE_FIELDS}
        if (
            len(arrow_schema) != len(ICEBERG_ARROW_SCHEMA)
            or any(
                arrow_schema.get_field_index(field.name) == -1 or not arrow_schema.field(field.name).equals(field)
                for field in ICEBERG_ARROW_SCHEMA
            )
            or len(self.spec().fields) != len(field_ids)
            or {field.source_id for field in self.spec().fields} != field_ids
            or any(not isinstance(field.transform, IdentityTransform) for field in self.spec().fields)
        ):
            raise IcebergDestinationError("table_contract")

    def _partition_files(self, scope: tuple[str, str, date], require_time: Callable[[], None]) -> set[DataFile]:
        snapshot = self.snapshot_by_name(MAIN_BRANCH)
        if (snapshot.snapshot_id if snapshot is not None else None) != self.metadata.current_snapshot_id:
            raise IcebergDestinationError("table_layout")
        if snapshot is None:
            return set()
        schema, spec = self.schema(), self.spec()
        ecosystem, tenant, day = scope
        partition_values = dict(
            zip(ICEBERG_SCOPE_FIELDS, (ecosystem, tenant, (day - date(1970, 1, 1)).days), strict=True)
        )
        ordered_scope = tuple(partition_values[schema.find_field(part.source_id).name] for part in spec.fields)
        predicate = And(
            *(
                EqualTo(term=Reference(name), value=literal(value))
                for name, value in zip(ICEBERG_SCOPE_FIELDS, scope, strict=True)
            )
        )
        partition_filter = inclusive_projection(schema, spec)(predicate)
        matches_manifest = manifest_evaluator(spec, schema, partition_filter)
        seen_partitions: dict[str, tuple[object, ...]] = {}
        files: set[DataFile] = set()
        for manifest in snapshot.manifests(self.io):
            require_time()
            if manifest.partition_spec_id != spec.spec_id or manifest.content != ManifestContent.DATA:
                raise IcebergDestinationError("table_layout")
            if not matches_manifest(manifest):
                continue
            for entry in manifest.fetch_manifest_entry(self.io, discard_deleted=True):
                require_time()
                data_file = entry.data_file
                record = data_file.partition
                path = data_file.file_path
                if (
                    data_file.content != DataFileContent.DATA
                    or data_file.file_format != FileFormat.PARQUET
                    or not isinstance(record, Record)
                    or len(record) != len(ordered_scope)
                    or not isinstance(path, str)
                    or not path
                ):
                    raise IcebergDestinationError("table_layout")
                partition = tuple(record[index] for index in range(len(ordered_scope)))
                if any(
                    type(value) is not type(expected) for value, expected in zip(partition, ordered_scope, strict=True)
                ):
                    raise IcebergDestinationError("table_layout")
                if seen_partitions.setdefault(path, partition) != partition:
                    raise IcebergDestinationError("table_layout")
                if partition == ordered_scope:
                    files.add(data_file)
        return files

    def overwrite(  # type: ignore[override]  # The publisher exposes a scoped write instead of the SDK's general one.
        self,
        rows: list[dict[str, object]],
        *,
        ecosystem: str,
        tenant_id: str,
        chargeback_date: date,
        deadline: float,
        max_bytes: int,
    ) -> int:
        return self.overwrite_batches(
            (rows,) if rows else (),
            ecosystem=ecosystem,
            tenant_id=tenant_id,
            chargeback_date=chargeback_date,
            deadline=deadline,
            max_bytes=max_bytes,
            max_files=len(rows),
        )

    def overwrite_batches(
        self,
        batches: Iterable[list[dict[str, object]]],
        *,
        ecosystem: str,
        tenant_id: str,
        chargeback_date: date,
        deadline: float,
        max_bytes: int,
        max_files: int,
        check_shutdown: Callable[[], bool] | None = None,
    ) -> int:
        def require_time() -> None:
            if check_shutdown is not None and check_shutdown():
                raise IcebergDestinationError("export_interrupted")
            remaining(deadline)

        def chunk_bytes(chunk: pa.RecordBatch) -> int:
            require_time()
            return int(chunk.nbytes)

        scope = (ecosystem, tenant_id, chargeback_date)
        fields: tuple[tuple[str, str | date], ...] = tuple(zip(ICEBERG_SCOPE_FIELDS, scope, strict=True))
        assert isinstance(self.catalog, IcebergCatalog)
        assert isinstance(self.io, IcebergFileIO)
        self.catalog.deadline = deadline
        self.io.deadline = deadline
        self.io.max_data_files = max_files
        self.io.partition_fields = self.spec().partition_type(self.schema()).fields
        self.confirmed_snapshot_id = None
        with _quiet_sdk():
            try:
                require_time()
                clear_manifest_cache()
                files = self._partition_files(scope, require_time)
                transaction = self.transaction()
                nonempty = filter(None, batches)
                rows = next(nonempty, None)
                require_time()
                operation = Operation.APPEND if self.metadata.current_snapshot_id is None else Operation.OVERWRITE
                with _PartitionOverwrite(operation=operation, transaction=transaction, io=self.io) as overwrite:
                    for data_file in files:
                        require_time()
                        overwrite.delete_data_file(data_file)
                    if rows is not None:
                        try:
                            target_file_bytes = property_as_int(
                                transaction.table_metadata.properties,
                                TableProperties.WRITE_TARGET_FILE_SIZE_BYTES,
                                TableProperties.WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT,
                            )
                        except ValueError:
                            raise IcebergDestinationError("table_contract") from None
                        if target_file_bytes is None or not 0 < target_file_bytes <= maxsize:
                            raise IcebergDestinationError("table_contract")
                        file_ids = count()
                        available_files = max_files
                        while rows is not None:
                            require_time()
                            if any(row.get(name) != value for row in rows for name, value in fields):
                                raise IcebergDestinationError("row_scope")
                            arrow = pa.Table.from_pylist(rows, schema=ICEBERG_ARROW_SCHEMA)
                            del rows
                            if arrow.nbytes > max_bytes:
                                raise IcebergDestinationError("source_size")
                            if available_files <= 0:
                                raise IcebergDestinationError("destination_file_limit")
                            # The identity filter keeps every row. Any added validity bitmap
                            # is smaller than the fixed cells or nested offsets already counted
                            # in nbytes. Twice that size bounds the partition's Arrow bytes.
                            if arrow.num_rows > available_files and target_file_bytes <= 2 * arrow.nbytes:
                                metadata = transaction.table_metadata
                                partition = next(iter(_determine_partitions(metadata.spec(), metadata.schema(), arrow)))
                                partition_rows = partition.arrow_table_partition
                                average_row_bytes = partition_rows.nbytes / partition_rows.num_rows
                                rows_per_chunk = max(1, int(target_file_bytes / average_row_bytes))
                                plan = PackingIterator(
                                    items=partition_rows.to_batches(max_chunksize=rows_per_chunk),
                                    target_weight=target_file_bytes,
                                    lookback=available_files,
                                    weight_func=chunk_bytes,
                                    largest_bin_first=False,
                                )
                                # The first yield leaves exactly `available_files` bins if
                                # packing opened one bin too many. Otherwise it drained the input.
                                next(plan, None)
                                if len(plan.bins) == available_files:
                                    raise IcebergDestinationError("destination_file_limit")
                                del plan, partition, partition_rows
                            require_time()
                            for data_file in _dataframe_to_data_files(
                                table_metadata=transaction.table_metadata,
                                df=arrow,
                                io=self.io,
                                write_uuid=overwrite.commit_uuid,
                                counter=file_ids,
                            ):
                                overwrite.append_data_file(data_file)
                                available_files -= 1
                            del arrow
                            rows = next(nonempty, None)
                    require_time()
                require_time()
                transaction.commit_transaction()
            except CommitFailedException:
                raise IcebergConflict from None
            except IcebergUnconfirmed, IcebergDestinationError:
                if self.confirmed_snapshot_id is None:
                    raise
            except Exception:
                if self.confirmed_snapshot_id is None:
                    raise IcebergDestinationError("table_write") from None
            finally:
                self.io.close()
                clear_manifest_cache()
        if self.confirmed_snapshot_id is None:
            raise IcebergUnconfirmed
        return self.confirmed_snapshot_id


@contextlib.contextmanager
def open_iceberg_catalog(
    settings: IcebergCatalogConfig, *, deadline: float, close_deadline: Callable[[], float] | None = None
) -> Iterator[IcebergCatalog]:
    with _quiet_sdk():
        try:
            catalog = IcebergCatalog(settings, deadline=deadline)
        except IcebergDestinationError:
            raise
        except Exception:
            raise IcebergDestinationError("catalog_connection") from None
    try:
        yield catalog
    finally:
        catalog.close(deadline=close_deadline() if close_deadline is not None else float("inf"))

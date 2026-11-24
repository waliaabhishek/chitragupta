"""Table-local S3 access and Avro budgets for the Iceberg SDK."""

from __future__ import annotations

import asyncio
import contextlib
import io
import logging
import os
import zlib
from threading import Lock
from time import monotonic
from typing import TYPE_CHECKING
from urllib.parse import urlsplit

from aiobotocore.session import AioSession  # type: ignore[import-untyped]
from fsspec.asyn import sync  # type: ignore[import-untyped]
from pyiceberg.avro.codecs import CODEC_MAPPING_ICEBERG_TO_AVRO
from pyiceberg.avro.decoder import new_decoder
from pyiceberg.avro.file import MAGIC, META_SCHEMA, AvroFileHeader
from pyiceberg.avro.resolver import construct_reader
from pyiceberg.io import FileIO, InputFile, OutputFile
from pyiceberg.io.fsspec import FsspecOutputFile
from pyiceberg.types import StructType
from s3fs import S3FileSystem  # type: ignore[import-untyped]

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable
    from typing import Any

    from pyiceberg.catalog.rest import TableResponse
    from pyiceberg.table.locations import LocationProvider
    from pyiceberg.types import NestedField

    from core.config.models import IcebergS3Config

_METADATA_FILE_BYTES = 4 * 1024 * 1024
_METADATA_TOTAL_BYTES = 64 * 1024 * 1024
_METADATA_READS = 512
_METADATA_RECORDS = 100_000
_S3_TIMEOUT_SECONDS = 1.0
_S3_CLOSE_SECONDS = 0.1
_AVRO_CODECS = {"null", "deflate"}
_AVRO_HEADER_READER = construct_reader(META_SCHEMA, {-1: AvroFileHeader})
_MANIFEST_DATA_FILE_ID = 2
_DATA_FILE_PARTITION_ID = 102
_MANIFEST_LIST_PATH_ID = 500
_logger = logging.getLogger(__name__)


class IcebergDestinationError(Exception):
    """A destination error with a safe reason code."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


def remaining(deadline: float) -> float:
    seconds = deadline - monotonic()
    if seconds <= 0:
        raise IcebergDestinationError("destination_deadline")
    return seconds


def close_before_deadline(
    loop: asyncio.AbstractEventLoop, close: Callable[[], Awaitable[object]], deadline: float, resource: str
) -> None:
    async def close_on_loop() -> None:
        try:
            async with asyncio.timeout(max(0, deadline - monotonic())):
                await close()
        except Exception:
            _logger.warning("Could not close Iceberg %s. Details suppressed.", resource)

    future = asyncio.run_coroutine_threadsafe(close_on_loop(), loop)
    # Still start a close after the cutoff. It can release local resources but
    # is canceled if it needs to wait for I/O.
    with contextlib.suppress(TimeoutError):
        future.result(timeout=max(0, deadline - monotonic()))


def validate_avro_write_codec(codec: str) -> None:
    if CODEC_MAPPING_ICEBERG_TO_AVRO.get(codec, codec) not in _AVRO_CODECS:
        raise IcebergDestinationError("table_contract")


def _s3_parts(value: str) -> tuple[str, str]:
    if not value or any(char.isspace() or ord(char) < 32 for char in value) or "?" in value or "#" in value:
        raise IcebergDestinationError("storage_location")
    uri = urlsplit(value)
    if uri.scheme != "s3" or not uri.hostname or uri.netloc != uri.hostname:
        raise IcebergDestinationError("storage_location")
    return uri.netloc, uri.path.rstrip("/")


def _covers(prefix: str, location: str) -> bool:
    return _contains(_s3_parts(prefix), _s3_parts(location))


def _contains(prefix: tuple[str, str], location: tuple[str, str]) -> bool:
    bucket, path = prefix
    obj_bucket, obj_path = location
    return bucket == obj_bucket and (path == obj_path or obj_path.startswith(path + "/"))


class _TimedS3FileSystem(S3FileSystem):  # type: ignore[misc]  # s3fs has no type stubs.
    """Apply the attempt deadline to operations from SDK worker threads."""

    def __init__(self, client_options: dict[str, str], deadline: float, /, **_: object) -> None:
        # fsspec's metaclass merges process-wide S3 defaults into keyword
        # arguments. The positional binding is the only source of S3 options.
        session = AioSession(
            session_vars={
                "profile": (None, None, None, None),
                "config_file": (None, None, os.devnull, None),
                "credentials_file": (None, None, os.devnull, None),
            }
        )
        super().__init__(
            anon=False,
            session=session,
            use_listings_cache=False,
            client_kwargs={**client_options, "verify": True},
            config_kwargs={
                "signature_version": "s3v4",
                "connect_timeout": _S3_TIMEOUT_SECONDS,
                "read_timeout": _S3_TIMEOUT_SECONDS,
                "retries": {"total_max_attempts": 1, "mode": "standard"},
                "proxies": {},
                "use_dualstack_endpoint": False,
                "use_fips_endpoint": False,
                "ignore_configured_endpoint_urls": True,
            },
        )
        self.deadline = deadline
        self.retries = 1

    async def _call_s3(self, method: str, *args: object, **kwargs: object) -> Any:
        timeout = remaining(self.deadline)
        return await asyncio.wait_for(super()._call_s3(method, *args, **kwargs), timeout=timeout)

    # s3fs creates the base synchronous alias at import time. Rebind it so
    # FsspecOutputFile writes go through our deadline-aware method as well.
    def call_s3(self, method: str, *args: object, **kwargs: object) -> Any:
        return sync(self.loop, self._call_s3, method, *args, timeout=remaining(self.deadline), **kwargs)

    @staticmethod
    def close_session(loop: asyncio.AbstractEventLoop | None, s3: Any, *, deadline: float = 0.0) -> None:
        # s3fs also calls this from its finalizer. That call starts cleanup but does not wait.
        if loop is None or not loop.is_running():
            S3FileSystem.close_session(loop, s3)
            return
        close_before_deadline(loop, lambda: s3.__aexit__(None, None, None), deadline, "S3 session")

    async def read_metadata(self, location: str) -> bytes:
        bucket, key, _ = self.split_path(location)
        response = await self._call_s3("get_object", Bucket=bucket, Key=key, Range=f"bytes=0-{_METADATA_FILE_BYTES}")
        body = response["Body"]
        try:
            content_range = response.get("ContentRange", "")
            total = int(content_range.rsplit("/", 1)[-1]) if content_range else response["ContentLength"]
            if total > _METADATA_FILE_BYTES:
                raise IcebergDestinationError("destination_size")
            data = bytearray()
            while len(data) < total:
                chunk = await body.read(total - len(data))
                if not chunk:
                    raise IcebergDestinationError("storage_read")
                data.extend(chunk)
            return bytes(data)
        finally:
            body.close()


class _MetadataInput(InputFile):
    def __init__(self, location: str, data: bytes) -> None:
        super().__init__(location)
        self._data = data

    def __len__(self) -> int:
        return len(self._data)

    def exists(self) -> bool:
        return True

    def open(self, seekable: bool = True) -> io.BytesIO:
        return io.BytesIO(self._data)


class IcebergFileIO(FileIO):
    """Allow only the metadata and data locations bound to this table."""

    def __init__(self, locations: LocationProvider, client_options: dict[str, str]) -> None:
        super().__init__({})
        self._metadata = locations.metadata_path
        self._data = locations.data_path
        self._client_options = client_options
        self._fs: _TimedS3FileSystem | None = None
        self._closed = False
        self.deadline = 0.0
        self.max_data_files = 0
        self.partition_fields: tuple[NestedField, ...] | None = None
        self._data_files = 0
        self._reads = 0
        self._bytes = 0
        self._records = 0
        self._lock = Lock()

    def _filesystem(self) -> _TimedS3FileSystem:
        remaining(self.deadline)
        with self._lock:
            if self._closed:
                raise IcebergDestinationError("storage_closed")
            if self._fs is None:
                self._fs = _TimedS3FileSystem(self._client_options, self.deadline, skip_instance_cache=True)
            return self._fs

    def new_input(self, location: str) -> InputFile:
        # Partition replacement reads old files only through Iceberg metadata.
        if not _covers(self._metadata, location) or not location.endswith(".avro"):
            raise IcebergDestinationError("table_layout")
        fs = self._filesystem()
        with self._lock:
            if self._reads >= _METADATA_READS:
                raise IcebergDestinationError("destination_size")
            self._reads += 1
            data = sync(fs.loop, fs.read_metadata, location, timeout=remaining(self.deadline))
            self._admit_avro(data)
        return _MetadataInput(location, data)

    def _admit_avro(self, data: bytes) -> None:
        # Check encoded bytes, decoded bytes and record count before the SDK
        # materializes an Avro manifest list or manifest entries.
        decoder = new_decoder(data)
        header = _AVRO_HEADER_READER.read(decoder)
        codec = header.meta.get("avro.codec", "null")
        if header.magic != MAGIC or codec not in _AVRO_CODECS:
            raise IcebergDestinationError("table_layout")
        if self.partition_fields is not None:
            self._validate_partition_schema(header, self.partition_fields)
        self._bytes += len(data)
        while decoder.tell() < len(data):
            remaining(self.deadline)
            records = decoder.read_int()
            self._records += records
            if records <= 0 or self._records > _METADATA_RECORDS or self._bytes > _METADATA_TOTAL_BYTES:
                raise IcebergDestinationError("destination_size")
            block = decoder.read_bytes()
            if codec == "deflate":
                limit = min(_METADATA_FILE_BYTES, _METADATA_TOTAL_BYTES - self._bytes)
                inflater = zlib.decompressobj(-15)
                expanded = inflater.decompress(block, limit + 1)
                if len(expanded) > limit:
                    raise IcebergDestinationError("destination_size")
                if not inflater.eof:
                    raise IcebergDestinationError("table_layout")
                self._bytes += len(expanded)
            if decoder.read(16) != header.sync:
                raise IcebergDestinationError("table_layout")
        if self._bytes > _METADATA_TOTAL_BYTES:
            raise IcebergDestinationError("destination_size")

    def _validate_partition_schema(self, header: AvroFileHeader, expected: tuple[NestedField, ...]) -> None:
        # PyIceberg 0.12 reads manifest partition values in the Avro file's
        # physical order. Both scope selection and manifest rewriting need spec order.
        try:
            schema = header.get_schema().as_struct()
            data_files = [field for field in schema.fields if field.field_id == _MANIFEST_DATA_FILE_ID]
            if not data_files and schema.field(_MANIFEST_LIST_PATH_ID) is not None:
                return
            (data_file,) = data_files
            if not isinstance(data_file.field_type, StructType):
                raise ValueError("Invalid manifest data file schema")
            (partition,) = (field for field in data_file.field_type.fields if field.field_id == _DATA_FILE_PARTITION_ID)
        except AttributeError, KeyError, RecursionError, TypeError, ValueError:
            raise IcebergDestinationError("table_layout") from None
        if not isinstance(partition.field_type, StructType) or tuple(
            (field.field_id, field.field_type) for field in partition.field_type.fields
        ) != tuple((field.field_id, field.field_type) for field in expected):
            raise IcebergDestinationError("table_layout")

    def new_output(self, location: str) -> OutputFile:
        if not (_covers(self._metadata, location) or _covers(self._data, location)):
            raise IcebergDestinationError("storage_location")
        if location.endswith(".parquet"):
            with self._lock:
                if self._data_files >= self.max_data_files:
                    raise IcebergDestinationError("destination_file_limit")
                self._data_files += 1
        return FsspecOutputFile(location, self._filesystem())

    def delete(self, location: str | InputFile | OutputFile) -> None:
        path = location if isinstance(location, str) else location.location
        if not (_covers(self._metadata, path) or _covers(self._data, path)):
            raise IcebergDestinationError("storage_location")
        self._filesystem().rm(path)

    def close(self) -> None:
        with self._lock:
            self._closed = True
            fs = self._fs
            self._fs = None
            if fs is not None:
                fs.deadline = min(fs.deadline, monotonic())
        if fs is not None and getattr(fs, "_s3creator", None) is not None:
            try:
                fs.close_session(fs.loop, fs._s3creator, deadline=min(self.deadline, monotonic() + _S3_CLOSE_SECONDS))
            except Exception:
                _logger.warning("Could not close Iceberg S3 session. Details suppressed.")


type _S3Credentials = tuple[str | None, str | None, str | None]


def _vended_credentials(table: TableResponse, locations: LocationProvider) -> _S3Credentials:
    metadata_path, data_path = _s3_parts(locations.metadata_path), _s3_parts(locations.data_path)
    grants = [
        (
            _s3_parts(credential.prefix),
            (
                credential.config.get("s3.access-key-id"),
                credential.config.get("s3.secret-access-key"),
                credential.config.get("s3.session-token"),
            ),
        )
        for credential in table.storage_credentials
        if credential.prefix.startswith("s3://")
    ]
    effective: set[_S3Credentials] = set()
    for location in (_s3_parts(table.metadata_location or ""), metadata_path, data_path):
        matches = [(prefix, credentials) for prefix, credentials in grants if _contains(prefix, location)]
        if not matches:
            raise IcebergDestinationError("storage_credentials")
        longest = max(len(prefix[1]) for prefix, _ in matches)
        effective.update(credentials for prefix, credentials in matches if len(prefix[1]) == longest)
    if len(effective) != 1:
        raise IcebergDestinationError("storage_credentials")
    selected = effective.pop()
    if not any(
        credentials == selected and _contains(prefix, metadata_path) and _contains(prefix, data_path)
        for prefix, credentials in grants
    ):
        raise IcebergDestinationError("storage_credentials")
    if any(
        credentials != selected and (_contains(metadata_path, prefix) or _contains(data_path, prefix))
        for prefix, credentials in grants
    ):
        raise IcebergDestinationError("storage_credentials")
    return selected


def storage_file_io(config: IcebergS3Config | None, table: TableResponse, locations: LocationProvider) -> IcebergFileIO:
    metadata_location = table.metadata_location or ""
    if not _covers(locations.metadata_path, metadata_location):
        raise IcebergDestinationError("storage_location")
    _s3_parts(locations.data_path)
    if table.storage_credentials:
        access_key, secret_key, token = _vended_credentials(table, locations)
    elif config is not None:
        access_key, secret_key, token = (
            secret.get_secret_value() if secret is not None else None
            for secret in (config.access_key_id, config.secret_access_key, config.session_token)
        )
    else:
        raise IcebergDestinationError("storage_credentials")
    if (
        not isinstance(access_key, str)
        or not access_key.strip()
        or not isinstance(secret_key, str)
        or not secret_key.strip()
        or (token is not None and (not isinstance(token, str) or not token.strip()))
    ):
        raise IcebergDestinationError("storage_credentials")
    client_options = {
        "aws_access_key_id": access_key,
        "aws_secret_access_key": secret_key,
        "region_name": config.region if config is not None and config.region is not None else "us-east-1",
    }
    if token is not None:
        client_options["aws_session_token"] = token
    if config is not None and config.endpoint is not None:
        client_options["endpoint_url"] = config.endpoint
    return IcebergFileIO(locations, client_options)

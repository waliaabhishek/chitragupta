"""Enforce export file and layout limits around the catalog's native FileIO."""

from __future__ import annotations

from io import BytesIO
from threading import Lock
from typing import TYPE_CHECKING
from urllib.parse import urlsplit

from pyiceberg.avro.file import AvroFile
from pyiceberg.io import FileIO, InputFile
from pyiceberg.types import StructType

from core.emitters.iceberg_process import IcebergDestinationError

if TYPE_CHECKING:
    from pyiceberg.io import OutputFile
    from pyiceberg.table.locations import LocationProvider
    from pyiceberg.typedef import Record
    from pyiceberg.types import IcebergType, NestedField

_METADATA_FILE_BYTES = 4 * 1024 * 1024
_METADATA_TOTAL_BYTES = 64 * 1024 * 1024
_METADATA_READS = 512


def _s3_parts(location: str) -> tuple[str, str]:
    uri = urlsplit(location)
    if uri.scheme != "s3" or not uri.hostname or uri.netloc != uri.hostname or uri.query or uri.fragment:
        raise IcebergDestinationError("storage_location")
    return uri.netloc, uri.path.rstrip("/")


def _within(prefix: tuple[str, str], location: tuple[str, str]) -> bool:
    bucket, path = prefix
    return bucket == location[0] and (path == location[1] or location[1].startswith(path + "/"))


class _MetadataInput(InputFile):
    def __init__(self, location: str, body: bytes) -> None:
        super().__init__(location)
        self._body = body

    def __len__(self) -> int:
        return len(self._body)

    def exists(self) -> bool:
        return True

    def open(self, seekable: bool = True) -> BytesIO:
        return BytesIO(self._body)


class IcebergFileIO(FileIO):
    """Use SDK credentials and I/O while bounding the work admitted to a publication."""

    def __init__(
        self,
        native: FileIO,
        locations: LocationProvider,
        partition_fields: tuple[NestedField, ...],
        metadata_location: str,
    ) -> None:
        super().__init__(native.properties)
        self._native = native
        self._metadata = _s3_parts(locations.metadata_path)
        self._data = _s3_parts(locations.data_path)
        if not _within(self._metadata, _s3_parts(metadata_location)):
            raise IcebergDestinationError("storage_location")
        self._partition_fields = self._fields(partition_fields)
        self._reads = self._bytes = self._files = 0
        self._lock = Lock()

    @staticmethod
    def _fields(fields: tuple[NestedField, ...]) -> tuple[tuple[int, IcebergType], ...]:
        return tuple((field.field_id, field.field_type) for field in fields)

    def new_input(self, location: str) -> InputFile:
        path = _s3_parts(location)
        if _within(self._metadata, path) and location.endswith(".avro"):
            return self._read_metadata(location)
        if _within(self._data, path) and location.endswith(".parquet"):
            return self._native.new_input(location)
        raise IcebergDestinationError("table_layout")

    def _read_metadata(self, location: str) -> InputFile:
        with self._lock:
            allowance = min(_METADATA_FILE_BYTES, _METADATA_TOTAL_BYTES - self._bytes)
            if allowance <= 0 or self._reads >= _METADATA_READS:
                raise IcebergDestinationError("destination_size")
            self._reads += 1
            source = self._native.new_input(location)
            if len(source) > allowance:
                raise IcebergDestinationError("destination_size")
            with source.open() as stream:
                body = stream.read(allowance + 1)
            if len(body) > allowance:
                raise IcebergDestinationError("destination_size")
            self._bytes += len(body)
        bounded = _MetadataInput(location, body)
        self._validate_partition_layout(bounded)
        return bounded

    def _validate_partition_layout(self, source: InputFile) -> None:
        # PyIceberg interprets manifest partition values in physical field order.
        # Inspect the SDK's decoded header before allowing it to select old files.
        try:
            avro: AvroFile[Record] = AvroFile(source)
            with avro as reader:
                schema = reader.schema.as_struct()
            data_file = schema.field(2)
            if data_file is None and schema.field(500) is not None:  # Manifest list.
                return
            if data_file is not None and isinstance(data_file.field_type, StructType):
                partition = data_file.field_type.field(102)
                if (
                    partition is not None
                    and isinstance(partition.field_type, StructType)
                    and self._fields(partition.field_type.fields) == self._partition_fields
                ):
                    return
        except AttributeError, KeyError, RecursionError, TypeError, ValueError:
            pass
        raise IcebergDestinationError("table_layout")

    def new_output(self, location: str) -> OutputFile:
        path = _s3_parts(location)
        if _within(self._data, path) and location.endswith(".parquet"):
            with self._lock:
                if self._files >= 128:
                    raise IcebergDestinationError("destination_file_limit")
                self._files += 1
        elif not _within(self._metadata, path):  # Manifests and manifest lists.
            raise IcebergDestinationError("storage_location")
        return self._native.new_output(location)

    def delete(self, location: str | InputFile | OutputFile) -> None:
        path = _s3_parts(location if isinstance(location, str) else location.location)
        if not (_within(self._metadata, path) or _within(self._data, path)):
            raise IcebergDestinationError("storage_location")
        self._native.delete(location)

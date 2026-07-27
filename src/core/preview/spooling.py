from __future__ import annotations

import hashlib
import logging
import shutil
import sqlite3
import tempfile
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO, overload

from core.preview.models import PreviewArtifactMetadata, PreviewArtifactPayload

logger = logging.getLogger(__name__)
SQLITE_BATCH_SIZE = 256


@contextmanager
def _catalog_connection(path: Path) -> Iterator[sqlite3.Connection]:
    connection = sqlite3.connect(path)
    try:
        with connection:
            yield connection
    finally:
        connection.close()


class PreviewGenerationSpoolLimitError(OSError):
    """A generation exceeded its configured disk-spool budget."""


@dataclass
class PreviewSpoolBudget:
    limit_bytes: int
    used_bytes: int = 0

    def __post_init__(self) -> None:
        if not isinstance(self.limit_bytes, int) or isinstance(self.limit_bytes, bool) or self.limit_bytes <= 0:
            raise ValueError("spool limit must be a positive integer")

    @property
    def remaining_bytes(self) -> int:
        return self.limit_bytes - self.used_bytes

    def consume(self, size_bytes: int) -> None:
        if not isinstance(size_bytes, int) or isinstance(size_bytes, bool) or size_bytes < 0:
            raise ValueError("spooled byte count must be a non-negative integer")
        if size_bytes > self.remaining_bytes:
            raise PreviewGenerationSpoolLimitError(
                "FOCUS Mapping Preview package exceeds the configured generation spool limit."
            )
        self.used_bytes += size_bytes


@dataclass(frozen=True)
class PreviewSpooledBody:
    path: Path
    size_bytes: int
    sha256: str

    def __len__(self) -> int:
        return self.size_bytes

    def open(self) -> BinaryIO:
        return self.path.open("rb")

    def iter_chunks(self, *, chunk_size: int = 64 * 1024) -> Iterator[bytes]:
        if chunk_size <= 0:
            raise ValueError("chunk_size must be positive")
        with self.open() as handle:
            while chunk := handle.read(chunk_size):
                yield chunk


class PreviewGenerationWorkspace:
    """A generation-owned disk workspace with a hard current-size ceiling."""

    def __init__(
        self,
        limit_bytes: int,
        *,
        root: Path | None = None,
        accounting_roots: tuple[Path, ...] = (),
    ) -> None:
        self._budget = PreviewSpoolBudget(limit_bytes)
        self._root = Path(tempfile.mkdtemp(prefix="chitragupta-preview-")) if root is None else root
        if root is not None:
            self._root.mkdir(parents=True, exist_ok=False)
        self._accounting_roots = (self._root, *accounting_roots)
        self._closed = False
        self._cleanup_failure_recorded = False

    @property
    def root(self) -> Path:
        if self._closed:
            raise RuntimeError("generation workspace is closed")
        return self._root

    @property
    def limit_bytes(self) -> int:
        return self._budget.limit_bytes

    @property
    def used_bytes(self) -> int:
        return self._budget.used_bytes

    def _disk_usage(self) -> int:
        return sum(
            path.stat().st_size
            for root in self._accounting_roots
            if root.exists()
            for path in root.rglob("*")
            if path.is_file()
        )

    def preflight_write(self, size_bytes: int) -> None:
        if self._closed:
            raise RuntimeError("generation workspace is closed")
        if not isinstance(size_bytes, int) or isinstance(size_bytes, bool) or size_bytes < 0:
            raise ValueError("workspace write size must be a non-negative integer")
        if size_bytes > self._budget.remaining_bytes:
            raise PreviewGenerationSpoolLimitError(
                "FOCUS Mapping Preview package exceeds the configured generation spool limit."
            )

    def record_write(self, size_bytes: int) -> None:
        self.preflight_write(size_bytes)
        self._budget.consume(size_bytes)

    def enforce_limit(self) -> None:
        if self._closed:
            raise RuntimeError("generation workspace is closed")
        used = self._disk_usage()
        if used > self._budget.limit_bytes:
            raise PreviewGenerationSpoolLimitError(
                "FOCUS Mapping Preview package exceeds the configured generation spool limit."
            )
        self._budget.used_bytes = used

    def enforce_used_bytes(self, used_bytes: int) -> None:
        if self._closed:
            raise RuntimeError("generation workspace is closed")
        if used_bytes < 0:
            raise ValueError("workspace byte count must not be negative")
        if used_bytes > self._budget.limit_bytes:
            raise PreviewGenerationSpoolLimitError(
                "FOCUS Mapping Preview package exceeds the configured generation spool limit."
            )
        self._budget.used_bytes = max(self._budget.used_bytes, used_bytes)

    def constrain_sqlite_growth(self, connection: sqlite3.Connection, path: Path) -> int:
        """Constrain one main-database write to the remaining generation budget."""

        if path.parent != self.root:
            raise ValueError("generation SQLite files must be direct workspace children")
        page_size_row = connection.execute("PRAGMA page_size").fetchone()
        page_count_row = connection.execute("PRAGMA page_count").fetchone()
        assert page_size_row is not None
        assert page_count_row is not None
        page_size = int(page_size_row[0])
        page_count = int(page_count_row[0])
        allowed_pages = page_count + self._budget.remaining_bytes // page_size
        connection.execute(f"PRAGMA max_page_count={allowed_pages}")
        return path.stat().st_size

    @contextmanager
    def sqlite_connection(self, path: Path) -> Iterator[sqlite3.Connection]:
        if path.parent != self.root:
            raise ValueError("generation SQLite files must be direct workspace children")
        self.preflight_write(0)
        connection = sqlite3.connect(path)
        try:
            connection.execute("PRAGMA journal_mode=MEMORY")
            connection.execute("PRAGMA synchronous=OFF")
            self.enforce_limit()
            self.constrain_sqlite_growth(connection, path)
            try:
                yield connection
            except sqlite3.OperationalError as exc:
                if getattr(exc, "sqlite_errorcode", None) == sqlite3.SQLITE_FULL:
                    raise PreviewGenerationSpoolLimitError(
                        "FOCUS Mapping Preview package exceeds the configured generation spool limit."
                    ) from None
                raise
        finally:
            connection.close()
            if not self._closed:
                self.enforce_limit()

    def close(self) -> None:
        if getattr(self, "_closed", True):
            return
        try:
            shutil.rmtree(self._root)
        except FileNotFoundError:
            pass
        except OSError:
            self._cleanup_failure_recorded = True
            logger.exception("FOCUS Mapping Preview generation workspace cleanup failed path=%s", self._root)
            raise
        finally:
            self._closed = not self._root.exists()

    def __del__(self) -> None:
        try:
            self.close()
        except OSError:
            if not getattr(self, "_cleanup_failure_recorded", False):
                logger.exception("FOCUS Mapping Preview generation workspace finalizer cleanup failed")


class PreviewSpooledArtifactCollection(Sequence[PreviewArtifactPayload]):
    """Rewindable artifact descriptors backed by the generation SQLite catalog."""

    def __init__(self, catalog_path: Path) -> None:
        self._catalog_path = catalog_path

    def __len__(self) -> int:
        with _catalog_connection(self._catalog_path) as connection:
            value = connection.execute("SELECT COUNT(*) FROM artifacts").fetchone()
        assert value is not None
        return int(value[0])

    @property
    def metadata(self) -> PreviewSpooledArtifactMetadataCollection:
        return PreviewSpooledArtifactMetadataCollection(self._catalog_path)

    def __iter__(self) -> Iterator[PreviewArtifactPayload]:
        last_order = 0
        while True:
            with _catalog_connection(self._catalog_path) as connection:
                rows = connection.execute(
                    """
                    SELECT name, media_type, file_order, path, size_bytes, sha256
                    FROM artifacts
                    WHERE file_order > ?
                    ORDER BY file_order
                    LIMIT ?
                    """,
                    (last_order, SQLITE_BATCH_SIZE),
                ).fetchall()
            if not rows:
                return
            for name, media_type, order, path, size_bytes, sha256 in rows:
                yield PreviewArtifactPayload(
                    name=str(name),
                    media_type=str(media_type),
                    order=int(order),
                    body=PreviewSpooledBody(
                        path=Path(str(path)),
                        size_bytes=int(size_bytes),
                        sha256=str(sha256),
                    ),
                )
            last_order = int(rows[-1][2])

    @overload
    def __getitem__(self, index: int) -> PreviewArtifactPayload: ...

    @overload
    def __getitem__(self, index: slice) -> Sequence[PreviewArtifactPayload]: ...

    def __getitem__(self, index: int | slice) -> PreviewArtifactPayload | Sequence[PreviewArtifactPayload]:
        if isinstance(index, slice):
            return tuple(iter(self))[index]
        count = len(self)
        normalized = index if index >= 0 else count + index
        if normalized < 0 or normalized >= count:
            raise IndexError(index)
        with _catalog_connection(self._catalog_path) as connection:
            row = connection.execute(
                """
                SELECT name, media_type, file_order, path, size_bytes, sha256
                FROM artifacts
                ORDER BY file_order
                LIMIT 1 OFFSET ?
                """,
                (normalized,),
            ).fetchone()
        assert row is not None
        name, media_type, order, path, size_bytes, sha256 = row
        return PreviewArtifactPayload(
            name=str(name),
            media_type=str(media_type),
            order=int(order),
            body=PreviewSpooledBody(Path(str(path)), int(size_bytes), str(sha256)),
        )


class PreviewSpooledArtifactMetadataCollection(Sequence[PreviewArtifactMetadata]):
    _preview_validated_catalog = True

    def __init__(self, catalog_path: Path) -> None:
        self._catalog_path = catalog_path

    def __len__(self) -> int:
        with _catalog_connection(self._catalog_path) as connection:
            value = connection.execute("SELECT COUNT(*) FROM artifacts").fetchone()
        assert value is not None
        return int(value[0])

    def __iter__(self) -> Iterator[PreviewArtifactMetadata]:
        last_order = 0
        while True:
            with _catalog_connection(self._catalog_path) as connection:
                rows = connection.execute(
                    """
                    SELECT name, media_type, size_bytes, sha256, file_order
                    FROM artifacts
                    WHERE file_order > ?
                    ORDER BY file_order
                    LIMIT ?
                    """,
                    (last_order, SQLITE_BATCH_SIZE),
                ).fetchall()
            if not rows:
                return
            for name, media_type, size_bytes, sha256, order in rows:
                yield PreviewArtifactMetadata(
                    str(name),
                    str(media_type),
                    int(size_bytes),
                    str(sha256),
                    int(order),
                )
            last_order = int(rows[-1][4])

    @overload
    def __getitem__(self, index: int) -> PreviewArtifactMetadata: ...

    @overload
    def __getitem__(self, index: slice) -> Sequence[PreviewArtifactMetadata]: ...

    def __getitem__(
        self,
        index: int | slice,
    ) -> PreviewArtifactMetadata | Sequence[PreviewArtifactMetadata]:
        if isinstance(index, slice):
            return tuple(iter(self))[index]
        count = len(self)
        normalized = index if index >= 0 else count + index
        if normalized < 0 or normalized >= count:
            raise IndexError(index)
        with _catalog_connection(self._catalog_path) as connection:
            row = connection.execute(
                """
                SELECT name, media_type, size_bytes, sha256, file_order
                FROM artifacts
                ORDER BY file_order
                LIMIT 1 OFFSET ?
                """,
                (normalized,),
            ).fetchone()
        assert row is not None
        name, media_type, size_bytes, sha256, order = row
        return PreviewArtifactMetadata(
            str(name),
            str(media_type),
            int(size_bytes),
            str(sha256),
            int(order),
        )


def spooled_body_metadata(body: bytes | PreviewSpooledBody) -> tuple[int, str]:
    if isinstance(body, PreviewSpooledBody):
        return body.size_bytes, body.sha256
    return len(body), hashlib.sha256(body).hexdigest()

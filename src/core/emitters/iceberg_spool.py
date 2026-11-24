"""Bounded scratch storage and exact rounding for one chargeback day."""

from __future__ import annotations

import json
import sqlite3
from datetime import date, datetime
from decimal import MAX_EMAX, MIN_EMIN, ROUND_CEILING, ROUND_FLOOR, ROUND_HALF_UP, Context, Decimal
from sys import getsizeof
from time import monotonic
from typing import TYPE_CHECKING, NoReturn, Self, cast

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator
    from types import TracebackType

_UNIT = Decimal("0.00001")
_MAX_AMOUNT = Decimal("999999999999999999999999999999999.99999")
_MIN_AMOUNT = _MAX_AMOUNT.copy_negate()


class IcebergScratchError(RuntimeError):
    """A local preparation failure with a code safe to record."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


def _encode_temporal(value: object) -> str:
    if isinstance(value, date):
        return value.isoformat()
    raise TypeError("Unsupported Iceberg scratch field type")


class IcebergDaySpool:
    """Own one disposable, disk-backed day and stream its rounded output in bounded batches."""

    def __init__(
        self,
        deadline: float,
        *,
        limit_bytes: int,
        batch_bytes: int,
        check_time: Callable[[float], None] | None = None,
    ) -> None:
        self._deadline = deadline
        self._limit_bytes = limit_bytes
        self._batch_bytes = batch_bytes
        self._check_time = check_time
        self._connection: sqlite3.Connection
        self._amount_context = Context(prec=40, Emin=MIN_EMIN, Emax=MAX_EMAX)
        self._lowest_exponent = self._highest_digit = -5
        self._total = self._base_total = Decimal(0)
        self._candidates = 0
        self._winners: int
        self.row_count = 0

    def __enter__(self) -> Self:
        self._require_time()
        # The Unix SQLite VFS unlinks the owner-only temporary file on open.
        # An unlinked file leaves no work for a restarted worker after a kill.
        try:
            self._connection = sqlite3.connect("")
        except sqlite3.Error as exc:
            self._raise_sqlite_error(exc)
        try:
            self._connection.set_progress_handler(lambda: int(monotonic() >= self._deadline), 1000)
            self._connection.execute("PRAGMA temp_store=FILE")
            # A failure discards the entire scratch database. It needs no journal.
            self._connection.execute("PRAGMA journal_mode=OFF")
            self._connection.execute("PRAGMA cache_size=-1024")
            page_size = self._connection.execute("PRAGMA page_size").fetchone()[0]
            max_pages = self._limit_bytes // page_size
            if max_pages < 2:
                raise IcebergScratchError("scratch_byte_limit")
            self._connection.execute(f"PRAGMA max_page_count={max_pages}")
            # This primary key supplies the winner order without a separate index
            # or a temporary sort. An empty remainder sorts after all candidates.
            self._connection.execute("""
                CREATE TABLE projected (
                    remainder TEXT NOT NULL,
                    timestamp TEXT NOT NULL,
                    dimension_id INTEGER NOT NULL,
                    amount TEXT NOT NULL,
                    byte_count INTEGER NOT NULL,
                    payload BLOB NOT NULL,
                    PRIMARY KEY (remainder DESC, timestamp, dimension_id)
                ) WITHOUT ROWID
            """)
        except sqlite3.Error as exc:
            self._connection.close()
            self._raise_sqlite_error(exc)
        except BaseException:
            self._connection.close()
            raise
        return self

    def __exit__(
        self, exc_type: type[BaseException] | None, exc: BaseException | None, traceback: TracebackType | None
    ) -> None:
        self._connection.close()

    def _require_time(self) -> None:
        if self._check_time is not None:
            self._check_time(self._deadline)
        if monotonic() >= self._deadline:
            raise IcebergScratchError("scratch_deadline")

    def _raise_sqlite_error(self, exc: sqlite3.Error) -> NoReturn:
        match getattr(exc, "sqlite_errorcode", None):
            case sqlite3.SQLITE_FULL:
                raise IcebergScratchError("scratch_byte_limit") from None
            case sqlite3.SQLITE_INTERRUPT:
                self._require_time()
                raise IcebergScratchError("scratch_deadline") from None
            case _:
                raise IcebergScratchError("scratch_unavailable") from None

    def _add_amount(self, value: Decimal) -> tuple[Decimal, str]:
        if not value.is_finite():
            raise IcebergScratchError("non_finite_amount")
        if value:
            adjusted = value.adjusted()
            if adjusted > 32:
                raise IcebergScratchError("unrepresentable_amount")
            # as_tuple creates one Python item per coefficient digit.
            if getsizeof(value) * 48 > self._batch_bytes:
                raise IcebergScratchError("source_byte_limit")
            self._lowest_exponent = min(self._lowest_exponent, cast("int", value.as_tuple().exponent))
            self._highest_digit = max(self._highest_digit, adjusted)
        else:
            value = Decimal(0)

        # Include row-count carry. Precision can only grow, so earlier additions
        # already represent the exact sum and require no second pass.
        precision = self._highest_digit - self._lowest_exponent + len(str(self.row_count + 1)) + 2
        if precision * 8 > self._batch_bytes:
            raise IcebergScratchError("source_byte_limit")
        context = self._amount_context
        context.prec = max(context.prec, precision)
        lower = value.quantize(_UNIT, rounding=ROUND_FLOOR, context=context)
        upper = value.quantize(_UNIT, rounding=ROUND_CEILING, context=context)
        remainder = ""
        if _MIN_AMOUNT <= lower <= _MAX_AMOUNT:
            base = lower
            if lower != upper and upper <= _MAX_AMOUNT:
                # Every candidate is in (0, 0.00001). Canonical fixed-point
                # text sorts exactly under SQLite's binary text collation.
                remainder = format(context.subtract(value, lower), "f").rstrip("0")
                self._candidates += 1
        elif _MIN_AMOUNT <= upper <= _MAX_AMOUNT:
            base = upper
        else:
            raise IcebergScratchError("unrepresentable_amount")
        self._total = context.add(self._total, value)
        self._base_total = context.add(self._base_total, base)
        return base, remainder

    def add(self, fields: dict[str, object], *, dimension_id: int) -> None:
        self._require_time()
        tags = cast("dict[str, str]", fields["tags"])
        byte_count = (
            getsizeof(fields)
            + sum(getsizeof(value) for value in fields.values())
            + sum(getsizeof(key) + getsizeof(value) for key, value in tags.items())
            + 32  # List entry and capacity.
        )
        if byte_count > self._batch_bytes:
            raise IcebergScratchError("source_byte_limit")
        base, remainder = self._add_amount(cast("Decimal", fields["amount"]))
        timestamp = cast("datetime", fields["timestamp"]).isoformat(timespec="microseconds")
        without_amount = fields.copy()
        del without_amount["amount"]
        encoded = json.dumps(
            without_amount, default=_encode_temporal, ensure_ascii=False, separators=(",", ":")
        ).encode()
        byte_count += len(encoded)
        if byte_count > self._batch_bytes:
            raise IcebergScratchError("source_byte_limit")
        try:
            self._connection.execute(
                "INSERT INTO projected VALUES (?, ?, ?, ?, ?, ?)",
                (remainder, timestamp, dimension_id, str(base), byte_count, encoded),
            )
        except sqlite3.Error as exc:
            self._raise_sqlite_error(exc)
        self.row_count += 1

    def finish(self) -> None:
        """Establish how many rows get one unit after all source rows are present."""
        self._require_time()
        context = self._amount_context
        rounded = self._total.quantize(_UNIT, rounding=ROUND_HALF_UP, context=context)
        change = context.subtract(rounded, self._base_total)
        self._winners = int(context.scaleb(change, 5))
        if not 0 <= self._winners <= self._candidates:
            raise IcebergScratchError("unrepresentable_amount")

    def batches(self, max_bytes: int, *, max_rows: int) -> Iterator[list[dict[str, object]]]:
        self._require_time()
        batch: list[dict[str, object]] = []
        used_bytes = 0
        try:
            cursor = self._connection.execute("""
                SELECT amount, byte_count, payload FROM projected ORDER BY remainder DESC, timestamp, dimension_id
            """)
            for index, (amount, byte_count, payload) in enumerate(cursor):
                self._require_time()
                if byte_count > max_bytes:
                    raise IcebergScratchError("source_byte_limit")
                if len(batch) == max_rows or used_bytes + byte_count > max_bytes:
                    yield batch
                    batch, used_bytes = [], 0
                    self._require_time()
                fields = cast("dict[str, object]", json.loads(payload))
                fields["chargeback_date"] = date.fromisoformat(cast("str", fields["chargeback_date"]))
                fields["timestamp"] = datetime.fromisoformat(cast("str", fields["timestamp"]))
                if (completed_at := fields["calculation_completed_at"]) is not None:
                    fields["calculation_completed_at"] = datetime.fromisoformat(cast("str", completed_at))
                base = Decimal(amount)
                fields["amount"] = self._amount_context.add(base, _UNIT) if index < self._winners else base
                batch.append(fields)
                used_bytes += byte_count
        except sqlite3.Error as exc:
            self._raise_sqlite_error(exc)
        if batch:
            self._require_time()
            yield batch

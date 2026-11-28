from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from itertools import permutations
from time import monotonic
from typing import TYPE_CHECKING, cast

import pytest

from core.emitters.iceberg_spool import IcebergDaySpool, IcebergScratchError

if TYPE_CHECKING:
    from collections.abc import Iterable

DAY = date(2026, 11, 18)
NOW = datetime(2026, 11, 18, tzinfo=UTC)
MIB = 1024**2
MAX_WHOLE = "9" * 33


def _fields(
    amount: str, dimension_id: int, timestamp: datetime = NOW, tags: dict[str, str] | None = None
) -> dict[str, object]:
    return {
        "ecosystem": "generic_metrics_only",
        "tenant_id": "tenant",
        "chargeback_date": DAY,
        "timestamp": timestamp,
        "identity_id": str(dimension_id),
        "product_category": "category",
        "product_type": "product",
        "cost_type": "shared",
        "resource_id": None,
        "allocation_method": None,
        "allocation_detail": None,
        "amount": Decimal(amount),
        "tags": tags or {},
        "env_id": None,
        "team": None,
        "calculation_id": "revision",
        "calculation_completed_at": NOW,
    }


def _amounts(terms: Iterable[tuple[int, str]], *, max_rows: int = 2) -> dict[str, Decimal]:
    with IcebergDaySpool(monotonic() + 5, limit_bytes=MIB, batch_bytes=MIB) as spool:
        for dimension_id, amount in terms:
            spool.add(_fields(amount, dimension_id), dimension_id=dimension_id)
        spool.finish()
        return {
            cast("str", row["identity_id"]): cast("Decimal", row["amount"])
            for batch in spool.batches(MIB, max_rows=max_rows)
            for row in batch
        }


def test_rounding_preserves_exact_day_total_across_input_orders() -> None:
    terms = [f"{MAX_WHOLE}.99999", f"-{MAX_WHOLE}.99999", "0.000005", "-1E-97"]
    expected = {"0": Decimal(terms[0]), "1": Decimal(terms[1]), "2": Decimal("0.00000"), "3": Decimal("0.00000")}
    for rows in permutations(enumerate(terms)):
        assert _amounts(rows) == expected


def test_rounding_ranks_close_amounts_before_timestamps_across_batches() -> None:
    with IcebergDaySpool(monotonic() + 5, limit_bytes=MIB, batch_bytes=MIB) as spool:
        earlier = _fields("0.00000500000000000000000001", 0)
        later = _fields("0.00000500000000000000000002", 1, NOW + timedelta(microseconds=1), {"team": "café"})
        spool.add(earlier, dimension_id=0)
        spool.add(later, dimension_id=1)
        spool.finish()
        earlier["amount"] = Decimal("0.00000")
        later["amount"] = Decimal("0.00001")
        assert list(spool.batches(MIB, max_rows=1)) == [[later], [earlier]]


def test_rounding_breaks_ties_by_timestamp_then_dimension_id() -> None:
    with IcebergDaySpool(monotonic() + 5, limit_bytes=MIB, batch_bytes=MIB) as spool:
        for dimension_id, timestamp in ((0, NOW + timedelta(seconds=1)), (2, NOW), (1, NOW)):
            spool.add(_fields("0.000004", dimension_id, timestamp), dimension_id=dimension_id)
        spool.finish()
        rows = next(spool.batches(MIB, max_rows=3))
        assert [(row["identity_id"], row["amount"]) for row in rows] == [
            ("1", Decimal("0.00001")),
            ("2", Decimal("0.00000")),
            ("0", Decimal("0.00000")),
        ]
    assert _amounts([(7, "-0.000004"), (3, "-0.000004")]) == {"7": Decimal("-0.00001"), "3": Decimal("0.00000")}
    assert _amounts([(0, "-0.000005")]) == {"0": Decimal("-0.00001")}


def test_rounding_clips_only_unrepresentable_choices() -> None:
    assert _amounts([(0, f"{MAX_WHOLE}.999996"), (1, f"-{MAX_WHOLE}.999996")]) == {
        "0": Decimal(f"{MAX_WHOLE}.99999"),
        "1": Decimal(f"-{MAX_WHOLE}.99999"),
    }
    assert _amounts([(0, f"{MAX_WHOLE}.99999"), (1, f"{MAX_WHOLE}.99999")]) == {
        "0": Decimal(f"{MAX_WHOLE}.99999"),
        "1": Decimal(f"{MAX_WHOLE}.99999"),
    }


@pytest.mark.parametrize(
    ("term", "error"),
    [
        (f"{MAX_WHOLE}.999995", "unrepresentable_amount"),
        ("-1E33", "unrepresentable_amount"),
        ("NaN", "non_finite_amount"),
        ("1E-1000000000", "source_byte_limit"),
    ],
)
def test_rounding_rejects_bad_days_safely(term: str, error: str) -> None:
    with pytest.raises(IcebergScratchError, match=error):
        _amounts([(1, term)])


def test_spool_caps_rows_and_bytes_and_needs_no_temporary_sort() -> None:
    with IcebergDaySpool(monotonic() + 5, limit_bytes=MIB, batch_bytes=MIB) as spool:
        for dimension_id in range(4):
            spool.add(_fields("1.12345", dimension_id, tags={"test": "x" * 1024}), dimension_id=dimension_id)
        spool.finish()
        assert [len(batch) for batch in spool.batches(MIB, max_rows=3)] == [3, 1]
        largest = spool._connection.execute("SELECT max(byte_count) FROM projected").fetchone()[0]
        assert [len(batch) for batch in spool.batches(largest * 2, max_rows=3)] == [2, 2]
        assert [len(batch) for batch in spool.batches(largest * 2 - 1, max_rows=3)] == [1, 1, 1, 1]
        plan = spool._connection.execute("""
            EXPLAIN QUERY PLAN
            SELECT amount, byte_count, payload FROM projected ORDER BY remainder DESC, timestamp, dimension_id
        """).fetchall()
        assert not any("TEMP B-TREE" in entry[-1] for entry in plan)


def test_spool_rejects_width_and_full_disk_before_any_output() -> None:
    with IcebergDaySpool(monotonic() + 5, limit_bytes=MIB, batch_bytes=MIB) as spool:
        with pytest.raises(IcebergScratchError, match="source_byte_limit"):
            spool.add(_fields("1", 0, tags={"test": "x" * MIB}), dimension_id=0)
        assert spool.row_count == 0

    with IcebergDaySpool(monotonic() + 5, limit_bytes=8 * 1024, batch_bytes=MIB) as spool:
        with pytest.raises(IcebergScratchError, match="scratch_byte_limit"):
            spool.add(_fields("1", 0, tags={"test": "x" * (32 * 1024)}), dimension_id=0)
        assert spool.row_count == 0


def test_spool_checks_time_during_preparation_and_batching(monkeypatch: pytest.MonkeyPatch) -> None:
    clock = 0
    monkeypatch.setattr("core.emitters.iceberg_spool.monotonic", lambda: clock)
    with IcebergDaySpool(5, limit_bytes=MIB, batch_bytes=MIB) as spool:
        spool.add(_fields("1", 0), dimension_id=0)
        spool.add(_fields("2", 1), dimension_id=1)
        spool.finish()
        rows = spool.batches(MIB, max_rows=1)
        assert len(next(rows)) == 1
        clock = 5
        with pytest.raises(IcebergScratchError, match="scratch_deadline"):
            spool.finish()
        with pytest.raises(IcebergScratchError, match="scratch_deadline"):
            next(rows)

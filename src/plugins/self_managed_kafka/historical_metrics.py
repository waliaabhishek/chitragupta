"""Bounded historical Prometheus acquisition for self-managed Kafka."""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Sequence
from datetime import UTC, datetime, time, timedelta
from math import gcd
from typing import TYPE_CHECKING

from core.metrics.protocol import MetricsQueryError
from core.models import MetricQuery

if TYPE_CHECKING:
    from core.metrics.protocol import MetricsSource
    from core.models import MetricRow

SECONDS_PER_DAY = 86_400


def utc_day_windows(start: datetime, end: datetime) -> tuple[tuple[datetime, datetime], ...]:
    """Return closed UTC calendar-day windows covering the supplied range."""
    if start.tzinfo is None or end.tzinfo is None:
        raise ValueError("historical acquisition requires timezone-aware datetimes")
    current = start.astimezone(UTC).replace(hour=0, minute=0, second=0, microsecond=0)
    final = end.astimezone(UTC).replace(hour=0, minute=0, second=0, microsecond=0)
    windows: list[tuple[datetime, datetime]] = []
    while current < final:
        next_day = current + timedelta(days=1)
        windows.append((current, next_day))
        current = next_day
    return tuple(windows)


def contiguous_window_runs(
    windows: Iterable[tuple[datetime, datetime]],
) -> tuple[tuple[tuple[datetime, datetime], ...], ...]:
    """Group daily windows into maximal contiguous runs."""
    ordered = sorted(windows)
    if not ordered:
        return ()
    runs: list[list[tuple[datetime, datetime]]] = [[ordered[0]]]
    for window in ordered[1:]:
        if window[0] == runs[-1][-1][1]:
            runs[-1].append(window)
        else:
            runs.append([window])
    return tuple(tuple(run) for run in runs)


def validate_utc_day_windows(windows: Sequence[tuple[datetime, datetime]]) -> None:
    """Validate calculation-owned windows before bounded historical planning."""
    previous_end: datetime | None = None
    for start, end in windows:
        if start.tzinfo is None or end.tzinfo is None:
            raise ValueError("historical acquisition requires timezone-aware datetimes")
        start_utc = start.astimezone(UTC)
        end_utc = end.astimezone(UTC)
        if start_utc.time() != time.min or end_utc != start_utc + timedelta(days=1):
            raise ValueError("historical acquisition requires closed 24-hour UTC calendar days")
        if previous_end is not None and start_utc < previous_end:
            raise ValueError("historical acquisition windows must be ordered and non-overlapping")
        previous_end = end_utc


def bounded_window_chunks(
    windows: Iterable[tuple[datetime, datetime]],
    chunk_days: int,
) -> tuple[tuple[tuple[datetime, datetime], ...], ...]:
    """Split each contiguous run into ordered chunks of at most ``chunk_days``."""
    if not 1 <= chunk_days <= 30:
        raise ValueError("chunk_days must be between 1 and 30")
    ordered = tuple(windows)
    validate_utc_day_windows(ordered)
    chunks: list[tuple[tuple[datetime, datetime], ...]] = []
    for run in contiguous_window_runs(ordered):
        for offset in range(0, len(run), chunk_days):
            chunks.append(run[offset : offset + chunk_days])
    return tuple(chunks)


def gauge_residue_groups(
    chunk: Sequence[tuple[datetime, datetime]],
    step: timedelta,
) -> tuple[tuple[tuple[datetime, datetime], ...], ...]:
    """Group gauge windows by the legacy daily-grid residue."""
    seconds = int(step.total_seconds())
    if seconds <= 0:
        raise ValueError("metrics step must be positive")
    period = seconds // gcd(seconds, SECONDS_PER_DAY)
    groups: dict[int, list[tuple[datetime, datetime]]] = {}
    for index, window in enumerate(chunk):
        groups.setdefault(index % period, []).append(window)
    return tuple(tuple(groups[key]) for key in sorted(groups))


def collect_daily_evidence(
    metrics_source: MetricsSource,
    queries: Sequence[MetricQuery],
    windows: Sequence[tuple[datetime, datetime]],
    *,
    step: timedelta,
    chunk_days: int,
    resource_id_filter: str | None = None,
) -> dict[tuple[datetime, datetime], dict[str, list[MetricRow]]]:
    """Acquire and reduce all supplied windows.

    Production callers that need bounded memory should use
    :func:`iter_daily_evidence`; this compatibility wrapper intentionally
    aggregates its yielded chunk mappings for callers that need a complete
    result.
    """
    result: dict[tuple[datetime, datetime], dict[str, list[MetricRow]]] = {}
    for _, chunk_result in iter_daily_evidence(
        metrics_source,
        queries,
        windows,
        step=step,
        chunk_days=chunk_days,
        resource_id_filter=resource_id_filter,
    ):
        result.update(chunk_result)
    return result


def iter_daily_evidence(
    metrics_source: MetricsSource,
    queries: Sequence[MetricQuery],
    windows: Sequence[tuple[datetime, datetime]],
    *,
    step: timedelta,
    chunk_days: int,
    resource_id_filter: str | None = None,
) -> Iterator[
    tuple[
        tuple[tuple[datetime, datetime], ...],
        dict[tuple[datetime, datetime], dict[str, list[MetricRow]]],
    ]
]:
    """Acquire one bounded chunk at a time and release it before the next."""
    if not windows or not queries:
        return
    for chunk in bounded_window_chunks(windows, chunk_days):
        chunk_result = _collect_daily_evidence_chunk(
            metrics_source,
            queries,
            chunk,
            step=step,
            resource_id_filter=resource_id_filter,
        )
        yield chunk, chunk_result
        del chunk_result


def _collect_daily_evidence_chunk(
    metrics_source: MetricsSource,
    queries: Sequence[MetricQuery],
    chunk: Sequence[tuple[datetime, datetime]],
    *,
    step: timedelta,
    resource_id_filter: str | None,
) -> dict[tuple[datetime, datetime], dict[str, list[MetricRow]]]:
    result: dict[tuple[datetime, datetime], dict[str, list[MetricRow]]] = {window: {} for window in chunk}
    for query in queries:
        if query.query_mode == "instant":
            _collect_counter_family(metrics_source, query, (chunk,), result, step, resource_id_filter)
        else:
            _collect_gauge_family(metrics_source, query, (chunk,), result, step, resource_id_filter)
    return result


def _collect_counter_family(
    source: MetricsSource,
    query: MetricQuery,
    chunks: Sequence[Sequence[tuple[datetime, datetime]]],
    result: dict[tuple[datetime, datetime], dict[str, list[MetricRow]]],
    step: timedelta,
    resource_id_filter: str | None,
) -> None:
    for chunk in chunks:
        start, end = chunk[0][1], chunk[-1][1]
        ranged_query = _range_query(query)
        try:
            response = source.query(
                queries=[ranged_query],
                start=start,
                end=end,
                step=timedelta(days=1),
                resource_id_filter=resource_id_filter,
            )
        except MetricsQueryError:
            _fallback_family_days(source, query, chunk, result, step, resource_id_filter)
            continue
        _partition_counter_rows(query.key, response.get(query.key, []), chunk, result)


def _collect_gauge_family(
    source: MetricsSource,
    query: MetricQuery,
    chunks: Sequence[Sequence[tuple[datetime, datetime]]],
    result: dict[tuple[datetime, datetime], dict[str, list[MetricRow]]],
    step: timedelta,
    resource_id_filter: str | None,
) -> None:
    for chunk in chunks:
        for group in gauge_residue_groups(chunk, step):
            start, end = group[0][0], group[-1][1]
            try:
                response = source.query(
                    queries=[query],
                    start=start,
                    end=end,
                    step=step,
                    resource_id_filter=resource_id_filter,
                )
            except MetricsQueryError:
                _fallback_family_days(source, query, group, result, step, resource_id_filter)
                continue
            rows = response.get(query.key, [])
            partitioned = _partition_gauge_rows_by_window(rows, group, step)
            for window, window_rows in partitioned.items():
                result[window][query.key] = window_rows


def _fallback_family_days(
    source: MetricsSource,
    query: MetricQuery,
    windows: Sequence[tuple[datetime, datetime]],
    result: dict[tuple[datetime, datetime], dict[str, list[MetricRow]]],
    step: timedelta,
    resource_id_filter: str | None,
) -> None:
    for start, end in windows:
        try:
            response = source.query(
                queries=[query],
                start=start,
                end=end,
                step=step,
                resource_id_filter=resource_id_filter,
            )
        except MetricsQueryError:
            result[(start, end)][query.key] = []
            continue
        rows = response.get(query.key, [])
        if query.query_mode == "instant":
            _partition_counter_rows(query.key, rows, ((start, end),), result)
        else:
            result[(start, end)][query.key] = _partition_gauge_rows(rows, (start, end), step)


def _partition_counter_rows(
    metric_key: str,
    rows: Sequence[MetricRow],
    windows: Sequence[tuple[datetime, datetime]],
    result: dict[tuple[datetime, datetime], dict[str, list[MetricRow]]],
) -> None:
    rows_by_end: dict[datetime, list[MetricRow]] = {}
    for row in rows:
        rows_by_end.setdefault(row.timestamp, []).append(row)
    for start, end in windows:
        # A counter evaluation at the exact day end owns only the preceding
        # half-open UTC day.  Start/interior samples never get fanned out.
        result[(start, end)][metric_key] = rows_by_end.get(end, [])


def _partition_gauge_rows(
    rows: Sequence[MetricRow],
    window: tuple[datetime, datetime],
    step: timedelta,
) -> list[MetricRow]:
    return _partition_gauge_rows_by_window(rows, (window,), step)[window]


def _partition_gauge_rows_by_window(
    rows: Sequence[MetricRow],
    windows: Sequence[tuple[datetime, datetime]],
    step: timedelta,
) -> dict[tuple[datetime, datetime], list[MetricRow]]:
    """Assign one bounded gauge response to its owned daily grids in one pass."""
    step_seconds = step.total_seconds()
    if step_seconds <= 0:
        raise ValueError("metrics step must be positive")
    by_day_start = {window[0]: window for window in windows}
    partitioned: dict[tuple[datetime, datetime], list[MetricRow]] = {window: [] for window in windows}
    for row in rows:
        row_day_start = row.timestamp.astimezone(UTC).replace(hour=0, minute=0, second=0, microsecond=0)
        window = by_day_start.get(row_day_start)
        if window is None:
            continue
        day_start, day_end = window
        if day_start <= row.timestamp < day_end and (row.timestamp - day_start).total_seconds() % step_seconds == 0:
            partitioned[window].append(row)
    return partitioned


def _range_query(query: MetricQuery) -> MetricQuery:
    """Use range evaluation for a bounded multi-day counter acquisition."""
    return MetricQuery(
        key=query.key,
        query_expression=query.query_expression,
        label_keys=query.label_keys,
        resource_label=query.resource_label,
        query_mode="range",
        metadata=query.metadata,
    )

"""Pure quota-backed principal attribution calculations."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import ROUND_DOWN, Decimal, InvalidOperation
from enum import StrEnum
from math import floor
from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    from core.models import MetricRow

_ZERO = Decimal("0")


class PrincipalAttributionState(StrEnum):
    """Terminal state for one principal-allocation direction."""

    READY = "ready"
    DEGRADED = "degraded"
    UNAVAILABLE = "unavailable"
    ZERO_USAGE = "zero_usage"
    POLICY_ONLY = "policy_only"


@dataclass(frozen=True)
class PrincipalWeight:
    """Canonical principal weight and its configuration-time ownership snapshot."""

    identity_id: str
    team: str
    weight: Decimal


@dataclass(frozen=True)
class PrincipalDirectionEvaluation:
    """Pure evidence evaluation for one direction and logical billing interval."""

    direction: Literal["ingress", "egress"]
    quota_type: Literal["Produce", "Fetch"]
    state: PrincipalAttributionState
    detail: str
    user_weights: tuple[PrincipalWeight, ...]
    client_only_weight: Decimal
    total_weight: Decimal
    coverage_complete: bool
    declared_scrape_interval: timedelta
    observed_deltas: tuple[Decimal, ...]


@dataclass(frozen=True)
class PrincipalMoneyAllocation:
    """Exact monetary result for a principal direction or fixed policy."""

    state: PrincipalAttributionState
    user_amounts: tuple[tuple[PrincipalWeight, Decimal], ...]
    client_only_amount: Decimal
    rounding_residual: Decimal
    balance: Decimal


def evaluate_quota_direction(
    rows: Sequence[MetricRow],
    *,
    direction: Literal["ingress", "egress"],
    start: datetime,
    end: datetime,
    scrape_interval: timedelta,
    max_gap: timedelta,
    principal_to_team: Mapping[str, str],
    default_team: str,
) -> PrincipalDirectionEvaluation:
    """Integrate complete quota gauges into canonical principal weights."""
    quota_type: Literal["Produce", "Fetch"] = "Produce" if direction == "ingress" else "Fetch"
    if not rows:
        return _unavailable(
            direction,
            quota_type,
            "principal_telemetry_not_observed",
            scrape_interval,
        )

    grouped: dict[tuple[str, str, str, str], list[tuple[datetime, Decimal]]] = defaultdict(list)
    source_series_by_group: dict[tuple[str, str, str, str], set[tuple[tuple[str, str], ...]]] = defaultdict(set)
    for row in rows:
        parsed = _parse_row(row, expected_quota_type=quota_type)
        if parsed is None:
            return _unavailable(
                direction,
                quota_type,
                "principal_telemetry_invalid",
                scrape_interval,
            )
        group, value = parsed
        grouped[group].append((row.timestamp, value))
        if row.source_series is not None:
            source_series_by_group[group].add(row.source_series)

    user_weights: defaultdict[str, Decimal] = defaultdict(lambda: _ZERO)
    client_only_weight = _ZERO
    observed_deltas: list[Decimal] = []
    for group, samples in grouped.items():
        _broker, scope, user, _client_id = group
        if len(source_series_by_group[group]) > 1:
            return _unavailable(
                direction,
                quota_type,
                "principal_telemetry_invalid",
                scrape_interval,
                observed_deltas,
            )
        normalized = _normalize_samples(samples)
        if normalized is None:
            return _unavailable(
                direction,
                quota_type,
                "principal_telemetry_invalid",
                scrape_interval,
                observed_deltas,
            )
        guard_candidates = [sample for sample in normalized if sample[0] <= start]
        if not guard_candidates:
            return _unavailable(
                direction,
                quota_type,
                "principal_telemetry_incomplete",
                scrape_interval,
                observed_deltas,
            )
        guard = guard_candidates[-1]
        if start - guard[0] > max_gap:
            return _unavailable(
                direction,
                quota_type,
                "principal_telemetry_incomplete",
                scrape_interval,
                observed_deltas,
            )
        selected = [guard, *(sample for sample in normalized if start < sample[0] <= end)]
        for previous, current in zip(selected, selected[1:], strict=False):
            delta = _seconds_between(previous[0], current[0])
            observed_deltas.append(delta)
            if not _matches_declared_cadence(delta, scrape_interval):
                return _unavailable(
                    direction,
                    quota_type,
                    "principal_telemetry_cadence_mismatch",
                    scrape_interval,
                    observed_deltas,
                )
            if current[0] - previous[0] > max_gap:
                return _unavailable(
                    direction,
                    quota_type,
                    "principal_telemetry_incomplete",
                    scrape_interval,
                    observed_deltas,
                )
        if end - selected[-1][0] > max_gap:
            return _unavailable(
                direction,
                quota_type,
                "principal_telemetry_incomplete",
                scrape_interval,
                observed_deltas,
            )
        weight = _integrate_samples(selected, start, end)
        if scope == "client-id":
            client_only_weight += weight
        else:
            user_weights[f"User:{user}"] += weight

    weights = tuple(
        PrincipalWeight(identity_id, principal_to_team.get(identity_id, default_team), weight)
        for identity_id, weight in sorted(user_weights.items())
        if weight > _ZERO
    )
    total_weight = sum((weight.weight for weight in weights), client_only_weight)
    if total_weight == _ZERO:
        state = PrincipalAttributionState.ZERO_USAGE
    elif client_only_weight > _ZERO:
        state = PrincipalAttributionState.DEGRADED
    else:
        state = PrincipalAttributionState.READY
    return PrincipalDirectionEvaluation(
        direction=direction,
        quota_type=quota_type,
        state=state,
        detail=state.value,
        user_weights=weights,
        client_only_weight=client_only_weight,
        total_weight=total_weight,
        coverage_complete=True,
        declared_scrape_interval=scrape_interval,
        observed_deltas=tuple(observed_deltas),
    )


def allocate_principal_money(
    evaluation: PrincipalDirectionEvaluation,
    *,
    pool: Decimal,
    quantum: Decimal = Decimal("0.0001"),
) -> PrincipalMoneyAllocation:
    """Allocate one monetary pool using the evaluator's sole quota denominator."""
    if evaluation.state not in {PrincipalAttributionState.READY, PrincipalAttributionState.DEGRADED}:
        return PrincipalMoneyAllocation(
            state=evaluation.state,
            user_amounts=(),
            client_only_amount=pool,
            rounding_residual=_ZERO,
            balance=pool,
        )
    user_amounts = tuple(
        (weight, (pool * weight.weight / evaluation.total_weight).quantize(quantum, rounding=ROUND_DOWN))
        for weight in evaluation.user_weights
    )
    client_only_amount = (pool * evaluation.client_only_weight / evaluation.total_weight).quantize(
        quantum,
        rounding=ROUND_DOWN,
    )
    allocated = sum((amount for _, amount in user_amounts), client_only_amount)
    return PrincipalMoneyAllocation(
        state=evaluation.state,
        user_amounts=user_amounts,
        client_only_amount=client_only_amount,
        rounding_residual=pool - allocated,
        balance=pool,
    )


def allocate_static_even(
    *,
    identities: Sequence[str],
    pool: Decimal,
    quantum: Decimal = Decimal("0.0001"),
) -> PrincipalMoneyAllocation:
    """Allocate a fixed policy pool evenly, preserving its rounding residual."""
    weights = tuple(PrincipalWeight(identity_id, "", Decimal("1")) for identity_id in sorted(set(identities)))
    if not weights:
        return PrincipalMoneyAllocation(
            state=PrincipalAttributionState.POLICY_ONLY,
            user_amounts=(),
            client_only_amount=pool,
            rounding_residual=_ZERO,
            balance=pool,
        )
    amount = (pool / len(weights)).quantize(quantum, rounding=ROUND_DOWN)
    user_amounts = tuple((weight, amount) for weight in weights)
    return PrincipalMoneyAllocation(
        state=PrincipalAttributionState.POLICY_ONLY,
        user_amounts=user_amounts,
        client_only_amount=_ZERO,
        rounding_residual=pool - sum(entry_amount for _, entry_amount in user_amounts),
        balance=pool,
    )


def _parse_row(
    row: MetricRow,
    *,
    expected_quota_type: Literal["Produce", "Fetch"],
) -> tuple[tuple[str, str, str, str], Decimal] | None:
    labels = row.labels
    broker = labels.get("broker")
    scope = labels.get("quota_scope")
    user = labels.get("user")
    client_id = labels.get("client_id")
    if (
        not broker
        or labels.get("quota_type") != expected_quota_type
        or scope not in {"user", "user-client", "client-id"}
        or user is None
        or client_id is None
    ):
        return None
    if scope == "user" and (not _identity_label(user) or client_id != "not_applicable"):
        return None
    if scope == "user-client" and (not _identity_label(user) or not _identity_label(client_id)):
        return None
    if scope == "client-id" and (user != "not_applicable" or not _identity_label(client_id)):
        return None
    try:
        value = Decimal(row.source_value) if row.source_value is not None else Decimal(str(row.value))
    except InvalidOperation, ValueError:
        return None
    if not value.is_finite() or value < _ZERO:
        return None
    return (broker, scope, user, client_id), value


def _normalize_samples(
    samples: Sequence[tuple[datetime, Decimal]],
) -> list[tuple[datetime, Decimal]] | None:
    normalized: list[tuple[datetime, Decimal]] = []
    for timestamp, value in sorted(samples, key=lambda sample: sample[0]):
        if normalized and timestamp == normalized[-1][0]:
            return None
        normalized.append((timestamp, value))
    return normalized


def _integrate_samples(samples: Sequence[tuple[datetime, Decimal]], start: datetime, end: datetime) -> Decimal:
    total = _ZERO
    for previous, current in zip(samples, samples[1:], strict=False):
        segment_start = max(previous[0], start)
        segment_end = min(current[0], end)
        if segment_end > segment_start:
            total += previous[1] * _seconds_between(segment_start, segment_end)
    last_timestamp, last_value = samples[-1]
    if end > last_timestamp:
        total += last_value * _seconds_between(max(last_timestamp, start), end)
    return total


def _matches_declared_cadence(delta: Decimal, scrape_interval: timedelta) -> bool:
    interval = _timedelta_seconds(scrape_interval)
    if interval <= _ZERO or delta <= _ZERO:
        return False
    multiple = max(1, floor(delta / interval + Decimal("0.5")))
    jitter = min(Decimal("1"), interval / Decimal("10"))
    return abs(delta - Decimal(multiple) * interval) <= jitter


def _timedelta_seconds(value: timedelta) -> Decimal:
    return Decimal(value.days * 86400 + value.seconds) + Decimal(value.microseconds) / Decimal(1_000_000)


def _seconds_between(start: datetime, end: datetime) -> Decimal:
    return _timedelta_seconds(end - start)


def _identity_label(value: str) -> bool:
    return bool(value) and value != "not_applicable"


def _unavailable(
    direction: Literal["ingress", "egress"],
    quota_type: Literal["Produce", "Fetch"],
    detail: str,
    scrape_interval: timedelta,
    observed_deltas: Sequence[Decimal] = (),
) -> PrincipalDirectionEvaluation:
    return PrincipalDirectionEvaluation(
        direction=direction,
        quota_type=quota_type,
        state=PrincipalAttributionState.UNAVAILABLE,
        detail=detail,
        user_weights=(),
        client_only_weight=_ZERO,
        total_weight=_ZERO,
        coverage_complete=False,
        declared_scrape_interval=scrape_interval,
        observed_deltas=tuple(observed_deltas),
    )

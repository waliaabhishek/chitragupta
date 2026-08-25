"""Behavioral coverage for quota-backed principal attribution."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Literal

import pytest

from core.models import MetricRow
from plugins.self_managed_kafka.principal_attribution import PrincipalDirectionEvaluation, evaluate_quota_direction

_START = datetime(2026, 8, 1, tzinfo=UTC)
_END = _START + timedelta(seconds=20)


def _row(
    timestamp: datetime,
    value: float,
    *,
    quota_type: str = "Produce",
    quota_scope: str = "user",
    user: str = "alice",
    client_id: str = "not_applicable",
    broker: str = "1",
    source_series: tuple[tuple[str, str], ...] | None = None,
) -> MetricRow:
    return MetricRow(
        timestamp=timestamp,
        metric_key="quota_byte_rate",
        value=value,
        labels={
            "broker": broker,
            "kafka_cluster_id": "kraft-a-001",
            "quota_type": quota_type,
            "quota_scope": quota_scope,
            "user": user,
            "client_id": client_id,
        },
        source_series=source_series,
    )


def _series(
    value: float,
    *,
    quota_type: str = "Produce",
    quota_scope: str = "user",
    user: str = "alice",
    client_id: str = "not_applicable",
    interval: timedelta = timedelta(seconds=5),
    end: datetime = _END,
    broker: str = "1",
    source_series: tuple[tuple[str, str], ...] | None = None,
) -> list[MetricRow]:
    timestamp = _START
    rows: list[MetricRow] = []
    while timestamp <= end:
        rows.append(
            _row(
                timestamp,
                value,
                quota_type=quota_type,
                quota_scope=quota_scope,
                user=user,
                client_id=client_id,
                broker=broker,
                source_series=source_series,
            )
        )
        timestamp += interval
    return rows


def _evaluate(
    rows: list[MetricRow],
    *,
    direction: Literal["ingress", "egress"] = "ingress",
    start: datetime = _START,
    end: datetime = _END,
    scrape_interval: timedelta = timedelta(seconds=5),
    max_gap: timedelta = timedelta(seconds=10),
    teams: dict[str, str] | None = None,
) -> PrincipalDirectionEvaluation:
    return evaluate_quota_direction(
        rows,
        direction=direction,
        start=start,
        end=end,
        scrape_interval=scrape_interval,
        max_gap=max_gap,
        principal_to_team=teams or {},
        default_team="UNASSIGNED",
    )


def _weights(evaluation: PrincipalDirectionEvaluation) -> dict[str, Decimal]:
    return {weight.identity_id: weight.weight for weight in evaluation.user_weights}


def test_evaluator_aggregates_user_and_user_client_scopes_with_case_sensitive_owner_mapping() -> None:
    evaluation = _evaluate(
        _series(3.0, user="alice")
        + _series(1.0, quota_scope="user-client", user="alice", client_id="producer-a")
        + _series(2.0, user="Alice")
        + _series(1.0, quota_scope="client-id", user="not_applicable", client_id="anonymous"),
        teams={"User:alice": "team-data"},
    )

    assert evaluation.state.value == "degraded"
    assert _weights(evaluation) == {"User:Alice": Decimal("40"), "User:alice": Decimal("80")}
    assert {weight.identity_id: weight.team for weight in evaluation.user_weights} == {
        "User:Alice": "UNASSIGNED",
        "User:alice": "team-data",
    }
    assert evaluation.client_only_weight == Decimal("20")
    assert evaluation.total_weight == Decimal("140")


def test_evaluator_does_not_assign_client_only_activity_to_a_user() -> None:
    evaluation = _evaluate(_series(2.0, quota_scope="client-id", user="not_applicable", client_id="anonymous"))

    assert evaluation.state.value == "degraded"
    assert evaluation.user_weights == ()
    assert evaluation.client_only_weight == Decimal("40")
    assert evaluation.total_weight == Decimal("40")


def test_evaluator_returns_ready_for_complete_user_evidence_and_zero_usage_for_complete_zero_weight() -> None:
    ready = _evaluate(_series(2.0))
    zero_usage = _evaluate(_series(0.0))

    assert ready.state.value == "ready"
    assert ready.coverage_complete is True
    assert ready.total_weight == Decimal("40")
    assert zero_usage.state.value == "zero_usage"
    assert zero_usage.coverage_complete is True
    assert zero_usage.total_weight == Decimal("0")


@pytest.mark.parametrize(
    "rows",
    [
        [],
        _series(1.0, quota_scope="user", user="not_applicable"),
        _series(-1.0),
        _series(float("inf")),
    ],
    ids=["absent", "malformed-identity", "negative", "non-finite"],
)
def test_evaluator_marks_absent_or_invalid_quota_evidence_unavailable(rows: list[MetricRow]) -> None:
    evaluation = _evaluate(rows)

    assert evaluation.state.value == "unavailable"
    assert evaluation.coverage_complete is False
    assert evaluation.user_weights == ()
    assert evaluation.client_only_weight == Decimal("0")


def test_evaluator_rejects_declared_cadence_mismatch_within_max_gap() -> None:
    evaluation = _evaluate(
        _series(1.0, interval=timedelta(seconds=6), end=_START + timedelta(seconds=18)),
    )

    assert evaluation.state.value == "unavailable"
    assert evaluation.detail == "principal_telemetry_cadence_mismatch"
    assert evaluation.coverage_complete is False


def test_evaluator_distinguishes_cadence_from_max_gap() -> None:
    evaluation = _evaluate(
        _series(1.0, interval=timedelta(seconds=15), end=_END),
    )

    assert evaluation.state.value == "unavailable"
    assert evaluation.detail == "principal_telemetry_incomplete"
    assert evaluation.observed_deltas == (Decimal("15"),)


@pytest.mark.parametrize(
    ("scrape_interval", "max_gap"),
    [
        (timedelta(seconds=5), timedelta(seconds=10)),
        (timedelta(seconds=30), timedelta(seconds=60)),
        (timedelta(seconds=60), timedelta(seconds=120)),
    ],
)
def test_evaluator_accepts_lab_and_sparse_configured_cadences(
    scrape_interval: timedelta,
    max_gap: timedelta,
) -> None:
    end = _START + scrape_interval * 2
    evaluation = _evaluate(
        _series(1.0, interval=scrape_interval, end=end),
        end=end,
        scrape_interval=scrape_interval,
        max_gap=max_gap,
    )

    assert evaluation.state.value == "ready"
    assert evaluation.declared_scrape_interval == scrape_interval
    assert evaluation.observed_deltas == (Decimal(str(scrape_interval.total_seconds())),) * 2


@pytest.mark.parametrize(
    "rows",
    [
        _series(1.0, interval=timedelta(seconds=5), end=_START + timedelta(seconds=5)),
        _series(1.0)[1:],
        [_row(_START - timedelta(seconds=15), 1.0), _row(_END, 1.0)],
    ],
    ids=["stale-trailing", "missing-leading-guard", "stale-leading-guard"],
)
def test_evaluator_rejects_missing_or_stale_boundary_coverage(rows: list[MetricRow]) -> None:
    evaluation = _evaluate(rows)

    assert evaluation.state.value == "unavailable"
    assert evaluation.detail == "principal_telemetry_incomplete"


def test_evaluator_clips_adjacent_windows_without_overlapping_held_duration() -> None:
    rows = [
        _row(_START, 1.0),
        _row(_START + timedelta(seconds=20), 3.0),
        _row(_START + timedelta(seconds=40), 3.0),
    ]
    first = _evaluate(rows, end=_START + timedelta(seconds=20), max_gap=timedelta(seconds=20))
    second = _evaluate(
        rows,
        start=_START + timedelta(seconds=20),
        end=_START + timedelta(seconds=40),
        max_gap=timedelta(seconds=20),
    )

    assert _weights(first) == {"User:alice": Decimal("20")}
    assert _weights(second) == {"User:alice": Decimal("60")}
    assert first.state.value == second.state.value == "ready"


def test_evaluator_rejects_conflicting_duplicate_timestamp_samples() -> None:
    rows = _series(1.0)
    rows.append(_row(_START + timedelta(seconds=5), 2.0))

    evaluation = _evaluate(rows)

    assert evaluation.state.value == "unavailable"
    assert evaluation.user_weights == ()


def test_evaluator_rejects_distinct_prometheus_source_series_for_one_logical_quota_series() -> None:
    first = _series(1.0, end=_START + timedelta(seconds=10), source_series=(("instance", "broker-a"),))
    second = _series(1.0, source_series=(("instance", "broker-b"),))[3:]

    evaluation = _evaluate(first + second)

    assert evaluation.state.value == "unavailable"
    assert evaluation.detail == "principal_telemetry_invalid"


def test_evaluator_rejects_duplicate_source_series_in_a_non_final_logical_group() -> None:
    duplicated_first_group = (
        _series(
            1.0,
            end=_START + timedelta(seconds=10),
            user="alice",
            source_series=(("instance", "broker-a"),),
        )
        + _series(
            1.0,
            user="alice",
            source_series=(("instance", "broker-b"),),
        )[3:]
    )
    final_group = _series(1.0, user="bob", source_series=(("instance", "broker-c"),))

    evaluation = _evaluate(duplicated_first_group + final_group)

    assert evaluation.state.value == "unavailable"
    assert evaluation.detail == "principal_telemetry_invalid"


@pytest.mark.parametrize(
    "rows",
    [
        _series(1.0, quota_scope="user", client_id="client-a"),
        _series(1.0, quota_scope="user-client", user="not_applicable", client_id="client-a"),
        _series(1.0, quota_scope="client-id", user="alice", client_id="client-a"),
        _series(1.0, quota_type="Fetch"),
    ],
    ids=["user-with-client", "user-client-without-user", "client-only-with-user", "wrong-direction-quota-type"],
)
def test_evaluator_rejects_each_invalid_documented_quota_scope_shape(rows: list[MetricRow]) -> None:
    evaluation = _evaluate(rows)

    assert evaluation.state.value == "unavailable"
    assert evaluation.detail == "principal_telemetry_invalid"


def test_money_allocation_rounds_down_users_and_preserves_client_and_rounding_residuals() -> None:
    from plugins.self_managed_kafka.principal_attribution import allocate_principal_money

    evaluation = _evaluate(
        _series(1.0, user="alice")
        + _series(1.0, user="bob")
        + _series(1.0, quota_scope="client-id", user="not_applicable", client_id="anonymous")
    )
    allocation = allocate_principal_money(evaluation, pool=Decimal("1.0000"))

    assert allocation.state.value == "degraded"
    assert [(weight.identity_id, amount) for weight, amount in allocation.user_amounts] == [
        ("User:alice", Decimal("0.3333")),
        ("User:bob", Decimal("0.3333")),
    ]
    assert allocation.client_only_amount == Decimal("0.3333")
    assert allocation.rounding_residual == Decimal("0.0001")
    assert allocation.balance == Decimal("1.0000")


def test_money_allocation_keeps_zero_pool_state_and_fully_unattributes_unavailable_or_zero_usage() -> None:
    from plugins.self_managed_kafka.principal_attribution import allocate_principal_money

    ready_zero_pool = allocate_principal_money(_evaluate(_series(1.0)), pool=Decimal("0.0000"))
    unavailable = allocate_principal_money(_evaluate([]), pool=Decimal("7.5000"))
    zero_usage = allocate_principal_money(_evaluate(_series(0.0)), pool=Decimal("2.0000"))

    assert ready_zero_pool.state.value == "ready"
    assert [amount for _, amount in ready_zero_pool.user_amounts] == [Decimal("0.0000")]
    assert unavailable.user_amounts == ()
    assert unavailable.client_only_amount == Decimal("7.5000")
    assert zero_usage.user_amounts == ()
    assert zero_usage.client_only_amount == Decimal("2.0000")


def test_static_even_policy_sorts_recipients_and_preserves_rounding_residual_or_full_unattributed_pool() -> None:
    from plugins.self_managed_kafka.principal_attribution import allocate_static_even

    allocated = allocate_static_even(
        identities=("Team:zeta", "Team:alpha", "Team:beta"),
        pool=Decimal("1.0000"),
    )
    empty = allocate_static_even(identities=(), pool=Decimal("2.0000"))

    assert allocated.state.value == "policy_only"
    assert [(weight.identity_id, amount) for weight, amount in allocated.user_amounts] == [
        ("Team:alpha", Decimal("0.3333")),
        ("Team:beta", Decimal("0.3333")),
        ("Team:zeta", Decimal("0.3333")),
    ]
    assert allocated.rounding_residual == Decimal("0.0001")
    assert empty.user_amounts == ()
    assert empty.client_only_amount == Decimal("2.0000")

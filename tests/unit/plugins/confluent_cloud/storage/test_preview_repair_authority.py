from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend
from plugins.confluent_cloud.storage.module import CCloudStorageModule

NOW = datetime(2026, 7, 22, 12, tzinfo=UTC)
JULY_1 = datetime(2026, 7, 1, tzinfo=UTC)
JULY_2 = datetime(2026, 7, 2, tzinfo=UTC)
JULY_3 = datetime(2026, 7, 3, tzinfo=UTC)
JULY_4 = datetime(2026, 7, 4, tzinfo=UTC)


@pytest.fixture
def backend(tmp_path: Path):
    value = SQLModelBackend(
        f"sqlite:///{tmp_path / 'repair-authority.db'}",
        CCloudStorageModule(),
        use_migrations=False,
        focus_preview_enabled=True,
    )
    value.create_tables()
    yield value
    value.dispose()


def _attempt(
    backend: object,
    *,
    token: str,
    start: datetime,
    end: datetime,
    status: str = "complete",
    offset: int = 0,
) -> object:
    from core.preview.evidence import (
        SourceAttemptFailureReason,
        SourceAttemptFinalStatus,
    )

    with backend.create_preview_evidence_unit_of_work() as uow:  # type: ignore[attr-defined]
        attempt = uow.source_readiness.begin_attempt(
            "confluent_cloud",
            "tenant-1",
            token,
            start,
            end,
            NOW + timedelta(minutes=offset),
        )
        if status == "pending":
            uow.commit()
            return attempt
        final_status = SourceAttemptFinalStatus(status)
        reason = None
        if final_status is SourceAttemptFinalStatus.FAILED:
            reason = SourceAttemptFailureReason.ATTEMPT_BEGIN_FAILED
        elif final_status is SourceAttemptFinalStatus.ABORTED:
            reason = SourceAttemptFailureReason.GENERIC_GATHER_FAILED
        attempt = uow.source_readiness.finalize_attempt(
            attempt.attempt_sequence,
            final_status,
            completed_at=NOW + timedelta(minutes=offset, seconds=1),
            reason=reason,
        )
        uow.commit()
        return attempt


def _resolve(
    backend: object,
    start: datetime,
    end: datetime,
) -> tuple[object, ...]:
    with backend.create_preview_generation_read_unit_of_work() as uow:  # type: ignore[attr-defined]
        return uow.source_readiness.resolve_authority(
            "confluent_cloud",
            "tenant-1",
            start,
            end,
        )


@pytest.mark.parametrize(
    ("token", "expected"),
    [
        ("repair:repair-1:2026-07-01", "repair"),
        ("repair:repair-1:2026-7-1", "ordinary"),
        ("repair::2026-07-01", "ordinary"),
        ("repair:repair-1:2026-07-01:extra", "ordinary"),
        ("ordinary-token", "ordinary"),
        ("bootstrap:legacy", "ordinary"),
    ],
)
def test_source_attempt_origin_uses_complete_reserved_token_grammar(
    token: str,
    expected: str,
) -> None:
    from core.preview.evidence import source_attempt_origin

    assert source_attempt_origin(token).value == expected


def test_one_day_repair_overlays_broad_ordinary_authority_only_for_its_day(
    backend: object,
) -> None:
    ordinary = _attempt(
        backend,
        token="ordinary-broad",
        start=JULY_1,
        end=JULY_4,
        offset=0,
    )
    repair = _attempt(
        backend,
        token="repair:repair-1:2026-07-02",
        start=JULY_2,
        end=JULY_3,
        offset=1,
    )

    slices = _resolve(backend, JULY_1, JULY_4)

    assert [(item.start, item.end, item.attempt.attempt_sequence) for item in slices] == [  # type: ignore[attr-defined]
        (JULY_1, JULY_2, ordinary.attempt_sequence),  # type: ignore[attr-defined]
        (JULY_2, JULY_3, repair.attempt_sequence),  # type: ignore[attr-defined]
        (JULY_3, JULY_4, ordinary.attempt_sequence),  # type: ignore[attr-defined]
    ]


@pytest.mark.parametrize("status", ["pending", "failed"])
def test_nonterminal_or_failed_repair_blocks_only_its_one_day(
    backend: object,
    status: str,
) -> None:
    ordinary = _attempt(
        backend,
        token="ordinary-broad",
        start=JULY_1,
        end=JULY_4,
    )
    repair = _attempt(
        backend,
        token="repair:repair-1:2026-07-02",
        start=JULY_2,
        end=JULY_3,
        status=status,
        offset=1,
    )

    slices = _resolve(backend, JULY_1, JULY_4)

    assert [item.attempt.attempt_sequence for item in slices] == [  # type: ignore[attr-defined]
        ordinary.attempt_sequence,  # type: ignore[attr-defined]
        repair.attempt_sequence,  # type: ignore[attr-defined]
        ordinary.attempt_sequence,  # type: ignore[attr-defined]
    ]
    assert slices[1].attempt.status.value == status  # type: ignore[attr-defined]


def test_aborted_repair_does_not_change_broad_ordinary_authority(backend: object) -> None:
    ordinary = _attempt(
        backend,
        token="ordinary-broad",
        start=JULY_1,
        end=JULY_4,
    )
    _attempt(
        backend,
        token="repair:repair-1:2026-07-02",
        start=JULY_2,
        end=JULY_3,
        status="aborted",
        offset=1,
    )

    slices = _resolve(backend, JULY_1, JULY_4)

    assert [(item.start, item.end) for item in slices] == [(JULY_1, JULY_4)]  # type: ignore[attr-defined]
    assert slices[0].attempt.attempt_sequence == ordinary.attempt_sequence  # type: ignore[attr-defined]


def test_later_broad_ordinary_attempt_supersedes_prior_repair_everywhere(
    backend: object,
) -> None:
    _attempt(
        backend,
        token="ordinary-old",
        start=JULY_1,
        end=JULY_4,
        offset=0,
    )
    _attempt(
        backend,
        token="repair:repair-1:2026-07-02",
        start=JULY_2,
        end=JULY_3,
        offset=1,
    )
    latest = _attempt(
        backend,
        token="ordinary-new",
        start=JULY_1,
        end=JULY_4,
        offset=2,
    )

    slices = _resolve(backend, JULY_1, JULY_4)

    assert len(slices) == 1
    assert slices[0].attempt.attempt_sequence == latest.attempt_sequence  # type: ignore[attr-defined]


def test_authority_returns_gap_free_slices_in_deterministic_order(backend: object) -> None:
    left = _attempt(
        backend,
        token="ordinary-left",
        start=JULY_1,
        end=JULY_2,
        offset=0,
    )
    right = _attempt(
        backend,
        token="ordinary-right",
        start=JULY_3,
        end=JULY_4,
        offset=1,
    )

    slices = _resolve(backend, JULY_1, JULY_4)

    assert [(item.start, item.end) for item in slices] == [  # type: ignore[attr-defined]
        (JULY_1, JULY_2),
        (JULY_2, JULY_3),
        (JULY_3, JULY_4),
    ]
    assert slices[0].attempt.attempt_sequence == left.attempt_sequence  # type: ignore[attr-defined]
    assert slices[1].attempt is None  # type: ignore[attr-defined]
    assert slices[2].attempt.attempt_sequence == right.attempt_sequence  # type: ignore[attr-defined]


def test_zero_width_selects_newest_ordinary_regardless_of_stored_bounds(
    backend: object,
) -> None:
    _attempt(
        backend,
        token="ordinary-old",
        start=JULY_1,
        end=JULY_2,
        offset=0,
    )
    newest = _attempt(
        backend,
        token="ordinary-new",
        start=JULY_3,
        end=JULY_4,
        status="failed",
        offset=1,
    )

    slices = _resolve(backend, JULY_2, JULY_2)

    assert len(slices) == 1
    assert (slices[0].start, slices[0].end) == (JULY_2, JULY_2)  # type: ignore[attr-defined]
    assert slices[0].attempt.attempt_sequence == newest.attempt_sequence  # type: ignore[attr-defined]
    assert slices[0].attempt.status.value == "failed"  # type: ignore[attr-defined]


def test_zero_width_ignores_newer_nonoverlapping_repair(backend: object) -> None:
    ordinary = _attempt(
        backend,
        token="ordinary",
        start=JULY_1,
        end=JULY_4,
        offset=0,
    )
    _attempt(
        backend,
        token="repair:repair-1:2026-07-03",
        start=JULY_3,
        end=JULY_4,
        offset=1,
    )

    slices = _resolve(backend, JULY_2, JULY_2)

    assert slices[0].attempt.attempt_sequence == ordinary.attempt_sequence  # type: ignore[attr-defined]


def test_zero_width_allows_containing_repair_to_compete_by_sequence(
    backend: object,
) -> None:
    _attempt(
        backend,
        token="ordinary",
        start=JULY_1,
        end=JULY_4,
        offset=0,
    )
    repair = _attempt(
        backend,
        token="repair:repair-1:2026-07-02",
        start=JULY_2,
        end=JULY_3,
        offset=1,
    )

    slices = _resolve(backend, JULY_2, JULY_2)

    assert slices[0].attempt.attempt_sequence == repair.attempt_sequence  # type: ignore[attr-defined]

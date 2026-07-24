from __future__ import annotations

import pytest
from pydantic import ValidationError

from core.config.models import PreviewConfig


def test_preview_capacity_defaults_are_finite_and_match_the_documented_safe_values() -> None:
    config = PreviewConfig()

    assert config.max_workers == 2
    assert config.max_queued_generations == 8
    assert config.max_running_generations_per_tenant == 1
    assert config.max_queued_generations_per_tenant == 2
    assert config.max_generation_spool_bytes == 2_147_483_648


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("max_workers", True),
        ("max_queued_generations", False),
        ("max_running_generations_per_tenant", True),
        ("max_queued_generations_per_tenant", False),
        ("max_generation_spool_bytes", True),
        ("max_csv_file_bytes", False),
    ],
)
def test_preview_capacity_integer_fields_reject_booleans(field: str, value: bool) -> None:
    with pytest.raises(ValidationError):
        PreviewConfig.model_validate({field: value})


def test_preview_capacity_accepts_zero_queue_only_for_both_limits() -> None:
    config = PreviewConfig(
        max_queued_generations=0,
        max_queued_generations_per_tenant=0,
    )

    assert config.max_queued_generations == 0
    assert config.max_queued_generations_per_tenant == 0


@pytest.mark.parametrize(
    ("global_limit", "tenant_limit"),
    [
        (0, 1),
        (1, 0),
        (1, 1),
        (1, 2),
        (8, 8),
        (8, 9),
    ],
)
def test_preview_capacity_rejects_mixed_zero_equal_and_tenant_greater_queue_limits(
    global_limit: int,
    tenant_limit: int,
) -> None:
    with pytest.raises(ValidationError):
        PreviewConfig(
            max_queued_generations=global_limit,
            max_queued_generations_per_tenant=tenant_limit,
        )


def test_preview_capacity_accepts_smallest_positive_queue_pair() -> None:
    config = PreviewConfig(
        max_queued_generations=2,
        max_queued_generations_per_tenant=1,
    )

    assert config.max_queued_generations == 2
    assert config.max_queued_generations_per_tenant == 1


def test_preview_capacity_rejects_tenant_running_limit_above_global_workers() -> None:
    with pytest.raises(ValidationError):
        PreviewConfig(max_workers=1, max_running_generations_per_tenant=2)


@pytest.mark.parametrize("value", [0, -1])
def test_preview_spool_limit_must_be_positive(value: int) -> None:
    with pytest.raises(ValidationError):
        PreviewConfig(max_generation_spool_bytes=value)

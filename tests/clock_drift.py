from __future__ import annotations

from collections.abc import Iterable
from datetime import UTC, datetime, tzinfo
from functools import cache
from importlib import import_module
from typing import Final

import pytest

_RUNTIME_CLOCK_MODULES: Final[tuple[str, ...]] = (
    "workflow_runner",
    "core.engine.orchestrator",
    "core.preview.service",
    "core.api.routes.focus_preview",
)

_AFFECTED_TEST_MODULES: Final[tuple[str, ...]] = (
    "tests.integration.core.api.test_focus_preview_capacity",
    "tests.integration.core.api.test_focus_preview",
    "tests.integration.core.api.test_focus_preview_allocation_lineage",
    "tests.integration.core.api.test_focus_preview_historical_repair_upgrade",
    "tests.integration.core.api.test_focus_preview_monthly_cutoff_v5",
    "tests.integration.core.api.test_focus_preview_pipeline",
    "tests.integration.test_pipeline_integration",
)

_TARGET_NODEID_PREFIXES: Final[tuple[str, ...]] = (
    "tests/integration/core/api/test_focus_preview_capacity.py::test_scheduled_publication_defers_under_real_scheduler_saturation_then_reacquires_backend",
    "tests/integration/core/api/test_focus_preview.py::test_real_startup_cleans_staging_and_fails_strictly_older_pending_rows",
    "tests/integration/core/api/test_focus_preview.py::test_transient_startup_recovery_failure_blocks_then_later_route_retries",
    "tests/integration/core/api/test_focus_preview.py::test_real_lifespan_isolates_recovery_for_distinct_sqlite_backends_with_shared_provider_id",
    "tests/integration/core/api/test_focus_preview_allocation_lineage.py::test_real_production_lineage_projects_multiple_origins_actual_portions_and_frozen_separate_tags",
    "tests/integration/core/api/test_focus_preview_historical_repair_upgrade.py::test_v210_retained_month_fails_then_production_rest_repair_enables_daily_and_monthly_preview",
    "tests/integration/core/api/test_focus_preview_monthly_cutoff_v5.py::test_settled_monthly_positive_sources_use_real_calculate_lineage_and_persist_exact_package",
    "tests/integration/core/api/test_focus_preview_pipeline.py::test_same_key_tiers_flow_from_provider_capture_through_sidecar_and_canonical_daily_monthly_api",
    "tests/integration/core/api/test_focus_preview_pipeline.py::test_real_calculate_unknown_allocator_publishes_daily_and_monthly_known_plus_fallback",
    "tests/integration/core/api/test_focus_preview_pipeline.py::test_real_bundle_known_native_lines_retain_current_mapping_and_context_behavior",
    "tests/integration/core/api/test_focus_preview_pipeline.py::test_production_lineage_integrity_error_logs_safe_owner_context_and_persists_unavailable_fallback",
    "tests/integration/test_pipeline_integration.py::TestEndToEndPipeline::test_full_gather_calculate",
)


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--clock-drift-now",
        action="store",
        default=None,
        help="Shift selected runtime/test module datetime.now() calls to the supplied ISO-8601 anchor.",
    )


@cache
def _parse_anchor(raw_value: str) -> datetime:
    normalized_value = f"{raw_value[:-1]}+00:00" if raw_value.endswith("Z") else raw_value
    anchor = datetime.fromisoformat(normalized_value)
    if anchor.tzinfo is None:
        raise pytest.UsageError("--clock-drift-now requires a timezone-aware ISO-8601 timestamp")
    return anchor.astimezone(UTC)


def _should_shift(nodeid: str) -> bool:
    return any(nodeid.startswith(prefix) for prefix in _TARGET_NODEID_PREFIXES)


def _shifted_datetime_type(offset_seconds: float) -> type[datetime]:
    class _ShiftedDateTimeMeta(type):
        def __instancecheck__(cls, instance: object) -> bool:
            del cls
            return isinstance(instance, datetime)

    class _ShiftedDateTime(datetime, metaclass=_ShiftedDateTimeMeta):
        def __new__(
            cls,
            year: int,
            month: int,
            day: int,
            hour: int = 0,
            minute: int = 0,
            second: int = 0,
            microsecond: int = 0,
            tzinfo: tzinfo | None = None,
            *,
            fold: int = 0,
        ) -> datetime:
            del cls
            return datetime(
                year,
                month,
                day,
                hour,
                minute,
                second,
                microsecond,
                tzinfo,
                fold=fold,
            )

        @classmethod
        def now(cls, tz: tzinfo | None = None) -> datetime:
            shifted = datetime.now(UTC).timestamp() + offset_seconds
            current = datetime.fromtimestamp(shifted, UTC)
            if tz is None:
                return current.replace(tzinfo=None)
            return current.astimezone(tz)

        @classmethod
        def utcnow(cls) -> datetime:
            shifted = datetime.now(UTC).timestamp() + offset_seconds
            return datetime.fromtimestamp(shifted, UTC).replace(tzinfo=None)

    return _ShiftedDateTime


def _patch_datetime_symbol(
    monkeypatch: pytest.MonkeyPatch,
    *,
    module_name: str,
    shifted_datetime: type[datetime],
) -> None:
    module = import_module(module_name)
    if hasattr(module, "datetime"):
        monkeypatch.setattr(module, "datetime", shifted_datetime)


def _patch_target_modules(
    monkeypatch: pytest.MonkeyPatch,
    *,
    shifted_datetime: type[datetime],
    module_names: Iterable[str],
) -> None:
    for module_name in module_names:
        _patch_datetime_symbol(
            monkeypatch,
            module_name=module_name,
            shifted_datetime=shifted_datetime,
        )


@pytest.fixture(autouse=True)
def _clock_drift(
    request: pytest.FixtureRequest,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    anchor_value = request.config.getoption("--clock-drift-now")
    if anchor_value is None or not _should_shift(request.node.nodeid):
        return

    anchor = _parse_anchor(anchor_value)
    offset_seconds = (anchor - datetime.now(UTC)).total_seconds()
    shifted_datetime = _shifted_datetime_type(offset_seconds)
    module_names = (
        *_RUNTIME_CLOCK_MODULES,
        *_AFFECTED_TEST_MODULES,
    )
    _patch_target_modules(
        monkeypatch,
        shifted_datetime=shifted_datetime,
        module_names=module_names,
    )

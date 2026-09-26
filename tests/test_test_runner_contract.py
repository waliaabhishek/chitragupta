from __future__ import annotations

import subprocess
from pathlib import Path

import yaml

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def test_make_test_provisions_docs_and_supports_serial_override() -> None:
    for make_args, workers in (([], "8"), (["PYTEST_WORKERS=0"], "0")):
        result = subprocess.run(
            ["make", "--dry-run", "test", *make_args],
            capture_output=True,
            check=True,
            cwd=PROJECT_ROOT,
            text=True,
            timeout=10,
        )
        commands = result.stdout.splitlines()
        sync = "uv sync --frozen --group docs"
        pytest = f"UV_NO_SYNC=1 uv run pytest -n {workers} --cov=src --cov-report=term-missing"
        assert commands.index(sync) < commands.index(pytest)


def test_ci_preserves_preflight_and_full_suite_coverage_contract() -> None:
    workflow = yaml.safe_load((PROJECT_ROOT / ".github/workflows/ci.yml").read_text())
    steps = workflow["jobs"]["quality"]["steps"]
    by_name = {step["name"]: (index, step) for index, step in enumerate(steps) if "name" in step}

    docs_index, docs_step = by_name["Install documentation test dependencies"]
    coverage_index, coverage_step = by_name["Test with coverage"]
    assert coverage_index == docs_index + 1
    assert docs_step["run"] == "uv sync --frozen --group docs"
    assert coverage_step["run"] == "UV_NO_SYNC=1 uv run pytest -n 4 --cov=src --cov-fail-under=75 --cov-report=xml"

    encoder_index, encoder_step = by_name["Check media encoder"]
    clock_index, clock_step = by_name["Test affected backend clock drift"]
    assert encoder_index < docs_index
    assert encoder_step["run"] == "uv run pytest -x tests/unit/scripts/test_demo_media.py -k test_encoder_"
    assert clock_index < docs_index
    for anchor in ("2035-06-15T12:00:00+00:00", "2035-01-01T00:00:00+00:00"):
        assert anchor in clock_step["run"]

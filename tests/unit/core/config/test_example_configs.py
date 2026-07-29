from __future__ import annotations

import csv
import hashlib
import json
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pytest

from core.config.loader import load_config
from core.preview.capability import FOCUS_PREVIEW_CAPABILITY
from core.preview.mapping import (
    FOCUS_1_4_FULL_PROFILE_COLUMNS,
    MAPPING_PROFILE_VERSION,
    PREVIEW_MANIFEST_SCHEMA_VERSION,
    PROFILE_NOT_APPLICABLE_COLUMNS,
    preview_manifest_known_gaps,
)

EXAMPLES_DIR = Path(__file__).parents[4] / "examples"

CCLOUD_ENV = {
    "CCLOUD_TENANT_ID": "ccloud-test",
    "CCLOUD_API_KEY": "TESTKEY",
    "CCLOUD_API_SECRET": "testsecret",
}
PROM_ENV = {"PROMETHEUS_URL": "http://localhost:9090"}


def _load(config_path: Path, env: dict[str, str], monkeypatch: pytest.MonkeyPatch) -> None:
    for k, v in env.items():
        monkeypatch.setenv(k, v)
    config = load_config(config_path)
    assert config.tenants, f"{config_path}: expected at least one tenant"


class TestCCloudExamples:
    def test_ccloud_grafana(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _load(EXAMPLES_DIR / "ccloud-grafana" / "config.yaml", CCLOUD_ENV, monkeypatch)

    def test_ccloud_full(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _load(EXAMPLES_DIR / "ccloud-full" / "config.yaml", CCLOUD_ENV, monkeypatch)

    def test_ccloud_full_documents_optional_focus_preview_contract(self) -> None:
        source = (EXAMPLES_DIR / "ccloud-full" / "config.yaml").read_text(encoding="utf-8")

        assert "focus_preview:" in source
        assert "commercial_profile: direct_payg" in source
        assert "billing_currency: USD" in source
        assert "effective_start_date:" in source
        assert "effective_end_date:" in source
        assert "lookback_days is not retention" in source

    def test_focus_preview_walkthrough(self, monkeypatch: pytest.MonkeyPatch) -> None:
        example = EXAMPLES_DIR / "focus-preview"
        _load(example / "config.yaml", CCLOUD_ENV, monkeypatch)

        request = json.loads((example / "request.json").read_text(encoding="utf-8"))
        manifest = json.loads((example / "sample-output" / "manifest.json").read_text(encoding="utf-8"))
        csv_body = (example / "sample-output" / "cost-and-usage.csv").read_bytes()
        csv_rows = list(csv.reader(csv_body.decode().splitlines()))

        assert request["column_profile"] == manifest["column_profile"] == "custom"
        assert request["grain"] == manifest["grain"] == "daily"
        assert request["start_date"] == manifest["start_date"]
        assert request["end_date"] == manifest["end_date"]
        assert tuple(request["columns"]) == tuple(manifest["effective_columns"]) == tuple(csv_rows[0])
        assert all(column in FOCUS_1_4_FULL_PROFILE_COLUMNS for column in request["columns"])
        assert len(csv_rows) == 2

        assert manifest["schema_version"] == PREVIEW_MANIFEST_SCHEMA_VERSION
        assert manifest["mapping_profile_version"] == MAPPING_PROFILE_VERSION
        assert manifest["target_focus_version"] == FOCUS_PREVIEW_CAPABILITY.target_focus_version
        assert manifest["conformance_status"] == FOCUS_PREVIEW_CAPABILITY.conformance_status
        assert manifest["known_gaps"] == preview_manifest_known_gaps()
        assert manifest["profile_not_applicable_columns"] == list(PROFILE_NOT_APPLICABLE_COLUMNS)
        assert manifest["reconciliation"]["difference"] == "0"
        assert manifest["reconciliation"]["quantity_difference"] == "0"

        [csv_metadata] = manifest["files"]
        assert csv_metadata["name"] == "cost-and-usage.csv"
        assert csv_metadata["size_bytes"] == len(csv_body)
        assert csv_metadata["sha256"] == hashlib.sha256(csv_body).hexdigest()


def test_ccloud_reference_documents_currency_authority_and_retention_limit() -> None:
    source = (Path(__file__).parents[4] / "docs" / "configuration" / "ccloud-reference.md").read_text(encoding="utf-8")
    normalized = " ".join(source.split())

    assert "focus_preview.commercial_profile" in source
    assert "focus_preview.billing_currency" in source
    assert "USD" in source
    assert "does not return" in source and "ISO currency" in source
    assert "no currency conversion" in source.casefold()
    assert "explicit customer/operator contract" in source
    assert "not provider-supplied record evidence" in source
    assert "Compatibility aggregate currency is not treated as commercial authority" in normalized
    assert "lookback_days" in source and "not retention" in source
    assert "`retention_days` is separate" in source
    assert "TASK-" not in source


class TestSelfManagedExamples:
    def test_self_managed_full(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _load(EXAMPLES_DIR / "self-managed-full" / "config.yaml", PROM_ENV, monkeypatch)

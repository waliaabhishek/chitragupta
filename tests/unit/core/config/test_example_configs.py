from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pytest

from core.config.loader import load_config

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


class TestSelfManagedExamples:
    def test_self_managed_full(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _load(EXAMPLES_DIR / "self-managed-full" / "config.yaml", PROM_ENV, monkeypatch)

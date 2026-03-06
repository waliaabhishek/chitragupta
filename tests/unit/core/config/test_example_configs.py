from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pytest

from core.config.loader import load_config

EXAMPLES_DIR = Path(__file__).parents[4] / "deployables" / "config" / "examples"

CCLOUD_ENV = {
    "CCLOUD_ORG_ID": "org-test123",
    "CCLOUD_API_KEY": "TESTKEY",
    "CCLOUD_API_SECRET": "testsecret",
}
PROM_ENV = {"PROMETHEUS_URL": "http://localhost:9090"}


def _load(filename: str, env: dict[str, str], monkeypatch: pytest.MonkeyPatch) -> None:
    for k, v in env.items():
        monkeypatch.setenv(k, v)
    config = load_config(EXAMPLES_DIR / filename)
    assert config.tenants, f"{filename}: expected at least one tenant"


class TestCCloudExamples:
    def test_minimal(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _load("ccloud-minimal.yaml", CCLOUD_ENV, monkeypatch)

    def test_complete(self, monkeypatch: pytest.MonkeyPatch) -> None:
        env = {
            **CCLOUD_ENV,
            **PROM_ENV,
            "PROM_USERNAME": "admin",
            "PROM_PASSWORD": "pw",
        }
        _load("ccloud-complete.yaml", env, monkeypatch)

    def test_multi_tenant(self, monkeypatch: pytest.MonkeyPatch) -> None:
        env = {
            "CCLOUD_PROD_ORG_ID": "org-prod",
            "CCLOUD_PROD_API_KEY": "KEY1",
            "CCLOUD_PROD_API_SECRET": "SEC1",
            "CCLOUD_STAGING_ORG_ID": "org-staging",
            "CCLOUD_STAGING_API_KEY": "KEY2",
            "CCLOUD_STAGING_API_SECRET": "SEC2",
        }
        _load("ccloud-multi-tenant.yaml", env, monkeypatch)

    def test_with_flink(self, monkeypatch: pytest.MonkeyPatch) -> None:
        env = {
            **CCLOUD_ENV,
            "FLINK_US_KEY": "FUSKEY",
            "FLINK_US_SECRET": "fussec",
            "FLINK_EU_KEY": "FEUKEY",
            "FLINK_EU_SECRET": "feusec",
        }
        _load("ccloud-with-flink.yaml", env, monkeypatch)


class TestSelfManagedExamples:
    def test_minimal(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _load("self-managed-minimal.yaml", PROM_ENV, monkeypatch)

    def test_complete(self, monkeypatch: pytest.MonkeyPatch) -> None:
        env = {
            **PROM_ENV,
            "PROM_USERNAME": "admin",
            "PROM_PASSWORD": "pw",
            "KAFKA_SASL_USERNAME": "sa",
            "KAFKA_SASL_PASSWORD": "pass",
        }
        _load("self-managed-complete.yaml", env, monkeypatch)


class TestGenericExamples:
    def test_postgres(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _load("generic-postgres.yaml", PROM_ENV, monkeypatch)

    def test_redis(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _load("generic-redis.yaml", PROM_ENV, monkeypatch)

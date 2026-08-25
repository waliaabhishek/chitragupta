from __future__ import annotations

import textwrap
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from core.config.loader import load_config

if TYPE_CHECKING:
    from core.config.models import AppSettings

SELF_MANAGED_FULL = Path(__file__).parents[1] / "examples" / "self-managed-full" / "config.yaml"

PROM_ENV = {"PROMETHEUS_URL": "http://localhost:9090"}


# ── Fixtures ──────────────────────────────────────────────────────────────────


@pytest.fixture
def self_managed_full_cfg(monkeypatch: pytest.MonkeyPatch) -> AppSettings:
    for k, v in PROM_ENV.items():
        monkeypatch.setenv(k, v)
    return load_config(SELF_MANAGED_FULL)


# ── 1. Verify override fields default to empty dicts ─────────────────────────


class TestSelfManagedFullOverrideFields:
    def test_allocator_overrides_is_empty_dict(self, self_managed_full_cfg: AppSettings) -> None:
        ps = self_managed_full_cfg.tenants["kafka-dc1"].plugin_settings
        assert ps.allocator_overrides == {}

    def test_identity_resolution_overrides_is_empty_dict(self, self_managed_full_cfg: AppSettings) -> None:
        ps = self_managed_full_cfg.tenants["kafka-dc1"].plugin_settings
        assert ps.identity_resolution_overrides == {}

    def test_allocator_params_is_empty_dict(self, self_managed_full_cfg: AppSettings) -> None:
        ps = self_managed_full_cfg.tenants["kafka-dc1"].plugin_settings
        assert ps.allocator_params == {}


# ── 2. allocator_params accepts int and bool values (type-system check) ────────


def test_allocator_params_int_and_bool_parse(tmp_path: Path) -> None:
    cfg_file = tmp_path / "test.yaml"
    cfg_file.write_text(
        textwrap.dedent("""\
            tenants:
              t1:
                ecosystem: generic_metrics_only
                tenant_id: t1
                plugin_settings:
                  ecosystem_name: test-eco
                  cluster_id: test-cluster
                  metrics:
                    type: prometheus
                    url: "http://localhost:9090"
                  cost_types:
                    - name: COMPUTE
                      product_category: compute
                      rate: "1.00"
                      cost_quantity:
                        type: fixed
                        count: 1
                      allocation_strategy: even_split
                  allocator_params:
                    min_bytes_threshold: 1073741824
                    fallback_to_even_split: true
        """)
    )
    cfg = load_config(cfg_file)
    params = cfg.tenants["t1"].plugin_settings.allocator_params
    assert params["min_bytes_threshold"] == 1073741824
    assert params["fallback_to_even_split"] is True


class TestSelfManagedKafkaTelemetryLabIsolation:
    LAB_DIR = Path(__file__).parents[1] / "examples" / "self-managed-kafka-telemetry-lab"

    def test_self_managed_full_config_does_not_reference_lab_generated_state(self) -> None:
        source = SELF_MANAGED_FULL.read_text(encoding="utf-8")

        for unexpected_reference in (
            "self-managed-kafka-telemetry-lab",
            "generated/",
            "evidence/",
            "client.properties",
            ".jaas",
        ):
            assert unexpected_reference not in source

    def test_lab_gitignore_keeps_generated_credentials_and_evidence_local(self) -> None:
        gitignore_path = self.LAB_DIR / ".gitignore"
        assert gitignore_path.exists(), f"Expected {gitignore_path} to exist"

        source = gitignore_path.read_text(encoding="utf-8")

        for ignored_path in (".env", "generated/", "evidence/"):
            assert ignored_path in source

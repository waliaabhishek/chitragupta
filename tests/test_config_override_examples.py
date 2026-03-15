from __future__ import annotations

import textwrap
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from core.config.loader import load_config

if TYPE_CHECKING:
    from core.config.models import AppSettings

EXAMPLES_DIR = Path(__file__).parents[1] / "deployables" / "config" / "examples"

PROM_ENV = {"PROMETHEUS_URL": "http://localhost:9090"}
SELF_MANAGED_ENV = {
    **PROM_ENV,
    "PROM_USERNAME": "admin",
    "PROM_PASSWORD": "pw",  # pragma: allowlist secret
    "KAFKA_SASL_USERNAME": "sa",
    "KAFKA_SASL_PASSWORD": "pass",  # pragma: allowlist secret
}


# ── Fixtures ──────────────────────────────────────────────────────────────────


@pytest.fixture
def self_managed_complete_cfg(monkeypatch: pytest.MonkeyPatch) -> AppSettings:
    for k, v in SELF_MANAGED_ENV.items():
        monkeypatch.setenv(k, v)
    return load_config(EXAMPLES_DIR / "self-managed-complete.yaml")


@pytest.fixture
def generic_postgres_cfg(monkeypatch: pytest.MonkeyPatch) -> AppSettings:
    monkeypatch.setenv("PROMETHEUS_URL", "http://localhost:9090")
    return load_config(EXAMPLES_DIR / "generic-postgres.yaml")


@pytest.fixture
def generic_redis_cfg(monkeypatch: pytest.MonkeyPatch) -> AppSettings:
    monkeypatch.setenv("PROMETHEUS_URL", "http://localhost:9090")
    return load_config(EXAMPLES_DIR / "generic-redis.yaml")


# ── 1. Load each YAML and verify override fields are empty dicts ───────────────


class TestSelfManagedCompleteOverrideFields:
    def test_allocator_overrides_is_empty_dict(self, self_managed_complete_cfg: AppSettings) -> None:
        ps = self_managed_complete_cfg.tenants["kafka-dc1"].plugin_settings
        assert ps.allocator_overrides == {}

    def test_identity_resolution_overrides_is_empty_dict(self, self_managed_complete_cfg: AppSettings) -> None:
        ps = self_managed_complete_cfg.tenants["kafka-dc1"].plugin_settings
        assert ps.identity_resolution_overrides == {}

    def test_allocator_params_is_empty_dict(self, self_managed_complete_cfg: AppSettings) -> None:
        ps = self_managed_complete_cfg.tenants["kafka-dc1"].plugin_settings
        assert ps.allocator_params == {}


class TestGenericPostgresOverrideFields:
    def test_allocator_overrides_is_empty_dict(self, generic_postgres_cfg: AppSettings) -> None:
        ps = generic_postgres_cfg.tenants["postgres-prod"].plugin_settings
        assert ps.allocator_overrides == {}

    def test_identity_resolution_overrides_is_empty_dict(self, generic_postgres_cfg: AppSettings) -> None:
        ps = generic_postgres_cfg.tenants["postgres-prod"].plugin_settings
        assert ps.identity_resolution_overrides == {}

    def test_allocator_params_is_empty_dict(self, generic_postgres_cfg: AppSettings) -> None:
        ps = generic_postgres_cfg.tenants["postgres-prod"].plugin_settings
        assert ps.allocator_params == {}


class TestGenericRedisOverrideFields:
    def test_allocator_overrides_is_empty_dict(self, generic_redis_cfg: AppSettings) -> None:
        ps = generic_redis_cfg.tenants["redis-prod"].plugin_settings
        assert ps.allocator_overrides == {}

    def test_identity_resolution_overrides_is_empty_dict(self, generic_redis_cfg: AppSettings) -> None:
        ps = generic_redis_cfg.tenants["redis-prod"].plugin_settings
        assert ps.identity_resolution_overrides == {}

    def test_allocator_params_is_empty_dict(self, generic_redis_cfg: AppSettings) -> None:
        ps = generic_redis_cfg.tenants["redis-prod"].plugin_settings
        assert ps.allocator_params == {}


# ── 2. Active empty-dict lines must be present in YAML source ─────────────────
#
# These fail until the YAML files are edited to include the three override fields.


@pytest.mark.parametrize(
    "filename,active_lines",
    [
        (
            "self-managed-complete.yaml",
            [
                "allocator_params: {}",
                "allocator_overrides: {}",
                "identity_resolution_overrides: {}",
            ],
        ),
        (
            "generic-postgres.yaml",
            [
                "allocator_params: {}",
                "allocator_overrides: {}",
                "identity_resolution_overrides: {}",
            ],
        ),
        (
            "generic-redis.yaml",
            [
                "allocator_params: {}",
                "allocator_overrides: {}",
                "identity_resolution_overrides: {}",
            ],
        ),
    ],
)
def test_override_active_values_present_in_yaml(filename: str, active_lines: list[str]) -> None:
    content = (EXAMPLES_DIR / filename).read_text()
    for line in active_lines:
        assert line in content, f"{filename!r} is missing active YAML line: {line!r}"


# ── 3. Commented examples must be present in YAML source ─────────────────────
#
# These fail until the YAML files are edited to include commented-out examples.


@pytest.mark.parametrize(
    "filename,expected_comments",
    [
        (
            "self-managed-complete.yaml",
            [
                "# allocator_params:",
                "# allocator_overrides:",
                "# identity_resolution_overrides:",
                "#   min_bytes_threshold:",
                "#   fallback_to_even_split:",
            ],
        ),
        (
            "generic-postgres.yaml",
            [
                "# --- Orchestration ---",
                "# allocator_params:",
                "# allocator_overrides:",
                "# identity_resolution_overrides:",
                "#   min_connections_threshold:",
                "#   weight_storage_by_size:",
            ],
        ),
        (
            "generic-redis.yaml",
            [
                "# --- Orchestration ---",
                "# allocator_params:",
                "# allocator_overrides:",
                "# identity_resolution_overrides:",
                "#   memory_smoothing_window:",
                "#   exclude_zero_usage:",
            ],
        ),
    ],
)
def test_override_comment_examples_present(filename: str, expected_comments: list[str]) -> None:
    content = (EXAMPLES_DIR / filename).read_text()
    for comment in expected_comments:
        assert comment in content, f"{filename!r} is missing comment: {comment!r}"


# ── 4. allocator_params accepts int and bool values (type-system check) ────────


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

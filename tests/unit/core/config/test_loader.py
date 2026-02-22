from __future__ import annotations

from pathlib import Path

import pytest

from core.config.loader import load_config, substitute_env_vars


class TestSubstituteEnvVars:
    def test_simple_var_resolved(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("MY_VAR", "hello")
        assert substitute_env_vars("${MY_VAR}") == "hello"

    def test_default_when_var_missing(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("MISSING", raising=False)
        assert substitute_env_vars("${MISSING:-fallback}") == "fallback"

    def test_default_ignored_when_var_present(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("PRESENT", "real")
        assert substitute_env_vars("${PRESENT:-fallback}") == "real"

    def test_missing_var_without_default_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("GONE", raising=False)
        with pytest.raises(ValueError, match="GONE"):
            substitute_env_vars("${GONE}")

    def test_nested_dict(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("A", "1")
        data = {"outer": {"inner": "${A}"}}
        result = substitute_env_vars(data)
        assert result == {"outer": {"inner": "1"}}

    def test_list(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("X", "val")
        assert substitute_env_vars(["${X}", "plain"]) == ["val", "plain"]

    def test_non_string_passthrough(self) -> None:
        assert substitute_env_vars(42) == 42
        assert substitute_env_vars(True) is True
        assert substitute_env_vars(None) is None
        assert substitute_env_vars(3.14) == 3.14

    def test_multiple_vars_in_one_string(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("HOST", "localhost")
        monkeypatch.setenv("PORT", "5432")
        assert substitute_env_vars("${HOST}:${PORT}") == "localhost:5432"

    def test_partial_substitution(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("NAME", "world")
        assert substitute_env_vars("hello-${NAME}-end") == "hello-world-end"

    def test_empty_default(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("EMPTY_DEF", raising=False)
        assert substitute_env_vars("${EMPTY_DEF:-}") == ""


class TestLoadConfig:
    def test_valid_yaml(self, tmp_path: Path) -> None:
        cfg_file = tmp_path / "config.yaml"
        cfg_file.write_text("tenants:\n  t1:\n    ecosystem: cc\n    tenant_id: id1\n")
        settings = load_config(cfg_file)
        assert settings.tenants["t1"].ecosystem == "cc"

    def test_missing_file_raises(self, tmp_path: Path) -> None:
        with pytest.raises(FileNotFoundError):
            load_config(tmp_path / "nonexistent.yaml")

    def test_malformed_yaml_raises(self, tmp_path: Path) -> None:
        cfg_file = tmp_path / "bad.yaml"
        cfg_file.write_text(":\n  - :\n  [invalid")
        with pytest.raises(ValueError, match="Malformed YAML"):
            load_config(cfg_file)

    def test_env_vars_substituted(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("TEST_ECO", "kafka")
        cfg_file = tmp_path / "config.yaml"
        cfg_file.write_text("tenants:\n  t1:\n    ecosystem: ${TEST_ECO}\n    tenant_id: id1\n")
        settings = load_config(cfg_file)
        assert settings.tenants["t1"].ecosystem == "kafka"

    def test_dotenv_loaded(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("DOT_VAR", raising=False)
        env_file = tmp_path / "custom.env"
        env_file.write_text("DOT_VAR=from_dotenv\n")
        cfg_file = tmp_path / "config.yaml"
        cfg_file.write_text("tenants:\n  t1:\n    ecosystem: ${DOT_VAR}\n    tenant_id: id1\n")
        settings = load_config(cfg_file, env_file=env_file)
        assert settings.tenants["t1"].ecosystem == "from_dotenv"

    def test_os_env_overrides_dotenv(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("OVER_VAR", "from_os")
        env_file = tmp_path / "custom.env"
        env_file.write_text("OVER_VAR=from_dotenv\n")
        cfg_file = tmp_path / "config.yaml"
        cfg_file.write_text("tenants:\n  t1:\n    ecosystem: ${OVER_VAR}\n    tenant_id: id1\n")
        settings = load_config(cfg_file, env_file=env_file)
        assert settings.tenants["t1"].ecosystem == "from_os"

    def test_dotenv_auto_discovery(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("AUTO_VAR", raising=False)
        (tmp_path / ".env").write_text("AUTO_VAR=discovered\n")
        cfg_file = tmp_path / "config.yaml"
        cfg_file.write_text("tenants:\n  t1:\n    ecosystem: ${AUTO_VAR}\n    tenant_id: id1\n")
        settings = load_config(cfg_file)
        assert settings.tenants["t1"].ecosystem == "discovered"

    def test_empty_yaml_returns_defaults(self, tmp_path: Path) -> None:
        cfg_file = tmp_path / "empty.yaml"
        cfg_file.write_text("")
        settings = load_config(cfg_file)
        assert settings.tenants == {}
        assert settings.logging.level == "INFO"

    def test_only_tenants_section(self, tmp_path: Path) -> None:
        cfg_file = tmp_path / "partial.yaml"
        cfg_file.write_text("tenants:\n  t1:\n    ecosystem: cc\n    tenant_id: id1\n")
        settings = load_config(cfg_file)
        assert settings.api.port == 8080
        assert settings.logging.level == "INFO"

    def test_port_coerced_from_env_var(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("PORT_VAL", "9999")
        cfg_file = tmp_path / "config.yaml"
        cfg_file.write_text("api:\n  port: ${PORT_VAL}\n")
        settings = load_config(cfg_file)
        assert settings.api.port == 9999

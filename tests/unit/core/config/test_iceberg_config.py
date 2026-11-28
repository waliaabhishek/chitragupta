from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import SecretStr

from core.config.fingerprint import tenant_config_fingerprint
from core.config.loader import load_config

_YAML = """\
tenants:
  source:
    ecosystem: generic_metrics_only
    tenant_id: first
    plugin_settings:
      source_secret: ${SOURCE_SECRET}
      emitters:
        - type: csv
        - type: iceberg
          params:
            catalog: &catalog
              type: rest
              uri: https://iceberg.example.test/catalog
              token: ${ICEBERG_TOKEN}
              s3:
                region: us-east-1
                access_key_id: ${OBJECT_KEY}
                secret_access_key: ${OBJECT_SECRET}
            table: finance.first
  other:
    ecosystem: self_managed_kafka
    tenant_id: second
    storage:
      connection_string: sqlite:///second.db
    plugin_settings:
      emitters:
        - type: iceberg
          params:
            catalog: *catalog
            table: finance.second
"""


def test_inline_iceberg_config_can_share_catalog_without_passing_secrets_to_plugins(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SOURCE_SECRET", "source-secret")
    monkeypatch.setenv("ICEBERG_TOKEN", "catalog-credential")
    monkeypatch.setenv("OBJECT_KEY", "object-key")
    monkeypatch.setenv("OBJECT_SECRET", "object-secret")
    path = tmp_path / "config.yaml"
    path.write_text(_YAML)

    config = load_config(path)
    source = config.tenants["source"]
    first = source.plugin_settings.iceberg_emitter
    second = config.tenants["other"].plugin_settings.iceberg_emitter
    assert first is not None and second is not None
    assert len({first.params.catalog, second.params.catalog}) == 1
    assert first.params.table == "finance.first"
    assert second.params.table == "finance.second"
    assert first.params.catalog.token == SecretStr("catalog-credential")
    assert first.params.catalog.s3 is not None
    assert first.params.catalog.s3.secret_access_key == SecretStr("object-secret")
    dumped = config.model_dump_json()
    for secret in ("catalog-credential", "object-key", "object-secret"):
        assert secret not in dumped
    payload = source.plugin_settings.plugin_payload()
    assert payload["source_secret"] == "source-secret"  # pragma: allowlist secret
    assert [emitter["type"] for emitter in payload["emitters"]] == ["csv"]

    original_fingerprint = tenant_config_fingerprint(source)
    monkeypatch.setenv("ICEBERG_TOKEN", "second-catalog-credential")
    path.write_text(_YAML.replace("finance.first", "finance.updated"))
    assert tenant_config_fingerprint(load_config(path).tenants["source"]) == original_fingerprint
    monkeypatch.setenv("SOURCE_SECRET", "different-source-secret")
    assert tenant_config_fingerprint(load_config(path).tenants["source"]) != original_fingerprint


@pytest.mark.parametrize(
    ("change", "match"),
    [
        ("token: [catalog-credential", "Malformed YAML"),
        ("token: [catalog-credential]", "catalog.token"),
        ("token: catalog-credential\n      invalid_extra: [", "Malformed YAML"),
        ("token: catalog-credential\n              uri: http://example.test", "catalog.uri"),
    ],
)
def test_iceberg_config_errors_hide_raw_credentials(
    tmp_path: Path, caplog: pytest.LogCaptureFixture, change: str, match: str
) -> None:
    path = tmp_path / "config.yaml"
    path.write_text(
        _YAML.replace("token: ${ICEBERG_TOKEN}", change)
        .replace("${OBJECT_KEY}", "object-key")
        .replace("${OBJECT_SECRET}", "object-secret")
        .replace("${SOURCE_SECRET}", "source-secret")
    )
    with pytest.raises(ValueError, match=match) as exc:
        load_config(path)
    for secret in ("catalog-credential", "object-key", "object-secret"):
        assert secret not in str(exc.value)
        assert secret not in caplog.text


@pytest.mark.parametrize(
    ("previous", "replacement", "match"),
    [
        ("finance.first", "unqualified", "params.table"),
        (
            "type: iceberg\n          params:",
            "type: iceberg\n          aggregation: daily\n          params:",
            "aggregation",
        ),
        (
            "type: iceberg\n          params:",
            "type: iceberg\n          lookback_days: 0\n          params:",
            "lookback_days",
        ),
        ("type: rest", "type: sql", "catalog.type"),
        ("                access_key_id: ${OBJECT_KEY}\n", "", "inline S3 credentials"),
        (
            "            table: finance.first",
            "            table: finance.first\n"
            "        - type: iceberg\n"
            "          params: {catalog: *catalog, table: finance.duplicate}",
            "one Iceberg emitter",
        ),
        (
            "      source_secret:",
            "      topic_attribution: {emitters: [{type: iceberg}]}\n      source_secret:",
            "topic_attribution.emitters",
        ),
    ],
)
def test_iceberg_config_rejects_unsupported_options(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, previous: str, replacement: str, match: str
) -> None:
    for var in ("SOURCE_SECRET", "ICEBERG_TOKEN", "OBJECT_KEY", "OBJECT_SECRET"):
        monkeypatch.setenv(var, "sample-value")
    path = tmp_path / "config.yaml"
    path.write_text(_YAML.replace(previous, replacement, 1))
    with pytest.raises(ValueError, match=match):
        load_config(path)

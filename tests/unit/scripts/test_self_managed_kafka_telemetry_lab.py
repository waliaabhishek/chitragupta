from __future__ import annotations

import importlib.util
import json
import os
import re
import shutil
import subprocess
import sys
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[3]
LAB_DIR = PROJECT_ROOT / "examples" / "self-managed-kafka-telemetry-lab"
FIXTURES_DIR = PROJECT_ROOT / "tests" / "fixtures" / "self_managed_kafka_telemetry_lab"
EXPECTED_CONTRACT = json.loads((FIXTURES_DIR / "expected_contract.json").read_text(encoding="utf-8"))

PROMETHEUS_LINE = re.compile(
    r"^(?P<name>[a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{(?P<labels>[^}]*)\})?\s+"
    r"(?P<value>NaN|[-+]?Inf|[-+]?(?:\d+(?:\.\d*)?|\.\d+))$"
)


def _assert_exists(path: Path) -> Path:
    assert path.exists(), f"Expected {path} to exist"
    return path


def _read_text(path: Path) -> str:
    return _assert_exists(path).read_text(encoding="utf-8")


def _read_yaml(path: Path) -> dict[str, Any]:
    loaded = yaml.safe_load(_read_text(path))
    assert isinstance(loaded, dict), f"Expected mapping YAML at {path}"
    return loaded


def _read_json(path: Path) -> Any:
    return json.loads(_read_text(path))


def _parse_prometheus_metrics(path: Path) -> dict[str, list[dict[str, str]]]:
    metrics: dict[str, list[dict[str, str]]] = {}

    for raw_line in _read_text(path).splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue

        match = PROMETHEUS_LINE.match(line)
        assert match is not None, f"Could not parse Prometheus sample line: {line}"
        labels_text = match.group("labels") or ""
        labels: dict[str, str] = {}
        if labels_text:
            for label_entry in labels_text.split(","):
                key, value = label_entry.split("=", 1)
                labels[key] = value.strip('"')
        metrics.setdefault(match.group("name"), []).append(labels)

    return metrics


def _parse_raw_jmx(path: Path) -> set[tuple[str, str]]:
    entries: set[tuple[str, str]] = set()
    for line in _read_text(path).splitlines():
        payload = json.loads(line)
        entries.add((payload["object_name"], payload["attribute"]))
    return entries


def _apply_exporter_rule(config: dict[str, Any], source: str) -> tuple[str, dict[str, str]]:
    for rule in config["rules"]:
        match = re.search(rule["pattern"], source)
        if match is None:
            continue

        def replace_group(reference: re.Match[str], source_match: re.Match[str] = match) -> str:
            return source_match.group(int(reference.group(1)))

        labels = {key: re.sub(r"\$(\d+)", replace_group, str(value)) for key, value in rule.get("labels", {}).items()}
        return str(rule["name"]), labels
    raise AssertionError(f"No exporter rule matched sanitized JMX input: {source}")


def _copy_lab(tmp_path: Path) -> Path:
    destination = tmp_path / "lab"
    shutil.copytree(
        LAB_DIR,
        destination,
        ignore=shutil.ignore_patterns(".env", ".restart-state.json", "generated", "evidence", "__pycache__"),
    )
    return destination


def _write_executable(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")
    path.chmod(0o755)


def _fake_command_environment(tmp_path: Path) -> tuple[dict[str, str], Path]:
    fake_bin = tmp_path / "fake-bin"
    fake_bin.mkdir()
    command_log = tmp_path / "commands.log"
    _write_executable(
        fake_bin / "docker-compose",
        """#!/usr/bin/env bash
set -euo pipefail
printf 'compose %s\n' "$*" >>"$LAB_FAKE_LOG"
if [[ ${1:-} == ps && ${2:-} == -q ]]; then
  printf 'fake-%s\n' "${3:-service}"
fi
""",
    )
    _write_executable(
        fake_bin / "docker",
        """#!/usr/bin/env bash
set -euo pipefail
printf 'docker %s\n' "$*" >>"$LAB_FAKE_LOG"
if [[ $* == *Health.Status* ]]; then
  printf 'healthy\n'
elif [[ ${1:-} == inspect ]]; then
  printf 'true\n'
fi
""",
    )
    _write_executable(
        fake_bin / "curl",
        """#!/usr/bin/env bash
set -euo pipefail
printf 'curl %s\n' "$*" >>"$LAB_FAKE_LOG"
""",
    )
    _write_executable(
        fake_bin / "uv",
        """#!/usr/bin/env bash
set -euo pipefail
[[ ${1:-} == run && ${2:-} == python ]]
shift 2
exec "$LAB_TEST_PYTHON" "$@"
""",
    )
    environment = os.environ.copy()
    environment.update(
        {
            "COMPOSE": "docker-compose",
            "LAB_FAKE_LOG": str(command_log),
            "LAB_TEST_PYTHON": sys.executable,
            "PATH": f"{fake_bin}:{environment['PATH']}",
        }
    )
    return environment, command_log


def _run_lab(lab_dir: Path, environment: dict[str, str], *arguments: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [str(lab_dir / "scripts" / "lab.sh"), *arguments],
        cwd=lab_dir,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )


def _prometheus_result(samples: list[tuple[dict[str, str], float]]) -> dict[str, Any]:
    timestamp = datetime.now(tz=UTC).timestamp()
    return {
        "status": "success",
        "data": {
            "resultType": "vector",
            "result": [{"metric": labels, "value": [timestamp, str(value)]} for labels, value in samples],
        },
    }


def _query_entry(query: str, samples: list[tuple[dict[str, str], float]]) -> dict[str, Any]:
    return {"query": query, "response": _prometheus_result(samples)}


def _quota_labels(cluster: str, quota_type: str, scope: str) -> dict[str, str]:
    identity = {
        "user": ("user-only", "not_applicable"),
        "client-id": ("not_applicable", "client-only"),
        "user-client": ("shared-user", "shared-client"),
    }[scope]
    return {
        "__name__": "kafka_server_quota_byte_rate",
        "lab_cluster": cluster,
        "kafka_cluster_id": f"kraft-{cluster[-1]}-001",
        "broker": "1",
        "quota_type": quota_type,
        "quota_scope": scope,
        "user": identity[0],
        "client_id": identity[1],
    }


def _valid_restart_manifest() -> dict[str, Any]:
    workloads = _read_yaml(LAB_DIR / "workloads" / "workloads.yaml")
    quotas = [
        {
            "profile": profile["profile"],
            "scope": profile["scope"],
            "user": profile.get("user", "") if profile["scope"] in {"user", "user-client"} else "",
            "client_id": (profile.get("client_id", "") if profile["scope"] in {"client-id", "user-client"} else ""),
            "producer_byte_rate": profile["quota_limit_bytes_per_second"],
            "consumer_byte_rate": profile["quota_limit_bytes_per_second"],
            "over_quota": profile.get("over_quota") is True,
        }
        for profile in workloads["quota_profiles"]
    ]
    traffic_profiles = [
        {
            "profile": topic["profile"],
            "topic": topic["name"],
            "expected_bytes_per_second": workloads["rates_bytes_per_second"][topic["profile"]],
        }
        for topic in workloads["topics"]
    ]
    required = {
        "users": sorted({profile["user"] for profile in workloads["quota_profiles"]}),
        "client_ids": sorted({profile["client_id"] for profile in workloads["quota_profiles"]}),
        "quotas": quotas,
        "topics": sorted(profile["topic"] for profile in traffic_profiles),
        "traffic_profiles": traffic_profiles,
    }
    observed = {
        **required,
        "traffic_profiles": [
            {**profile, "producer_active": True, "consumer_active": True} for profile in traffic_profiles
        ],
    }
    return {
        "generation_id": "generation-2",
        "previous_generation_id": "generation-1",
        "state_recreated": True,
        "clusters": {
            cluster: {"required": json.loads(json.dumps(required)), "observed": json.loads(json.dumps(observed))}
            for cluster in ("cluster-a", "cluster-b")
        },
    }


def _build_valid_evidence(tmp_path: Path) -> Path:
    evidence = tmp_path / "evidence"
    evidence.mkdir()
    for cluster in ("a", "b"):
        shutil.copy(FIXTURES_DIR / f"raw-jmx-cluster-{cluster}.jsonl", evidence)
        shutil.copy(FIXTURES_DIR / f"exporter-cluster-{cluster}.metrics", evidence)

    topic_samples: dict[str, list[tuple[dict[str, str], float]]] = {
        "alltopics_bytes_in": [],
        "alltopics_bytes_out": [],
        "topic_bytes_in": [],
        "topic_bytes_out": [],
        "partition_log_size": [],
        "topic_bytes_in_rate": [],
        "topic_bytes_out_rate": [],
    }
    quota_byte_samples: list[tuple[dict[str, str], float]] = []
    quota_throttle_samples: list[tuple[dict[str, str], float]] = []
    up_samples: list[tuple[dict[str, str], float]] = []
    scrape_error_samples: list[tuple[dict[str, str], float]] = []
    for cluster in ("cluster-a", "cluster-b"):
        cluster_id = f"kraft-{cluster[-1]}-001"
        common = {"lab_cluster": cluster, "kafka_cluster_id": cluster_id, "broker": "1"}
        topic_samples["alltopics_bytes_in"].append(
            ({"__name__": "kafka_server_brokertopicmetrics_alltopics_bytesin_total", **common}, 8192)
        )
        topic_samples["alltopics_bytes_out"].append(
            ({"__name__": "kafka_server_brokertopicmetrics_alltopics_bytesout_total", **common}, 4096)
        )
        topic_samples["topic_bytes_in"].append(
            ({"__name__": "kafka_server_brokertopicmetrics_bytesin_total", **common, "topic": "shared-topic"}, 4096)
        )
        topic_samples["topic_bytes_out"].append(
            ({"__name__": "kafka_server_brokertopicmetrics_bytesout_total", **common, "topic": "shared-topic"}, 2048)
        )
        topic_samples["partition_log_size"].append(
            ({"__name__": "kafka_log_log_size", **common, "topic": "shared-topic", "partition": "0"}, 8192)
        )
        for query_name in ("topic_bytes_in_rate", "topic_bytes_out_rate"):
            topic_samples[query_name].extend(
                [
                    ({**common, "topic": "shared-topic"}, 8192),
                    ({**common, "topic": "shared-topic-high"}, 65536),
                    ({**common, "topic": "shared-topic-over-quota"}, 131072),
                ]
            )
        for quota_type in ("Produce", "Fetch"):
            for scope, value in (("user", 8192), ("client-id", 65536), ("user-client", 131072)):
                labels = _quota_labels(cluster, quota_type, scope)
                quota_byte_samples.append((labels, value))
                throttle_labels = {**labels, "__name__": "kafka_server_quota_throttle_time_ms"}
                throttle = 7 if scope == "user-client" else 0
                quota_throttle_samples.append((throttle_labels, throttle))
        target = {
            "job": "kafka-jmx",
            "instance": f"jmx-{cluster[-1]}:7071",
            "lab_cluster": cluster,
            "kafka_cluster_id": cluster_id,
        }
        up_samples.append(({"__name__": "up", **target}, 1))
        scrape_error_samples.append(({"__name__": "jmx_scrape_error", **target}, 0))

    queries = {name: _query_entry(name, samples) for name, samples in topic_samples.items()}
    queries.update(
        {
            "quota_byte_rate": _query_entry("quota byte rate", quota_byte_samples),
            "quota_throttle_time": _query_entry("quota throttle time", quota_throttle_samples),
            "up": _query_entry("up", up_samples),
            "jmx_scrape_error": _query_entry("jmx scrape error", scrape_error_samples),
            "produce_throttle_max": _query_entry(
                "produce throttle max",
                [sample for sample in quota_throttle_samples if sample[0]["quota_type"] == "Produce"],
            ),
            "fetch_throttle_max": _query_entry(
                "fetch throttle max",
                [sample for sample in quota_throttle_samples if sample[0]["quota_type"] == "Fetch"],
            ),
        }
    )
    (evidence / "prometheus-query-results.json").write_text(json.dumps(queries), encoding="utf-8")
    for artifact, payload in (
        ("prometheus-targets.json", {"status": "success", "data": {}}),
        ("prometheus-metadata.json", {"status": "success", "data": {}}),
        ("quota-descriptions.json", {"a": "shared-user producer_byte_rate", "b": "shared-user producer_byte_rate"}),
        ("topic-descriptions.json", {"a": "shared-topic", "b": "shared-topic"}),
        (
            "cluster-id-comparison.json",
            {"cluster_ids": {"cluster-a": "kraft-a-001", "cluster-b": "kraft-b-001"}, "distinct": True},
        ),
        (
            "clean-restart-manifest.json",
            _valid_restart_manifest(),
        ),
        ("footprint.json", {"services": {"prometheus": {"storage_bytes": 1024}}}),
        ("cleanup-result.json", {"status": "complete"}),
        ("evidence-manifest.json", {"captured_at": datetime.now(tz=UTC).isoformat(), "window": "5m"}),
    ):
        (evidence / artifact).write_text(json.dumps(payload), encoding="utf-8")
    return evidence


def _run_validator(evidence: Path, *, require_recreated_state: bool = False) -> subprocess.CompletedProcess[str]:
    command = [sys.executable, str(LAB_DIR / "scripts" / "validate_evidence.py"), "--evidence-dir", str(evidence)]
    if require_recreated_state:
        command.append("--require-recreated-state")
    return subprocess.run(command, capture_output=True, text=True, check=False)


def _load_capture_module() -> Any:
    path = LAB_DIR / "scripts" / "capture_evidence.py"
    spec = importlib.util.spec_from_file_location("lab_capture_evidence", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_lab_shell_has_valid_bash_syntax() -> None:
    lab_shell = _assert_exists(LAB_DIR / "scripts" / "lab.sh")
    result = subprocess.run(["bash", "-n", str(lab_shell)], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr


def test_docker_compose_provisions_two_pinned_clusters_and_bounded_prometheus_storage() -> None:
    compose = _read_yaml(LAB_DIR / "docker-compose.yml")
    services = compose.get("services")
    assert isinstance(services, dict), "Expected docker-compose services mapping"

    assert "zookeeper" not in services

    for cluster in ("a", "b"):
        kafka_service = services.get(f"kafka-{cluster}")
        jmx_service = services.get(f"jmx-{cluster}")
        workload_service = services.get(f"workload-{cluster}")

        assert isinstance(kafka_service, dict), f"Missing kafka-{cluster} service"
        assert isinstance(jmx_service, dict), f"Missing jmx-{cluster} service"
        assert isinstance(workload_service, dict), f"Missing workload-{cluster} service"
        assert kafka_service.get("image") == "apache/kafka:4.3.1"

    assert any("setup" in service_name for service_name in services), "Expected a setup service"
    assert any("dump" in service_name for service_name in services), "Expected a raw JMX dump service"

    prometheus_service = services.get("prometheus")
    assert isinstance(prometheus_service, dict), "Missing prometheus service"

    prometheus_text = json.dumps(prometheus_service, sort_keys=True)
    assert "--storage.tsdb.retention.time=14d" in prometheus_text
    assert "--storage.tsdb.retention.size=1GB" in prometheus_text
    assert "1536m" in prometheus_text

    for service_name in ("kafka-a", "kafka-b", "prometheus"):
        service = services[service_name]
        assert "logging" in service, f"Expected explicit log bounds for {service_name}"
        logging_text = json.dumps(service["logging"], sort_keys=True)
        assert "max-size" in logging_text or "max_file" in logging_text or "max-file" in logging_text


def test_jmx_exporter_config_allowlists_required_metrics_without_catchall() -> None:
    config = _read_yaml(LAB_DIR / "jmx" / "kafka-jmx.yml")

    assert config.get("excludeJvmMetrics") is True

    rules = config.get("rules")
    assert isinstance(rules, list) and rules, "Expected explicit JMX exporter rules"

    config_text = json.dumps(config, sort_keys=True)
    assert "includeObjectNames" in config
    assert "includeObjectNameAttributes" in config

    for required_mapping in EXPECTED_CONTRACT["raw_mbeans"]:
        assert required_mapping["attribute"] in config_text
        assert required_mapping["series"] in config_text
    for wildcard_mapping in (
        "topic=*",
        "type=Produce,user=*",
        "type=Fetch,user=*",
        "type=Produce,client-id=*",
        "type=Fetch,client-id=*",
        "type=Produce,user=*,client-id=*",
        "type=Fetch,user=*,client-id=*",
    ):
        assert wildcard_mapping in config_text

    for rule in rules:
        if not isinstance(rule, dict):
            continue
        assert rule.get("pattern") != ".*", "JMX exporter config must not retain a catch-all rule"


def test_jmx_exporter_rules_match_live_canonical_bean_order_and_values() -> None:
    config = _read_yaml(LAB_DIR / "jmx" / "kafka-jmx.yml")
    sanitized_inputs = [
        (
            "kafka.server<type=BrokerTopicMetrics, name=BytesInPerSec><>Count: 3175386",
            "kafka_server_brokertopicmetrics_alltopics_bytesin_total",
            {},
        ),
        (
            "kafka.server<type=BrokerTopicMetrics, name=BytesOutPerSec><>Count: 3175386",
            "kafka_server_brokertopicmetrics_alltopics_bytesout_total",
            {},
        ),
        (
            "kafka.server<type=BrokerTopicMetrics, name=BytesInPerSec, topic=shared-topic><>Count: 3175386",
            "kafka_server_brokertopicmetrics_bytesin_total",
            {"topic": "shared-topic"},
        ),
        (
            "kafka.server<type=BrokerTopicMetrics, name=BytesOutPerSec, topic=shared-topic><>Count: 3175386",
            "kafka_server_brokertopicmetrics_bytesout_total",
            {"topic": "shared-topic"},
        ),
        (
            "kafka.log<type=Log, name=Size, topic=shared-topic, partition=0><>Value: 3175386",
            "kafka_log_log_size",
            {"topic": "shared-topic", "partition": "0"},
        ),
    ]
    quota_sources = {
        "user": "user=user-only",
        "client-id": "client-id=client-only",
        "user-client": "user=shared-user, client-id=shared-client",
    }
    quota_labels = {
        "user": {"user": "user-only", "client_id": "not_applicable"},
        "client-id": {"user": "not_applicable", "client_id": "client-only"},
        "user-client": {"user": "shared-user", "client_id": "shared-client"},
    }
    for quota_type in ("Produce", "Fetch"):
        for scope, entity in quota_sources.items():
            for attribute, value, series in (
                ("byte-rate", "18463.947990543737", "kafka_server_quota_byte_rate"),
                (
                    "throttle-time",
                    "1397.6" if scope == "user-client" else "NaN",
                    "kafka_server_quota_throttle_time_ms",
                ),
            ):
                sanitized_inputs.append(
                    (
                        f"kafka.server<type={quota_type}, {entity}><>{attribute}: {value}",
                        series,
                        {"quota_type": quota_type, "quota_scope": scope, **quota_labels[scope]},
                    )
                )

    for source, expected_series, expected_labels in sanitized_inputs:
        series, labels = _apply_exporter_rule(config, source)
        assert series == expected_series
        assert labels == expected_labels


def test_generated_jmx_attribute_allowlist_uses_exact_matrix_object_names(tmp_path: Path) -> None:
    lab = _copy_lab(tmp_path)
    generator = lab / "scripts" / "generate_local_config.py"
    result = subprocess.run([sys.executable, str(generator)], cwd=lab, capture_output=True, text=True, check=False)
    assert result.returncode == 0, result.stderr

    generated = _read_yaml(lab / "generated" / "jmx-a.yml")
    attributes = generated["includeObjectNameAttributes"]
    assert isinstance(attributes, dict)
    assert all("*" not in object_name for object_name in attributes)
    workloads = _read_yaml(lab / "workloads" / "workloads.yaml")
    for topic in workloads["topics"]:
        name = topic["name"]
        assert attributes[f"kafka.server:type=BrokerTopicMetrics,name=BytesInPerSec,topic={name}"] == ["Count"]
        assert attributes[f"kafka.server:type=BrokerTopicMetrics,name=BytesOutPerSec,topic={name}"] == ["Count"]
        assert attributes[f"kafka.log:type=Log,name=Size,topic={name},partition=0"] == ["Value"]
    assert attributes["kafka.server:type=BrokerTopicMetrics,name=BytesInPerSec"] == ["Count"]
    assert attributes["kafka.server:type=BrokerTopicMetrics,name=BytesOutPerSec"] == ["Count"]
    for profile in workloads["quota_profiles"]:
        entity = {
            "user": f"user={profile['user']}",
            "client-id": f"client-id={profile['client_id']}",
            "user-client": f"user={profile['user']},client-id={profile['client_id']}",
        }[profile["scope"]]
        for quota_type in ("Produce", "Fetch"):
            assert attributes[f"kafka.server:type={quota_type},{entity}"] == ["byte-rate", "throttle-time"]


def test_metric_contract_declares_required_series_labels_categories_and_artifacts() -> None:
    contract_text = _read_text(LAB_DIR / "contracts" / "metric-contract.yaml")

    for series_name in EXPECTED_CONTRACT["series_names"]:
        assert series_name in contract_text

    contract = _read_yaml(LAB_DIR / "contracts" / "metric-contract.yaml")
    workloads = _read_yaml(LAB_DIR / "workloads" / "workloads.yaml")
    topics = {topic["profile"]: topic["name"] for topic in workloads["topics"]}
    profiles = {profile["profile"]: profile for profile in workloads["quota_profiles"]}
    rendered_mappings = {
        (
            metric["raw_object_name"].format(
                topic=topics[metric["workload_profile"]],
                user=profiles.get(metric["workload_profile"], {}).get("user", ""),
                client_id=profiles.get(metric["workload_profile"], {}).get("client_id", ""),
            ),
            metric["raw_attribute"],
        )
        for metric in contract["metrics"]
        if "raw_object_name" in metric
    }
    for mapping in EXPECTED_CONTRACT["raw_mbeans"]:
        assert (mapping["object_name"], mapping["attribute"]) in rendered_mappings

    for label_name in EXPECTED_CONTRACT["cluster_labels"]:
        assert label_name in contract_text

    for category in EXPECTED_CONTRACT["failure_categories"]:
        assert category in contract_text

    for artifact_name in EXPECTED_CONTRACT["completion_evidence"]:
        assert artifact_name in contract_text


def test_workloads_cover_deterministic_rates_scopes_and_overlapping_names() -> None:
    workloads_text = _read_text(LAB_DIR / "workloads" / "workloads.yaml")

    for rate in EXPECTED_CONTRACT["workload_rates"].values():
        assert str(rate) in workloads_text

    for scope in EXPECTED_CONTRACT["quota_scopes"]:
        assert scope in workloads_text

    overlap = EXPECTED_CONTRACT["overlap_names"]
    for value in overlap.values():
        assert value in workloads_text


def test_shell_contract_uses_standalone_compose_and_declares_error_prefixes() -> None:
    lab_shell = _read_text(LAB_DIR / "scripts" / "lab.sh")

    assert "${COMPOSE:-docker-compose}" in lab_shell

    for subcommand in EXPECTED_CONTRACT["documented_commands"]:
        assert subcommand in lab_shell

    for exit_code, prefix in EXPECTED_CONTRACT["exit_contract"].items():
        assert prefix in lab_shell
        assert exit_code in lab_shell

    assert "--require-recreated-state" in lab_shell


def test_readme_documents_live_validation_artifacts_matching_fixture_bundle() -> None:
    readme_text = _read_text(LAB_DIR / "README.md")

    for artifact_name in EXPECTED_CONTRACT["completion_evidence"]:
        assert artifact_name in readme_text

    assert "./scripts/lab.sh validate --window 5m" in readme_text
    assert "./scripts/lab.sh evidence" not in readme_text
    assert "`validate` is the canonical complete evidence-producing command" in readme_text
    assert "The standalone `evidence` subcommand is capture-only" in readme_text
    assert "run `validate` again before `cleanup`" in readme_text


def test_static_evidence_samples_require_explicit_cluster_identity_for_overlapping_names() -> None:
    cluster_a_metrics = _parse_prometheus_metrics(FIXTURES_DIR / "exporter-cluster-a.metrics")
    cluster_b_metrics = _parse_prometheus_metrics(FIXTURES_DIR / "exporter-cluster-b.metrics")

    bytes_in_a = cluster_a_metrics["kafka_server_brokertopicmetrics_bytesin_total"][0]
    bytes_in_b = cluster_b_metrics["kafka_server_brokertopicmetrics_bytesin_total"][0]

    assert bytes_in_a["topic"] == bytes_in_b["topic"] == EXPECTED_CONTRACT["overlap_names"]["topic"]
    assert bytes_in_a["lab_cluster"] != bytes_in_b["lab_cluster"]
    assert bytes_in_a["kafka_cluster_id"] != bytes_in_b["kafka_cluster_id"]

    all_topics_in_a = cluster_a_metrics["kafka_server_brokertopicmetrics_alltopics_bytesin_total"][0]
    all_topics_in_b = cluster_b_metrics["kafka_server_brokertopicmetrics_alltopics_bytesin_total"][0]
    all_topics_out_a = cluster_a_metrics["kafka_server_brokertopicmetrics_alltopics_bytesout_total"][0]
    all_topics_out_b = cluster_b_metrics["kafka_server_brokertopicmetrics_alltopics_bytesout_total"][0]

    for all_topics_sample in (all_topics_in_a, all_topics_in_b, all_topics_out_a, all_topics_out_b):
        assert "topic" not in all_topics_sample
        assert {"lab_cluster", "kafka_cluster_id", "broker"} <= all_topics_sample.keys()
    assert all_topics_in_a["kafka_cluster_id"] != all_topics_in_b["kafka_cluster_id"]
    assert all_topics_out_a["kafka_cluster_id"] != all_topics_out_b["kafka_cluster_id"]

    quota_a = [
        sample for sample in cluster_a_metrics["kafka_server_quota_byte_rate"] if sample["quota_scope"] == "user-client"
    ]
    quota_b = [
        sample for sample in cluster_b_metrics["kafka_server_quota_byte_rate"] if sample["quota_scope"] == "user-client"
    ]
    assert (
        {sample["user"] for sample in quota_a if sample["user"]}
        == {sample["user"] for sample in quota_b if sample["user"]}
        == {EXPECTED_CONTRACT["overlap_names"]["user"]}
    )
    assert (
        {sample["client_id"] for sample in quota_a if sample["client_id"]}
        == {sample["client_id"] for sample in quota_b if sample["client_id"]}
        == {EXPECTED_CONTRACT["overlap_names"]["client_id"]}
    )


def test_static_raw_jmx_samples_cover_every_required_mapping() -> None:
    cluster_a_entries = _parse_raw_jmx(FIXTURES_DIR / "raw-jmx-cluster-a.jsonl")
    cluster_b_entries = _parse_raw_jmx(FIXTURES_DIR / "raw-jmx-cluster-b.jsonl")

    expected_entries = {(mapping["object_name"], mapping["attribute"]) for mapping in EXPECTED_CONTRACT["raw_mbeans"]}

    assert expected_entries <= cluster_a_entries
    assert expected_entries <= cluster_b_entries


def test_quota_fixtures_cover_every_type_attribute_and_scope_on_both_clusters() -> None:
    expected_combinations = {
        (quota_type, scope) for quota_type in ("Produce", "Fetch") for scope in EXPECTED_CONTRACT["quota_scopes"]
    }
    for cluster in ("a", "b"):
        metrics = _parse_prometheus_metrics(FIXTURES_DIR / f"exporter-cluster-{cluster}.metrics")
        for series in ("kafka_server_quota_byte_rate", "kafka_server_quota_throttle_time_ms"):
            actual = {(sample["quota_type"], sample["quota_scope"]) for sample in metrics[series]}
            assert actual == expected_combinations


def test_steady_high_quota_is_above_steady_high_rate_and_only_combined_profile_is_over_quota() -> None:
    workloads = _read_yaml(LAB_DIR / "workloads" / "workloads.yaml")
    profiles = {profile["scope"]: profile for profile in workloads["quota_profiles"]}

    assert profiles["user"]["quota_limit_bytes_per_second"] > workloads["rates_bytes_per_second"]["steady_low"]
    assert profiles["client-id"]["quota_limit_bytes_per_second"] > workloads["rates_bytes_per_second"]["steady_high"]
    assert profiles["user-client"]["quota_limit_bytes_per_second"] < workloads["rates_bytes_per_second"]["over_quota"]
    assert profiles["user-client"]["over_quota"] is True
    setup_source = _read_text(LAB_DIR / "scripts" / "setup_kafka.sh")
    workload_source = _read_text(LAB_DIR / "scripts" / "workload.sh")
    assert 'source "$GENERATED/runtime-plan.sh"' in setup_source
    assert 'source "$GENERATED/runtime-plan.sh"' in workload_source
    for stale_value in ("shared-topic", "user-only", "client-only", "shared-user", "8192", "65536", "131072"):
        assert stale_value not in setup_source
        assert stale_value not in workload_source


def test_config_generation_permissions_idempotence_and_restart_marker(tmp_path: Path) -> None:
    lab = _copy_lab(tmp_path)
    generator = lab / "scripts" / "generate_local_config.py"

    first_run = subprocess.run([sys.executable, str(generator)], cwd=lab, capture_output=True, text=True, check=False)
    assert first_run.returncode == 0, first_run.stderr
    first_generation = json.loads((lab / "generated" / "generation.json").read_text(encoding="utf-8"))

    assert (lab / ".env").stat().st_mode & 0o777 == 0o600
    for secret in (lab / "generated").glob("*.properties"):
        assert secret.stat().st_mode & 0o777 == 0o600
    for secret in (lab / "generated").glob("*.conf"):
        assert secret.stat().st_mode & 0o777 == 0o600
    for rendered in ("jmx-a.yml", "jmx-b.yml", "prometheus.yml", "runtime-plan.sh", "generation.json"):
        assert (lab / "generated" / rendered).stat().st_mode & 0o777 == 0o644

    second_run = subprocess.run([sys.executable, str(generator)], cwd=lab, capture_output=True, text=True, check=False)
    assert second_run.returncode == 0, second_run.stderr
    assert json.loads((lab / "generated" / "generation.json").read_text(encoding="utf-8")) == first_generation

    shutil.copy(lab / "generated" / "generation.json", lab / ".restart-state.json")
    shutil.rmtree(lab / "generated")
    (lab / ".env").unlink()
    restarted = subprocess.run([sys.executable, str(generator)], cwd=lab, capture_output=True, text=True, check=False)
    assert restarted.returncode == 0, restarted.stderr
    restarted_generation = json.loads((lab / "generated" / "generation.json").read_text(encoding="utf-8"))
    assert restarted_generation["generation_id"] != first_generation["generation_id"]
    assert restarted_generation["previous_generation_id"] == first_generation["generation_id"]


def test_workload_matrix_change_drives_generated_plan_setup_and_workload_runtime(tmp_path: Path) -> None:
    lab = _copy_lab(tmp_path)
    generator = lab / "scripts" / "generate_local_config.py"
    initial_generation = subprocess.run(
        [sys.executable, str(generator)], cwd=lab, capture_output=True, text=True, check=False
    )
    assert initial_generation.returncode == 0, initial_generation.stderr
    initial_plan = _read_text(lab / "generated" / "runtime-plan.sh")
    matrix_path = lab / "workloads" / "workloads.yaml"
    matrix = _read_yaml(matrix_path)
    high_topic = next(topic for topic in matrix["topics"] if topic["profile"] == "steady_high")
    high_profile = next(profile for profile in matrix["quota_profiles"] if profile["profile"] == "steady_high")
    high_topic["name"] = "matrix-topic"
    high_profile.update(
        {
            "user": "matrix-user",
            "client_id": "matrix-client",
            "group": "matrix-group",
            "quota_limit_bytes_per_second": 196608,
        }
    )
    matrix["rates_bytes_per_second"]["steady_high"] = 98304
    matrix_path.write_text(yaml.safe_dump(matrix, sort_keys=False), encoding="utf-8")

    generated = subprocess.run(
        [sys.executable, str(generator)],
        cwd=lab,
        capture_output=True,
        text=True,
        check=False,
    )
    assert generated.returncode == 0, generated.stderr
    runtime_plan = _read_text(lab / "generated" / "runtime-plan.sh")
    assert runtime_plan != initial_plan
    for propagated in ("matrix-topic", "matrix-user", "matrix-client", "matrix-group", "98304", "196608"):
        assert propagated in runtime_plan
    assert (lab / "generated" / "matrix-user-a.properties").is_file()
    generated_jmx = _read_text(lab / "generated" / "jmx-a.yml")
    assert "topic=matrix-topic" in generated_jmx
    assert "client-id=matrix-client" in generated_jmx
    assert "topic=shared-topic-high" not in generated_jmx
    assert "client-id=client-only" not in generated_jmx

    fake_bin = tmp_path / "kafka-bin"
    fake_bin.mkdir()
    kafka_log = tmp_path / "kafka.log"
    fake_kafka_tool = """#!/usr/bin/env bash
set -u
printf '%s %s\n' "$(basename "$0")" "$*" >>"$MATRIX_KAFKA_LOG"
case "$(basename "$0")" in
  kafka-producer-perf-test.sh)
    exit 1
    ;;
  kafka-consumer-perf-test.sh)
    sleep 0.1
    exit 1
    ;;
esac
"""
    for tool in (
        "kafka-broker-api-versions.sh",
        "kafka-topics.sh",
        "kafka-configs.sh",
        "kafka-producer-perf-test.sh",
        "kafka-consumer-perf-test.sh",
    ):
        _write_executable(fake_bin / tool, fake_kafka_tool)
    environment = os.environ.copy()
    environment.update(
        {
            "GENERATED": str(lab / "generated"),
            "KAFKA_BIN": str(fake_bin),
            "MATRIX_KAFKA_LOG": str(kafka_log),
            "WORK_DIR": str(tmp_path / "work"),
        }
    )
    Path(environment["WORK_DIR"]).mkdir()

    setup = subprocess.run(
        ["bash", str(lab / "scripts" / "setup_kafka.sh")],
        cwd=lab,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )
    assert setup.returncode == 0, setup.stderr
    setup_log = kafka_log.read_text(encoding="utf-8")
    assert "--topic matrix-topic" in setup_log
    assert "producer_byte_rate=196608,consumer_byte_rate=196608" in setup_log
    assert "--entity-name matrix-client" in setup_log
    assert "--topic shared-topic-high" not in setup_log
    assert "--entity-name client-only" not in setup_log

    workload = subprocess.Popen(
        [
            "bash",
            str(lab / "scripts" / "workload.sh"),
            "cluster-a",
            "kafka-a:9092",
            str(lab / "generated"),
        ],
        cwd=lab,
        env=environment,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    workload_log = ""
    try:
        for _ in range(100):
            workload_log = kafka_log.read_text(encoding="utf-8")
            if "--topic matrix-topic" in workload_log and "--group matrix-group" in workload_log:
                break
            time.sleep(0.02)
    finally:
        workload.terminate()
        try:
            workload.communicate(timeout=3)
        except subprocess.TimeoutExpired:
            workload.kill()
            workload.communicate(timeout=3)
    assert "--topic matrix-topic" in workload_log
    assert "--throughput 96" in workload_log
    assert "--group matrix-group" in workload_log
    matrix_properties = _read_text(Path(environment["WORK_DIR"]) / "matrix-topic-producer.properties")
    assert "client.id=matrix-client" in matrix_properties
    assert "buffer.memory=1048576" in matrix_properties
    assert "delivery.timeout.ms=300000" in matrix_properties
    assert "max.block.ms=300000" in matrix_properties


def test_public_lab_entrypoint_dispatches_lifecycle_and_recreates_state(tmp_path: Path) -> None:
    lab = _copy_lab(tmp_path)
    environment, command_log = _fake_command_environment(tmp_path)

    for arguments in (
        ("prereq",),
        ("start",),
        ("ready",),
        ("workload", "start"),
        ("workload", "status"),
        ("workload", "stop"),
        ("stop",),
    ):
        result = _run_lab(lab, environment, *arguments)
        assert result.returncode == 0, result.stderr

    first_generation = json.loads((lab / "generated" / "generation.json").read_text(encoding="utf-8"))
    evidence_run = lab / "evidence" / "run-1"
    evidence_run.mkdir(parents=True)
    footprint = {"services": {"prometheus": {"storage_bytes": 1024}}}
    (evidence_run / "footprint.json").write_text(json.dumps(footprint), encoding="utf-8")
    (evidence_run / "cleanup-result.json").write_text(json.dumps({"status": "pending"}), encoding="utf-8")
    (lab / "evidence" / "latest").symlink_to(evidence_run.name, target_is_directory=True)

    cleanup = _run_lab(lab, environment, "cleanup")
    assert cleanup.returncode == 0, cleanup.stderr
    assert not (lab / ".env").exists()
    assert not (lab / "generated").exists()
    assert json.loads((evidence_run / "footprint.json").read_text(encoding="utf-8")) == footprint
    cleanup_result = json.loads((evidence_run / "cleanup-result.json").read_text(encoding="utf-8"))
    assert cleanup_result["status"] == "complete"
    assert cleanup_result["generated_secrets_removed"] is True

    restart = _run_lab(lab, environment, "start")
    assert restart.returncode == 0, restart.stderr
    restarted_generation = json.loads((lab / "generated" / "generation.json").read_text(encoding="utf-8"))
    assert restarted_generation["generation_id"] != first_generation["generation_id"]
    assert restarted_generation["previous_generation_id"] == first_generation["generation_id"]

    commands = command_log.read_text(encoding="utf-8")
    for expected in (
        "compose up -d --build kafka-a kafka-b jmx-a jmx-b prometheus",
        "compose run --rm kafka-setup",
        "compose up -d workload-a workload-b",
        "compose ps workload-a workload-b",
        "compose stop workload-a workload-b prometheus jmx-a jmx-b kafka-a kafka-b",
        "compose down --volumes --remove-orphans",
    ):
        assert expected in commands


def test_validation_preflight_fails_local_checks_before_compose_or_docker(tmp_path: Path) -> None:
    lab = _copy_lab(tmp_path)
    environment, command_log = _fake_command_environment(tmp_path)

    result = _run_lab(lab, environment, "validate", "--window", "5m")

    assert result.returncode == 7
    assert "validation_failed:" in result.stderr
    assert not command_log.exists()
    details = json.loads((lab / "evidence" / "latest" / "validator-result.json").read_text(encoding="utf-8"))
    assert details["failures"] == [
        {"category": "missing_metric", "message": "required local file is absent: .env", "artifact": None}
    ]


def test_validator_accepts_complete_evidence_and_clean_restart_state(tmp_path: Path) -> None:
    evidence = _build_valid_evidence(tmp_path)

    result = _run_validator(evidence, require_recreated_state=True)

    assert result.returncode == 0, result.stderr
    details = json.loads((evidence / "validator-result.json").read_text(encoding="utf-8"))
    assert details["status"] == "pass"
    assert details["failures"] == []


def test_validator_keeps_unthrottled_nan_quota_samples_as_scope_evidence(tmp_path: Path) -> None:
    evidence = _build_valid_evidence(tmp_path)
    for cluster in ("a", "b"):
        exporter_path = evidence / f"exporter-cluster-{cluster}.metrics"
        lines = exporter_path.read_text(encoding="utf-8").splitlines()
        exporter_path.write_text(
            "\n".join(
                re.sub(r" 0$", " NaN", line)
                if line.startswith("kafka_server_quota_throttle_time_ms") and 'quota_scope="user-client"' not in line
                else line
                for line in lines
            )
            + "\n",
            encoding="utf-8",
        )

    queries_path = evidence / "prometheus-query-results.json"
    queries = _read_json(queries_path)
    for sample in queries["quota_throttle_time"]["response"]["data"]["result"]:
        if sample["metric"]["quota_scope"] != "user-client":
            sample["value"][1] = "NaN"
    queries_path.write_text(json.dumps(queries), encoding="utf-8")

    result = _run_validator(evidence)

    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize(
    ("query_name", "quota_type", "non_finite"),
    [
        ("produce_throttle_max", "Produce", "NaN"),
        ("fetch_throttle_max", "Fetch", "+Inf"),
    ],
)
def test_validator_rejects_non_finite_over_quota_throttle_values(
    tmp_path: Path, query_name: str, quota_type: str, non_finite: str
) -> None:
    evidence = _build_valid_evidence(tmp_path)
    queries_path = evidence / "prometheus-query-results.json"
    queries = _read_json(queries_path)
    for sample in queries[query_name]["response"]["data"]["result"]:
        if sample["metric"]["quota_type"] == quota_type and sample["metric"]["quota_scope"] == "user-client":
            sample["value"][1] = non_finite
    queries_path.write_text(json.dumps(queries), encoding="utf-8")

    result = _run_validator(evidence)

    assert result.returncode == 1
    details = _read_json(evidence / "validator-result.json")
    assert {failure["category"] for failure in details["failures"]} == {"throttle_observation"}


@pytest.mark.parametrize(
    ("query_name", "topic", "non_finite"),
    [
        ("topic_bytes_in_rate", "shared-topic", "NaN"),
        ("topic_bytes_in_rate", "shared-topic-high", "+Inf"),
        ("topic_bytes_out_rate", "shared-topic", "-Inf"),
        ("topic_bytes_out_rate", "shared-topic-high", "NaN"),
    ],
)
def test_validator_rejects_non_finite_low_or_high_rate_values(
    tmp_path: Path, query_name: str, topic: str, non_finite: str
) -> None:
    evidence = _build_valid_evidence(tmp_path)
    queries_path = evidence / "prometheus-query-results.json"
    queries = _read_json(queries_path)
    sample = next(
        item
        for item in queries[query_name]["response"]["data"]["result"]
        if item["metric"]["lab_cluster"] == "cluster-a" and item["metric"]["topic"] == topic
    )
    sample["value"][1] = non_finite
    queries_path.write_text(json.dumps(queries), encoding="utf-8")

    result = _run_validator(evidence)

    assert result.returncode == 1
    details = _read_json(evidence / "validator-result.json")
    assert {failure["category"] for failure in details["failures"]} == {"rate_distinction"}


def test_capture_builds_complete_per_cluster_restart_observations_from_contracts(tmp_path: Path) -> None:
    capture = _load_capture_module()
    requirements = capture._restart_requirements()
    evidence = _build_valid_evidence(tmp_path)
    query_results = _read_json(evidence / "prometheus-query-results.json")
    identities = {"users": requirements["users"], "client_ids": requirements["client_ids"]}
    topics = "\n".join(f"Topic: {topic}\tPartitionCount: 1" for topic in requirements["topics"])
    quota_entities = {
        "user": lambda quota: f"user-principal '{quota['user']}'",
        "client-id": lambda quota: f"client-id '{quota['client_id']}'",
        "user-client": lambda quota: f"user-principal '{quota['user']}', client-id '{quota['client_id']}'",
    }
    quotas = "\n".join(
        f"Quota configs for {quota_entities[quota['scope']](quota)} are "
        f"consumer_byte_rate={float(quota['consumer_byte_rate'])}, "
        f"producer_byte_rate={float(quota['producer_byte_rate'])}"
        for quota in requirements["quotas"]
    )

    observed = capture._restart_cluster_observations(
        "cluster-a", requirements, identities, topics, quotas, query_results
    )

    assert observed["users"] == requirements["users"]
    assert observed["client_ids"] == requirements["client_ids"]
    assert observed["quotas"] == requirements["quotas"]
    assert observed["topics"] == requirements["topics"]
    assert all(profile["producer_active"] and profile["consumer_active"] for profile in observed["traffic_profiles"])


def test_capture_describes_each_quota_with_exact_matrix_entity_selectors(monkeypatch: pytest.MonkeyPatch) -> None:
    capture = _load_capture_module()
    requirements = capture._restart_requirements()
    calls: list[tuple[str, ...]] = []
    descriptions = {
        "user": (
            "Quota configs for user-principal 'user-only' are consumer_byte_rate=16384.0, producer_byte_rate=16384.0\n"
        ),
        "client-id": (
            "Quota configs for client-id 'client-only' are consumer_byte_rate=131072.0, producer_byte_rate=131072.0\n"
        ),
        "user-client": (
            "Quota configs for user-principal 'shared-user', client-id 'shared-client' are "
            "consumer_byte_rate=16384.0, producer_byte_rate=16384.0\n"
        ),
    }

    def fake_compose(*arguments: str) -> subprocess.CompletedProcess[str]:
        calls.append(arguments)
        if "users" in arguments and "clients" in arguments:
            output = descriptions["user-client"]
        elif "users" in arguments:
            output = descriptions["user"]
        else:
            output = descriptions["client-id"]
        return subprocess.CompletedProcess(arguments, 0, output, "")

    monkeypatch.setattr(capture, "_compose", fake_compose)
    output = capture._describe_quotas("a", requirements["quotas"])

    assert len(calls) == 3
    assert any(
        call[-8:]
        == (
            "--entity-type",
            "users",
            "--entity-name",
            "shared-user",
            "--entity-type",
            "clients",
            "--entity-name",
            "shared-client",
        )
        for call in calls
    )
    assert capture._observed_quotas(output) == {
        ("user", "user-only", "", 16384, 16384),
        ("client-id", "", "client-only", 131072, 131072),
        ("user-client", "shared-user", "shared-client", 16384, 16384),
    }


def test_validator_rejects_missing_raw_quota_scope_mapping(tmp_path: Path) -> None:
    evidence = _build_valid_evidence(tmp_path)
    raw_path = evidence / "raw-jmx-cluster-b.jsonl"
    missing_object = "kafka.server:type=Fetch,client-id=client-only"
    retained = [
        line
        for line in raw_path.read_text(encoding="utf-8").splitlines()
        if not (missing_object in line and '"attribute":"throttle-time"' in line)
    ]
    raw_path.write_text("\n".join(retained) + "\n", encoding="utf-8")

    result = _run_validator(evidence)

    assert result.returncode == 1
    details = json.loads((evidence / "validator-result.json").read_text(encoding="utf-8"))
    failures = [failure for failure in details["failures"] if failure["category"] == "missing_metric"]
    assert any("Fetch" in failure["message"] and "client-only" in failure["message"] for failure in failures)


@pytest.mark.parametrize(
    ("state_class", "name", "field"),
    [
        *(("user", name, "") for name in ("user-only", "client-only-user", "shared-user")),
        *(("client_id", name, "") for name in ("steady-low", "client-only", "shared-client")),
        *(
            ("quota", profile, field)
            for profile in ("steady_low", "steady_high", "over_quota")
            for field in ("producer_byte_rate", "consumer_byte_rate")
        ),
        *(("topic", name, "") for name in ("shared-topic", "shared-topic-high", "shared-topic-over-quota")),
        *(
            ("traffic", profile, field)
            for profile in ("steady_low", "steady_high", "over_quota")
            for field in ("producer_active", "consumer_active")
        ),
    ],
)
def test_validator_rejects_partially_recreated_state(tmp_path: Path, state_class: str, name: str, field: str) -> None:
    evidence = _build_valid_evidence(tmp_path)
    restart_path = evidence / "clean-restart-manifest.json"
    restart = json.loads(restart_path.read_text(encoding="utf-8"))
    observed = restart["clusters"]["cluster-a"]["observed"]
    if state_class == "user":
        observed["users"].remove(name)
    elif state_class == "client_id":
        observed["client_ids"].remove(name)
    elif state_class == "quota":
        quota = next(item for item in observed["quotas"] if item["profile"] == name)
        quota[field] += 1
    elif state_class == "topic":
        observed["topics"].remove(name)
    elif state_class == "traffic":
        profile = next(item for item in observed["traffic_profiles"] if item["profile"] == name)
        profile[field] = False
    else:
        raise AssertionError(f"unsupported state class: {state_class}")
    restart_path.write_text(json.dumps(restart), encoding="utf-8")

    result = _run_validator(evidence, require_recreated_state=True)

    assert result.returncode == 1
    details = json.loads((evidence / "validator-result.json").read_text(encoding="utf-8"))
    failures = [failure for failure in details["failures"] if failure["category"] == "evidence_stale"]
    assert any(name in failure["message"] for failure in failures)


def test_validator_rejects_manifest_that_omits_a_required_restart_entity(tmp_path: Path) -> None:
    evidence = _build_valid_evidence(tmp_path)
    restart_path = evidence / "clean-restart-manifest.json"
    restart = json.loads(restart_path.read_text(encoding="utf-8"))
    restart["clusters"]["cluster-b"]["required"]["topics"].remove("shared-topic-high")
    restart_path.write_text(json.dumps(restart), encoding="utf-8")

    result = _run_validator(evidence, require_recreated_state=True)

    assert result.returncode == 1
    details = json.loads((evidence / "validator-result.json").read_text(encoding="utf-8"))
    failures = [failure for failure in details["failures"] if failure["category"] == "evidence_stale"]
    assert any(
        "manifest omits required cluster-b topics" in failure["message"] and "shared-topic-high" in failure["message"]
        for failure in failures
    )


def _mutate_evidence(evidence: Path, category: str) -> None:
    if category == "missing_metric":
        raw_path = evidence / "raw-jmx-cluster-a.jsonl"
        lines = raw_path.read_text(encoding="utf-8").splitlines()
        raw_path.write_text("\n".join(lines[1:]) + "\n", encoding="utf-8")
        return
    if category == "wrong_type":
        exporter = evidence / "exporter-cluster-a.metrics"
        source = exporter.read_text(encoding="utf-8").replace(
            "# TYPE kafka_log_log_size gauge", "# TYPE kafka_log_log_size counter"
        )
        exporter.write_text(source, encoding="utf-8")
        return
    if category == "high_cardinality":
        exporter = evidence / "exporter-cluster-a.metrics"
        with exporter.open("a", encoding="utf-8") as handle:
            handle.write("# TYPE kafka_network_requestmetrics_request_total counter\n")
            handle.write("kafka_network_requestmetrics_request_total 1\n")
        return
    if category == "evidence_stale":
        manifest = _read_json(evidence / "evidence-manifest.json")
        manifest["captured_at"] = (datetime.now(tz=UTC) - timedelta(days=1)).isoformat()
        (evidence / "evidence-manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
        return

    queries_path = evidence / "prometheus-query-results.json"
    queries = _read_json(queries_path)
    if category == "missing_label":
        queries["topic_bytes_in"]["response"]["data"]["result"][0]["metric"].pop("broker")
    elif category == "quota_scope":
        samples = queries["quota_byte_rate"]["response"]["data"]["result"]
        sample = next(
            item
            for item in samples
            if item["metric"]["lab_cluster"] == "cluster-a"
            and item["metric"]["quota_type"] == "Produce"
            and item["metric"]["quota_scope"] == "client-id"
        )
        sample["metric"]["quota_scope"] = "user"
    elif category == "throttle_observation":
        for sample in queries["produce_throttle_max"]["response"]["data"]["result"]:
            sample["value"][1] = "0"
    elif category == "rate_distinction":
        for query_name in ("topic_bytes_in_rate", "topic_bytes_out_rate"):
            for sample in queries[query_name]["response"]["data"]["result"]:
                if sample["metric"]["topic"] == "shared-topic-high":
                    sample["value"][1] = "8192"
    elif category == "target_health":
        samples = queries["up"]["response"]["data"]["result"]
        next(item for item in samples if item["metric"]["lab_cluster"] == "cluster-b")["value"][1] = "0"
    elif category == "cluster_selector":
        samples = queries["topic_bytes_in"]["response"]["data"]["result"]
        cluster_b = next(item for item in samples if item["metric"]["lab_cluster"] == "cluster-b")
        cluster_b["metric"]["lab_cluster"] = "cluster-a"
        cluster_b["metric"]["kafka_cluster_id"] = "kraft-a-001"
    else:
        raise AssertionError(f"unsupported category: {category}")
    queries_path.write_text(json.dumps(queries), encoding="utf-8")


@pytest.mark.parametrize(
    "category",
    [
        "missing_metric",
        "missing_label",
        "wrong_type",
        "quota_scope",
        "throttle_observation",
        "rate_distinction",
        "target_health",
        "cluster_selector",
        "high_cardinality",
        "evidence_stale",
    ],
)
def test_validator_fails_closed_with_distinct_json_categories(tmp_path: Path, category: str) -> None:
    evidence = _build_valid_evidence(tmp_path)
    _mutate_evidence(evidence, category)

    result = _run_validator(evidence)

    assert result.returncode == 1
    details = json.loads((evidence / "validator-result.json").read_text(encoding="utf-8"))
    assert {failure["category"] for failure in details["failures"]} == {category}
    matching_failures = [failure for failure in details["failures"] if failure["category"] == category]
    assert matching_failures, details
    assert all(failure["message"] for failure in matching_failures)
    assert all("artifact" in failure for failure in matching_failures)

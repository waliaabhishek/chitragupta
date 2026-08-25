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
from decimal import Decimal, InvalidOperation
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


def _run_principal_validator(evidence: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            sys.executable,
            str(LAB_DIR / "scripts" / "validate_evidence.py"),
            "--evidence-dir",
            str(evidence),
            "--principal-contract",
        ],
        capture_output=True,
        text=True,
        check=False,
    )


def _run_principal_validator_for_lab(lab_dir: Path, evidence: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            sys.executable,
            str(lab_dir / "scripts" / "validate_evidence.py"),
            "--evidence-dir",
            str(evidence),
            "--principal-contract",
        ],
        capture_output=True,
        text=True,
        check=False,
    )


def _lab_with_principal_expected_states(
    tmp_path: Path,
    *,
    ingress: str,
    egress: str,
    scrape_interval_seconds: int = 60,
    max_gap_seconds: int = 120,
) -> Path:
    lab = _copy_lab(tmp_path / "lab")
    contract_path = lab / "contracts" / "principal-allocation-contract.yaml"
    contract = _read_yaml(contract_path)
    expected = EXPECTED_CONTRACT["principal_contract"]
    contract["scrape_interval_seconds"] = scrape_interval_seconds
    contract["max_gap_seconds"] = max_gap_seconds
    contract["expected_live_identities"] = expected["required_live_identities"]
    contract["expected_live_states"] = {"ingress": ingress, "egress": egress}
    contract["expected_live_balances"] = expected["required_live_balances"]
    contract_path.write_text(yaml.safe_dump(contract, sort_keys=False), encoding="utf-8")
    return lab


def _principal_fixture(name: str = "principal-prometheus-complete.json") -> dict[str, Any]:
    payload = _read_json(FIXTURES_DIR / name)
    assert isinstance(payload, dict), f"Expected mapping fixture at {name}"
    return payload


def _principal_query(payload: dict[str, Any], name: str) -> dict[str, Any]:
    query = payload["queries"][name]
    assert isinstance(query, dict), f"Expected query fixture {name}"
    return query


def _principal_matrix_results(payload: dict[str, Any], name: str) -> list[dict[str, Any]]:
    results = _principal_query(payload, name)["response"]["data"]["result"]
    assert isinstance(results, list), f"Expected matrix result list for {name}"
    return results


def _principal_observed_timestamp_deltas(payload: dict[str, Any], name: str) -> list[list[int | float]]:
    query = payload["queries"].get(name)
    response = query.get("response") if isinstance(query, dict) else None
    data = response.get("data") if isinstance(response, dict) else None
    results = data.get("result") if isinstance(data, dict) else None
    if not isinstance(results, list):
        return []
    deltas_by_series: list[list[int | float]] = []
    for result in results:
        values = result.get("values") if isinstance(result, dict) else None
        if not isinstance(values, list):
            deltas_by_series.append([])
            continue
        timestamps: list[Decimal] = []
        for sample in values:
            if not isinstance(sample, list) or len(sample) != 2:
                timestamps = []
                break
            try:
                timestamp = Decimal(str(sample[0]))
            except InvalidOperation, TypeError, ValueError:
                timestamps = []
                break
            if not timestamp.is_finite():
                timestamps = []
                break
            timestamps.append(timestamp)
        timestamps.sort()
        deltas: list[int | float] = []
        for previous, current in zip(timestamps, timestamps[1:], strict=False):
            delta = current - previous
            deltas.append(int(delta) if delta == delta.to_integral_value() else float(delta))
        deltas_by_series.append(deltas)
    return deltas_by_series


def _write_principal_evidence(tmp_path: Path, payload: dict[str, Any] | None = None) -> Path:
    tmp_path.mkdir(parents=True, exist_ok=True)
    evidence = _build_valid_evidence(tmp_path)
    principal = payload or _principal_fixture()
    sampling_contract = EXPECTED_CONTRACT["principal_contract"]
    principal_window = {
        **principal["window"],
        "sampling_resolution": {
            "declared_scrape_interval_seconds": principal["window"]["scrape_interval_seconds"],
            "declared_max_gap_seconds": principal["window"]["max_gap_seconds"],
            "observed_timestamp_deltas_seconds": {
                name: _principal_observed_timestamp_deltas(principal, name)
                for name in ("scope", "quota_produce", "quota_fetch")
            },
            "quota_rate_window": sampling_contract["quota_rate_window"],
            "estimate": sampling_contract["sampling_semantics"]["estimate"],
            "byte_exact": sampling_contract["sampling_semantics"]["byte_exact"],
            "limitation": "quota weights are monitoring-resolution estimates, not byte-exact totals",
        },
    }
    (evidence / "principal-window.json").write_text(json.dumps(principal_window), encoding="utf-8")
    queries = {name: principal["queries"][name] for name in ("scope", "quota_produce", "quota_fetch")}
    (evidence / "principal-raw-query-results.json").write_text(json.dumps(queries), encoding="utf-8")
    scope_evidence = {
        **principal["scope_evidence"],
        "query": principal["queries"]["scope"]["query"],
        "window": principal["window"],
        "response": principal["queries"]["scope"]["response"],
    }
    (evidence / "principal-scope-evidence.json").write_text(json.dumps(scope_evidence), encoding="utf-8")
    return evidence


def _set_principal_weights(
    payload: dict[str, Any],
    direction: str,
    *,
    user: int,
    client_only: int,
    integration_seconds: int | None = None,
) -> None:
    query_name = {"ingress": "quota_produce", "egress": "quota_fetch"}[direction]
    duration = Decimal(str(integration_seconds or payload["window"]["duration_seconds"]))
    user_first = Decimal(user) / 2
    values_by_scope = {
        "user": user_first / duration,
        "user-client": (Decimal(user) - user_first) / duration,
        "client-id": Decimal(client_only) / duration,
    }
    for series in _principal_matrix_results(payload, query_name):
        scope = series["metric"]["quota_scope"]
        value = format(values_by_scope[scope], "f")
        series["values"] = [[timestamp, value] for timestamp, _ in series["values"]]


def _introduce_internal_quota_gap(payload: dict[str, Any], direction: str) -> None:
    query_name = {"ingress": "quota_produce", "egress": "quota_fetch"}[direction]
    start = payload["window"]["start_timestamp"]
    for series in _principal_matrix_results(payload, query_name):
        series["values"] = [sample for sample in series["values"] if sample[0] not in {start + 60, start + 120}]


def _shift_principal_source_window(payload: dict[str, Any], seconds: int) -> None:
    for key in ("start_timestamp", "end_timestamp", "evaluation_timestamp"):
        payload["window"][key] += seconds
    for query in payload["queries"].values():
        results = query["response"]["data"]["result"]
        for result in results:
            if "values" in result:
                result["values"] = [[timestamp + seconds, value] for timestamp, value in result["values"]]
            if "value" in result:
                result["value"][0] += seconds


def _principal_direction(artifact: dict[str, Any], direction: str) -> dict[str, Any]:
    clusters = artifact["clusters"]
    assert isinstance(clusters, dict)
    cluster = clusters["cluster-a"]
    assert isinstance(cluster, dict)
    directions = cluster["directions"]
    assert isinstance(directions, dict)
    result = directions[direction]
    assert isinstance(result, dict)
    return result


def _load_capture_module() -> Any:
    path = LAB_DIR / "scripts" / "capture_evidence.py"
    spec = importlib.util.spec_from_file_location("lab_capture_evidence", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _load_validator_module(lab_dir: Path = LAB_DIR) -> Any:
    path = lab_dir / "scripts" / "validate_evidence.py"
    spec = importlib.util.spec_from_file_location("lab_validate_evidence", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_principal_validator_imports_the_production_quota_evaluator_instead_of_owning_parallel_math() -> None:
    source = _read_text(LAB_DIR / "scripts" / "validate_evidence.py")

    assert "plugins.self_managed_kafka.principal_attribution" in source
    assert "evaluate_quota_direction" in source
    assert "allocate_principal_money" in source
    assert "allocate_static_even" in source


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
    assert "./scripts/lab.sh evidence" in readme_text
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


def test_principal_contract_declares_canonical_identity_temporal_policy_and_independent_marginals() -> None:
    contract = _read_yaml(LAB_DIR / "contracts" / "principal-allocation-contract.yaml")

    assert contract["version"] == 1
    assert contract["identity"] == {
        "canonical_prefix": "User:",
        "preserve_suffix": "case-sensitive",
        "user_scope_client_id": "not_applicable",
        "ownership": {
            "mappings": {"User:user-only": "team-data"},
            "default_owner": "UNASSIGNED",
        },
    }
    assert contract["logical_billing_interval"] == "[start,end)"
    assert contract["provider_source_membership"] == "(start,end]"
    assert contract["scope_first"] is True
    assert contract["scrape_interval_seconds"] == 5
    assert contract["max_gap_seconds"] == 10
    assert contract["quota_rate_window"] == {
        "source": "kafka_default",
        "complete_windows": 10,
        "window_seconds": 1,
        "includes_current_window": True,
    }
    assert contract["sampling_semantics"] == {
        "completeness_basis": "configured_datasource_cadence_and_gap",
        "estimate": "monitoring_resolution",
        "byte_exact": False,
        "production_interval_fixed": False,
    }
    assert contract["retention_days"] >= 14
    assert contract["quota_scopes"] == ["user", "client-id", "user-client"]
    assert contract["expected_live_identities"] == {
        "mapped_user": {"identity": "User:user-only", "owner": "team-data"},
        "unmapped_user": {"identity": "User:shared-user", "owner": "UNASSIGNED"},
        "client_only_label": "client-only",
    }
    assert contract["expected_live_states"] == {"ingress": "degraded", "egress": "degraded"}
    assert contract["expected_live_balances"] == {"ingress": "12.0000", "egress": "3.0000"}
    assert "overcoverage_tolerance" not in contract
    assert contract["directions"] == {
        "ingress": {"quota_type": "Produce"},
        "egress": {"quota_type": "Fetch"},
    }
    assert contract["allocation_rules"] == {
        "weights": {
            "user": {"source_scopes": ["user", "user-client"], "symbol": "q_i"},
            "client_only": {"source_scopes": ["client-id"], "symbol": "c"},
            "denominator": "total_valid_quota_weight",
        },
        "completeness": {
            "required": ["configuration", "target", "samples", "identity"],
            "samples": "configured_datasource_cadence_and_gap",
        },
        "state_matrix": [
            {"when": "invalid_or_incomplete", "state": "unavailable"},
            {"when": "complete_total_weight_zero", "state": "zero_usage"},
            {"when": "complete_positive_weight_without_client_only", "state": "ready"},
            {"when": "complete_positive_weight_with_client_only", "state": "degraded"},
        ],
        "monetary": {
            "pool_source": "configured_direction_monetary_pool",
            "client_only": "unallocated",
            "rounding": {"quantum": "0.0001", "mode": "ROUND_DOWN", "residual": "explicit_unallocated"},
        },
        "fixed_categories": {
            "shared": {"policy": "unattributed", "measured_usage": False, "rounding": "ROUND_DOWN"},
            "static_even_v1": {
                "identities": "sorted_ascending",
                "measured_usage": False,
                "rounding": "ROUND_DOWN",
            },
        },
    }
    assert contract["principal_topic_relationship"] == "independent_marginals"
    assert contract["forbidden_inferences"] == ["principal_by_topic", "topic_owner_rollup"]
    assert contract["money"] == {"quantum": "0.0001", "rounding": "ROUND_DOWN"}
    assert contract["identity_churn"] == "case-sensitive identities remain distinct"
    assert contract["lifecycle"] == {
        "principal_day_terminal": True,
        "later_days_continue": True,
        "topic_lane_independent": True,
        "reprocess": {"mode": "explicit", "replaces_date": True, "scope": "cluster-day"},
    }
    assert contract["demonstration_pools"] == {
        "ingress": "12.0000",
        "egress": "3.0000",
        "compute": "4.0000",
        "storage": "5.0000",
        "shared": "2.0000",
    }
    direction_schema = contract["artifact_schema"]["direction"]
    assert direction_schema["required"] == [
        "pool",
        "weights",
        "coverage",
        "sampling_resolution",
        "state",
        "reason",
        "users",
        "client_only",
        "rounding_residual",
        "unallocated",
        "balance",
    ]
    assert {"quantities", "uncovered"}.isdisjoint(direction_schema["types"])
    assert (
        contract["artifact_schema"]["fixed_category"]
        == EXPECTED_CONTRACT["principal_contract"]["artifact_schema"]["fixed_category"]
    )
    assert contract["error_categories"] == [
        "principal_scope",
        "principal_contract",
        "principal_identity",
        "principal_window",
        "principal_metric",
        "principal_reconciliation",
        "principal_expected_state",
    ]


def test_principal_contract_preserves_broker_topic_metrics_as_non_principal_telemetry() -> None:
    for cluster in ("a", "b"):
        metrics = _parse_prometheus_metrics(FIXTURES_DIR / f"exporter-cluster-{cluster}.metrics")
        for name in (
            "kafka_server_brokertopicmetrics_alltopics_bytesin_total",
            "kafka_server_brokertopicmetrics_alltopics_bytesout_total",
            "kafka_server_brokertopicmetrics_bytesin_total",
            "kafka_server_brokertopicmetrics_bytesout_total",
        ):
            for labels in metrics[name]:
                assert "user" not in labels
                assert "principal" not in labels
                assert "client_id" not in labels


@pytest.mark.parametrize(
    "mutation",
    [
        pytest.param("remove", id="remove"),
        pytest.param("add", id="add"),
        pytest.param("change", id="change"),
    ],
)
def test_principal_direction_is_invariant_to_broker_topic_metric_changes(mutation: str) -> None:
    validator = _load_validator_module()
    contract = _read_yaml(LAB_DIR / "contracts" / "principal-allocation-contract.yaml")
    complete = _principal_fixture()
    mutated = _principal_fixture()
    broker_topic_query_names = ("pool_ingress_raw", "pool_ingress", "pool_egress_raw", "pool_egress")
    if mutation == "remove":
        for name in broker_topic_query_names:
            mutated["queries"].pop(name)
    elif mutation == "add":
        for name in broker_topic_query_names:
            mutated["queries"][name] = {
                "query": "broker_topic_metric_is_not_a_principal_input",
                "response": {"status": "error", "error": "not a principal query"},
            }
    else:
        for name in broker_topic_query_names:
            query = mutated["queries"][name]
            query["response"]["data"]["result"][0]["values"] = [[1787097540, "999999999"]]

    complete_direction, complete_failure = validator._principal_direction(
        complete["queries"],
        "ingress",
        contract["directions"]["ingress"],
        complete["window"],
        contract,
    )
    changed_direction, changed_failure = validator._principal_direction(
        mutated["queries"],
        "ingress",
        contract["directions"]["ingress"],
        mutated["window"],
        contract,
    )

    assert complete_failure is None
    assert changed_failure is None
    assert complete_direction is not None
    assert changed_direction is not None
    assert changed_direction["state"] == complete_direction["state"]
    assert changed_direction["weights"] == complete_direction["weights"]
    assert changed_direction["users"] == complete_direction["users"]
    assert changed_direction["client_only"] == complete_direction["client_only"]
    assert changed_direction["unallocated"] == complete_direction["unallocated"]
    assert changed_direction["balance"] == complete_direction["balance"]


def test_principal_and_topic_marginals_are_alternative_exact_balances_not_a_combined_total() -> None:
    contract = _read_yaml(LAB_DIR / "contracts" / "principal-allocation-contract.yaml")
    monetary_pool = Decimal(contract["demonstration_pools"]["ingress"])
    marginals = {
        "principal": {"allocated": Decimal("10.2856"), "unallocated": Decimal("1.7144")},
        "topic": {"allocated": Decimal("12.0000"), "unallocated": Decimal("0.0000")},
    }

    assert contract["principal_topic_relationship"] == "independent_marginals"
    assert contract["forbidden_inferences"] == ["principal_by_topic", "topic_owner_rollup"]
    assert all(summary["allocated"] + summary["unallocated"] == monetary_pool for summary in marginals.values())
    assert sum(summary["allocated"] + summary["unallocated"] for summary in marginals.values()) == monetary_pool * 2


def test_lab_validate_entrypoint_accepts_principal_contract_flag_before_runtime_preflight(tmp_path: Path) -> None:
    lab = _copy_lab(tmp_path)
    environment, _ = _fake_command_environment(tmp_path)

    result = _run_lab(lab, environment, "validate", "--window", "5m", "--principal-contract")

    assert result.returncode == 7
    assert "unknown validation argument" not in result.stderr
    assert "required local file is absent: .env" in result.stderr
    details = _read_json(lab / "evidence" / "latest" / "validator-result.json")
    assert details["failures"] == [
        {"category": "missing_metric", "message": "required local file is absent: .env", "artifact": None}
    ]


@pytest.mark.parametrize(
    ("condition", "message"),
    [
        pytest.param("missing", "principal allocation contract is absent", id="missing"),
        pytest.param("malformed", "principal allocation contract is invalid", id="malformed"),
        pytest.param("nested", "principal allocation contract is invalid", id="nested"),
    ],
)
def test_lab_validate_rejects_invalid_principal_contracts_before_runtime_preflight(
    tmp_path: Path, condition: str, message: str
) -> None:
    lab = _copy_lab(tmp_path / "lab")
    contract_path = lab / "contracts" / "principal-allocation-contract.yaml"
    if condition == "missing":
        contract_path.unlink()
    elif condition == "malformed":
        contract_path.write_text("[not-a-mapping", encoding="utf-8")
    else:
        contract = _read_yaml(contract_path)
        contract["identity"]["ownership"]["mappings"] = []
        contract_path.write_text(yaml.safe_dump(contract, sort_keys=False), encoding="utf-8")

    (lab / ".env").write_text("PROMETHEUS_PORT=9090\n", encoding="utf-8")
    generated = lab / "generated"
    generated.mkdir()
    for name in ("generation.json", "prometheus.yml", "jmx-a.yml", "jmx-b.yml"):
        (generated / name).write_text("{}\n", encoding="utf-8")
    environment, command_log = _fake_command_environment(tmp_path)

    result = _run_lab(lab, environment, "validate", "--window", "5m", "--principal-contract")

    assert result.returncode == 7
    assert "evidence_capture_failed:" not in result.stderr
    assert "missing_metric" not in result.stderr
    assert _read_json(lab / "evidence" / "latest" / "validator-result.json")["failures"] == [
        {
            "category": "principal_contract",
            "message": message,
            "artifact": "principal-allocation-contract.yaml",
        }
    ]
    assert not command_log.exists()


def test_principal_capture_blocks_before_quota_or_pool_dependencies_when_target_scope_is_not_proven(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    capture = _load_capture_module()
    fixture = _principal_fixture()
    blocked_response = json.loads(json.dumps(fixture["queries"]["scope"]["response"]))
    blocked_response["data"]["result"][0]["values"][-1][1] = "0"
    calls: list[tuple[str, dict[str, str] | None]] = []
    frozen_end = datetime.fromtimestamp(fixture["window"]["end_timestamp"], tz=UTC)

    class FrozenDatetime:
        @classmethod
        def now(cls, tz: Any | None = None) -> datetime:
            assert tz is UTC
            return frozen_end

    capture.EVIDENCE_DIR = tmp_path / "evidence"
    monkeypatch.setattr(capture, "datetime", FrozenDatetime)
    monkeypatch.setattr(capture, "_load_env", lambda: {"PROMETHEUS_PORT": "9090"})

    def fake_prometheus_api(base_url: str, path: str, parameters: dict[str, str] | None = None) -> dict[str, Any]:
        assert base_url == "http://127.0.0.1:9090"
        calls.append((path, parameters))
        assert path == "/api/v1/query"
        assert parameters is not None
        assert parameters["query"] == (
            'up{job="kafka-jmx",lab_cluster="cluster-a",kafka_cluster_id="kraft-a-001"}[310s]'
        )
        return blocked_response

    monkeypatch.setattr(capture, "_prometheus_api", fake_prometheus_api)
    monkeypatch.setattr(
        capture,
        "_compose",
        lambda *arguments: pytest.fail(f"scope failure must suppress compose dependency: {arguments}"),
    )

    run_dir = capture.capture(label="scope-blocked", window="5m", principal_contract=True)

    assert calls == [
        (
            "/api/v1/query",
            {
                "query": 'up{job="kafka-jmx",lab_cluster="cluster-a",kafka_cluster_id="kraft-a-001"}[310s]',
                "time": str(int(frozen_end.timestamp())),
            },
        )
    ]
    scope = _read_json(run_dir / "principal-scope-evidence.json")
    assert scope == {
        "status": "blocked",
        "category": "principal_scope",
        "message": "expected target scope is not healthy for the complete logical interval",
        "artifact": "principal-scope-evidence.json",
        "query": 'up{job="kafka-jmx",lab_cluster="cluster-a",kafka_cluster_id="kraft-a-001"}[310s]',
        "window": {**fixture["window"], "scrape_interval_seconds": 5, "max_gap_seconds": 10},
        "response": blocked_response,
    }
    assert sorted(path.name for path in run_dir.iterdir()) == ["principal-scope-evidence.json"]


def test_principal_scope_coverage_accepts_fractional_prometheus_timestamps() -> None:
    capture = _load_capture_module()
    fixture = _principal_fixture()
    scope_response = json.loads(json.dumps(fixture["queries"]["scope"]["response"]))
    for result in scope_response["data"]["result"]:
        for sample in result["values"]:
            sample[0] = float(sample[0]) + 0.5

    assert capture._scope_is_proven(scope_response, fixture["window"], expected_targets=1)


def test_principal_capture_preserves_scope_query_error_without_requesting_non_scope_artifacts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    capture = _load_capture_module()
    fixture = _principal_fixture()
    frozen_end = datetime.fromtimestamp(fixture["window"]["end_timestamp"], tz=UTC)
    scope_error = {
        "status": "error",
        "errorType": "timeout",
        "error": "query timed out",
        "data": {"resultType": "matrix", "result": []},
    }

    class FrozenDatetime:
        @classmethod
        def now(cls, tz: Any | None = None) -> datetime:
            assert tz is UTC
            return frozen_end

    capture.EVIDENCE_DIR = tmp_path / "evidence"
    monkeypatch.setattr(capture, "datetime", FrozenDatetime)
    monkeypatch.setattr(capture, "_load_env", lambda: {"PROMETHEUS_PORT": "9090"})
    monkeypatch.setattr(capture, "_http_text", lambda url: json.dumps(scope_error))
    monkeypatch.setattr(
        capture,
        "_compose",
        lambda *arguments: pytest.fail(f"scope query error must suppress compose dependency: {arguments}"),
    )

    run_dir = capture.capture(label="scope-error", window="5m", principal_contract=True)

    scope = _read_json(run_dir / "principal-scope-evidence.json")
    assert scope == {
        "status": "blocked",
        "category": "principal_scope",
        "message": "principal scope query failed: query timed out",
        "artifact": "principal-scope-evidence.json",
        "response": scope_error,
    }
    assert not (run_dir / "principal-raw-query-results.json").exists()
    assert not (run_dir / "principal-allocation-demonstration.json").exists()
    result = _run_principal_validator(run_dir)
    assert result.returncode == 7
    assert _read_json(run_dir / "validator-result.json")["failures"] == [
        {
            "category": "principal_scope",
            "message": "principal scope query failed: query timed out",
            "artifact": "principal-scope-evidence.json",
        }
    ]


@pytest.mark.parametrize(
    ("failure", "message"),
    [
        pytest.param("transport", "principal scope query transport failed: connection reset", id="transport"),
        pytest.param("decode", "principal scope query response could not be decoded", id="decode"),
    ],
)
def test_principal_capture_routes_scope_transport_and_decode_failures_to_validation_contract(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failure: str, message: str
) -> None:
    capture = _load_capture_module()
    fixture = _principal_fixture()
    frozen_end = datetime.fromtimestamp(fixture["window"]["end_timestamp"], tz=UTC)

    class FrozenDatetime:
        @classmethod
        def now(cls, tz: Any | None = None) -> datetime:
            assert tz is UTC
            return frozen_end

    def fake_http_text(url: str) -> str:
        assert url.startswith("http://127.0.0.1:9090/api/v1/query?")
        if failure == "transport":
            raise OSError("connection reset")
        return "{not-json"

    capture.EVIDENCE_DIR = tmp_path / "evidence"
    monkeypatch.setattr(capture, "datetime", FrozenDatetime)
    monkeypatch.setattr(capture, "_load_env", lambda: {"PROMETHEUS_PORT": "9090"})
    monkeypatch.setattr(capture, "_http_text", fake_http_text)
    monkeypatch.setattr(
        capture,
        "_compose",
        lambda *arguments: pytest.fail(f"scope failure must suppress compose dependency: {arguments}"),
    )

    run_dir = capture.capture(label=f"scope-{failure}", window="5m", principal_contract=True)

    assert _read_json(run_dir / "principal-scope-evidence.json") == {
        "status": "blocked",
        "category": "principal_scope",
        "message": message,
        "artifact": "principal-scope-evidence.json",
    }
    assert not (run_dir / "principal-raw-query-results.json").exists()
    result = _run_principal_validator(run_dir)
    assert result.returncode == 7
    assert _read_json(run_dir / "validator-result.json")["failures"] == [
        {
            "category": "principal_scope",
            "message": message,
            "artifact": "principal-scope-evidence.json",
        }
    ]


@pytest.mark.parametrize(
    ("condition", "message"),
    [
        pytest.param("missing", "principal allocation contract is absent", id="missing"),
        pytest.param("malformed", "principal allocation contract is invalid", id="malformed"),
    ],
)
def test_principal_validator_routes_missing_and_malformed_contracts_to_exit_seven(
    tmp_path: Path, condition: str, message: str
) -> None:
    lab = _copy_lab(tmp_path / "lab")
    contract_path = lab / "contracts" / "principal-allocation-contract.yaml"
    if condition == "missing":
        contract_path.unlink()
    else:
        contract_path.write_text("[not-a-mapping", encoding="utf-8")
    evidence = _write_principal_evidence(tmp_path / "evidence")

    result = _run_principal_validator_for_lab(lab, evidence)

    assert result.returncode == 7
    assert _read_json(evidence / "validator-result.json")["failures"] == [
        {
            "category": "principal_contract",
            "message": message,
            "artifact": "principal-allocation-contract.yaml",
        }
    ]
    assert not (evidence / "principal-allocation-demonstration.json").exists()


@pytest.mark.parametrize(
    "mutation",
    [
        pytest.param("retention", id="retention"),
        pytest.param("identity-suffix", id="identity-suffix"),
        pytest.param("client-label", id="client-label"),
        pytest.param("identity-churn", id="identity-churn"),
        pytest.param("marginal-relationship", id="marginal-relationship"),
        pytest.param("forbidden-inference", id="forbidden-inference"),
        pytest.param("reprocess", id="reprocess"),
        pytest.param("money-quantum", id="money-quantum"),
    ],
)
def test_principal_validator_rejects_mutated_policy_contract_invariants(tmp_path: Path, mutation: str) -> None:
    lab = _lab_with_principal_expected_states(tmp_path, ingress="degraded", egress="degraded")
    contract_path = lab / "contracts" / "principal-allocation-contract.yaml"
    contract = _read_yaml(contract_path)
    if mutation == "retention":
        contract["retention_days"] = 13
    elif mutation == "identity-suffix":
        contract["identity"]["preserve_suffix"] = "case-insensitive"
    elif mutation == "client-label":
        contract["identity"]["user_scope_client_id"] = "client-id"
    elif mutation == "identity-churn":
        contract["identity_churn"] = "case-insensitive identities merge"
    elif mutation == "marginal-relationship":
        contract["principal_topic_relationship"] = "topic-derived-principals"
    elif mutation == "forbidden-inference":
        contract["forbidden_inferences"] = ["principal_by_topic"]
    elif mutation == "reprocess":
        contract["lifecycle"]["reprocess"] = {"mode": "automatic", "replaces_date": True, "scope": "cluster-day"}
    else:
        contract["money"]["quantum"] = "0.01"
    contract_path.write_text(yaml.safe_dump(contract, sort_keys=False), encoding="utf-8")
    evidence = _write_principal_evidence(tmp_path / "evidence")

    result = _run_principal_validator_for_lab(lab, evidence)

    assert result.returncode == 7
    assert _read_json(evidence / "validator-result.json")["failures"] == [
        {
            "category": "principal_contract",
            "message": "principal allocation contract is invalid",
            "artifact": "principal-allocation-contract.yaml",
        }
    ]
    assert not (evidence / "principal-allocation-demonstration.json").exists()


def test_principal_capture_uses_only_quota_queries_at_an_accepted_production_cadence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    lab = _lab_with_principal_expected_states(
        tmp_path,
        ingress="degraded",
        egress="degraded",
        scrape_interval_seconds=30,
        max_gap_seconds=60,
    )
    capture = _load_capture_module()
    fixture = _principal_fixture()
    frozen_end = datetime.fromtimestamp(fixture["window"]["end_timestamp"], tz=UTC)
    requested_queries: list[tuple[str, dict[str, str] | None]] = []
    generated = lab / "generated"
    generated.mkdir()
    (generated / "generation.json").write_text(
        json.dumps(
            {
                "generation_id": "generation-2",
                "previous_generation_id": "generation-1",
                "kafka_cluster_ids": {"cluster-a": "kraft-a-001", "cluster-b": "kraft-b-001"},
            }
        ),
        encoding="utf-8",
    )

    class FrozenDatetime:
        @classmethod
        def now(cls, tz: Any | None = None) -> datetime:
            assert tz is UTC
            return frozen_end

    def fake_prometheus_api(base_url: str, path: str, parameters: dict[str, str] | None = None) -> dict[str, Any]:
        assert base_url == "http://127.0.0.1:9090"
        requested_queries.append((path, parameters))
        if path != "/api/v1/query":
            return {"status": "success", "data": {}}
        assert parameters is not None
        if parameters["query"].startswith("up{"):
            return json.loads(json.dumps(fixture["queries"]["scope"]["response"]))
        if 'quota_type="Produce"' in parameters["query"]:
            return json.loads(json.dumps(fixture["queries"]["quota_produce"]["response"]))
        if 'quota_type="Fetch"' in parameters["query"]:
            return json.loads(json.dumps(fixture["queries"]["quota_fetch"]["response"]))
        pytest.fail(f"principal capture issued a non-quota query: {parameters['query']}")

    def fake_compose(*arguments: str) -> subprocess.CompletedProcess[str]:
        output = "0 /tmp\n" if arguments[:2] == ("exec", "-T") and "du" in arguments else ""
        return subprocess.CompletedProcess(arguments, 0, stdout=output, stderr="")

    capture.LAB_DIR = lab
    capture.PRINCIPAL_CONTRACT_PATH = lab / "contracts" / "principal-allocation-contract.yaml"
    capture.EVIDENCE_DIR = tmp_path / "evidence"
    monkeypatch.setattr(capture, "datetime", FrozenDatetime)
    monkeypatch.setattr(
        capture,
        "_load_env",
        lambda: {"PROMETHEUS_PORT": "9090", "JMX_EXPORTER_A_PORT": "7071", "JMX_EXPORTER_B_PORT": "7072"},
    )
    monkeypatch.setattr(capture, "_prometheus_api", fake_prometheus_api)
    monkeypatch.setattr(capture, "_compose", fake_compose)
    monkeypatch.setattr(capture, "_http_text", lambda url: "")

    run_dir = capture.capture(label="principal-contract", window="5m", principal_contract=True)

    expected_window = {**fixture["window"], "scrape_interval_seconds": 30, "max_gap_seconds": 60}
    principal_window = _read_json(run_dir / "principal-window.json")
    assert set(principal_window) == {*expected_window, "sampling_resolution"}
    assert {field: principal_window[field] for field in expected_window} == expected_window
    assert principal_window["sampling_resolution"] == {
        "declared_scrape_interval_seconds": 30,
        "declared_max_gap_seconds": 60,
        "observed_timestamp_deltas_seconds": {
            "scope": [[60, 60, 60, 60, 60, 60]],
            "quota_produce": [[60, 60, 60, 60, 60, 60]] * 3,
            "quota_fetch": [[60, 60, 60, 60, 60, 60]] * 3,
        },
        "quota_rate_window": {
            "source": "kafka_default",
            "complete_windows": 10,
            "window_seconds": 1,
            "includes_current_window": True,
        },
        "estimate": "monitoring_resolution",
        "byte_exact": False,
        "limitation": "quota weights are monitoring-resolution estimates, not byte-exact totals",
    }
    assert _read_json(run_dir / "principal-scope-evidence.json") == fixture["scope_evidence"]
    raw_queries = _read_json(run_dir / "principal-raw-query-results.json")
    assert set(raw_queries) == {"scope", "quota_produce", "quota_fetch"}
    assert raw_queries["scope"]["response"] == fixture["queries"]["scope"]["response"]
    assert raw_queries["quota_produce"]["response"] == fixture["queries"]["quota_produce"]["response"]
    assert raw_queries["quota_fetch"]["response"] == fixture["queries"]["quota_fetch"]["response"]
    expected_requests = [
        (
            "/api/v1/query",
            {
                "query": raw_queries[query_name]["query"],
                "time": str(fixture["window"]["end_timestamp"]),
            },
        )
        for query_name in ("scope", "quota_produce", "quota_fetch")
    ]
    assert requested_queries[: len(expected_requests)] == expected_requests
    assert all("brokertopicmetrics" not in query["query"] for query in raw_queries.values())
    assert (capture.EVIDENCE_DIR / "latest").resolve() == run_dir


def test_actual_principal_capture_output_passes_through_the_actual_validator(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    lab = _lab_with_principal_expected_states(
        tmp_path,
        ingress="degraded",
        egress="degraded",
        scrape_interval_seconds=30,
        max_gap_seconds=60,
    )
    capture = _load_capture_module()
    validator = _load_validator_module(lab)
    principal = _principal_fixture()
    response_fixture_root = tmp_path / "response-fixtures"
    response_fixture_root.mkdir()
    fixture_evidence = _build_valid_evidence(response_fixture_root)
    standard_queries = _read_json(fixture_evidence / "prometheus-query-results.json")
    frozen_end = datetime.fromtimestamp(principal["window"]["end_timestamp"], tz=UTC)
    generated = lab / "generated"
    generated.mkdir()
    (generated / "generation.json").write_text(
        json.dumps(
            {
                "generation_id": "generation-2",
                "previous_generation_id": "generation-1",
                "kafka_cluster_ids": {"cluster-a": "kraft-a-001", "cluster-b": "kraft-b-001"},
            }
        ),
        encoding="utf-8",
    )

    class FrozenDatetime:
        @classmethod
        def now(cls, tz: Any | None = None) -> datetime:
            assert tz is UTC
            return frozen_end

        @classmethod
        def fromisoformat(cls, value: str) -> datetime:
            return datetime.fromisoformat(value)

    responses_by_query = {
        "kafka_server_brokertopicmetrics_alltopics_bytesin_total": standard_queries["alltopics_bytes_in"]["response"],
        "kafka_server_brokertopicmetrics_alltopics_bytesout_total": standard_queries["alltopics_bytes_out"]["response"],
        "kafka_server_brokertopicmetrics_bytesin_total": standard_queries["topic_bytes_in"]["response"],
        "kafka_server_brokertopicmetrics_bytesout_total": standard_queries["topic_bytes_out"]["response"],
        "kafka_log_log_size": standard_queries["partition_log_size"]["response"],
        "kafka_server_quota_byte_rate": standard_queries["quota_byte_rate"]["response"],
        "kafka_server_quota_throttle_time_ms": standard_queries["quota_throttle_time"]["response"],
        'up{job="kafka-jmx"}': standard_queries["up"]["response"],
        'jmx_scrape_error{job="kafka-jmx"}': standard_queries["jmx_scrape_error"]["response"],
        "rate(kafka_server_brokertopicmetrics_bytesin_total[5m])": standard_queries["topic_bytes_in_rate"]["response"],
        "rate(kafka_server_brokertopicmetrics_bytesout_total[5m])": standard_queries["topic_bytes_out_rate"][
            "response"
        ],
        (
            'max_over_time(kafka_server_quota_throttle_time_ms{quota_type="Produce",quota_scope="user-client"}[5m])'
        ): standard_queries["produce_throttle_max"]["response"],
        (
            'max_over_time(kafka_server_quota_throttle_time_ms{quota_type="Fetch",quota_scope="user-client"}[5m])'
        ): standard_queries["fetch_throttle_max"]["response"],
    }

    def fake_prometheus_api(base_url: str, path: str, parameters: dict[str, str] | None = None) -> dict[str, Any]:
        assert base_url == "http://127.0.0.1:9090"
        if path == "/api/v1/targets":
            return _read_json(fixture_evidence / "prometheus-targets.json")
        if path == "/api/v1/metadata":
            return _read_json(fixture_evidence / "prometheus-metadata.json")
        assert path == "/api/v1/query"
        assert parameters is not None
        if parameters["query"].startswith("up{") and "lab_cluster" in parameters["query"]:
            return json.loads(json.dumps(principal["queries"]["scope"]["response"]))
        if 'quota_type="Produce"' in parameters["query"]:
            return json.loads(json.dumps(principal["queries"]["quota_produce"]["response"]))
        if 'quota_type="Fetch"' in parameters["query"]:
            return json.loads(json.dumps(principal["queries"]["quota_fetch"]["response"]))
        response = responses_by_query.get(parameters["query"])
        assert response is not None, parameters["query"]
        return json.loads(json.dumps(response))

    def fake_compose(*arguments: str) -> subprocess.CompletedProcess[str]:
        if arguments[:3] == ("run", "--rm", "--no-deps"):
            cluster = arguments[-1].removeprefix("jmx-dump-")
            return subprocess.CompletedProcess(
                arguments,
                0,
                stdout=_read_text(FIXTURES_DIR / f"raw-jmx-cluster-{cluster}.jsonl"),
                stderr="",
            )
        if arguments[:2] == ("exec", "-T") and "du" in arguments:
            return subprocess.CompletedProcess(arguments, 0, stdout="1024 /tmp\n", stderr="")
        return subprocess.CompletedProcess(arguments, 0, stdout="", stderr="")

    def fake_http_text(url: str) -> str:
        if ":7071/metrics" in url:
            return _read_text(FIXTURES_DIR / "exporter-cluster-a.metrics")
        if ":7072/metrics" in url:
            return _read_text(FIXTURES_DIR / "exporter-cluster-b.metrics")
        raise AssertionError(f"Unexpected direct HTTP request: {url}")

    capture.LAB_DIR = lab
    capture.PRINCIPAL_CONTRACT_PATH = lab / "contracts" / "principal-allocation-contract.yaml"
    capture.EVIDENCE_DIR = tmp_path / "captured-evidence"
    monkeypatch.setattr(capture, "datetime", FrozenDatetime)
    monkeypatch.setattr(validator, "datetime", FrozenDatetime)
    monkeypatch.setattr(
        capture,
        "_load_env",
        lambda: {"PROMETHEUS_PORT": "9090", "JMX_EXPORTER_A_PORT": "7071", "JMX_EXPORTER_B_PORT": "7072"},
    )
    monkeypatch.setattr(capture, "_prometheus_api", fake_prometheus_api)
    monkeypatch.setattr(capture, "_compose", fake_compose)
    monkeypatch.setattr(capture, "_http_text", fake_http_text)

    captured = capture.capture(label="principal-contract", window="5m", principal_contract=True)
    validation = validator.validate(captured, require_recreated_state=False, principal_contract=True)

    assert validation["status"] == "pass", validation["failures"]
    assert validation["failures"] == []
    artifact = _read_json(captured / "principal-allocation-demonstration.json")
    assert artifact["clusters"]["cluster-a"]["state"] == "degraded"
    assert set(artifact["clusters"]["cluster-a"]["directions"]) == {"ingress", "egress"}
    for direction, client_only_weight in (("ingress", "15000"), ("egress", "6000")):
        allocation = _principal_direction(artifact, direction)
        users = {row["identity"]: row for row in allocation["users"]}
        assert users["User:user-only"]["owner"] == "team-data"
        assert users["User:shared-user"]["owner"] == "UNASSIGNED"
        assert allocation["client_only"]["weight"] == client_only_weight
        sampling = allocation["sampling_resolution"]
        assert sampling["declared_scrape_interval_seconds"] == 30
        assert sampling["declared_max_gap_seconds"] == 60
        assert sampling["estimate"] == "monitoring_resolution"
        assert sampling["byte_exact"] is False


@pytest.mark.parametrize(
    ("fixture_name", "mutation", "expected_complete", "expected_guard"),
    [
        ("principal-prometheus-complete.json", "none", True, 1787097600),
        ("principal-prometheus-missing-leading.json", "missing-leading", False, None),
        ("principal-prometheus-missing-trailing.json", "missing-trailing", False, 1787097600),
    ],
)
def test_principal_validator_uses_only_bounded_actual_source_samples_for_guards_and_coverage(
    tmp_path: Path, fixture_name: str, mutation: str, expected_complete: bool, expected_guard: int | None
) -> None:
    expected_states = {
        "none": ("degraded", "degraded"),
        "missing-leading": ("unavailable", "unavailable"),
        "missing-trailing": ("unavailable", "degraded"),
    }
    lab = _lab_with_principal_expected_states(
        tmp_path, ingress=expected_states[mutation][0], egress=expected_states[mutation][1]
    )
    payload = _principal_fixture()
    if mutation != "none":
        response = _principal_fixture(fixture_name)["response"]
        source_values = response["data"]["result"][0]["values"]
        if mutation == "missing-leading":
            for direction in ("quota_produce", "quota_fetch"):
                for series in _principal_matrix_results(payload, direction):
                    series["values"] = json.loads(json.dumps(source_values))
        elif mutation == "missing-trailing":
            for series in _principal_matrix_results(payload, "quota_produce"):
                series["values"] = json.loads(json.dumps(source_values))
    evidence = _write_principal_evidence(tmp_path / "evidence", payload)

    result = _run_principal_validator_for_lab(lab, evidence)

    if not expected_complete:
        assert result.returncode == 0, result.stderr
        artifact = _read_json(evidence / "principal-allocation-demonstration.json")
        assert _principal_direction(artifact, "ingress")["state"] == "unavailable"
        assert _principal_direction(artifact, "ingress")["unallocated"] == "12.0000"
        assert _principal_direction(artifact, "egress")["state"] == expected_states[mutation][1]
        return

    assert result.returncode == 0, result.stderr
    artifact = _read_json(evidence / "principal-allocation-demonstration.json")
    ingress = _principal_direction(artifact, "ingress")
    assert ingress["source_membership"] == "(start,end]"
    assert ingress["logical_billing_interval"] == "[start,end)"
    assert ingress["coverage"]["complete"] is True
    assert ingress["quota_sources"]["selected_leading_guard_timestamp"] == expected_guard
    assert ingress["quota_sources"]["synthetic_samples"] == []
    assert ingress["quota_sources"]["lookback_samples"] == []
    assert ingress["quota_sources"]["interpolated_segments"] == []
    assert ingress["quota_sources"]["over_max_gap_holds"] == []


def test_principal_validator_integrates_fractional_quota_timestamps_with_bounded_guards(
    tmp_path: Path,
) -> None:
    lab = _lab_with_principal_expected_states(tmp_path, ingress="degraded", egress="degraded")
    payload = _principal_fixture()
    for query_name in ("quota_produce", "quota_fetch"):
        for series in _principal_matrix_results(payload, query_name):
            for sample in series["values"]:
                sample[0] = float(sample[0]) + 0.5
    evidence = _write_principal_evidence(tmp_path / "evidence", payload)

    result = _run_principal_validator_for_lab(lab, evidence)

    assert result.returncode == 0, result.stderr
    artifact = _read_json(evidence / "principal-allocation-demonstration.json")
    for direction in ("ingress", "egress"):
        allocation = _principal_direction(artifact, direction)
        assert allocation["coverage"]["complete"] is True
        assert allocation["quota_sources"]["selected_leading_guard_timestamp"] == 1787097540.5
        assert allocation["quota_sources"]["series"][0]["raw_samples"][0]["timestamp"] == 1787097540.5


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        pytest.param("failed", "retained scope capture failed", id="failed"),
        pytest.param(
            "missing-response",
            "principal scope evidence is missing retained response",
            id="missing-response",
        ),
        pytest.param(
            "wrong-target",
            "principal scope evidence does not prove the configured target",
            id="wrong-target",
        ),
        pytest.param(
            "wrong-configuration",
            "principal scope evidence does not match the configured window",
            id="wrong-configuration",
        ),
    ],
)
def test_principal_validator_derives_readiness_from_retained_scope_and_configuration_evidence(
    tmp_path: Path, mutation: str, message: str
) -> None:
    lab = _lab_with_principal_expected_states(tmp_path, ingress="degraded", egress="degraded")
    evidence = _write_principal_evidence(tmp_path / "evidence")
    scope_path = evidence / "principal-scope-evidence.json"
    scope = _read_json(scope_path)
    if mutation == "failed":
        scope.update({"status": "blocked", "category": "principal_scope", "message": message})
    elif mutation == "missing-response":
        scope.pop("response")
    elif mutation == "wrong-target":
        scope["response"]["data"]["result"][0]["metric"]["kafka_cluster_id"] = "kraft-b-001"
    else:
        scope["window"]["selector"]["lab_cluster"] = "cluster-b"
    scope_path.write_text(json.dumps(scope), encoding="utf-8")

    result = _run_principal_validator_for_lab(lab, evidence)

    assert result.returncode == 7
    assert _read_json(evidence / "validator-result.json")["failures"] == [
        {
            "category": "principal_scope",
            "message": message,
            "artifact": "principal-scope-evidence.json",
        }
    ]
    assert not (evidence / "principal-allocation-demonstration.json").exists()


def test_principal_source_analysis_marks_a_leading_guard_one_second_past_the_limit_incomplete() -> None:
    validator = _load_validator_module()
    start = 1787097600

    analysis = validator._analyse_source_samples(
        [[start - 121, "100"], [start + 60, "100"], [start + 120, "100"]],
        start=start,
        end=start + 300,
        max_gap=120,
    )

    assert analysis["selected_leading_guard_timestamp"] == start - 121
    assert analysis["segments"] == []
    assert analysis["gaps"] == [{"reason": "old_leading_guard", "start_timestamp": start - 121, "end_timestamp": start}]
    assert analysis["complete"] is False


def test_principal_direction_marks_duplicate_quota_identity_evidence_unavailable() -> None:
    validator = _load_validator_module()
    contract = _read_yaml(LAB_DIR / "contracts" / "principal-allocation-contract.yaml")
    payload = _principal_fixture()
    quota_results = _principal_matrix_results(payload, "quota_produce")
    quota_results.append(json.loads(json.dumps(quota_results[0])))

    ingress, failure = validator._principal_direction(
        payload["queries"],
        "ingress",
        contract["directions"]["ingress"],
        payload["window"],
        contract,
    )

    assert failure is None
    assert ingress is not None
    assert ingress["state"] == "unavailable"
    assert ingress["users"] == []
    assert ingress["unallocated"] == ingress["pool"]


def test_quota_matrix_adapter_reuses_one_source_series_per_prometheus_series() -> None:
    validator = _load_validator_module()
    results = [
        {
            "metric": _quota_labels("cluster-a", "Produce", "user"),
            "values": [[1787097600, "1.0"], [1787097660, "1.0"]],
        }
    ]

    rows = validator._quota_metric_rows(results, "Produce")

    assert len(rows) == 2
    assert rows[0].source_series is rows[1].source_series


@pytest.mark.parametrize(
    ("failure", "category", "message"),
    [
        pytest.param(
            "query",
            "principal_metric",
            "quota Produce query failed: query timed out",
            id="query",
        ),
        pytest.param(
            "non-finite",
            "principal_metric",
            "quota Produce contains a non-finite sample",
            id="non-finite",
        ),
        pytest.param(
            "negative",
            "principal_metric",
            "quota Produce contains a negative sample",
            id="negative",
        ),
        pytest.param(
            "invalid-sample",
            "principal_metric",
            "quota Produce contains an invalid sample",
            id="invalid-sample",
        ),
        pytest.param(
            "identity",
            "principal_identity",
            "quota Produce contains invalid quota identity labels",
            id="identity",
        ),
    ],
)
def test_principal_validator_isolates_direction_local_source_failures_and_retains_sibling_artifact(
    tmp_path: Path, failure: str, category: str, message: str
) -> None:
    lab = _lab_with_principal_expected_states(tmp_path, ingress="unavailable", egress="degraded")
    payload = _principal_fixture()
    produce = _principal_query(payload, "quota_produce")
    if failure == "query":
        produce["response"] = {
            "status": "error",
            "errorType": "timeout",
            "error": "query timed out",
            "data": {"resultType": "matrix", "result": []},
        }
    elif failure == "non-finite":
        _principal_matrix_results(payload, "quota_produce")[0]["values"][0][1] = "NaN"
    elif failure == "negative":
        _principal_matrix_results(payload, "quota_produce")[0]["values"][0][1] = "-1"
    elif failure == "invalid-sample":
        samples = _principal_matrix_results(payload, "quota_produce")[0]["values"]
        samples[1] = [samples[0][0], "999"]
    else:
        _principal_matrix_results(payload, "quota_produce")[0]["metric"]["client_id"] = "unexpected-client"
    evidence = _write_principal_evidence(tmp_path / "evidence", payload)

    result = _run_principal_validator_for_lab(lab, evidence)

    assert result.returncode == 7
    assert _read_json(evidence / "validator-result.json")["failures"] == [
        {
            "category": category,
            "message": message,
            "artifact": "principal-raw-query-results.json",
        }
    ]
    artifact = _read_json(evidence / "principal-allocation-demonstration.json")
    ingress = _principal_direction(artifact, "ingress")
    egress = _principal_direction(artifact, "egress")
    assert ingress["state"] == "unavailable"
    assert ingress["coverage"]["complete"] is False
    assert ingress["users"] == []
    assert ingress["unallocated"] == ingress["pool"] == "12.0000"
    assert ingress["balance"] == "12.0000"
    assert egress["state"] == "degraded"
    assert egress["weights"] == {"user_total": "30000", "client_only": "6000", "total": "36000"}
    assert egress["balance"] == "3.0000"


def test_principal_validator_clips_the_shared_exact_end_sample_to_only_one_logical_day(tmp_path: Path) -> None:
    lab = _lab_with_principal_expected_states(tmp_path, ingress="degraded", egress="degraded")
    prior_payload = _principal_fixture()
    _shift_principal_source_window(prior_payload, -300)
    prior_evidence = _write_principal_evidence(tmp_path / "prior", prior_payload)
    current_evidence = _write_principal_evidence(tmp_path / "current")

    prior_result = _run_principal_validator_for_lab(lab, prior_evidence)
    current_result = _run_principal_validator_for_lab(lab, current_evidence)

    assert prior_result.returncode == 0, prior_result.stderr
    assert current_result.returncode == 0, current_result.stderr
    prior = _principal_direction(_read_json(prior_evidence / "principal-allocation-demonstration.json"), "ingress")
    current = _principal_direction(_read_json(current_evidence / "principal-allocation-demonstration.json"), "ingress")
    shared_boundary = 1787097600
    assert prior["quota_sources"]["selected_leading_guard_timestamp"] == 1787097300
    assert current["quota_sources"]["selected_leading_guard_timestamp"] == shared_boundary
    assert sum(segment["duration_seconds"] for segment in prior["quota_sources"]["segments"]) == 300
    assert sum(segment["duration_seconds"] for segment in current["quota_sources"]["segments"]) == 300
    assert all(segment["end_timestamp"] <= shared_boundary for segment in prior["quota_sources"]["segments"])
    assert all(segment["start_timestamp"] >= shared_boundary for segment in current["quota_sources"]["segments"])


def test_principal_validator_canonicalizes_and_maps_users_without_client_identity_leakage(tmp_path: Path) -> None:
    lab = _lab_with_principal_expected_states(tmp_path, ingress="degraded", egress="degraded")
    evidence = _write_principal_evidence(tmp_path / "evidence")

    result = _run_principal_validator_for_lab(lab, evidence)

    assert result.returncode == 0, result.stderr
    ingress = _principal_direction(_read_json(evidence / "principal-allocation-demonstration.json"), "ingress")
    users = {row["identity"]: row for row in ingress["users"]}
    assert users["User:user-only"] == {
        "identity": "User:user-only",
        "owner": "team-data",
        "weight": "30000",
        "raw_amount": "3.428571428571428571428571429",
        "amount": "3.4285",
    }
    assert users["User:shared-user"] == {
        "identity": "User:shared-user",
        "owner": "UNASSIGNED",
        "weight": "60000",
        "raw_amount": "6.857142857142857142857142857",
        "amount": "6.8571",
    }
    assert all("client" not in identity for identity in users)


def test_principal_direction_evaluator_aggregates_user_and_user_client_scopes_by_exact_case_sensitive_user() -> None:
    validator = _load_validator_module()
    contract = _read_yaml(LAB_DIR / "contracts" / "principal-allocation-contract.yaml")
    payload = _principal_fixture()
    for series in _principal_matrix_results(payload, "quota_produce"):
        if series["metric"]["quota_scope"] == "user-client":
            series["metric"]["user"] = "user-only"
            series["metric"]["client_id"] = "another-client"

    ingress, failure = validator._principal_direction(
        payload["queries"],
        "ingress",
        contract["directions"]["ingress"],
        payload["window"],
        contract,
    )

    assert failure is None
    assert ingress is not None
    assert ingress["users"] == [
        {
            "identity": "User:user-only",
            "owner": "team-data",
            "weight": "90000",
            "raw_amount": "10.28571428571428571428571429",
            "amount": "10.2857",
        }
    ]


def test_principal_direction_evaluator_treats_case_changed_identities_as_distinct_and_unmapped() -> None:
    validator = _load_validator_module()
    contract = _read_yaml(LAB_DIR / "contracts" / "principal-allocation-contract.yaml")
    payload = _principal_fixture()
    for series in _principal_matrix_results(payload, "quota_produce"):
        if series["metric"]["quota_scope"] == "user":
            series["metric"]["user"] = "User-Only"

    ingress, failure = validator._principal_direction(
        payload["queries"],
        "ingress",
        contract["directions"]["ingress"],
        payload["window"],
        contract,
    )

    assert failure is None
    assert ingress is not None
    users = {row["identity"]: row for row in ingress["users"]}
    assert "User:user-only" not in users
    assert users["User:User-Only"]["owner"] == "UNASSIGNED"
    assert users["User:User-Only"]["weight"] == "30000"


@pytest.mark.parametrize(
    ("missing_scope", "required_field"),
    [
        ("user", "mapped_user"),
        ("user-client", "unmapped_user"),
        ("client-id", "client_only_label"),
    ],
)
def test_principal_validator_rejects_false_pass_when_required_live_identity_evidence_is_missing(
    tmp_path: Path, missing_scope: str, required_field: str
) -> None:
    lab = _lab_with_principal_expected_states(tmp_path, ingress="unavailable", egress="unavailable")
    payload = _principal_fixture()
    for query_name in ("quota_produce", "quota_fetch"):
        results = _principal_matrix_results(payload, query_name)
        _principal_query(payload, query_name)["response"]["data"]["result"] = [
            result for result in results if result["metric"]["quota_scope"] != missing_scope
        ]
    evidence = _write_principal_evidence(tmp_path / "evidence", payload)

    result = _run_principal_validator_for_lab(lab, evidence)

    assert result.returncode == 7
    details = _read_json(evidence / "validator-result.json")
    assert any(failure["category"] == "principal_expected_state" for failure in details["failures"])
    assert any(required_field in failure["message"] for failure in details["failures"])
    artifact = _read_json(evidence / "principal-allocation-demonstration.json")
    for direction, pool in (("ingress", "12.0000"), ("egress", "3.0000")):
        allocation = _principal_direction(artifact, direction)
        assert allocation["state"] == "unavailable"
        assert allocation["unallocated"] == allocation["balance"] == allocation["pool"] == pool


@pytest.mark.parametrize(
    ("direction", "scope", "label", "replacement", "message"),
    [
        pytest.param(
            "ingress",
            "user",
            "user",
            "different-user",
            "required ingress mapped identity User:user-only owned by team-data, observed User:different-user",
            id="ingress-mapped-user",
        ),
        pytest.param(
            "ingress",
            "user-client",
            "user",
            "different-user",
            "required ingress unmapped identity User:shared-user owned by UNASSIGNED, observed User:different-user",
            id="ingress-unmapped-user",
        ),
        pytest.param(
            "ingress",
            "client-id",
            "client_id",
            "different-client",
            "required ingress client-only label client-only, observed different-client",
            id="ingress-client-only",
        ),
        pytest.param(
            "egress",
            "user",
            "user",
            "different-user",
            "required egress mapped identity User:user-only owned by team-data, observed User:different-user",
            id="egress-mapped-user",
        ),
        pytest.param(
            "egress",
            "user-client",
            "user",
            "different-user",
            "required egress unmapped identity User:shared-user owned by UNASSIGNED, observed User:different-user",
            id="egress-unmapped-user",
        ),
        pytest.param(
            "egress",
            "client-id",
            "client_id",
            "different-client",
            "required egress client-only label client-only, observed different-client",
            id="egress-client-only",
        ),
    ],
)
def test_principal_validator_requires_exact_configured_live_identities_per_direction(
    tmp_path: Path,
    direction: str,
    scope: str,
    label: str,
    replacement: str,
    message: str,
) -> None:
    expected_states = {"ingress": "degraded", "egress": "degraded"}
    expected_states[direction] = "unavailable"
    lab = _lab_with_principal_expected_states(
        tmp_path,
        ingress=expected_states["ingress"],
        egress=expected_states["egress"],
    )
    payload = _principal_fixture()
    query_name = {"ingress": "quota_produce", "egress": "quota_fetch"}[direction]
    for series in _principal_matrix_results(payload, query_name):
        if series["metric"]["quota_scope"] == scope:
            series["metric"][label] = replacement
    evidence = _write_principal_evidence(tmp_path / "evidence", payload)

    result = _run_principal_validator_for_lab(lab, evidence)

    assert result.returncode == 7
    assert _read_json(evidence / "validator-result.json")["failures"] == [
        {
            "category": "principal_identity",
            "message": message,
            "artifact": "principal-raw-query-results.json",
        }
    ]
    artifact = _read_json(evidence / "principal-allocation-demonstration.json")
    affected = _principal_direction(artifact, direction)
    sibling = _principal_direction(artifact, "egress" if direction == "ingress" else "ingress")
    assert affected["state"] == "unavailable"
    assert affected["users"] == []
    assert affected["unallocated"] == affected["balance"] == affected["pool"]
    assert sibling["state"] == "degraded"


def test_principal_validator_rejects_false_pass_when_a_required_direction_state_or_balance_is_not_met(
    tmp_path: Path,
) -> None:
    lab = _lab_with_principal_expected_states(tmp_path, ingress="degraded", egress="degraded")
    contract_path = lab / "contracts" / "principal-allocation-contract.yaml"
    contract = _read_yaml(contract_path)
    contract["expected_live_states"] = {"ingress": "ready", "egress": "degraded"}
    contract["expected_live_balances"] = {"ingress": "12.0000", "egress": "3.0000"}
    contract_path.write_text(yaml.safe_dump(contract, sort_keys=False), encoding="utf-8")
    evidence = _write_principal_evidence(tmp_path / "evidence")

    result = _run_principal_validator_for_lab(lab, evidence)

    assert result.returncode == 7
    details = _read_json(evidence / "validator-result.json")
    assert details["failures"] == [
        {
            "category": "principal_expected_state",
            "message": "required ingress state is ready, observed degraded",
            "artifact": "principal-allocation-demonstration.json",
        }
    ]


def test_principal_validator_rejects_false_pass_when_a_required_direction_balance_is_not_met(tmp_path: Path) -> None:
    lab = _lab_with_principal_expected_states(tmp_path, ingress="degraded", egress="degraded")
    contract_path = lab / "contracts" / "principal-allocation-contract.yaml"
    contract = _read_yaml(contract_path)
    expected = EXPECTED_CONTRACT["principal_contract"]
    contract["expected_live_identities"] = expected["required_live_identities"]
    contract["expected_live_states"] = expected["required_live_states"]
    contract["expected_live_balances"] = {"ingress": "11.9999", "egress": "3.0000"}
    contract_path.write_text(yaml.safe_dump(contract, sort_keys=False), encoding="utf-8")
    evidence = _write_principal_evidence(tmp_path / "evidence")

    result = _run_principal_validator_for_lab(lab, evidence)

    assert result.returncode == 7
    details = _read_json(evidence / "validator-result.json")
    assert details["failures"] == [
        {
            "category": "principal_expected_state",
            "message": "required ingress balance is 11.9999, observed 12.0000",
            "artifact": "principal-allocation-demonstration.json",
        }
    ]


@pytest.mark.parametrize(
    ("user", "client_only", "coverage", "expected_state"),
    [
        pytest.param(0, 0, "absent", "unavailable", id="absent-quota-is-unavailable"),
        pytest.param(0, 0, "complete", "zero_usage", id="complete-zero-weight-is-zero-usage"),
        pytest.param(60000, 0, "complete", "ready", id="complete-user-weight-is-ready"),
        pytest.param(60000, 30000, "complete", "degraded", id="complete-client-weight-is-degraded"),
        pytest.param(0, 60000, "complete", "degraded", id="complete-client-only-weight-is-degraded"),
        pytest.param(60000, 0, "gap", "unavailable", id="gapped-user-weight-is-unavailable"),
    ],
)
def test_principal_direction_evaluator_applies_the_quota_weight_state_matrix(
    user: int, client_only: int, coverage: str, expected_state: str
) -> None:
    validator = _load_validator_module()
    contract = _read_yaml(LAB_DIR / "contracts" / "principal-allocation-contract.yaml")
    payload = _principal_fixture()
    if coverage == "absent":
        _principal_query(payload, "quota_produce")["response"]["data"]["result"] = []
    else:
        _set_principal_weights(
            payload,
            "ingress",
            user=user,
            client_only=client_only,
        )
        if coverage == "gap":
            _introduce_internal_quota_gap(payload, "ingress")

    ingress, failure = validator._principal_direction(
        payload["queries"], "ingress", contract["directions"]["ingress"], payload["window"], contract
    )

    assert failure is None
    assert ingress is not None
    assert ingress["state"] == expected_state
    if expected_state == "unavailable":
        assert ingress["users"] == []
        assert ingress["unallocated"] == ingress["pool"]


def test_principal_direction_evaluator_preserves_allocation_shares_for_proportional_weight_sets() -> None:
    validator = _load_validator_module()
    contract = _read_yaml(LAB_DIR / "contracts" / "principal-allocation-contract.yaml")
    smaller = _principal_fixture()
    larger = _principal_fixture()
    _set_principal_weights(smaller, "ingress", user=30000, client_only=15000)
    _set_principal_weights(larger, "ingress", user=60000, client_only=30000)

    smaller_result, smaller_failure = validator._principal_direction(
        smaller["queries"], "ingress", contract["directions"]["ingress"], smaller["window"], contract
    )
    larger_result, larger_failure = validator._principal_direction(
        larger["queries"], "ingress", contract["directions"]["ingress"], larger["window"], contract
    )

    assert smaller_failure is None
    assert larger_failure is None
    assert smaller_result is not None
    assert larger_result is not None
    assert smaller_result["state"] == larger_result["state"] == "degraded"
    assert [row["amount"] for row in smaller_result["users"]] == [row["amount"] for row in larger_result["users"]]
    assert smaller_result["client_only"]["amount"] == larger_result["client_only"]["amount"]


def test_principal_validator_uses_quota_weights_and_exact_client_only_money_residuals(tmp_path: Path) -> None:
    lab = _lab_with_principal_expected_states(tmp_path, ingress="degraded", egress="degraded")
    evidence = _write_principal_evidence(tmp_path / "evidence")

    result = _run_principal_validator_for_lab(lab, evidence)

    assert result.returncode == 0, result.stderr
    artifact = _read_json(evidence / "principal-allocation-demonstration.json")
    ingress = _principal_direction(artifact, "ingress")
    assert ingress["state"] == "degraded"
    assert ingress["weights"] == {"user_total": "90000", "client_only": "15000", "total": "105000"}
    assert {row["identity"]: row["amount"] for row in ingress["users"]} == {
        "User:user-only": "3.4285",
        "User:shared-user": "6.8571",
    }
    assert ingress["client_only"]["weight"] == "15000"
    assert ingress["client_only"]["amount"] == "1.7142"
    assert ingress["rounding_residual"] == "0.0002"
    assert ingress["unallocated"] == "1.7144"
    assert ingress["balance"] == "12.0000"
    assert {"quantities", "uncovered"}.isdisjoint(ingress)
    assert sum(Decimal(row["amount"]) for row in ingress["users"]) + Decimal(ingress["unallocated"]) == Decimal(
        ingress["pool"]
    )


def test_principal_validator_keeps_fixed_policy_categories_independent_of_quota_weights(
    tmp_path: Path,
) -> None:
    lab = _lab_with_principal_expected_states(tmp_path, ingress="degraded", egress="degraded")
    payload = _principal_fixture()
    evidence = _write_principal_evidence(tmp_path / "evidence", payload)

    result = _run_principal_validator_for_lab(lab, evidence)

    assert result.returncode == 0, result.stderr
    artifact = _read_json(evidence / "principal-allocation-demonstration.json")
    ingress = _principal_direction(artifact, "ingress")
    egress = _principal_direction(artifact, "egress")
    assert ingress["pool"] == "12.0000"
    assert ingress["balance"] == "12.0000"
    assert egress["pool"] == "3.0000"
    assert egress["balance"] == "3.0000"

    fixed = artifact["clusters"]["cluster-a"]["fixed_categories"]
    assert fixed["compute"] == {
        "state": "policy_only",
        "policy": "static_even_v1",
        "shared": True,
        "measured_usage": False,
        "users": [
            {"identity": "Team:analytics", "amount": "2.0000"},
            {"identity": "Team:platform", "amount": "2.0000"},
        ],
        "pool": "4.0000",
        "rounding_residual": "0.0000",
        "unallocated": "0.0000",
        "balance": "4.0000",
    }
    for category, pool in (("storage", "5.0000"), ("shared", "2.0000")):
        assert fixed[category] == {
            "state": "unattributed",
            "policy": "unattributed",
            "shared": True,
            "measured_usage": False,
            "users": [],
            "pool": pool,
            "rounding_residual": "0.0000",
            "unallocated": pool,
            "balance": pool,
        }


@pytest.mark.parametrize(
    ("policy", "identities"),
    [
        pytest.param("static_even_v1", [], id="empty-static-even"),
        pytest.param("unsupported", ["Team:analytics"], id="invalid-policy"),
    ],
)
def test_principal_validator_falls_back_to_full_unattributed_fixed_category_for_invalid_static_policy(
    tmp_path: Path, policy: str, identities: list[str]
) -> None:
    lab = _lab_with_principal_expected_states(tmp_path, ingress="degraded", egress="degraded")
    contract_path = lab / "contracts" / "principal-allocation-contract.yaml"
    contract = _read_yaml(contract_path)
    contract["fixed_policy_inputs"]["compute"] = {"policy": policy, "identities": identities}
    contract_path.write_text(yaml.safe_dump(contract, sort_keys=False), encoding="utf-8")
    evidence = _write_principal_evidence(tmp_path / "evidence")

    result = _run_principal_validator_for_lab(lab, evidence)

    assert result.returncode == 0, result.stderr
    compute = _read_json(evidence / "principal-allocation-demonstration.json")["clusters"]["cluster-a"][
        "fixed_categories"
    ]["compute"]
    assert compute == {
        "state": "unattributed",
        "policy": "unattributed",
        "shared": True,
        "measured_usage": False,
        "users": [],
        "pool": "4.0000",
        "rounding_residual": "0.0000",
        "unallocated": "4.0000",
        "balance": "4.0000",
    }


def test_principal_validator_retains_fixed_static_policy_rounding_residual(tmp_path: Path) -> None:
    lab = _lab_with_principal_expected_states(tmp_path, ingress="degraded", egress="degraded")
    contract_path = lab / "contracts" / "principal-allocation-contract.yaml"
    contract = _read_yaml(contract_path)
    contract["demonstration_pools"]["compute"] = "4.0001"
    contract_path.write_text(yaml.safe_dump(contract, sort_keys=False), encoding="utf-8")
    evidence = _write_principal_evidence(tmp_path / "evidence")

    result = _run_principal_validator_for_lab(lab, evidence)

    assert result.returncode == 0, result.stderr
    compute = _read_json(evidence / "principal-allocation-demonstration.json")["clusters"]["cluster-a"][
        "fixed_categories"
    ]["compute"]
    assert compute["shared"] is True
    assert compute["measured_usage"] is False
    assert compute["users"] == [
        {"identity": "Team:analytics", "amount": "2.0000"},
        {"identity": "Team:platform", "amount": "2.0000"},
    ]
    assert compute["rounding_residual"] == "0.0001"
    assert compute["unallocated"] == "0.0001"
    assert compute["balance"] == compute["pool"] == "4.0001"


def test_principal_validator_rounds_down_each_component_and_retains_exact_rounding_residual(tmp_path: Path) -> None:
    lab = _lab_with_principal_expected_states(tmp_path, ingress="degraded", egress="degraded")
    payload = _principal_fixture()
    _set_principal_weights(payload, "ingress", user=1800, client_only=118500)
    evidence = _write_principal_evidence(tmp_path / "evidence", payload)

    result = _run_principal_validator_for_lab(lab, evidence)

    assert result.returncode == 0, result.stderr
    ingress = _principal_direction(_read_json(evidence / "principal-allocation-demonstration.json"), "ingress")
    assert ingress["state"] == "degraded"
    assert [row["amount"] for row in ingress["users"]] == ["0.0897", "0.0897"]
    assert ingress["client_only"]["amount"] == "11.8204"
    assert ingress["rounding_residual"] == "0.0002"
    assert ingress["unallocated"] == "11.8206"
    assert ingress["balance"] == "12.0000"
    assert sum(Decimal(row["amount"]) for row in ingress["users"]) + Decimal(ingress["unallocated"]) == Decimal(
        ingress["pool"]
    )


def test_principal_validator_preserves_positive_quota_state_when_the_monetary_pool_is_zero(tmp_path: Path) -> None:
    lab = _lab_with_principal_expected_states(tmp_path, ingress="degraded", egress="degraded")
    contract_path = lab / "contracts" / "principal-allocation-contract.yaml"
    contract = _read_yaml(contract_path)
    contract["demonstration_pools"]["ingress"] = "0.0000"
    contract["expected_live_balances"]["ingress"] = "0.0000"
    contract_path.write_text(yaml.safe_dump(contract, sort_keys=False), encoding="utf-8")
    evidence = _write_principal_evidence(tmp_path / "evidence")

    result = _run_principal_validator_for_lab(lab, evidence)

    assert result.returncode == 0, result.stderr
    ingress = _principal_direction(_read_json(evidence / "principal-allocation-demonstration.json"), "ingress")
    assert ingress["state"] == "degraded"
    assert ingress["weights"] == {"user_total": "90000", "client_only": "15000", "total": "105000"}
    assert {row["amount"] for row in ingress["users"]} == {"0.0000"}
    assert ingress["client_only"]["amount"] == "0.0000"
    assert ingress["rounding_residual"] == "0.0000"
    assert ingress["unallocated"] == ingress["balance"] == ingress["pool"] == "0.0000"


def test_principal_validator_keeps_a_complete_client_only_direction_fully_unallocated(tmp_path: Path) -> None:
    lab = _lab_with_principal_expected_states(tmp_path, ingress="degraded", egress="degraded")
    payload = _principal_fixture()
    _set_principal_weights(payload, "ingress", user=0, client_only=60000)
    evidence = _write_principal_evidence(tmp_path / "evidence", payload)

    result = _run_principal_validator_for_lab(lab, evidence)

    assert result.returncode == 0, result.stderr
    ingress = _principal_direction(_read_json(evidence / "principal-allocation-demonstration.json"), "ingress")
    assert ingress["state"] == "degraded"
    assert ingress["weights"] == {"user_total": "0", "client_only": "60000", "total": "60000"}
    assert ingress["users"] == []
    assert ingress["client_only"]["amount"] == "12.0000"
    assert ingress["rounding_residual"] == "0.0000"
    assert ingress["unallocated"] == ingress["balance"] == ingress["pool"] == "12.0000"


@pytest.mark.parametrize("pool", [pytest.param("-1.0000", id="negative"), pytest.param("NaN", id="non-finite")])
def test_principal_validator_rejects_invalid_monetary_pool_values(tmp_path: Path, pool: str) -> None:
    lab = _lab_with_principal_expected_states(tmp_path, ingress="degraded", egress="degraded")
    contract_path = lab / "contracts" / "principal-allocation-contract.yaml"
    contract = _read_yaml(contract_path)
    contract["demonstration_pools"]["ingress"] = pool
    contract_path.write_text(yaml.safe_dump(contract, sort_keys=False), encoding="utf-8")
    evidence = _write_principal_evidence(tmp_path / "evidence")

    result = _run_principal_validator_for_lab(lab, evidence)

    assert result.returncode == 7
    assert _read_json(evidence / "validator-result.json")["failures"] == [
        {
            "category": "principal_contract",
            "message": "principal allocation contract is invalid",
            "artifact": "principal-allocation-contract.yaml",
        }
    ]
    assert not (evidence / "principal-allocation-demonstration.json").exists()


def test_principal_validator_uses_no_duplicate_direction_or_money_helpers() -> None:
    validator = _load_validator_module()

    assert not hasattr(validator, "_direction_state")
    assert not hasattr(validator, "_quantized_amount")
    assert not hasattr(validator, "_direction_money")


def test_principal_validator_reports_directions_independently_and_uses_worst_cluster_day_state(tmp_path: Path) -> None:
    lab = _lab_with_principal_expected_states(tmp_path, ingress="unavailable", egress="degraded")
    payload = _principal_fixture()
    _set_principal_weights(payload, "ingress", user=60000, client_only=0)
    _introduce_internal_quota_gap(payload, "ingress")
    _set_principal_weights(payload, "egress", user=30000, client_only=6000)
    evidence = _write_principal_evidence(tmp_path / "evidence", payload)

    result = _run_principal_validator_for_lab(lab, evidence)

    assert result.returncode == 0, result.stderr
    artifact = _read_json(evidence / "principal-allocation-demonstration.json")
    ingress = _principal_direction(artifact, "ingress")
    egress = _principal_direction(artifact, "egress")
    assert ingress["state"] == "unavailable"
    assert ingress["unallocated"] == "12.0000"
    assert egress["state"] == "degraded"
    assert egress["weights"] == {"user_total": "30000", "client_only": "6000", "total": "36000"}
    assert egress["users"]
    assert artifact["clusters"]["cluster-a"]["state"] == "unavailable"


@pytest.mark.parametrize(
    ("query_name", "description"),
    [
        ("quota_produce", "quota Produce"),
        ("quota_fetch", "quota Fetch"),
    ],
)
def test_principal_validator_uses_the_shared_metric_category_for_query_errors(
    tmp_path: Path, query_name: str, description: str
) -> None:
    failed_direction = {"quota_produce": "ingress", "quota_fetch": "egress"}[query_name]
    expected_states = {"ingress": "degraded", "egress": "degraded"}
    expected_states[failed_direction] = "unavailable"
    lab = _lab_with_principal_expected_states(
        tmp_path,
        ingress=expected_states["ingress"],
        egress=expected_states["egress"],
    )
    payload = _principal_fixture()
    _principal_query(payload, query_name)["response"] = {
        "status": "error",
        "errorType": "timeout",
        "error": "query timed out",
        "data": {"resultType": "matrix", "result": []},
    }
    evidence = _write_principal_evidence(tmp_path / "evidence", payload)

    result = _run_principal_validator_for_lab(lab, evidence)

    assert result.returncode == 7
    details = _read_json(evidence / "validator-result.json")
    assert details["failures"] == [
        {
            "category": "principal_metric",
            "message": f"{description} query failed: query timed out",
            "artifact": "principal-raw-query-results.json",
        }
    ]
    artifact = _read_json(evidence / "principal-allocation-demonstration.json")
    affected = _principal_direction(artifact, failed_direction)
    sibling = _principal_direction(artifact, "egress" if failed_direction == "ingress" else "ingress")
    assert affected["state"] == "unavailable"
    assert affected["users"] == []
    assert affected["unallocated"] == affected["balance"] == affected["pool"]
    assert sibling["state"] == "degraded"
    baseline = _run_validator(evidence)
    assert baseline.returncode == 0, baseline.stderr


@pytest.mark.parametrize(
    ("query_name", "mutate_query"),
    [
        pytest.param(
            "quota_produce",
            'kafka_server_quota_byte_rate{lab_cluster="cluster-b",kafka_cluster_id="kraft-a-001",quota_type="Produce"}[420s]',
            id="quota-selector",
        ),
        pytest.param(
            "quota_fetch",
            'kafka_server_quota_byte_rate{job="kafka-jmx",lab_cluster="cluster-a",kafka_cluster_id="kraft-a-001",quota_type="Fetch"}[299s]',
            id="quota-window",
        ),
    ],
)
def test_principal_validator_rejects_query_selector_window_and_evaluation_drift(
    tmp_path: Path,
    query_name: str,
    mutate_query: str | None,
) -> None:
    lab = _lab_with_principal_expected_states(tmp_path, ingress="degraded", egress="degraded")
    payload = _principal_fixture()
    query = _principal_query(payload, query_name)
    if mutate_query is not None:
        query["query"] = mutate_query
    evidence = _write_principal_evidence(tmp_path / "evidence", payload)

    result = _run_principal_validator_for_lab(lab, evidence)

    assert result.returncode == 7
    details = _read_json(evidence / "validator-result.json")
    assert details["failures"] == [
        {
            "category": "principal_window",
            "message": f"{query_name} query does not match the principal window contract",
            "artifact": "principal-raw-query-results.json",
        }
    ]
    assert not (evidence / "principal-allocation-demonstration.json").exists()


def test_lab_entrypoint_wires_principal_capture_output_to_the_principal_validator(tmp_path: Path) -> None:
    lab = _lab_with_principal_expected_states(tmp_path, ingress="degraded", egress="degraded")
    environment, command_log = _fake_command_environment(tmp_path)
    evidence = _write_principal_evidence(tmp_path / "captured-evidence")
    generated = lab / "generated"
    generated.mkdir()
    (lab / ".env").write_text(
        "PROMETHEUS_PORT=9090\nJMX_EXPORTER_A_PORT=7071\nJMX_EXPORTER_B_PORT=7072\n",
        encoding="utf-8",
    )
    (generated / "generation.json").write_text(
        json.dumps({"kafka_cluster_ids": {"cluster-a": "kraft-a-001", "cluster-b": "kraft-b-001"}}),
        encoding="utf-8",
    )
    for name in ("prometheus.yml", "jmx-a.yml", "jmx-b.yml"):
        (generated / name).write_text("generated\n", encoding="utf-8")
    fake_bin = Path(environment["PATH"].split(":", maxsplit=1)[0])
    _write_executable(
        fake_bin / "uv",
        """#!/usr/bin/env bash
set -euo pipefail
printf 'uv %s\\n' "$*" >>"$LAB_FAKE_LOG"
[[ ${1:-} == run && ${2:-} == python ]]
if [[ ${3:-} == scripts/capture_evidence.py ]]; then
  mkdir -p "$LAB_ENTRYPOINT_LAB/evidence"
  ln -s "$LAB_ENTRYPOINT_EVIDENCE" "$LAB_ENTRYPOINT_LAB/evidence/latest"
  exit 0
fi
shift 2
exec "$LAB_TEST_PYTHON" "$@"
""",
    )
    environment.update(
        {
            "LAB_ENTRYPOINT_LAB": str(lab),
            "LAB_ENTRYPOINT_EVIDENCE": str(evidence),
        }
    )

    result = _run_lab(lab, environment, "validate", "--window", "5m", "--principal-contract")

    assert result.returncode == 0, result.stderr
    commands = command_log.read_text(encoding="utf-8")
    assert "uv run python scripts/capture_evidence.py --window 5m --principal-contract" in commands
    assert "uv run python scripts/validate_evidence.py --evidence-dir" in commands
    assert "--principal-contract" in commands
    artifact = _read_json(evidence / "principal-allocation-demonstration.json")
    assert artifact["clusters"]["cluster-a"]["directions"].keys() == {"ingress", "egress"}


def test_telemetry_only_validation_never_requires_or_emits_principal_contract_artifacts(tmp_path: Path) -> None:
    evidence = _build_valid_evidence(tmp_path)

    result = _run_validator(evidence)

    assert result.returncode == 0, result.stderr
    assert _read_json(evidence / "validator-result.json")["failures"] == []
    assert not (evidence / "principal-window.json").exists()
    assert not (evidence / "principal-allocation-demonstration.json").exists()

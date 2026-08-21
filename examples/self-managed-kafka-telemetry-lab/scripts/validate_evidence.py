from __future__ import annotations

import argparse
import json
import math
import re
import sys
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import yaml

LAB_DIR = Path(__file__).resolve().parents[1]
CONTRACT_PATH = LAB_DIR / "contracts" / "metric-contract.yaml"
WORKLOADS_PATH = LAB_DIR / "workloads" / "workloads.yaml"
NOT_APPLICABLE_LABEL = "not_applicable"
SAMPLE_RE = re.compile(
    r"^(?P<name>[a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{(?P<labels>[^}]*)\})?\s+"
    r"(?P<value>NaN|[-+]?Inf|[-+]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][-+]?\d+)?)$"
)


@dataclass(frozen=True)
class Failure:
    category: str
    message: str
    artifact: str | None = None


def _read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _parse_labels(text: str) -> dict[str, str]:
    labels: dict[str, str] = {}
    if not text:
        return labels
    for entry in re.split(r',(?=[a-zA-Z_][a-zA-Z0-9_]*=")', text):
        key, raw_value = entry.split("=", 1)
        labels[key] = json.loads(raw_value)
    return labels


def _parse_metrics(path: Path) -> tuple[dict[str, str], dict[str, list[tuple[dict[str, str], float]]]]:
    types: dict[str, str] = {}
    samples: dict[str, list[tuple[dict[str, str], float]]] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if line.startswith("# TYPE "):
            _, _, name, metric_type = line.split(maxsplit=3)
            types[name] = metric_type
            continue
        if not line or line.startswith("#"):
            continue
        match = SAMPLE_RE.match(line)
        if match is None:
            continue
        value = float(match.group("value"))
        samples.setdefault(match.group("name"), []).append((_parse_labels(match.group("labels") or ""), value))
    return types, samples


def _normalise_object_name(value: str) -> tuple[str, tuple[tuple[str, str], ...]]:
    domain, properties = value.split(":", 1)
    pairs = [entry.split("=", 1) for entry in properties.split(",")]
    return domain, tuple(sorted((key, property_value) for key, property_value in pairs))


def _workload_profiles() -> dict[str, dict[str, Any]]:
    workloads = yaml.safe_load(WORKLOADS_PATH.read_text(encoding="utf-8"))
    if not isinstance(workloads, dict):
        raise TypeError("workload contract must be a mapping")
    topics = workloads.get("topics")
    quotas = workloads.get("quota_profiles")
    rates = workloads.get("rates_bytes_per_second")
    if not isinstance(topics, list) or not isinstance(quotas, list) or not isinstance(rates, dict):
        raise TypeError("workload contract is incomplete")
    profiles = {
        str(topic["profile"]): {
            "topic": str(topic["name"]),
            "rate": int(rates[str(topic["profile"])]),
        }
        for topic in topics
    }
    for quota in quotas:
        profile = profiles[str(quota["profile"])]
        profile.update(quota)
    return profiles


def _resolved_raw_object_name(metric: dict[str, Any], profiles: dict[str, dict[str, Any]]) -> str:
    profile = profiles[str(metric["workload_profile"])]
    return str(metric["raw_object_name"]).format(
        topic=profile["topic"],
        user=profile.get("user", ""),
        client_id=profile.get("client_id", ""),
    )


def _raw_jmx_entries(path: Path) -> set[tuple[tuple[str, tuple[tuple[str, str], ...]], str]]:
    entries: set[tuple[tuple[str, tuple[tuple[str, str], ...]], str]] = set()
    for line in path.read_text(encoding="utf-8").splitlines():
        payload = json.loads(line)
        value = payload.get("value")
        if not isinstance(value, (int, float)) or isinstance(value, bool):
            continue
        entries.add((_normalise_object_name(payload["object_name"]), payload["attribute"]))
    return entries


def _query_samples(queries: dict[str, Any], name: str) -> list[tuple[dict[str, str], float]]:
    results = queries.get(name, {}).get("response", {}).get("data", {}).get("result", [])
    samples: list[tuple[dict[str, str], float]] = []
    for result in results:
        metric = result.get("metric", {})
        value = result.get("value", [None, None])[1]
        try:
            numeric_value = float(value)
        except TypeError, ValueError:
            continue
        if isinstance(metric, dict):
            samples.append(({str(key): str(label) for key, label in metric.items()}, numeric_value))
    return samples


def _check_required_files(evidence_dir: Path, contract: dict[str, Any], failures: list[Failure]) -> None:
    for artifact in contract["completion_evidence"]:
        if artifact == "validator-result.json":
            continue
        if not (evidence_dir / artifact).is_file():
            failures.append(Failure("missing_metric", f"required evidence artifact is absent: {artifact}", artifact))


def _check_freshness(evidence_dir: Path, contract: dict[str, Any], failures: list[Failure]) -> None:
    manifest_path = evidence_dir / "evidence-manifest.json"
    if not manifest_path.is_file():
        failures.append(Failure("evidence_stale", "evidence-manifest.json is absent", manifest_path.name))
        return
    captured_at = datetime.fromisoformat(_read_json(manifest_path)["captured_at"])
    age = (datetime.now(tz=UTC) - captured_at).total_seconds()
    if age < 0 or age > int(contract["evidence_max_age_seconds"]):
        failures.append(Failure("evidence_stale", f"evidence age is outside the allowed window: {age:.0f}s"))


def _check_raw_jmx(evidence_dir: Path, contract: dict[str, Any], failures: list[Failure]) -> None:
    profiles = _workload_profiles()
    expected = {
        (_normalise_object_name(_resolved_raw_object_name(metric, profiles)), metric["raw_attribute"])
        for metric in contract["metrics"]
        if "raw_object_name" in metric
    }
    for cluster in ("a", "b"):
        path = evidence_dir / f"raw-jmx-cluster-{cluster}.jsonl"
        if not path.is_file():
            continue
        actual = _raw_jmx_entries(path)
        for missing in sorted(expected - actual, key=str):
            failures.append(
                Failure("missing_metric", f"raw JMX mapping absent for cluster-{cluster}: {missing}", path.name)
            )


def _check_exporters(evidence_dir: Path, contract: dict[str, Any], failures: list[Failure]) -> None:
    required_types = {metric["prometheus_series"]: metric["type"] for metric in contract["metrics"]}
    for cluster in ("a", "b"):
        path = evidence_dir / f"exporter-cluster-{cluster}.metrics"
        if not path.is_file():
            continue
        types, samples = _parse_metrics(path)
        for name, expected_type in required_types.items():
            if name in {"up"}:
                continue
            if name not in samples:
                failures.append(Failure("missing_metric", f"{name} absent from cluster-{cluster} exporter", path.name))
            if name in types and types[name].lower() != expected_type:
                failures.append(
                    Failure(
                        "wrong_type",
                        f"{name} has type {types[name]}, expected {expected_type}",
                        path.name,
                    )
                )
        forbidden_prefixes = ("java_", "kafka_network_", "kafka_request_")
        unexpected = sorted(name for name in samples if name.startswith(forbidden_prefixes))
        if unexpected:
            failures.append(
                Failure("high_cardinality", f"excluded metric families were exported: {unexpected}", path.name)
            )


def _check_prometheus(evidence_dir: Path, contract: dict[str, Any], failures: list[Failure]) -> None:
    path = evidence_dir / "prometheus-query-results.json"
    if not path.is_file():
        return
    queries = _read_json(path)
    profiles = _workload_profiles()
    steady_profiles = sorted(
        (profile for profile in profiles.values() if not profile.get("over_quota")),
        key=lambda profile: profile["rate"],
    )
    if len(steady_profiles) < 2:
        raise ValueError("workload contract must define at least two steady profiles")
    low_topic = str(steady_profiles[0]["topic"])
    high_topic = str(steady_profiles[-1]["topic"])
    over_quota_profiles = [profile for profile in profiles.values() if profile.get("over_quota") is True]
    if len(over_quota_profiles) != 1:
        raise ValueError("workload contract must define exactly one over-quota profile")
    over_quota_scope = str(over_quota_profiles[0]["scope"])
    query_by_series = {
        "kafka_server_brokertopicmetrics_alltopics_bytesin_total": "alltopics_bytes_in",
        "kafka_server_brokertopicmetrics_alltopics_bytesout_total": "alltopics_bytes_out",
        "kafka_server_brokertopicmetrics_bytesin_total": "topic_bytes_in",
        "kafka_server_brokertopicmetrics_bytesout_total": "topic_bytes_out",
        "kafka_log_log_size": "partition_log_size",
        "kafka_server_quota_byte_rate": "quota_byte_rate",
        "kafka_server_quota_throttle_time_ms": "quota_throttle_time",
        "up": "up",
        "jmx_scrape_error": "jmx_scrape_error",
    }
    for metric in contract["metrics"]:
        name = metric["prometheus_series"]
        samples = _query_samples(queries, query_by_series[name])
        if not samples:
            failures.append(Failure("missing_metric", f"Prometheus has no samples for {name}", path.name))
            continue
        for labels, _ in samples:
            missing_labels = sorted(set(metric["required_labels"]) - labels.keys())
            if missing_labels:
                failures.append(Failure("missing_label", f"{name} sample lacks labels: {missing_labels}", path.name))
                break
            forbidden_labels = metric.get("forbidden_labels", [])
            present_forbidden_labels = sorted(set(forbidden_labels) & labels.keys())
            if present_forbidden_labels:
                failures.append(
                    Failure(
                        "cluster_selector",
                        f"{name} broker-wide sample has forbidden labels: {present_forbidden_labels}",
                        path.name,
                    )
                )
                break

    quota_queries = {
        "kafka_server_quota_byte_rate": "quota_byte_rate",
        "kafka_server_quota_throttle_time_ms": "quota_throttle_time",
    }
    for metric in contract["metrics"]:
        if "quota_scope" not in metric:
            continue
        series = metric["prometheus_series"]
        samples = _query_samples(queries, quota_queries[series])
        _, object_properties = _normalise_object_name(_resolved_raw_object_name(metric, profiles))
        properties = dict(object_properties)
        for cluster in ("cluster-a", "cluster-b"):
            matching = [
                value
                for labels, value in samples
                if labels.get("lab_cluster") == cluster
                and labels.get("quota_type") == metric["quota_type"]
                and labels.get("quota_scope") == metric["quota_scope"]
                and labels.get("user") == properties.get("user", NOT_APPLICABLE_LABEL)
                and labels.get("client_id") == properties.get("client-id", NOT_APPLICABLE_LABEL)
            ]
            if not matching:
                failures.append(
                    Failure(
                        "quota_scope",
                        f"{series} lacks {metric['quota_type']} {metric['quota_scope']} mapping for {cluster}",
                        path.name,
                    )
                )

    minimum_throttle = float(contract["rate_thresholds"]["minimum_throttle_time_ms"])
    for query_name, quota_type in (("produce_throttle_max", "Produce"), ("fetch_throttle_max", "Fetch")):
        samples = _query_samples(queries, query_name)
        matching = [
            value
            for labels, value in samples
            if labels.get("quota_type") == quota_type
            and labels.get("quota_scope") == over_quota_scope
            and math.isfinite(value)
        ]
        if not matching or max(matching) <= minimum_throttle:
            failures.append(
                Failure(
                    "throttle_observation",
                    f"positive {quota_type} throttle time was not observed for {over_quota_scope} scope",
                    path.name,
                )
            )

    minimum_ratio = float(contract["rate_thresholds"]["minimum_high_to_low_ratio"])
    for query_name in ("topic_bytes_in_rate", "topic_bytes_out_rate"):
        samples = _query_samples(queries, query_name)
        for cluster in ("cluster-a", "cluster-b"):
            low = [
                value
                for labels, value in samples
                if labels.get("lab_cluster") == cluster and labels.get("topic") == low_topic and math.isfinite(value)
            ]
            high = [
                value
                for labels, value in samples
                if labels.get("lab_cluster") == cluster and labels.get("topic") == high_topic and math.isfinite(value)
            ]
            if not low or not high or max(high) < max(low) * minimum_ratio:
                failures.append(
                    Failure(
                        "rate_distinction",
                        f"{query_name} lacks the required low/high separation for {cluster}",
                        path.name,
                    )
                )

    for query_name, expected_value in (("up", 1.0), ("jmx_scrape_error", 0.0)):
        samples = _query_samples(queries, query_name)
        healthy_clusters = {
            labels.get("lab_cluster")
            for labels, value in samples
            if value == expected_value and labels.get("kafka_cluster_id")
        }
        if healthy_clusters != {"cluster-a", "cluster-b"}:
            failures.append(Failure("target_health", f"{query_name} is not healthy for both targets", path.name))

    bytes_in = _query_samples(queries, "topic_bytes_in")
    shared_topic = [(labels, value) for labels, value in bytes_in if labels.get("topic") == low_topic]
    lab_clusters = {labels.get("lab_cluster") for labels, _ in shared_topic}
    kafka_cluster_ids = {labels.get("kafka_cluster_id") for labels, _ in shared_topic}
    if lab_clusters != {"cluster-a", "cluster-b"} or len(kafka_cluster_ids - {None}) != 2:
        failures.append(
            Failure(
                "cluster_selector",
                "overlapping topic series are not isolated by two explicit cluster identities",
                path.name,
            )
        )

    for query_name in ("alltopics_bytes_in", "alltopics_bytes_out"):
        samples = _query_samples(queries, query_name)
        cluster_ids = {
            labels.get("kafka_cluster_id")
            for labels, _ in samples
            if labels.get("lab_cluster") in {"cluster-a", "cluster-b"}
        }
        lab_clusters = {labels.get("lab_cluster") for labels, _ in samples}
        if lab_clusters != {"cluster-a", "cluster-b"} or len(cluster_ids - {None}) != 2:
            failures.append(
                Failure(
                    "cluster_selector",
                    f"{query_name} broker-wide series are not isolated by two explicit cluster identities",
                    path.name,
                )
            )


def _restart_requirements(contract: dict[str, Any]) -> dict[str, Any]:
    workloads = yaml.safe_load(WORKLOADS_PATH.read_text(encoding="utf-8"))
    if not isinstance(workloads, dict):
        raise TypeError("workload contract must be a mapping")
    quota_profiles = workloads.get("quota_profiles")
    topics = workloads.get("topics")
    rates = workloads.get("rates_bytes_per_second")
    if not isinstance(quota_profiles, list) or not isinstance(topics, list) or not isinstance(rates, dict):
        raise TypeError("workload contract is incomplete")
    if {str(profile["scope"]) for profile in quota_profiles} != set(contract["quota_scopes"]):
        raise ValueError("workload quota scopes do not match the metric contract")
    quotas = [
        {
            "profile": str(profile["profile"]),
            "scope": str(profile["scope"]),
            "user": str(profile.get("user", "")) if str(profile["scope"]) in {"user", "user-client"} else "",
            "client_id": str(profile.get("client_id", ""))
            if str(profile["scope"]) in {"client-id", "user-client"}
            else "",
            "producer_byte_rate": int(profile["quota_limit_bytes_per_second"]),
            "consumer_byte_rate": int(profile["quota_limit_bytes_per_second"]),
            "over_quota": profile.get("over_quota") is True,
        }
        for profile in quota_profiles
    ]
    traffic_profiles = [
        {
            "profile": str(topic["profile"]),
            "topic": str(topic["name"]),
            "expected_bytes_per_second": int(rates[str(topic["profile"])]),
        }
        for topic in topics
    ]
    return {
        "users": sorted({str(profile["user"]) for profile in quota_profiles}),
        "client_ids": sorted({str(profile["client_id"]) for profile in quota_profiles}),
        "quotas": quotas,
        "topics": sorted(profile["topic"] for profile in traffic_profiles),
        "traffic_profiles": traffic_profiles,
    }


def _missing_dicts(required: list[dict[str, Any]], observed: Any) -> list[dict[str, Any]]:
    if not isinstance(observed, list):
        return required
    return [item for item in required if item not in observed]


def _check_restart_manifest(evidence_dir: Path, contract: dict[str, Any], failures: list[Failure]) -> None:
    restart_path = evidence_dir / "clean-restart-manifest.json"
    restart = _read_json(restart_path) if restart_path.is_file() else {}
    if not isinstance(restart, dict):
        failures.append(Failure("evidence_stale", "clean restart manifest is not an object", restart_path.name))
        return
    generation_id = restart.get("generation_id")
    previous_generation_id = restart.get("previous_generation_id")
    if (
        restart.get("state_recreated") is not True
        or not isinstance(generation_id, str)
        or not isinstance(previous_generation_id, str)
        or generation_id == previous_generation_id
    ):
        failures.append(
            Failure("evidence_stale", "clean restart generation identity was not recreated", restart_path.name)
        )

    requirements = _restart_requirements(contract)
    clusters = restart.get("clusters")
    if not isinstance(clusters, dict):
        clusters = {}
    for cluster in ("cluster-a", "cluster-b"):
        cluster_state = clusters.get(cluster)
        if not isinstance(cluster_state, dict):
            cluster_state = {}
        declared = cluster_state.get("required")
        observed = cluster_state.get("observed")
        if not isinstance(declared, dict):
            declared = {}
        if not isinstance(observed, dict):
            observed = {}

        for key in ("users", "client_ids", "topics"):
            missing_declared = sorted(set(requirements[key]) - set(declared.get(key, [])))
            if missing_declared:
                failures.append(
                    Failure(
                        "evidence_stale",
                        f"clean restart manifest omits required {cluster} {key}: {missing_declared}",
                        restart_path.name,
                    )
                )
            missing_observed = sorted(set(requirements[key]) - set(observed.get(key, [])))
            if missing_observed:
                failures.append(
                    Failure(
                        "evidence_stale",
                        f"clean restart did not observe {cluster} {key}: {missing_observed}",
                        restart_path.name,
                    )
                )

        for key in ("quotas", "traffic_profiles"):
            missing_declared_dicts = _missing_dicts(requirements[key], declared.get(key))
            if missing_declared_dicts:
                failures.append(
                    Failure(
                        "evidence_stale",
                        f"clean restart manifest omits required {cluster} {key}: {missing_declared_dicts}",
                        restart_path.name,
                    )
                )

        missing_quotas = _missing_dicts(requirements["quotas"], observed.get("quotas"))
        if missing_quotas:
            failures.append(
                Failure(
                    "evidence_stale",
                    f"clean restart did not observe {cluster} producer and consumer quotas: {missing_quotas}",
                    restart_path.name,
                )
            )

        observed_traffic = observed.get("traffic_profiles")
        if not isinstance(observed_traffic, list):
            observed_traffic = []
        for required_profile in requirements["traffic_profiles"]:
            matching = [
                profile
                for profile in observed_traffic
                if isinstance(profile, dict)
                and all(profile.get(key) == value for key, value in required_profile.items())
            ]
            if not matching or any(
                profile.get("producer_active") is not True or profile.get("consumer_active") is not True
                for profile in matching
            ):
                failures.append(
                    Failure(
                        "evidence_stale",
                        f"clean restart did not observe active {cluster} traffic profile: {required_profile}",
                        restart_path.name,
                    )
                )


def validate(evidence_dir: Path, require_recreated_state: bool) -> dict[str, Any]:
    contract = yaml.safe_load(CONTRACT_PATH.read_text(encoding="utf-8"))
    if not isinstance(contract, dict):
        raise ValueError("metric contract must be a mapping")
    failures: list[Failure] = []

    _check_required_files(evidence_dir, contract, failures)
    _check_freshness(evidence_dir, contract, failures)
    _check_raw_jmx(evidence_dir, contract, failures)
    _check_exporters(evidence_dir, contract, failures)
    _check_prometheus(evidence_dir, contract, failures)

    cluster_ids_path = evidence_dir / "cluster-id-comparison.json"
    if cluster_ids_path.is_file() and not _read_json(cluster_ids_path).get("distinct"):
        failures.append(Failure("cluster_selector", "Kafka cluster IDs are not distinct", cluster_ids_path.name))

    if require_recreated_state:
        _check_restart_manifest(evidence_dir, contract, failures)

    return {
        "status": "pass" if not failures else "fail",
        "validated_at": datetime.now(tz=UTC).isoformat(),
        "evidence_dir": str(evidence_dir),
        "failures": [asdict(failure) for failure in failures],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate Kafka telemetry lab evidence")
    parser.add_argument("--evidence-dir", type=Path, default=LAB_DIR / "evidence" / "latest")
    parser.add_argument("--require-recreated-state", action="store_true")
    args = parser.parse_args()

    result = validate(args.evidence_dir.resolve(), args.require_recreated_state)
    result_path = args.evidence_dir.resolve() / "validator-result.json"
    result_path.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(result_path)
    if result["status"] != "pass":
        sys.exit(1)


if __name__ == "__main__":
    main()

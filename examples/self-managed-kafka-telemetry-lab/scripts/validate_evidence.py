from __future__ import annotations

import argparse
import json
import math
import re
import sys
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from decimal import ROUND_DOWN, Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import yaml

LAB_DIR = Path(__file__).resolve().parents[1]
CONTRACT_PATH = LAB_DIR / "contracts" / "metric-contract.yaml"
PRINCIPAL_CONTRACT_PATH = LAB_DIR / "contracts" / "principal-allocation-contract.yaml"
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


def _principal_contract() -> tuple[dict[str, Any] | None, Failure | None]:
    try:
        contract = yaml.safe_load(PRINCIPAL_CONTRACT_PATH.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None, Failure(
            "principal_contract", "principal allocation contract is absent", PRINCIPAL_CONTRACT_PATH.name
        )
    except OSError, yaml.YAMLError:
        return None, Failure(
            "principal_contract", "principal allocation contract is invalid", PRINCIPAL_CONTRACT_PATH.name
        )
    if not isinstance(contract, dict):
        return None, Failure(
            "principal_contract", "principal allocation contract is invalid", PRINCIPAL_CONTRACT_PATH.name
        )
    return contract, None


def _decimal_from_source(value: Any) -> Decimal | None:
    if isinstance(value, bool):
        return None
    try:
        parsed = Decimal(str(value))
    except InvalidOperation, TypeError, ValueError:
        return None
    return parsed if parsed.is_finite() else None


def _source_number(value: Decimal) -> str:
    return format(value, "f")


def _source_timestamp(value: Decimal) -> int | float:
    if value == value.to_integral_value():
        return int(value)
    return float(value)


def _principal_query(
    queries: dict[str, Any], name: str, description: str, result_type: str
) -> tuple[list[dict[str, Any]] | None, Failure | None]:
    query = queries.get(name)
    if not isinstance(query, dict):
        return None, Failure(
            "principal_contract", f"principal query is absent: {name}", "principal-raw-query-results.json"
        )
    response = query.get("response")
    if not isinstance(response, dict):
        return None, Failure(
            "principal_contract", f"principal query response is absent: {name}", "principal-raw-query-results.json"
        )
    if response.get("status") != "success":
        error = response.get("error")
        message = str(error) if error else "unknown Prometheus error"
        return None, Failure(
            "principal_metric", f"{description} query failed: {message}", "principal-raw-query-results.json"
        )
    data = response.get("data")
    results = data.get("result") if isinstance(data, dict) else None
    if not isinstance(data, dict) or data.get("resultType") != result_type or not isinstance(results, list):
        return None, Failure(
            "principal_contract",
            f"{description} query does not contain a {result_type} response",
            "principal-raw-query-results.json",
        )
    return [result for result in results if isinstance(result, dict)], None


def _parse_raw_samples(raw_values: Any) -> tuple[list[tuple[Decimal, Decimal]] | None, bool]:
    if not isinstance(raw_values, list):
        return None, True
    parsed: dict[Decimal, Decimal] = {}
    for raw_sample in raw_values:
        if not isinstance(raw_sample, list) or len(raw_sample) != 2:
            return None, True
        raw_timestamp, raw_value = raw_sample
        if not isinstance(raw_timestamp, (int, float)) or isinstance(raw_timestamp, bool):
            return None, True
        timestamp = _decimal_from_source(raw_timestamp)
        if timestamp is None:
            return None, True
        value = _decimal_from_source(raw_value)
        if value is None or value < 0:
            return None, True
        previous = parsed.get(timestamp)
        if previous is not None and previous != value:
            return None, True
        parsed[timestamp] = value
    return sorted(parsed.items()), False


def _quota_sample_failure(results: list[dict[str, Any]], description: str) -> Failure | None:
    for result in results:
        values = result.get("values")
        if not isinstance(values, list):
            return Failure(
                "principal_metric",
                f"{description} contains an invalid sample",
                "principal-raw-query-results.json",
            )
        timestamps: set[Decimal] = set()
        for sample in values:
            if not isinstance(sample, list) or len(sample) != 2:
                return Failure(
                    "principal_metric",
                    f"{description} contains an invalid sample",
                    "principal-raw-query-results.json",
                )
            timestamp = _decimal_from_source(sample[0])
            if timestamp is None or timestamp in timestamps:
                return Failure(
                    "principal_metric",
                    f"{description} contains an invalid sample",
                    "principal-raw-query-results.json",
                )
            timestamps.add(timestamp)
            value = _decimal_from_source(sample[1])
            if value is None:
                message = (
                    f"{description} contains a non-finite sample"
                    if str(sample[1]).casefold() in {"nan", "inf", "+inf", "-inf"}
                    else f"{description} contains an invalid sample"
                )
                return Failure("principal_metric", message, "principal-raw-query-results.json")
            if value < 0:
                return Failure(
                    "principal_metric",
                    f"{description} contains a negative sample",
                    "principal-raw-query-results.json",
                )
    return None


def _analyse_source_samples(raw_values: Any, start: int, end: int, max_gap: int) -> dict[str, Any]:
    samples, invalid = _parse_raw_samples(raw_values)
    analysis: dict[str, Any] = {
        "raw_samples": [],
        "selected_leading_guard_timestamp": None,
        "segments": [],
        "gaps": [],
        "synthetic_samples": [],
        "lookback_samples": [],
        "interpolated_segments": [],
        "over_max_gap_holds": [],
        "complete": False,
        "invalid": invalid,
        "integrated_quantity": Decimal(0),
    }
    if samples is None:
        return analysis
    start_timestamp = Decimal(start)
    end_timestamp = Decimal(end)
    max_gap_seconds = Decimal(max_gap)
    analysis["raw_samples"] = [
        {"timestamp": _source_timestamp(timestamp), "value": _source_number(value)} for timestamp, value in samples
    ]
    leading = [sample for sample in samples if sample[0] <= start_timestamp]
    if not leading:
        analysis["gaps"].append(
            {
                "reason": "missing_leading_guard",
                "start_timestamp": start,
                "end_timestamp": start,
            }
        )
        return analysis
    guard = leading[-1]
    analysis["selected_leading_guard_timestamp"] = _source_timestamp(guard[0])
    if start_timestamp - guard[0] > max_gap_seconds:
        analysis["gaps"].append(
            {
                "reason": "old_leading_guard",
                "start_timestamp": _source_timestamp(guard[0]),
                "end_timestamp": start,
            }
        )
        return analysis

    points = [guard, *(sample for sample in samples if start_timestamp < sample[0] <= end_timestamp)]
    for previous, current in zip(points, points[1:], strict=False):
        duration = current[0] - previous[0]
        if duration <= 0:
            analysis["invalid"] = True
            continue
        if duration > max_gap_seconds:
            analysis["gaps"].append(
                {
                    "reason": "internal_gap",
                    "start_timestamp": _source_timestamp(previous[0]),
                    "end_timestamp": _source_timestamp(current[0]),
                }
            )
            continue
        clipped_start = max(start_timestamp, previous[0])
        clipped_end = min(end_timestamp, current[0])
        if clipped_end <= clipped_start:
            continue
        integrated = previous[1] * (clipped_end - clipped_start)
        analysis["segments"].append(
            {
                "start_timestamp": _source_timestamp(clipped_start),
                "end_timestamp": _source_timestamp(clipped_end),
                "duration_seconds": _source_timestamp(clipped_end - clipped_start),
                "held_value": _source_number(previous[1]),
                "integrated_quantity": _source_number(integrated),
            }
        )
        analysis["integrated_quantity"] += integrated

    last = points[-1]
    if last[0] < end_timestamp:
        trailing = end_timestamp - last[0]
        if trailing > max_gap_seconds:
            analysis["gaps"].append(
                {
                    "reason": "stale_trailing_sample",
                    "start_timestamp": _source_timestamp(last[0]),
                    "end_timestamp": end,
                }
            )
        else:
            integrated = last[1] * trailing
            analysis["segments"].append(
                {
                    "start_timestamp": _source_timestamp(last[0]),
                    "end_timestamp": end,
                    "duration_seconds": _source_timestamp(trailing),
                    "held_value": _source_number(last[1]),
                    "integrated_quantity": _source_number(integrated),
                }
            )
            analysis["integrated_quantity"] += integrated

    analysis["complete"] = not analysis["invalid"] and not analysis["gaps"] and last[0] <= end_timestamp
    return analysis


def _source_summary(analyses: list[dict[str, Any]]) -> dict[str, Any]:
    if not analyses:
        return {
            "series": [],
            "selected_leading_guard_timestamp": None,
            "segments": [],
            "gaps": [{"reason": "no_series"}],
            "synthetic_samples": [],
            "lookback_samples": [],
            "interpolated_segments": [],
            "over_max_gap_holds": [],
            "complete": False,
        }
    primary = analyses[0]
    return {
        "series": [
            {
                "raw_samples": analysis["raw_samples"],
                "selected_leading_guard_timestamp": analysis["selected_leading_guard_timestamp"],
                "segments": analysis["segments"],
                "gaps": analysis["gaps"],
                "complete": analysis["complete"],
            }
            for analysis in analyses
        ],
        "selected_leading_guard_timestamp": primary["selected_leading_guard_timestamp"],
        "segments": primary["segments"],
        "gaps": [gap for analysis in analyses for gap in analysis["gaps"]],
        "synthetic_samples": [],
        "lookback_samples": [],
        "interpolated_segments": [],
        "over_max_gap_holds": [hold for analysis in analyses for hold in analysis["over_max_gap_holds"]],
        "complete": all(analysis["complete"] for analysis in analyses),
    }


def _observed_timestamp_deltas(response: Any) -> list[list[int | float]] | None:
    data = response.get("data") if isinstance(response, dict) else None
    results = data.get("result") if isinstance(data, dict) else None
    if not isinstance(results, list):
        return None
    deltas_by_series: list[list[int | float]] = []
    for result in results:
        values = result.get("values") if isinstance(result, dict) else None
        if not isinstance(values, list):
            return None
        timestamps: list[Decimal] = []
        for sample in values:
            if not isinstance(sample, list) or len(sample) != 2:
                return None
            timestamp = _decimal_from_source(sample[0])
            if timestamp is None:
                return None
            timestamps.append(timestamp)
        timestamps.sort()
        deltas_by_series.append(
            [
                _source_timestamp(current - previous)
                for previous, current in zip(timestamps, timestamps[1:], strict=False)
            ]
        )
    return deltas_by_series


def _valid_quota_labels(metric: dict[str, str], selector: dict[str, str], quota_type: str) -> bool:
    metric_selector = {key: value for key, value in selector.items() if key != "job"}
    if metric.get("quota_type") != quota_type or any(
        metric.get(key) != value for key, value in metric_selector.items()
    ):
        return False
    scope = metric.get("quota_scope")
    user = metric.get("user")
    client_id = metric.get("client_id")
    if scope == "user":
        return bool(user) and user != NOT_APPLICABLE_LABEL and client_id == NOT_APPLICABLE_LABEL
    if scope == "user-client":
        return bool(user) and user != NOT_APPLICABLE_LABEL and bool(client_id) and client_id != NOT_APPLICABLE_LABEL
    if scope == "client-id":
        return user == NOT_APPLICABLE_LABEL and bool(client_id) and client_id != NOT_APPLICABLE_LABEL
    return False


def _direction_state(
    complete: bool,
    total_weight: Decimal,
    client_only_weight: Decimal,
    structural_invalid: bool,
) -> str:
    if structural_invalid or not complete:
        return "unavailable"
    if total_weight == 0:
        return "zero_usage"
    return "degraded" if client_only_weight > 0 else "ready"


def _quantized_amount(value: Decimal, quantum: Decimal) -> Decimal:
    return value.quantize(quantum, rounding=ROUND_DOWN)


def _direction_money(
    state: str,
    pool: Decimal,
    weights_by_user: dict[str, Decimal],
    client_only_weight: Decimal,
    total_weight: Decimal,
    owners: dict[str, str],
    default_owner: str,
    quantum: Decimal,
) -> dict[str, Any]:
    if state == "unavailable" or state == "zero_usage":
        return {
            "users": [],
            "client_only": {
                "weight": _source_number(client_only_weight),
                "raw_amount": "0.0000",
                "amount": "0.0000",
            },
            "rounding_residual": "0.0000",
            "unallocated": _source_number(pool),
            "balance": _source_number(pool),
        }

    raw_users = {identity: pool * weight / total_weight for identity, weight in weights_by_user.items()}
    users = [
        {
            "identity": identity,
            "owner": owners.get(identity, default_owner),
            "weight": _source_number(weight),
            "raw_amount": _source_number(raw_users[identity]),
            "amount": _source_number(_quantized_amount(raw_users[identity], quantum)),
        }
        for identity, weight in sorted(weights_by_user.items())
    ]
    raw_client_only = pool * client_only_weight / total_weight
    client_amount = _quantized_amount(raw_client_only, quantum)
    user_amount = sum((Decimal(row["amount"]) for row in users), Decimal(0))
    rounding_residual = pool - user_amount - client_amount
    if rounding_residual < 0:
        return {
            "users": [],
            "client_only": {
                "weight": _source_number(client_only_weight),
                "raw_amount": "0.0000",
                "amount": "0.0000",
            },
            "rounding_residual": _source_number(pool),
            "unallocated": _source_number(pool),
            "balance": _source_number(pool),
            "structural_invalid": True,
        }
    unallocated = client_amount + rounding_residual
    return {
        "users": users,
        "client_only": {
            "weight": _source_number(client_only_weight),
            "raw_amount": _source_number(raw_client_only),
            "amount": _source_number(client_amount),
        },
        "rounding_residual": _source_number(rounding_residual),
        "unallocated": _source_number(unallocated),
        "balance": _source_number(user_amount + unallocated),
    }


def _sampling_resolution(queries: dict[str, Any], window: dict[str, Any], contract: dict[str, Any]) -> dict[str, Any]:
    recorded = window.get("sampling_resolution")
    if isinstance(recorded, dict):
        return recorded
    return {
        "declared_scrape_interval_seconds": int(window["scrape_interval_seconds"]),
        "declared_max_gap_seconds": int(window["max_gap_seconds"]),
        "observed_timestamp_deltas_seconds": {
            name: _observed_timestamp_deltas(query.get("response"))
            for name, query in queries.items()
            if isinstance(query, dict)
        },
        "quota_rate_window": contract["quota_rate_window"],
        "estimate": contract["sampling_semantics"]["estimate"],
        "byte_exact": contract["sampling_semantics"]["byte_exact"],
        "limitation": "quota weights are monitoring-resolution estimates, not byte-exact totals",
    }


def _scope_response_proves_target(response: Any, window: dict[str, Any], contract: dict[str, Any]) -> bool:
    data = response.get("data") if isinstance(response, dict) else None
    results = data.get("result") if isinstance(data, dict) else None
    selector = window.get("selector")
    if (
        not isinstance(response, dict)
        or response.get("status") != "success"
        or not isinstance(data, dict)
        or data.get("resultType") != "matrix"
        or not isinstance(results, list)
        or len(results) != contract["expected_targets"]
        or not isinstance(selector, dict)
    ):
        return False
    try:
        start = int(window["start_timestamp"])
        end = int(window["end_timestamp"])
        max_gap = int(window["max_gap_seconds"])
    except KeyError, TypeError, ValueError:
        return False
    for result in results:
        metric = result.get("metric") if isinstance(result, dict) else None
        if not isinstance(metric, dict) or any(metric.get(key) != value for key, value in selector.items()):
            return False
        analysis = _analyse_source_samples(result.get("values"), start, end, max_gap)
        values = analysis["raw_samples"]
        if not analysis["complete"] or any(Decimal(sample["value"]) <= 0 for sample in values):
            return False
    return True


def _unavailable_direction(
    queries: dict[str, Any],
    direction: str,
    window: dict[str, Any],
    contract: dict[str, Any],
    *,
    failure: Failure | None,
    quota_summary: dict[str, Any] | None = None,
    configuration_complete: bool = True,
    target_complete: bool = True,
    samples_complete: bool = False,
    identity_complete: bool = False,
) -> dict[str, Any]:
    pool = _decimal_from_source(contract["demonstration_pools"][direction])
    assert pool is not None
    ownership = contract["identity"]["ownership"]
    rows = _direction_money(
        "unavailable",
        pool,
        {},
        Decimal(0),
        Decimal(0),
        {str(key): str(value) for key, value in ownership["mappings"].items()},
        str(ownership["default_owner"]),
        Decimal(contract["money"]["quantum"]),
    )
    return {
        "logical_billing_interval": contract["logical_billing_interval"],
        "source_membership": contract["provider_source_membership"],
        "pool": _source_number(pool),
        "weights": {"user_total": "0", "client_only": "0", "total": "0"},
        "coverage": {
            "configuration": configuration_complete,
            "target": target_complete,
            "samples": samples_complete,
            "identity": identity_complete,
            "complete": False,
        },
        "sampling_resolution": _sampling_resolution(queries, window, contract),
        "quota_sources": quota_summary or _source_summary([]),
        "state": "unavailable",
        "reason": failure.message if failure is not None else "principal quota evidence is unavailable",
        "error": asdict(failure) if failure is not None else None,
        **rows,
    }


def _principal_direction(
    queries: dict[str, Any],
    direction: str,
    definition: dict[str, Any],
    window: dict[str, Any],
    contract: dict[str, Any],
) -> tuple[dict[str, Any] | None, Failure | None]:
    quota_type = str(definition["quota_type"])
    configuration_complete = True
    target_complete = True
    quota_results, failure = _principal_query(queries, f"quota_{quota_type.lower()}", f"quota {quota_type}", "matrix")
    if failure is not None:
        return (
            _unavailable_direction(
                queries,
                direction,
                window,
                contract,
                failure=failure,
                configuration_complete=configuration_complete,
                target_complete=target_complete,
            ),
            failure,
        )
    assert quota_results is not None

    sample_failure = _quota_sample_failure(quota_results, f"quota {quota_type}")
    if sample_failure is not None:
        return (
            _unavailable_direction(
                queries,
                direction,
                window,
                contract,
                failure=sample_failure,
                configuration_complete=configuration_complete,
                target_complete=target_complete,
            ),
            sample_failure,
        )

    start = int(window["start_timestamp"])
    end = int(window["end_timestamp"])
    max_gap = int(window["max_gap_seconds"])
    selector = {str(key): str(value) for key, value in window["selector"].items()}
    quota_analyses: list[dict[str, Any]] = []
    weights_by_user: dict[str, Decimal] = {}
    client_only_weight = Decimal(0)
    structural_invalid = False
    quota_identities: set[tuple[str, str, str, str]] = set()

    for result in quota_results:
        metric = result.get("metric")
        if not isinstance(metric, dict):
            failure = Failure(
                "principal_identity",
                f"quota {quota_type} contains invalid quota identity labels",
                "principal-raw-query-results.json",
            )
            return (
                _unavailable_direction(
                    queries,
                    direction,
                    window,
                    contract,
                    failure=failure,
                    configuration_complete=configuration_complete,
                    target_complete=target_complete,
                ),
                failure,
            )
        labels = {str(key): str(value) for key, value in metric.items()}
        analysis = _analyse_source_samples(result.get("values"), start, end, max_gap)
        quota_analyses.append(analysis)
        if not _valid_quota_labels(labels, selector, quota_type):
            failure = Failure(
                "principal_identity",
                f"quota {quota_type} contains invalid quota identity labels",
                "principal-raw-query-results.json",
            )
            return _unavailable_direction(
                queries,
                direction,
                window,
                contract,
                failure=failure,
                quota_summary=_source_summary(quota_analyses),
                configuration_complete=configuration_complete,
                target_complete=target_complete,
            ), failure
        identity_key = (
            labels["quota_scope"],
            labels["user"],
            labels["client_id"],
            labels.get("broker", ""),
        )
        if identity_key in quota_identities:
            structural_invalid = True
        quota_identities.add(identity_key)
        weight = analysis["integrated_quantity"]
        if labels["quota_scope"] == "client-id":
            client_only_weight += weight
        elif weight > 0:
            identity = f"{contract['identity']['canonical_prefix']}{labels['user']}"
            weights_by_user[identity] = weights_by_user.get(identity, Decimal(0)) + weight

    quota_summary = _source_summary(quota_analyses)
    samples_complete = bool(quota_analyses) and quota_summary["complete"]
    identity_complete = not structural_invalid
    complete = configuration_complete and target_complete and samples_complete and identity_complete
    user_weight = sum(weights_by_user.values(), Decimal(0))
    total_weight = user_weight + client_only_weight
    state = _direction_state(complete, total_weight, client_only_weight, structural_invalid)

    money = contract["money"]
    quantum = _decimal_from_source(money.get("quantum"))
    if quantum is None or quantum <= 0:
        return None, Failure("principal_contract", "principal money policy is invalid", PRINCIPAL_CONTRACT_PATH.name)
    pool_amount = _decimal_from_source(contract["demonstration_pools"].get(direction))
    if pool_amount is None:
        return None, Failure(
            "principal_contract", f"principal {direction} money pool is invalid", PRINCIPAL_CONTRACT_PATH.name
        )
    if pool_amount < 0:
        return None, Failure(
            "principal_reconciliation",
            "principal money reconciliation produced a negative rounding residual",
            "principal-allocation-demonstration.json",
        )
    ownership = contract["identity"]["ownership"]
    owners = {str(key): str(value) for key, value in ownership["mappings"].items()}
    rows = _direction_money(
        state,
        pool_amount,
        weights_by_user,
        client_only_weight,
        total_weight,
        owners,
        str(ownership["default_owner"]),
        quantum,
    )
    if rows.pop("structural_invalid", False):
        return None, Failure(
            "principal_reconciliation",
            "principal money reconciliation produced a negative rounding residual",
            "principal-allocation-demonstration.json",
        )
    return {
        "logical_billing_interval": contract["logical_billing_interval"],
        "source_membership": contract["provider_source_membership"],
        "pool": _source_number(pool_amount),
        "weights": {
            "user_total": _source_number(user_weight),
            "client_only": _source_number(client_only_weight),
            "total": _source_number(total_weight),
        },
        "coverage": {
            "configuration": configuration_complete,
            "target": target_complete,
            "samples": samples_complete,
            "identity": identity_complete,
            "complete": complete,
        },
        "sampling_resolution": _sampling_resolution(queries, window, contract),
        "quota_sources": quota_summary,
        "state": state,
        "reason": "principal quota weights evaluated independently of topic metrics",
        "error": None,
        **rows,
    }, None


def _fixed_category_results(contract: dict[str, Any]) -> dict[str, dict[str, Any]]:
    results: dict[str, dict[str, Any]] = {}
    quantum = _decimal_from_source(contract["money"].get("quantum"))
    if quantum is None or quantum <= 0:
        raise ValueError("principal money policy is invalid")
    for category, policy_input in contract["fixed_policy_inputs"].items():
        pool = Decimal(str(contract["demonstration_pools"][category]))
        policy = str(policy_input.get("policy", "")) if isinstance(policy_input, dict) else ""
        identities = policy_input.get("identities", []) if isinstance(policy_input, dict) else []
        if (
            policy == "static_even_v1"
            and isinstance(identities, list)
            and identities
            and all(isinstance(identity, str) and identity for identity in identities)
        ):
            sorted_identities = sorted(str(identity) for identity in identities)
            raw_amount = pool / Decimal(len(sorted_identities))
            users = [
                {"identity": identity, "amount": _source_number(_quantized_amount(raw_amount, quantum))}
                for identity in sorted_identities
            ]
            allocated = sum((Decimal(user["amount"]) for user in users), Decimal(0))
            rounding_residual = pool - allocated
            results[category] = {
                "state": "policy_only",
                "policy": policy,
                "shared": True,
                "measured_usage": False,
                "users": users,
                "pool": _source_number(pool),
                "rounding_residual": _source_number(rounding_residual),
                "unallocated": _source_number(rounding_residual),
                "balance": _source_number(allocated + rounding_residual),
            }
            continue
        results[category] = {
            "state": "unattributed",
            "policy": "unattributed",
            "shared": True,
            "measured_usage": False,
            "users": [],
            "pool": _source_number(pool),
            "rounding_residual": "0.0000",
            "unallocated": _source_number(pool),
            "balance": _source_number(pool),
        }
    return results


def _principal_contract_failure(contract: dict[str, Any]) -> Failure | None:
    required = {
        "version",
        "logical_billing_interval",
        "provider_source_membership",
        "selector",
        "expected_targets",
        "retention_days",
        "identity",
        "quota_scopes",
        "expected_live_identities",
        "expected_live_states",
        "expected_live_balances",
        "scope_first",
        "scrape_interval_seconds",
        "max_gap_seconds",
        "quota_rate_window",
        "sampling_semantics",
        "principal_topic_relationship",
        "forbidden_inferences",
        "identity_churn",
        "directions",
        "money",
        "demonstration_pools",
        "fixed_policy_inputs",
        "allocation_rules",
        "state_precedence",
        "artifact_schema",
        "error_categories",
        "error_schema",
        "lifecycle",
    }
    if not required <= contract.keys():
        return Failure("principal_contract", "principal allocation contract is invalid", PRINCIPAL_CONTRACT_PATH.name)

    selector = contract["selector"]
    directions = contract["directions"]
    identity = contract["identity"]
    expected_identities = contract["expected_live_identities"]
    expected_states = contract["expected_live_states"]
    expected_balances = contract["expected_live_balances"]
    money = contract["money"]
    pools = contract["demonstration_pools"]
    policies = contract["fixed_policy_inputs"]
    if (
        not isinstance(contract["version"], int)
        or isinstance(contract["version"], bool)
        or not isinstance(selector, dict)
        or not selector
        or any(not isinstance(key, str) or not isinstance(value, str) or not value for key, value in selector.items())
        or not isinstance(contract["expected_targets"], int)
        or isinstance(contract["expected_targets"], bool)
        or contract["expected_targets"] <= 0
        or not isinstance(contract["retention_days"], int)
        or isinstance(contract["retention_days"], bool)
        or contract["retention_days"] < 14
        or not isinstance(directions, dict)
        or set(directions) != {"ingress", "egress"}
        or any(
            not isinstance(definition, dict)
            or not isinstance(definition.get("quota_type"), str)
            or set(definition) != {"quota_type"}
            for definition in directions.values()
        )
        or contract["scope_first"] is not True
        or not isinstance(contract["scrape_interval_seconds"], int)
        or isinstance(contract["scrape_interval_seconds"], bool)
        or contract["scrape_interval_seconds"] <= 0
        or not isinstance(contract["max_gap_seconds"], int)
        or isinstance(contract["max_gap_seconds"], bool)
        or contract["max_gap_seconds"] <= 0
        or contract["quota_rate_window"]
        != {
            "source": "kafka_default",
            "complete_windows": 10,
            "window_seconds": 1,
            "includes_current_window": True,
        }
        or contract["sampling_semantics"]
        != {
            "completeness_basis": "configured_datasource_cadence_and_gap",
            "estimate": "monitoring_resolution",
            "byte_exact": False,
            "production_interval_fixed": False,
        }
        or contract["quota_scopes"] != ["user", "client-id", "user-client"]
        or not isinstance(identity, dict)
        or identity.get("canonical_prefix") != "User:"
        or identity.get("preserve_suffix") != "case-sensitive"
        or identity.get("user_scope_client_id") != NOT_APPLICABLE_LABEL
        or not isinstance(identity.get("ownership"), dict)
        or not isinstance(identity["ownership"].get("mappings"), dict)
        or not isinstance(identity["ownership"].get("default_owner"), str)
        or not isinstance(expected_identities, dict)
        or not isinstance(expected_identities.get("mapped_user"), dict)
        or not isinstance(expected_identities.get("unmapped_user"), dict)
        or not isinstance(expected_identities.get("client_only_label"), str)
        or any(
            not isinstance(expected_identities[field].get("identity"), str)
            or not isinstance(expected_identities[field].get("owner"), str)
            for field in ("mapped_user", "unmapped_user")
        )
        or not isinstance(expected_states, dict)
        or set(expected_states) != set(directions)
        or any(state not in _PRINCIPAL_STATES for state in expected_states.values())
        or not isinstance(expected_balances, dict)
        or set(expected_balances) != set(directions)
        or any(_decimal_from_source(balance) is None for balance in expected_balances.values())
        or money != {"quantum": "0.0001", "rounding": "ROUND_DOWN"}
        or not isinstance(pools, dict)
        or set(pools) != {"ingress", "egress", "compute", "storage", "shared"}
        or any((value := _decimal_from_source(pool)) is None or value < 0 for pool in pools.values())
        or not isinstance(policies, dict)
        or set(policies) != {"compute", "storage", "shared"}
        or contract["principal_topic_relationship"] != "independent_marginals"
        or contract["forbidden_inferences"] != ["principal_by_topic", "topic_owner_rollup"]
        or contract["identity_churn"] != "case-sensitive identities remain distinct"
    ):
        return Failure("principal_contract", "principal allocation contract is invalid", PRINCIPAL_CONTRACT_PATH.name)

    quantum = _decimal_from_source(money["quantum"])
    if quantum is None or quantum <= 0:
        return Failure("principal_contract", "principal allocation contract is invalid", PRINCIPAL_CONTRACT_PATH.name)
    owners = identity["ownership"]
    mapped = expected_identities["mapped_user"]
    unmapped = expected_identities["unmapped_user"]
    if (
        owners["mappings"].get(mapped["identity"]) != mapped["owner"]
        or unmapped["owner"] != owners["default_owner"]
        or not expected_identities["client_only_label"]
    ):
        return Failure("principal_contract", "principal allocation contract is invalid", PRINCIPAL_CONTRACT_PATH.name)
    if not _valid_allocation_rules(contract["allocation_rules"]):
        return Failure("principal_contract", "principal allocation contract is invalid", PRINCIPAL_CONTRACT_PATH.name)
    if not _valid_fixed_policies(policies):
        return Failure("principal_contract", "principal allocation contract is invalid", PRINCIPAL_CONTRACT_PATH.name)
    if not _valid_artifact_schema(contract["artifact_schema"]):
        return Failure("principal_contract", "principal allocation contract is invalid", PRINCIPAL_CONTRACT_PATH.name)

    error_categories = contract["error_categories"]
    error_schema = contract["error_schema"]
    if (
        error_categories != _PRINCIPAL_ERROR_CATEGORIES
        or not isinstance(error_schema, dict)
        or error_schema.get("required") != ["category", "message", "artifact"]
        or error_schema.get("types")
        != {"category": "error_category_enum", "message": "nonempty_string", "artifact": "string_or_null"}
        or error_schema.get("category_values") != _PRINCIPAL_ERROR_CATEGORIES
        or contract["lifecycle"]
        != {
            "principal_day_terminal": True,
            "later_days_continue": True,
            "topic_lane_independent": True,
            "reprocess": {"mode": "explicit", "replaces_date": True, "scope": "cluster-day"},
        }
        or contract["state_precedence"] != _PRINCIPAL_STATE_PRECEDENCE
    ):
        return Failure("principal_contract", "principal allocation contract is invalid", PRINCIPAL_CONTRACT_PATH.name)
    return None


_PRINCIPAL_STATES = {
    "target_scope_blocked",
    "unavailable",
    "degraded",
    "ready",
    "zero_usage",
    "policy_only",
    "unattributed",
}
_PRINCIPAL_STATE_PRECEDENCE = [
    "target_scope_blocked",
    "unavailable",
    "degraded",
    "ready",
    "zero_usage",
    "policy_only",
]
_PRINCIPAL_ERROR_CATEGORIES = [
    "principal_scope",
    "principal_contract",
    "principal_identity",
    "principal_window",
    "principal_metric",
    "principal_reconciliation",
    "principal_expected_state",
]


def _valid_allocation_rules(value: Any) -> bool:
    if not isinstance(value, dict):
        return False
    weights = value.get("weights")
    completeness = value.get("completeness")
    monetary = value.get("monetary")
    fixed_categories = value.get("fixed_categories")
    state_matrix = value.get("state_matrix")
    return (
        isinstance(weights, dict)
        and weights
        == {
            "user": {"source_scopes": ["user", "user-client"], "symbol": "q_i"},
            "client_only": {"source_scopes": ["client-id"], "symbol": "c"},
            "denominator": "total_valid_quota_weight",
        }
        and isinstance(completeness, dict)
        and completeness
        == {
            "required": ["configuration", "target", "samples", "identity"],
            "samples": "configured_datasource_cadence_and_gap",
        }
        and state_matrix
        == [
            {"when": "invalid_or_incomplete", "state": "unavailable"},
            {"when": "complete_total_weight_zero", "state": "zero_usage"},
            {"when": "complete_positive_weight_without_client_only", "state": "ready"},
            {"when": "complete_positive_weight_with_client_only", "state": "degraded"},
        ]
        and monetary
        == {
            "pool_source": "configured_direction_monetary_pool",
            "client_only": "unallocated",
            "rounding": {"quantum": "0.0001", "mode": "ROUND_DOWN", "residual": "explicit_unallocated"},
        }
        and fixed_categories
        == {
            "shared": {"policy": "unattributed", "measured_usage": False, "rounding": "ROUND_DOWN"},
            "static_even_v1": {
                "identities": "sorted_ascending",
                "measured_usage": False,
                "rounding": "ROUND_DOWN",
            },
        }
    )


def _valid_fixed_policies(policies: dict[str, Any]) -> bool:
    compute = policies.get("compute")
    storage = policies.get("storage")
    shared = policies.get("shared")
    return (
        isinstance(compute, dict)
        and isinstance(compute.get("policy"), str)
        and isinstance(compute.get("identities"), list)
        and all(isinstance(identity, str) for identity in compute["identities"])
        and isinstance(storage, dict)
        and storage == {"policy": "unattributed", "identities": []}
        and isinstance(shared, dict)
        and shared == {"policy": "unattributed", "identities": []}
    )


def _valid_artifact_schema(schema: Any) -> bool:
    if not isinstance(schema, dict):
        return False
    expected = {
        "top_level": {
            "required": ["version", "clusters"],
            "types": {"version": "integer", "clusters": "object"},
        },
        "cluster": {
            "required": ["state", "directions", "fixed_categories"],
            "types": {"state": "state_enum", "directions": "object", "fixed_categories": "object"},
        },
        "direction": {
            "required": [
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
            ],
            "types": {
                "pool": "decimal_string",
                "weights": "weight_object",
                "coverage": "coverage_object",
                "sampling_resolution": "sampling_resolution_object",
                "state": "state_enum",
                "reason": "nonempty_string",
                "users": "array",
                "client_only": "allocation_object",
                "rounding_residual": "decimal_string",
                "unallocated": "decimal_string",
                "balance": "decimal_string",
            },
        },
        "fixed_category": {
            "required": [
                "state",
                "policy",
                "shared",
                "measured_usage",
                "users",
                "pool",
                "rounding_residual",
                "unallocated",
                "balance",
            ],
            "types": {
                "state": "fixed_state_enum",
                "policy": "fixed_policy_enum",
                "shared": "boolean",
                "measured_usage": "boolean",
                "users": "array",
                "pool": "decimal_string",
                "rounding_residual": "decimal_string",
                "unallocated": "decimal_string",
                "balance": "decimal_string",
            },
        },
    }
    return schema == expected


def _resolved_principal_selector(contract: dict[str, Any]) -> dict[str, str] | None:
    selector = contract["selector"]
    if not isinstance(selector, dict):
        return None
    resolved = {str(key): str(value) for key, value in selector.items()}
    cluster_name = resolved.get("lab_cluster")
    generation_path = LAB_DIR / "generated" / "generation.json"
    if not generation_path.is_file() or not cluster_name:
        return resolved
    try:
        generation = _read_json(generation_path)
    except OSError, json.JSONDecodeError:
        return None
    cluster_ids = generation.get("kafka_cluster_ids") if isinstance(generation, dict) else None
    cluster_id = cluster_ids.get(cluster_name) if isinstance(cluster_ids, dict) else None
    if not isinstance(cluster_id, str) or not cluster_id:
        return None
    resolved["kafka_cluster_id"] = cluster_id
    return resolved


def _expected_principal_queries(window: dict[str, Any], contract: dict[str, Any]) -> dict[str, str]:
    selector = _resolved_principal_selector(contract)
    if selector is None:
        return {}
    selector_text = ",".join(f"{key}={json.dumps(value)}" for key, value in selector.items())
    source_window = int(window["duration_seconds"]) + int(window["max_gap_seconds"])
    expected = {"scope": f"up{{{selector_text}}}[{source_window}s]"}
    for definition in contract["directions"].values():
        quota_type = str(definition["quota_type"])
        quota_selector = {**selector, "quota_type": quota_type}
        quota_text = ",".join(f"{key}={json.dumps(value)}" for key, value in quota_selector.items())
        expected[f"quota_{quota_type.lower()}"] = f"kafka_server_quota_byte_rate{{{quota_text}}}[{source_window}s]"
    return expected


def _principal_query_integrity(
    queries: dict[str, Any], window: dict[str, Any], contract: dict[str, Any]
) -> Failure | None:
    expected_queries = _expected_principal_queries(window, contract)
    if set(queries) != set(expected_queries):
        return Failure(
            "principal_window",
            "principal queries do not match the quota-only contract",
            "principal-raw-query-results.json",
        )
    for name, expected_query in expected_queries.items():
        query = queries.get(name)
        if not isinstance(query, dict) or query.get("query") != expected_query:
            return Failure(
                "principal_window",
                f"{name} query does not match the principal window contract",
                "principal-raw-query-results.json",
            )
        response = query.get("response")
        if not isinstance(response, dict) or response.get("status") != "success":
            continue
    return None


def _scope_evidence_failure(
    scope: Any, window: dict[str, Any], queries: dict[str, Any], contract: dict[str, Any]
) -> Failure | None:
    if not isinstance(scope, dict) or scope.get("status") != "pass":
        message = scope.get("message") if isinstance(scope, dict) else None
        return Failure(
            "principal_scope",
            str(message) if isinstance(message, str) and message else "retained scope capture failed",
            "principal-scope-evidence.json",
        )

    retained_fields = {"query", "window", "response"}
    present_fields = retained_fields & scope.keys()
    if present_fields and present_fields != retained_fields:
        return Failure(
            "principal_scope",
            "principal scope evidence is missing retained response",
            "principal-scope-evidence.json",
        )
    expected_query = _expected_principal_queries(window, contract).get("scope")
    expected_window = {key: value for key, value in window.items() if key != "sampling_resolution"}
    if present_fields:
        if scope["window"] != expected_window or scope["query"] != expected_query:
            return Failure(
                "principal_scope",
                "principal scope evidence does not match the configured window",
                "principal-scope-evidence.json",
            )
        response = scope["response"]
    else:
        raw_scope = queries.get("scope")
        response = raw_scope.get("response") if isinstance(raw_scope, dict) else None

    if not _scope_response_proves_target(response, window, contract):
        return Failure(
            "principal_scope",
            "principal scope evidence does not prove the configured target",
            "principal-scope-evidence.json",
        )
    return None


def _quota_results(queries: dict[str, Any], quota_type: str) -> list[dict[str, Any]]:
    query = queries.get(f"quota_{quota_type.lower()}")
    response = query.get("response") if isinstance(query, dict) else None
    data = response.get("data") if isinstance(response, dict) else None
    results = data.get("result") if isinstance(data, dict) else None
    return [result for result in results if isinstance(result, dict)] if isinstance(results, list) else []


def _required_live_identity_failure(
    direction: str, queries: dict[str, Any], contract: dict[str, Any]
) -> Failure | None:
    quota_type = str(contract["directions"][direction]["quota_type"])
    requirements = contract["expected_live_identities"]
    ownership = contract["identity"]["ownership"]
    owners = {str(identity): str(owner) for identity, owner in ownership["mappings"].items()}
    default_owner = str(ownership["default_owner"])
    prefix = str(contract["identity"]["canonical_prefix"])
    results = _quota_results(queries, quota_type)
    requirement_by_scope = {
        "user": ("mapped_user", "mapped identity"),
        "user-client": ("unmapped_user", "unmapped identity"),
        "client-id": ("client_only_label", "client-only label"),
    }
    for scope, (field, description) in requirement_by_scope.items():
        matching = [
            result["metric"]
            for result in results
            if isinstance(result.get("metric"), dict) and result["metric"].get("quota_scope") == scope
        ]
        if not matching:
            return Failure(
                "principal_expected_state",
                f"required live identity evidence is missing: {field}",
                "principal-allocation-demonstration.json",
            )
        if field == "client_only_label":
            expected = str(requirements[field])
            if any(metric.get("client_id") == expected for metric in matching):
                continue
            observed = str(matching[0].get("client_id"))
            return Failure(
                "principal_identity",
                f"required {direction} {description} {expected}, observed {observed}",
                "principal-raw-query-results.json",
            )
        expected_identity = str(requirements[field]["identity"])
        expected_owner = str(requirements[field]["owner"])
        observed_identities = [f"{prefix}{metric.get('user')}" for metric in matching]
        if any(
            identity == expected_identity and owners.get(identity, default_owner) == expected_owner
            for identity in observed_identities
        ):
            continue
        return Failure(
            "principal_identity",
            f"required {direction} {description} {expected_identity} owned by {expected_owner}, "
            f"observed {observed_identities[0]}",
            "principal-raw-query-results.json",
        )
    return None


def _expected_live_failure(
    directions: dict[str, dict[str, Any]], queries: dict[str, Any], contract: dict[str, Any]
) -> Failure | None:
    del queries
    expected_states = contract["expected_live_states"]
    for direction, expected_state in expected_states.items():
        observed = directions[direction]["state"]
        if observed != expected_state:
            return Failure(
                "principal_expected_state",
                f"required {direction} state is {expected_state}, observed {observed}",
                "principal-allocation-demonstration.json",
            )
    expected_balances = contract["expected_live_balances"]
    for direction, expected_balance in expected_balances.items():
        observed = directions[direction]["balance"]
        if observed != expected_balance:
            return Failure(
                "principal_expected_state",
                f"required {direction} balance is {expected_balance}, observed {observed}",
                "principal-allocation-demonstration.json",
            )
    return None


def _decimal_string(value: Any) -> Decimal | None:
    return _decimal_from_source(value) if isinstance(value, str) else None


def _allocation_object_is_valid(value: Any) -> bool:
    if not isinstance(value, dict) or set(value) != {"weight", "raw_amount", "amount"}:
        return False
    for raw_value in value.values():
        parsed = _decimal_string(raw_value)
        if parsed is None or parsed < 0:
            return False
    return True


def _nonnegative_decimal_fields(value: dict[str, Any], fields: set[str]) -> dict[str, Decimal] | None:
    if not fields <= value.keys():
        return None
    parsed: dict[str, Decimal] = {}
    for field in fields:
        amount = _decimal_string(value[field])
        if amount is None or amount < 0:
            return None
        parsed[field] = amount
    return parsed


def _sampling_resolution_is_valid(value: Any) -> bool:
    if not isinstance(value, dict):
        return False
    required = {
        "declared_scrape_interval_seconds",
        "declared_max_gap_seconds",
        "observed_timestamp_deltas_seconds",
        "quota_rate_window",
        "estimate",
        "byte_exact",
        "limitation",
    }
    return (
        required <= value.keys()
        and isinstance(value["declared_scrape_interval_seconds"], int)
        and not isinstance(value["declared_scrape_interval_seconds"], bool)
        and value["declared_scrape_interval_seconds"] > 0
        and isinstance(value["declared_max_gap_seconds"], int)
        and not isinstance(value["declared_max_gap_seconds"], bool)
        and value["declared_max_gap_seconds"] > 0
        and isinstance(value["observed_timestamp_deltas_seconds"], dict)
        and set(value["observed_timestamp_deltas_seconds"]) == {"scope", "quota_produce", "quota_fetch"}
        and all(
            isinstance(series_deltas, list)
            and all(
                isinstance(deltas, list)
                and all(
                    isinstance(delta, (int, float))
                    and not isinstance(delta, bool)
                    and math.isfinite(delta)
                    and delta >= 0
                    for delta in deltas
                )
                for deltas in series_deltas
            )
            for series_deltas in value["observed_timestamp_deltas_seconds"].values()
        )
        and value["quota_rate_window"]
        == {
            "source": "kafka_default",
            "complete_windows": 10,
            "window_seconds": 1,
            "includes_current_window": True,
        }
        and value["estimate"] == "monitoring_resolution"
        and value["byte_exact"] is False
        and isinstance(value["limitation"], str)
        and bool(value["limitation"])
    )


def _artifact_schema_failure(artifact: dict[str, Any], contract: dict[str, Any]) -> Failure | None:
    schema = contract["artifact_schema"]
    required_top_level = set(schema["top_level"]["required"])
    if (
        not required_top_level <= artifact.keys()
        or not isinstance(artifact.get("version"), int)
        or isinstance(artifact.get("version"), bool)
        or not isinstance(artifact.get("clusters"), dict)
    ):
        return Failure("principal_contract", "principal artifact schema is invalid", PRINCIPAL_CONTRACT_PATH.name)
    for cluster in artifact["clusters"].values():
        if not isinstance(cluster, dict) or not set(schema["cluster"]["required"]) <= cluster.keys():
            return Failure("principal_contract", "principal artifact schema is invalid", PRINCIPAL_CONTRACT_PATH.name)
        directions = cluster.get("directions")
        fixed_categories = cluster.get("fixed_categories")
        if (
            cluster.get("state") not in _PRINCIPAL_STATES
            or not isinstance(directions, dict)
            or set(directions) != set(contract["directions"])
            or not isinstance(fixed_categories, dict)
            or set(fixed_categories) != set(contract["fixed_policy_inputs"])
        ):
            return Failure("principal_contract", "principal artifact schema is invalid", PRINCIPAL_CONTRACT_PATH.name)
        for allocation in directions.values():
            failure = _direction_artifact_failure(allocation, schema["direction"]["required"])
            if failure is not None:
                return failure
        for category, allocation in fixed_categories.items():
            failure = _fixed_artifact_failure(allocation, schema["fixed_category"]["required"], category)
            if failure is not None:
                return failure
    return None


def _direction_artifact_failure(allocation: Any, required: list[str]) -> Failure | None:
    if not isinstance(allocation, dict) or not set(required) <= allocation.keys():
        return Failure("principal_contract", "principal artifact schema is invalid", PRINCIPAL_CONTRACT_PATH.name)
    pool = _decimal_string(allocation.get("pool"))
    weights = allocation.get("weights")
    coverage = allocation.get("coverage")
    sampling_resolution = allocation.get("sampling_resolution")
    users = allocation.get("users")
    client_only = allocation.get("client_only")
    rounding = _decimal_string(allocation.get("rounding_residual"))
    unallocated = _decimal_string(allocation.get("unallocated"))
    balance = _decimal_string(allocation.get("balance"))
    if (
        pool is None
        or pool < 0
        or not isinstance(weights, dict)
        or set(weights) != {"user_total", "client_only", "total"}
        or not isinstance(coverage, dict)
        or set(coverage) != {"configuration", "target", "samples", "identity", "complete"}
        or any(not isinstance(value, bool) for value in coverage.values())
        or coverage["complete"]
        != (coverage["configuration"] and coverage["target"] and coverage["samples"] and coverage["identity"])
        or not _sampling_resolution_is_valid(sampling_resolution)
        or allocation.get("state") not in _PRINCIPAL_STATES
        or not isinstance(allocation.get("reason"), str)
        or not allocation["reason"]
        or not isinstance(users, list)
        or not _allocation_object_is_valid(client_only)
        or rounding is None
        or rounding < 0
        or unallocated is None
        or unallocated < 0
        or balance is None
    ):
        return Failure("principal_contract", "principal artifact schema is invalid", PRINCIPAL_CONTRACT_PATH.name)
    weight_values = _nonnegative_decimal_fields(weights, {"user_total", "client_only", "total"})
    if weight_values is None or weight_values["total"] != weight_values["user_total"] + weight_values["client_only"]:
        return Failure("principal_contract", "principal artifact schema is invalid", PRINCIPAL_CONTRACT_PATH.name)
    assert isinstance(client_only, dict)
    user_amount = Decimal(0)
    identities: set[str] = set()
    for user in users:
        if (
            not isinstance(user, dict)
            or not isinstance(user.get("identity"), str)
            or not isinstance(user.get("owner"), str)
            or set(user) != {"identity", "owner", "weight", "raw_amount", "amount"}
            or user["identity"] in identities
        ):
            return Failure("principal_contract", "principal artifact schema is invalid", PRINCIPAL_CONTRACT_PATH.name)
        user_fields = _nonnegative_decimal_fields(user, {"weight", "raw_amount", "amount"})
        if user_fields is None:
            return Failure("principal_contract", "principal artifact schema is invalid", PRINCIPAL_CONTRACT_PATH.name)
        identities.add(user["identity"])
        user_amount += user_fields["amount"]
    client_amount = _decimal_string(client_only["amount"])
    assert client_amount is not None
    if allocation["state"] in {"unavailable", "zero_usage"}:
        if users or client_amount != 0 or rounding != 0 or unallocated != pool or balance != pool:
            return Failure("principal_contract", "principal artifact schema is invalid", PRINCIPAL_CONTRACT_PATH.name)
        return None
    if unallocated != client_amount + rounding or balance != user_amount + unallocated or balance != pool:
        return Failure(
            "principal_reconciliation",
            "principal artifact balance is invalid",
            "principal-allocation-demonstration.json",
        )
    return None


def _fixed_artifact_failure(allocation: Any, required: list[str], category: str) -> Failure | None:
    if not isinstance(allocation, dict) or not set(required) <= allocation.keys():
        return Failure("principal_contract", "principal artifact schema is invalid", PRINCIPAL_CONTRACT_PATH.name)
    pool = _decimal_string(allocation.get("pool"))
    rounding_residual = _decimal_string(allocation.get("rounding_residual"))
    unallocated = _decimal_string(allocation.get("unallocated"))
    balance = _decimal_string(allocation.get("balance"))
    users = allocation.get("users")
    if (
        allocation.get("state") not in {"policy_only", "unattributed"}
        or allocation.get("policy") not in {"static_even_v1", "unattributed"}
        or (allocation.get("policy") == "static_even_v1" and allocation.get("state") != "policy_only")
        or (allocation.get("policy") == "unattributed" and allocation.get("state") != "unattributed")
        or allocation.get("shared") is not True
        or allocation.get("measured_usage") is not False
        or not isinstance(users, list)
        or pool is None
        or pool < 0
        or rounding_residual is None
        or rounding_residual < 0
        or unallocated is None
        or unallocated < 0
        or balance is None
    ):
        return Failure("principal_contract", "principal artifact schema is invalid", PRINCIPAL_CONTRACT_PATH.name)
    allocated = Decimal(0)
    for user in users:
        if not isinstance(user, dict) or set(user) != {"identity", "amount"} or not isinstance(user["identity"], str):
            return Failure("principal_contract", "principal artifact schema is invalid", PRINCIPAL_CONTRACT_PATH.name)
        amount = _decimal_string(user["amount"])
        if amount is None or amount < 0:
            return Failure("principal_contract", "principal artifact schema is invalid", PRINCIPAL_CONTRACT_PATH.name)
        allocated += amount
    if balance != allocated + unallocated or balance != pool:
        return Failure(
            "principal_reconciliation",
            "principal artifact balance is invalid",
            "principal-allocation-demonstration.json",
        )
    if allocation["policy"] == "static_even_v1" and unallocated != rounding_residual:
        return Failure(
            "principal_reconciliation",
            "principal artifact balance is invalid",
            "principal-allocation-demonstration.json",
        )
    if allocation["policy"] == "unattributed" and (users or rounding_residual != 0 or unallocated != pool):
        return Failure("principal_contract", "principal artifact schema is invalid", PRINCIPAL_CONTRACT_PATH.name)
    if category == "shared" and allocation["measured_usage"] is not False:
        return Failure("principal_contract", "principal artifact schema is invalid", PRINCIPAL_CONTRACT_PATH.name)
    return None


def _principal_validation(evidence_dir: Path) -> list[Failure]:
    scope_path = evidence_dir / "principal-scope-evidence.json"
    if not scope_path.is_file():
        return [Failure("principal_scope", "principal scope evidence is absent", scope_path.name)]
    scope = _read_json(scope_path)
    if not isinstance(scope, dict) or scope.get("status") != "pass":
        message = scope.get("message") if isinstance(scope, dict) else "principal scope evidence is invalid"
        category = scope.get("category") if isinstance(scope, dict) else None
        artifact = scope.get("artifact") if isinstance(scope, dict) else None
        return [
            Failure(
                str(category) if category in _PRINCIPAL_ERROR_CATEGORIES else "principal_scope",
                str(message),
                str(artifact) if isinstance(artifact, str) else scope_path.name,
            )
        ]
    contract, contract_load_failure = _principal_contract()
    if contract_load_failure is not None:
        return [contract_load_failure]
    assert contract is not None
    contract_failure = _principal_contract_failure(contract)
    if contract_failure is not None:
        return [contract_failure]
    window_path = evidence_dir / "principal-window.json"
    raw_path = evidence_dir / "principal-raw-query-results.json"
    if not window_path.is_file() or not raw_path.is_file():
        return [Failure("principal_contract", "principal evidence is incomplete", None)]
    window = _read_json(window_path)
    queries = _read_json(raw_path)
    if not isinstance(window, dict) or not isinstance(queries, dict):
        return [Failure("principal_contract", "principal evidence must contain objects", raw_path.name)]
    required_window = {
        "start_timestamp",
        "end_timestamp",
        "evaluation_timestamp",
        "duration_seconds",
        "max_gap_seconds",
        "scrape_interval_seconds",
        "logical_billing_interval",
        "provider_source_membership",
        "selector",
        "sampling_resolution",
    }
    if not required_window <= window.keys() or window.get("end_timestamp") != window.get("evaluation_timestamp"):
        return [Failure("principal_window", "principal window is invalid", window_path.name)]
    if not isinstance(window["selector"], dict):
        return [Failure("principal_window", "principal window bounds are invalid", window_path.name)]
    expected_selector = _resolved_principal_selector(contract)
    if expected_selector is None:
        return [Failure("principal_contract", "principal contract selector is invalid", PRINCIPAL_CONTRACT_PATH.name)]
    try:
        start = int(window["start_timestamp"])
        end = int(window["end_timestamp"])
        duration = int(window["duration_seconds"])
        max_gap = int(window["max_gap_seconds"])
        scrape_interval = int(window["scrape_interval_seconds"])
    except TypeError, ValueError:
        return [Failure("principal_window", "principal window bounds are invalid", window_path.name)]
    if (
        end <= start
        or duration != end - start
        or max_gap != contract["max_gap_seconds"]
        or scrape_interval != contract["scrape_interval_seconds"]
        or window["logical_billing_interval"] != contract["logical_billing_interval"]
        or window["provider_source_membership"] != contract["provider_source_membership"]
        or window["selector"] != expected_selector
    ):
        return [Failure("principal_window", "principal window bounds are invalid", window_path.name)]
    scope_failure = _scope_evidence_failure(scope, window, queries, contract)
    if scope_failure is not None:
        return [scope_failure]
    query_failure = _principal_query_integrity(queries, window, contract)
    if query_failure is not None:
        return [query_failure]
    sampling_resolution = window["sampling_resolution"]
    expected_deltas = {
        name: _observed_timestamp_deltas(query.get("response"))
        for name, query in queries.items()
        if isinstance(query, dict)
    }
    if (
        not _sampling_resolution_is_valid(sampling_resolution)
        or sampling_resolution.get("declared_scrape_interval_seconds") != scrape_interval
        or sampling_resolution.get("declared_max_gap_seconds") != max_gap
        or sampling_resolution.get("quota_rate_window") != contract["quota_rate_window"]
        or sampling_resolution.get("estimate") != contract["sampling_semantics"]["estimate"]
        or sampling_resolution.get("byte_exact") != contract["sampling_semantics"]["byte_exact"]
        or sampling_resolution.get("observed_timestamp_deltas_seconds") != expected_deltas
        or any(deltas is None for deltas in expected_deltas.values())
    ):
        return [Failure("principal_window", "principal sampling resolution is invalid", window_path.name)]
    directions: dict[str, dict[str, Any]] = {}
    direction_failures: list[Failure] = []
    for direction, definition in contract["directions"].items():
        result, failure = _principal_direction(queries, str(direction), definition, window, contract)
        assert result is not None
        if failure is not None:
            direction_failures.append(failure)
        else:
            identity_failure = _required_live_identity_failure(str(direction), queries, contract)
            if identity_failure is not None:
                result = _unavailable_direction(
                    queries,
                    str(direction),
                    window,
                    contract,
                    failure=identity_failure,
                    quota_summary=result["quota_sources"],
                    configuration_complete=result["coverage"]["configuration"],
                    target_complete=result["coverage"]["target"],
                    samples_complete=result["coverage"]["samples"],
                )
                direction_failures.append(identity_failure)
        directions[str(direction)] = result
    precedence = {
        state: len(contract["state_precedence"]) - index for index, state in enumerate(contract["state_precedence"])
    }
    cluster_state = max(directions.values(), key=lambda result: precedence[result["state"]])["state"]
    cluster = str(window["selector"].get("lab_cluster", "unknown"))
    artifact = {
        "version": contract["version"],
        "clusters": {
            cluster: {
                "state": cluster_state,
                "directions": directions,
                "fixed_categories": _fixed_category_results(contract),
            }
        },
    }
    schema_failure = _artifact_schema_failure(artifact, contract)
    if schema_failure is not None:
        return [*direction_failures, schema_failure]
    (evidence_dir / "principal-allocation-demonstration.json").write_text(
        json.dumps(artifact, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    expected_failure = _expected_live_failure(directions, queries, contract)
    if expected_failure is not None:
        direction_failures.append(expected_failure)
    return direction_failures


def validate(evidence_dir: Path, require_recreated_state: bool, principal_contract: bool = False) -> dict[str, Any]:
    contract = yaml.safe_load(CONTRACT_PATH.read_text(encoding="utf-8"))
    if not isinstance(contract, dict):
        raise ValueError("metric contract must be a mapping")
    failures: list[Failure] = []

    if not principal_contract:
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

    if principal_contract:
        failures.extend(_principal_validation(evidence_dir))

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
    parser.add_argument("--principal-contract", action="store_true")
    args = parser.parse_args()

    result = validate(args.evidence_dir.resolve(), args.require_recreated_state, args.principal_contract)
    result_path = args.evidence_dir.resolve() / "validator-result.json"
    result_path.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(result_path)
    if result["status"] != "pass":
        sys.exit(7 if args.principal_contract else 1)


if __name__ == "__main__":
    main()

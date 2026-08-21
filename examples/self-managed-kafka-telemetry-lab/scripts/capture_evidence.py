from __future__ import annotations

import argparse
import json
import math
import os
import re
import subprocess
import urllib.parse
import urllib.request
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import yaml

LAB_DIR = Path(__file__).resolve().parents[1]
EVIDENCE_DIR = LAB_DIR / "evidence"
PRINCIPAL_CONTRACT_PATH = LAB_DIR / "contracts" / "principal-allocation-contract.yaml"
COMPOSE = os.environ.get("COMPOSE", "docker-compose")
QUOTA_DESCRIPTION_RE = re.compile(
    r"^Quota configs for "
    r"(?:(?:user-principal '(?P<user>[^']+)')(?:, )?)?"
    r"(?:(?:client-id '(?P<client_id>[^']+)'))?"
    r" are (?P<configs>.+)$"
)


def _load_env() -> dict[str, str]:
    values: dict[str, str] = {}
    for line in (LAB_DIR / ".env").read_text(encoding="utf-8").splitlines():
        if line and not line.startswith("#"):
            key, value = line.split("=", 1)
            values[key] = value
    return values


def _run(command: list[str], *, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, cwd=LAB_DIR, check=check, capture_output=True, text=True)


def _compose(*arguments: str) -> subprocess.CompletedProcess[str]:
    return _run([COMPOSE, *arguments])


def _http_text(url: str) -> str:
    with urllib.request.urlopen(url, timeout=20) as response:
        body = response.read()
    if not isinstance(body, bytes):
        raise TypeError(f"HTTP response body is not bytes: {url}")
    return body.decode("utf-8")


def _prometheus_api(base_url: str, path: str, parameters: dict[str, str] | None = None) -> dict[str, Any]:
    query = urllib.parse.urlencode(parameters or {})
    url = f"{base_url}{path}{'?' + query if query else ''}"
    payload: Any = json.loads(_http_text(url))
    if not isinstance(payload, dict):
        raise TypeError(f"Prometheus API response is not an object: {path}")
    return payload


def _successful_prometheus_api(base_url: str, path: str, parameters: dict[str, str] | None = None) -> dict[str, Any]:
    payload = _prometheus_api(base_url, path, parameters)
    if payload.get("status") != "success":
        raise RuntimeError(f"Prometheus API request failed: {path}")
    return payload


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _principal_contract() -> dict[str, Any]:
    contract = yaml.safe_load(PRINCIPAL_CONTRACT_PATH.read_text(encoding="utf-8"))
    if not isinstance(contract, dict):
        raise TypeError("principal allocation contract must be a mapping")
    return contract


def _principal_error_response(message: str) -> dict[str, Any]:
    return {"status": "error", "error": message, "data": {"result": []}}


def _principal_query_response(prometheus_url: str, query: str, end: int) -> dict[str, Any]:
    try:
        return _prometheus_api(prometheus_url, "/api/v1/query", {"query": query, "time": str(end)})
    except OSError as error:
        return _principal_error_response(f"transport failed: {error}")
    except json.JSONDecodeError:
        return _principal_error_response("response could not be decoded")


def _window_seconds(window: str) -> int:
    match = re.fullmatch(r"(?P<value>[1-9][0-9]*)(?P<unit>[smhd])", window)
    if match is None:
        raise ValueError(f"window must be a positive whole number with s, m, h, or d suffix: {window}")
    multiplier = {"s": 1, "m": 60, "h": 3600, "d": 86400}[match.group("unit")]
    return int(match.group("value")) * multiplier


def _principal_selector(contract: dict[str, Any]) -> dict[str, str]:
    selector = contract.get("selector")
    if not isinstance(selector, dict):
        raise TypeError("principal allocation contract selector must be a mapping")
    result = {str(key): str(value) for key, value in selector.items()}
    cluster_name = result.get("lab_cluster")
    generation_path = LAB_DIR / "generated" / "generation.json"
    if generation_path.is_file() and cluster_name:
        generation = _read_json(generation_path)
        cluster_ids = generation.get("kafka_cluster_ids") if isinstance(generation, dict) else None
        cluster_id = cluster_ids.get(cluster_name) if isinstance(cluster_ids, dict) else None
        if not isinstance(cluster_id, str) or not cluster_id:
            raise ValueError(f"generated configuration lacks Kafka cluster ID for {cluster_name}")
        result["kafka_cluster_id"] = cluster_id
    return result


def _read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _promql_selector(selector: dict[str, str]) -> str:
    return ",".join(f"{key}={json.dumps(value)}" for key, value in selector.items())


def _bounded_scope_coverage(values: Any, start: int, end: int, max_gap: int) -> bool:
    if not isinstance(values, list):
        return False
    samples: list[tuple[Decimal, float]] = []
    for sample in values:
        if not isinstance(sample, list) or len(sample) != 2:
            return False
        timestamp, raw_value = sample
        if not isinstance(timestamp, (int, float)) or isinstance(timestamp, bool):
            return False
        try:
            timestamp_value = Decimal(str(timestamp))
        except InvalidOperation, TypeError, ValueError:
            return False
        if not timestamp_value.is_finite():
            return False
        try:
            value = float(raw_value)
        except TypeError, ValueError:
            return False
        if not math.isfinite(value) or value <= 0:
            return False
        samples.append((timestamp_value, value))
    samples.sort()
    if len({timestamp for timestamp, _ in samples}) != len(samples):
        return False
    start_timestamp = Decimal(start)
    end_timestamp = Decimal(end)
    max_gap_seconds = Decimal(max_gap)
    guards = [sample for sample in samples if sample[0] <= start_timestamp]
    if not guards or start_timestamp - guards[-1][0] > max_gap_seconds:
        return False
    relevant = [
        guards[-1],
        *(sample for sample in samples if start_timestamp < sample[0] <= end_timestamp),
    ]
    intervals = zip(relevant, relevant[1:], strict=False)
    if any(
        current[0] - previous[0] <= 0 or current[0] - previous[0] > max_gap_seconds for previous, current in intervals
    ):
        return False
    return bool(relevant) and end_timestamp - relevant[-1][0] <= max_gap_seconds


def _scope_is_proven(response: dict[str, Any], window: dict[str, Any], expected_targets: int) -> bool:
    data = response.get("data")
    results = data.get("result") if isinstance(data, dict) else None
    selector = window["selector"]
    if response.get("status") != "success" or not isinstance(results, list) or len(results) != expected_targets:
        return False
    for result in results:
        metric = result.get("metric") if isinstance(result, dict) else None
        if not isinstance(metric, dict) or any(metric.get(key) != value for key, value in selector.items()):
            return False
        if not _bounded_scope_coverage(
            result.get("values"),
            int(window["start_timestamp"]),
            int(window["end_timestamp"]),
            int(window["max_gap_seconds"]),
        ):
            return False
    return True


def _json_decimal(value: Decimal) -> int | float:
    if value == value.to_integral_value():
        return int(value)
    return float(value)


def _observed_timestamp_deltas(response: dict[str, Any]) -> list[list[int | float]]:
    data = response.get("data")
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
        deltas_by_series.append(
            [_json_decimal(current - previous) for previous, current in zip(timestamps, timestamps[1:], strict=False)]
        )
    return deltas_by_series


def _set_latest(run_dir: Path) -> None:
    EVIDENCE_DIR.mkdir(parents=True, exist_ok=True)
    latest = EVIDENCE_DIR / "latest"
    latest.unlink(missing_ok=True)
    latest.symlink_to(run_dir.name, target_is_directory=True)


def _capture_principal_contract_evidence(run_dir: Path, prometheus_url: str, window: str) -> dict[str, Any] | None:
    try:
        contract = _principal_contract()
    except FileNotFoundError:
        _write_json(
            run_dir / "principal-scope-evidence.json",
            {
                "status": "blocked",
                "category": "principal_contract",
                "message": "principal allocation contract is absent",
                "artifact": PRINCIPAL_CONTRACT_PATH.name,
            },
        )
        _set_latest(run_dir)
        return None
    except TypeError, yaml.YAMLError:
        _write_json(
            run_dir / "principal-scope-evidence.json",
            {
                "status": "blocked",
                "category": "principal_contract",
                "message": "principal allocation contract is invalid",
                "artifact": PRINCIPAL_CONTRACT_PATH.name,
            },
        )
        _set_latest(run_dir)
        return None
    scrape_interval = int(contract["scrape_interval_seconds"])
    max_gap = int(contract["max_gap_seconds"])
    duration = _window_seconds(window)
    end = int(datetime.now(tz=UTC).timestamp())
    end -= end % scrape_interval
    selector = _principal_selector(contract)
    principal_window = {
        "logical_billing_interval": contract["logical_billing_interval"],
        "provider_source_membership": contract["provider_source_membership"],
        "start_timestamp": end - duration,
        "end_timestamp": end,
        "evaluation_timestamp": end,
        "duration_seconds": duration,
        "scrape_interval_seconds": scrape_interval,
        "max_gap_seconds": max_gap,
        "selector": selector,
    }
    selector_text = _promql_selector(selector)
    source_window = duration + max_gap
    scope_query = f"up{{{selector_text}}}[{source_window}s]"
    try:
        scope_response = _prometheus_api(
            prometheus_url,
            "/api/v1/query",
            {"query": scope_query, "time": str(end)},
        )
    except OSError as error:
        scope_response = None
        scope_message = f"principal scope query transport failed: {error}"
    except json.JSONDecodeError:
        scope_response = None
        scope_message = "principal scope query response could not be decoded"
    else:
        assert scope_response is not None
        scope_message = (
            f"principal scope query failed: {scope_response.get('error', 'unknown Prometheus error')}"
            if scope_response.get("status") != "success"
            else "expected target scope is not healthy for the complete logical interval"
        )
    if scope_response is None or not _scope_is_proven(
        scope_response, principal_window, int(contract["expected_targets"])
    ):
        scope_evidence: dict[str, Any] = {
            "status": "blocked",
            "category": "principal_scope",
            "message": scope_message,
            "artifact": "principal-scope-evidence.json",
        }
        if scope_response is not None and scope_response.get("status") == "success":
            scope_evidence.update(
                {
                    "query": scope_query,
                    "window": principal_window,
                    "response": scope_response,
                }
            )
        elif scope_response is not None:
            scope_evidence["response"] = scope_response
        _write_json(
            run_dir / "principal-scope-evidence.json",
            scope_evidence,
        )
        _set_latest(run_dir)
        return None

    _write_json(
        run_dir / "principal-scope-evidence.json",
        {
            "status": "pass",
            "category": None,
            "message": "expected target scope is healthy for the complete logical interval",
            "artifact": "principal-scope-evidence.json",
        },
    )
    queries: dict[str, dict[str, Any]] = {"scope": {"query": scope_query, "response": scope_response}}
    for definition in contract["directions"].values():
        quota_type = str(definition["quota_type"])
        quota_selector = {**selector, "quota_type": quota_type}
        quota_query = f"kafka_server_quota_byte_rate{{{_promql_selector(quota_selector)}}}[{source_window}s]"
        queries[f"quota_{quota_type.lower()}"] = {
            "query": quota_query,
            "response": _principal_query_response(prometheus_url, quota_query, end),
        }
    principal_window["sampling_resolution"] = {
        "declared_scrape_interval_seconds": scrape_interval,
        "declared_max_gap_seconds": max_gap,
        "observed_timestamp_deltas_seconds": {
            name: _observed_timestamp_deltas(query["response"]) for name, query in queries.items()
        },
        "quota_rate_window": contract["quota_rate_window"],
        "estimate": contract["sampling_semantics"]["estimate"],
        "byte_exact": contract["sampling_semantics"]["byte_exact"],
        "limitation": "quota weights are monitoring-resolution estimates, not byte-exact totals",
    }
    _write_json(run_dir / "principal-window.json", principal_window)
    _write_json(run_dir / "principal-raw-query-results.json", queries)
    return principal_window


def _describe_topics(cluster: str) -> str:
    bootstrap = f"kafka-{cluster}:9092"
    config = "/opt/kafka/config/lab/admin.properties"
    command = f"/opt/kafka/bin/kafka-topics.sh --bootstrap-server {bootstrap} --command-config {config} --describe"
    return _compose("exec", "-T", f"kafka-{cluster}", "bash", "-c", command).stdout


def _quota_entity_arguments(quota: dict[str, Any]) -> list[str]:
    scope = quota["scope"]
    if scope == "user":
        return ["--entity-type", "users", "--entity-name", str(quota["user"])]
    if scope == "client-id":
        return ["--entity-type", "clients", "--entity-name", str(quota["client_id"])]
    if scope == "user-client":
        return [
            "--entity-type",
            "users",
            "--entity-name",
            str(quota["user"]),
            "--entity-type",
            "clients",
            "--entity-name",
            str(quota["client_id"]),
        ]
    raise ValueError(f"unsupported quota scope: {scope}")


def _describe_quotas(cluster: str, quotas: list[dict[str, Any]]) -> str:
    bootstrap = f"kafka-{cluster}:9092"
    config = "/opt/kafka/config/lab/admin.properties"
    descriptions = []
    for quota in quotas:
        result = _compose(
            "exec",
            "-T",
            f"kafka-{cluster}",
            "/opt/kafka/bin/kafka-configs.sh",
            "--bootstrap-server",
            bootstrap,
            "--command-config",
            config,
            "--describe",
            *_quota_entity_arguments(quota),
        )
        descriptions.append(result.stdout.rstrip())
    return "\n".join(description for description in descriptions if description) + "\n"


def _workload_identities(cluster: str) -> dict[str, list[str]]:
    command = r"""
for file in /tmp/*-producer.properties /tmp/*-consumer.properties; do
  [[ -f "$file" ]] || continue
  sed -n 's/^client\.id=/client_id=/p' "$file"
  sed -n 's/.*username="\([^"]*\)".*/user=\1/p' "$file"
done | sort -u
"""
    output = _compose("exec", "-T", f"workload-{cluster}", "bash", "-c", command).stdout
    identities: dict[str, set[str]] = {"users": set(), "client_ids": set()}
    for line in output.splitlines():
        key, separator, value = line.partition("=")
        if not separator or not value:
            continue
        if key == "user":
            identities["users"].add(value)
        elif key == "client_id":
            identities["client_ids"].add(value)
    return {key: sorted(values) for key, values in identities.items()}


def _restart_requirements() -> dict[str, Any]:
    workloads = yaml.safe_load((LAB_DIR / "workloads" / "workloads.yaml").read_text(encoding="utf-8"))
    contract = yaml.safe_load((LAB_DIR / "contracts" / "metric-contract.yaml").read_text(encoding="utf-8"))
    if not isinstance(workloads, dict) or not isinstance(contract, dict):
        raise TypeError("workload and metric contracts must be mappings")
    quota_profiles = workloads.get("quota_profiles")
    topics = workloads.get("topics")
    rates = workloads.get("rates_bytes_per_second")
    if not isinstance(quota_profiles, list) or not isinstance(topics, list) or not isinstance(rates, dict):
        raise TypeError("workload contract is incomplete")
    declared_scopes = {str(profile["scope"]) for profile in quota_profiles}
    if declared_scopes != set(contract.get("quota_scopes", [])):
        raise ValueError("workload quota scopes do not match the metric contract")

    required_quotas = [
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
    required_traffic = [
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
        "quotas": required_quotas,
        "topics": sorted(traffic["topic"] for traffic in required_traffic),
        "traffic_profiles": required_traffic,
    }


def _observed_quotas(description: str) -> set[tuple[str, str, str, int, int]]:
    observed: set[tuple[str, str, str, int, int]] = set()
    for line in description.splitlines():
        match = QUOTA_DESCRIPTION_RE.fullmatch(line)
        if match is None:
            continue
        user = match.group("user") or ""
        client_id = match.group("client_id") or ""
        configs = {
            key.strip(): float(value.strip())
            for entry in match.group("configs").split(",")
            for key, value in [entry.split("=", 1)]
        }
        if "producer_byte_rate" not in configs or "consumer_byte_rate" not in configs:
            continue
        scope = "user-client" if user and client_id else "user" if user else "client-id"
        observed.add(
            (
                scope,
                user,
                client_id,
                int(configs["producer_byte_rate"]),
                int(configs["consumer_byte_rate"]),
            )
        )
    return observed


def _quota_is_observed(description: str, quota: dict[str, Any]) -> bool:
    expected = (
        str(quota["scope"]),
        str(quota["user"]),
        str(quota["client_id"]),
        int(quota["producer_byte_rate"]),
        int(quota["consumer_byte_rate"]),
    )
    return expected in _observed_quotas(description)


def _query_has_positive_sample(
    query_results: dict[str, dict[str, Any]], query_name: str, cluster: str, topic: str
) -> bool:
    results = query_results[query_name]["response"]["data"]["result"]
    return any(
        result.get("metric", {}).get("lab_cluster") == cluster
        and result.get("metric", {}).get("topic") == topic
        and float(result.get("value", [0, 0])[1]) > 0
        for result in results
    )


def _restart_cluster_observations(
    cluster: str,
    requirements: dict[str, Any],
    identities: dict[str, list[str]],
    topic_description: str,
    quota_description: str,
    query_results: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    observed_quotas = [quota for quota in requirements["quotas"] if _quota_is_observed(quota_description, quota)]
    observed_traffic = []
    for profile in requirements["traffic_profiles"]:
        observed_traffic.append(
            {
                **profile,
                "producer_active": _query_has_positive_sample(
                    query_results, "topic_bytes_in_rate", cluster, profile["topic"]
                ),
                "consumer_active": _query_has_positive_sample(
                    query_results, "topic_bytes_out_rate", cluster, profile["topic"]
                ),
            }
        )
    return {
        "users": identities["users"],
        "client_ids": identities["client_ids"],
        "quotas": observed_quotas,
        "topics": [
            topic
            for topic in requirements["topics"]
            if re.search(rf"(?m)(?:^|\s)Topic:\s*{re.escape(topic)}(?=\s|$)", topic_description)
        ],
        "traffic_profiles": observed_traffic,
    }


def _container_footprint() -> dict[str, Any]:
    services: dict[str, Any] = {}
    for service in ("kafka-a", "kafka-b", "jmx-a", "jmx-b", "prometheus", "workload-a", "workload-b"):
        container_id = _compose("ps", "-q", service).stdout.strip()
        if not container_id:
            services[service] = {"running": False}
            continue
        inspect = _run(
            [
                "docker",
                "inspect",
                "--size",
                "--format",
                "{{json .State.Status}} {{json .SizeRw}} {{json .HostConfig.LogConfig}}",
                container_id,
            ]
        ).stdout.strip()
        services[service] = {"running": True, "docker_inspect": inspect}
    for service, path in (
        ("kafka-a", "/tmp/kraft-combined-logs"),
        ("kafka-b", "/tmp/kraft-combined-logs"),
        ("prometheus", "/prometheus"),
    ):
        result = _compose("exec", "-T", service, "du", "-sb", path)
        services.setdefault(service, {})["storage_bytes"] = int(result.stdout.split()[0])
    return {"captured_at": datetime.now(tz=UTC).isoformat(), "services": services}


def capture(label: str | None, window: str, principal_contract: bool = False) -> Path:
    env = _load_env()
    timestamp = datetime.now(tz=UTC).strftime("%Y%m%dT%H%M%SZ")
    safe_label = f"-{label}" if label else ""
    run_dir = EVIDENCE_DIR / f"{timestamp}{safe_label}"
    run_dir.mkdir(parents=True, exist_ok=False)

    prometheus_url = f"http://127.0.0.1:{env['PROMETHEUS_PORT']}"
    if principal_contract:
        if _capture_principal_contract_evidence(run_dir, prometheus_url, window) is None:
            return run_dir
        _set_latest(run_dir)
        return run_dir

    for cluster in ("a", "b"):
        raw_jmx = _compose("run", "--rm", "--no-deps", f"jmx-dump-{cluster}").stdout
        (run_dir / f"raw-jmx-cluster-{cluster}.jsonl").write_text(raw_jmx, encoding="utf-8")
        exporter_port = env[f"JMX_EXPORTER_{cluster.upper()}_PORT"]
        exporter = _http_text(f"http://127.0.0.1:{exporter_port}/metrics")
        (run_dir / f"exporter-cluster-{cluster}.metrics").write_text(exporter, encoding="utf-8")

    _write_json(run_dir / "prometheus-targets.json", _successful_prometheus_api(prometheus_url, "/api/v1/targets"))
    _write_json(run_dir / "prometheus-metadata.json", _successful_prometheus_api(prometheus_url, "/api/v1/metadata"))

    restart_requirements = _restart_requirements()
    over_quota_profiles = [quota for quota in restart_requirements["quotas"] if quota["over_quota"]]
    if len(over_quota_profiles) != 1:
        raise ValueError("workload contract must define exactly one over-quota profile")
    over_quota_scope = over_quota_profiles[0]["scope"]
    instant_queries = {
        "alltopics_bytes_in": "kafka_server_brokertopicmetrics_alltopics_bytesin_total",
        "alltopics_bytes_out": "kafka_server_brokertopicmetrics_alltopics_bytesout_total",
        "topic_bytes_in": "kafka_server_brokertopicmetrics_bytesin_total",
        "topic_bytes_out": "kafka_server_brokertopicmetrics_bytesout_total",
        "partition_log_size": "kafka_log_log_size",
        "quota_byte_rate": "kafka_server_quota_byte_rate",
        "quota_throttle_time": "kafka_server_quota_throttle_time_ms",
        "up": 'up{job="kafka-jmx"}',
        "jmx_scrape_error": 'jmx_scrape_error{job="kafka-jmx"}',
        "topic_bytes_in_rate": f"rate(kafka_server_brokertopicmetrics_bytesin_total[{window}])",
        "topic_bytes_out_rate": f"rate(kafka_server_brokertopicmetrics_bytesout_total[{window}])",
        "produce_throttle_max": (
            f'max_over_time(kafka_server_quota_throttle_time_ms{{quota_type="Produce",quota_scope="{over_quota_scope}"}}[{window}])'
        ),
        "fetch_throttle_max": (
            f'max_over_time(kafka_server_quota_throttle_time_ms{{quota_type="Fetch",quota_scope="{over_quota_scope}"}}[{window}])'
        ),
    }
    query_results: dict[str, dict[str, Any]] = {
        name: {
            "query": query,
            "response": _successful_prometheus_api(prometheus_url, "/api/v1/query", {"query": query}),
        }
        for name, query in instant_queries.items()
    }
    _write_json(run_dir / "prometheus-query-results.json", query_results)

    topic_descriptions = {cluster: _describe_topics(cluster) for cluster in ("a", "b")}
    quota_descriptions = {cluster: _describe_quotas(cluster, restart_requirements["quotas"]) for cluster in ("a", "b")}
    _write_json(run_dir / "topic-descriptions.json", topic_descriptions)
    _write_json(run_dir / "quota-descriptions.json", quota_descriptions)

    generation = json.loads((LAB_DIR / "generated" / "generation.json").read_text(encoding="utf-8"))
    cluster_ids = generation["kafka_cluster_ids"]
    _write_json(
        run_dir / "cluster-id-comparison.json",
        {"cluster_ids": cluster_ids, "distinct": len(set(cluster_ids.values())) == 2},
    )
    restart_clusters = {
        f"cluster-{cluster}": {
            "required": restart_requirements,
            "observed": _restart_cluster_observations(
                f"cluster-{cluster}",
                restart_requirements,
                _workload_identities(cluster),
                topic_descriptions[cluster],
                quota_descriptions[cluster],
                query_results,
            ),
        }
        for cluster in ("a", "b")
    }
    restart_manifest = {
        "generation_id": generation["generation_id"],
        "previous_generation_id": generation.get("previous_generation_id"),
        "state_recreated": bool(
            generation.get("previous_generation_id")
            and generation["generation_id"] != generation["previous_generation_id"]
        ),
        "clusters": restart_clusters,
    }
    _write_json(run_dir / "clean-restart-manifest.json", restart_manifest)
    _write_json(run_dir / "footprint.json", _container_footprint())
    _write_json(run_dir / "cleanup-result.json", {"status": "pending", "recorded_at": None})
    _write_json(
        run_dir / "evidence-manifest.json",
        {"captured_at": datetime.now(tz=UTC).isoformat(), "window": window, "label": label},
    )

    _set_latest(run_dir)
    return run_dir


def main() -> None:
    parser = argparse.ArgumentParser(description="Capture live Kafka telemetry lab evidence")
    parser.add_argument("--label")
    parser.add_argument("--window", default="5m")
    parser.add_argument("--principal-contract", action="store_true")
    args = parser.parse_args()
    print(capture(args.label, args.window, args.principal_contract))


if __name__ == "__main__":
    main()

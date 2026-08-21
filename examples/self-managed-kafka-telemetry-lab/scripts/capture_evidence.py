from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import urllib.parse
import urllib.request
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import yaml

LAB_DIR = Path(__file__).resolve().parents[1]
EVIDENCE_DIR = LAB_DIR / "evidence"
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
    if payload.get("status") != "success":
        raise RuntimeError(f"Prometheus API request failed: {path}")
    return payload


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


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


def capture(label: str | None, window: str) -> Path:
    env = _load_env()
    timestamp = datetime.now(tz=UTC).strftime("%Y%m%dT%H%M%SZ")
    safe_label = f"-{label}" if label else ""
    run_dir = EVIDENCE_DIR / f"{timestamp}{safe_label}"
    run_dir.mkdir(parents=True, exist_ok=False)

    for cluster in ("a", "b"):
        raw_jmx = _compose("run", "--rm", "--no-deps", f"jmx-dump-{cluster}").stdout
        (run_dir / f"raw-jmx-cluster-{cluster}.jsonl").write_text(raw_jmx, encoding="utf-8")
        exporter_port = env[f"JMX_EXPORTER_{cluster.upper()}_PORT"]
        exporter = _http_text(f"http://127.0.0.1:{exporter_port}/metrics")
        (run_dir / f"exporter-cluster-{cluster}.metrics").write_text(exporter, encoding="utf-8")

    prometheus_url = f"http://127.0.0.1:{env['PROMETHEUS_PORT']}"
    _write_json(run_dir / "prometheus-targets.json", _prometheus_api(prometheus_url, "/api/v1/targets"))
    _write_json(run_dir / "prometheus-metadata.json", _prometheus_api(prometheus_url, "/api/v1/metadata"))

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
        name: {"query": query, "response": _prometheus_api(prometheus_url, "/api/v1/query", {"query": query})}
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

    EVIDENCE_DIR.mkdir(parents=True, exist_ok=True)
    latest = EVIDENCE_DIR / "latest"
    latest.unlink(missing_ok=True)
    latest.symlink_to(run_dir.name, target_is_directory=True)
    return run_dir


def main() -> None:
    parser = argparse.ArgumentParser(description="Capture live Kafka telemetry lab evidence")
    parser.add_argument("--label")
    parser.add_argument("--window", default="5m")
    args = parser.parse_args()
    print(capture(args.label, args.window))


if __name__ == "__main__":
    main()

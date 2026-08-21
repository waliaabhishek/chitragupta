from __future__ import annotations

import base64
import json
import os
import secrets
import shlex
import stat
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import yaml

LAB_DIR = Path(__file__).resolve().parents[1]
GENERATED_DIR = LAB_DIR / "generated"


def _existing_generation_is_complete(identities: list[str], runtime_plan: str, jmx_configs: dict[str, str]) -> bool:
    required = [
        LAB_DIR / ".env",
        GENERATED_DIR / "generation.json",
        GENERATED_DIR / "prometheus.yml",
        GENERATED_DIR / "jmx-a.yml",
        GENERATED_DIR / "jmx-b.yml",
        GENERATED_DIR / "runtime-plan.sh",
    ]
    for cluster in ("a", "b"):
        required.extend(
            [
                GENERATED_DIR / f"server-{cluster}-jaas.conf",
                GENERATED_DIR / f"admin-{cluster}.properties",
                *(GENERATED_DIR / f"{identity}-{cluster}.properties" for identity in identities),
            ]
        )
    return (
        all(path.is_file() for path in required)
        and (GENERATED_DIR / "runtime-plan.sh").read_text(encoding="utf-8") == runtime_plan
        and all(
            (GENERATED_DIR / f"jmx-{cluster}.yml").read_text(encoding="utf-8") == config
            for cluster, config in jmx_configs.items()
        )
    )


def _load_workload_matrix() -> dict[str, Any]:
    matrix = yaml.safe_load((LAB_DIR / "workloads" / "workloads.yaml").read_text(encoding="utf-8"))
    if not isinstance(matrix, dict):
        raise TypeError("workload matrix must be a mapping")
    topics = matrix.get("topics")
    profiles = matrix.get("quota_profiles")
    rates = matrix.get("rates_bytes_per_second")
    topic_config = matrix.get("topic_config")
    runtime = matrix.get("runtime")
    if (
        not isinstance(topics, list)
        or not isinstance(profiles, list)
        or not isinstance(rates, dict)
        or not isinstance(topic_config, dict)
        or not isinstance(runtime, dict)
    ):
        raise TypeError("workload matrix is incomplete")
    topic_profiles = {str(topic["profile"]) for topic in topics}
    quota_profiles = {str(profile["profile"]) for profile in profiles}
    if len(topic_profiles) != len(topics) or len(quota_profiles) != len(profiles) or topic_profiles != quota_profiles:
        raise ValueError("topics and quota profiles must have a one-to-one profile mapping")
    record_size = int(runtime["record_size_bytes"])
    for profile in profiles:
        profile_name = str(profile["profile"])
        rate = int(rates[profile_name])
        if rate <= 0 or record_size <= 0 or rate % record_size:
            raise ValueError(f"profile rate must be a positive multiple of record size: {profile_name}")
        for key in ("scope", "user", "client_id", "group", "quota_limit_bytes_per_second"):
            if key not in profile:
                raise ValueError(f"quota profile {profile_name} is missing {key}")
    return matrix


def _shell_array(name: str, values: list[str | int]) -> str:
    encoded = " ".join(shlex.quote(str(value)) for value in values)
    return f"{name}=({encoded})\nreadonly -a {name}\n"


def _runtime_plan(matrix: dict[str, Any]) -> str:
    topics = matrix["topics"]
    profiles = matrix["quota_profiles"]
    rates = matrix["rates_bytes_per_second"]
    topic_config = matrix["topic_config"]
    runtime = matrix["runtime"]
    topic_by_profile = {str(topic["profile"]): str(topic["name"]) for topic in topics}
    content = ["# Generated from workloads/workloads.yaml. Do not edit.\n"]
    for name, value in (
        ("LAB_TOPIC_PARTITIONS", topic_config["partitions"]),
        ("LAB_TOPIC_RETENTION_MS", topic_config["retention_ms"]),
        ("LAB_TOPIC_RETENTION_BYTES", topic_config["retention_bytes_per_partition"]),
        ("LAB_RECORD_SIZE_BYTES", runtime["record_size_bytes"]),
        ("LAB_PRODUCER_BATCH_SECONDS", runtime["producer_batch_seconds"]),
        ("LAB_PRODUCER_BUFFER_MEMORY_BYTES", runtime["producer_buffer_memory_bytes"]),
        ("LAB_PRODUCER_DELIVERY_TIMEOUT_MS", runtime["producer_delivery_timeout_ms"]),
        ("LAB_PRODUCER_MAX_BLOCK_MS", runtime["producer_max_block_ms"]),
        ("LAB_CONSUMER_RECORDS_PER_RUN", runtime["consumer_records_per_run"]),
        ("LAB_CONSUMER_TIMEOUT_MS", runtime["consumer_timeout_ms"]),
    ):
        content.append(f"readonly {name}={shlex.quote(str(value))}\n")
    content.extend(
        (
            _shell_array("LAB_TOPIC_NAMES", [str(topic["name"]) for topic in topics]),
            _shell_array("LAB_PROFILE_NAMES", [str(profile["profile"]) for profile in profiles]),
            _shell_array(
                "LAB_PROFILE_TOPICS",
                [topic_by_profile[str(profile["profile"])] for profile in profiles],
            ),
            _shell_array("LAB_PROFILE_SCOPES", [str(profile["scope"]) for profile in profiles]),
            _shell_array("LAB_PROFILE_USERS", [str(profile["user"]) for profile in profiles]),
            _shell_array("LAB_PROFILE_CLIENT_IDS", [str(profile["client_id"]) for profile in profiles]),
            _shell_array("LAB_PROFILE_GROUPS", [str(profile["group"]) for profile in profiles]),
            _shell_array(
                "LAB_PROFILE_RATES_BYTES_PER_SECOND",
                [int(rates[str(profile["profile"])]) for profile in profiles],
            ),
            _shell_array(
                "LAB_PROFILE_QUOTA_LIMITS",
                [int(profile["quota_limit_bytes_per_second"]) for profile in profiles],
            ),
        )
    )
    return "".join(content)


def _jmx_object_name_attributes(matrix: dict[str, Any]) -> dict[str, list[str]]:
    attributes: dict[str, list[str]] = {
        "kafka.server:type=BrokerTopicMetrics,name=BytesInPerSec": ["Count"],
        "kafka.server:type=BrokerTopicMetrics,name=BytesOutPerSec": ["Count"],
    }
    partitions = int(matrix["topic_config"]["partitions"])
    for topic in matrix["topics"]:
        topic_name = str(topic["name"])
        attributes[f"kafka.server:type=BrokerTopicMetrics,name=BytesInPerSec,topic={topic_name}"] = ["Count"]
        attributes[f"kafka.server:type=BrokerTopicMetrics,name=BytesOutPerSec,topic={topic_name}"] = ["Count"]
        for partition in range(partitions):
            attributes[f"kafka.log:type=Log,name=Size,topic={topic_name},partition={partition}"] = ["Value"]

    for profile in matrix["quota_profiles"]:
        scope = str(profile["scope"])
        entity = {
            "user": f"user={profile['user']}",
            "client-id": f"client-id={profile['client_id']}",
            "user-client": f"user={profile['user']},client-id={profile['client_id']}",
        }[scope]
        for quota_type in ("Produce", "Fetch"):
            attributes[f"kafka.server:type={quota_type},{entity}"] = ["byte-rate", "throttle-time"]
    return attributes


def _jmx_config(matrix: dict[str, Any], host: str) -> str:
    template = yaml.safe_load((LAB_DIR / "jmx" / "kafka-jmx.yml").read_text(encoding="utf-8"))
    if not isinstance(template, dict):
        raise TypeError("JMX exporter template must be a mapping")
    template["hostPort"] = f"{host}:9999"
    template["includeObjectNameAttributes"] = _jmx_object_name_attributes(matrix)
    return yaml.safe_dump(template, sort_keys=False)


def _kafka_cluster_id() -> str:
    return base64.urlsafe_b64encode(uuid.uuid4().bytes).decode("ascii").rstrip("=")


def _write(path: Path, content: str, mode: int) -> None:
    path.write_text(content, encoding="utf-8")
    path.chmod(mode)


def _write_private(path: Path, content: str) -> None:
    _write(path, content, stat.S_IRUSR | stat.S_IWUSR)


def _write_readable(path: Path, content: str) -> None:
    _write(path, content, stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IROTH)


def _client_properties(username: str, password: str) -> str:
    return (
        "security.protocol=SASL_PLAINTEXT\n"
        "sasl.mechanism=PLAIN\n"
        "sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required "
        f'username="{username}" password="{password}";\n'
    )


def _server_jaas(admin_password: str, users: dict[str, str]) -> str:
    options = ['username="admin"', f'password="{admin_password}"', f'user_admin="{admin_password}"']
    options.extend(f'user_{username}="{password}"' for username, password in users.items())
    return (
        "KafkaServer {\n  org.apache.kafka.common.security.plain.PlainLoginModule required\n    "
        + "\n    ".join(f"{option}{';' if index == len(options) - 1 else ''}" for index, option in enumerate(options))
        + "\n};\n"
    )


def generate() -> None:
    workload_matrix = _load_workload_matrix()
    identities = sorted({str(profile["user"]) for profile in workload_matrix["quota_profiles"]})
    runtime_plan = _runtime_plan(workload_matrix)
    jmx_configs = {cluster: _jmx_config(workload_matrix, f"kafka-{cluster}") for cluster in ("a", "b")}
    if _existing_generation_is_complete(identities, runtime_plan, jmx_configs):
        return
    GENERATED_DIR.mkdir(mode=stat.S_IRWXU, parents=True, exist_ok=True)
    GENERATED_DIR.chmod(stat.S_IRWXU)

    restart_state_path = LAB_DIR / ".restart-state.json"
    previous_generation_id: str | None = None
    if restart_state_path.exists():
        restart_state = json.loads(restart_state_path.read_text(encoding="utf-8"))
        previous_generation_id = str(restart_state.get("generation_id") or "") or None

    generation_id = str(uuid.uuid4())
    cluster_ids = {"a": _kafka_cluster_id(), "b": _kafka_cluster_id()}
    env = {
        "KAFKA_CLUSTER_A_ID": cluster_ids["a"],
        "KAFKA_CLUSTER_B_ID": cluster_ids["b"],
        "PROMETHEUS_PORT": "9090",
        "JMX_EXPORTER_A_PORT": "7071",
        "JMX_EXPORTER_B_PORT": "7072",
    }
    _write_private(LAB_DIR / ".env", "".join(f"{key}={value}\n" for key, value in env.items()))

    for cluster in ("a", "b"):
        admin_password = secrets.token_urlsafe(24)
        user_passwords = {identity: secrets.token_urlsafe(24) for identity in identities}
        _write_private(GENERATED_DIR / f"server-{cluster}-jaas.conf", _server_jaas(admin_password, user_passwords))
        _write_private(GENERATED_DIR / f"admin-{cluster}.properties", _client_properties("admin", admin_password))
        for identity, password in user_passwords.items():
            _write_private(
                GENERATED_DIR / f"{identity}-{cluster}.properties",
                _client_properties(identity, password),
            )

    for cluster, config in jmx_configs.items():
        _write_readable(GENERATED_DIR / f"jmx-{cluster}.yml", config)

    prometheus_template = (LAB_DIR / "prometheus" / "prometheus.yml.template").read_text(encoding="utf-8")
    prometheus_config = prometheus_template.replace("__KAFKA_CLUSTER_A_ID__", cluster_ids["a"]).replace(
        "__KAFKA_CLUSTER_B_ID__", cluster_ids["b"]
    )
    _write_readable(GENERATED_DIR / "prometheus.yml", prometheus_config)
    _write_readable(GENERATED_DIR / "runtime-plan.sh", runtime_plan)

    manifest = {
        "generation_id": generation_id,
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "kafka_cluster_ids": {"cluster-a": cluster_ids["a"], "cluster-b": cluster_ids["b"]},
        "previous_generation_id": previous_generation_id,
    }
    _write_readable(GENERATED_DIR / "generation.json", json.dumps(manifest, indent=2, sort_keys=True) + "\n")


if __name__ == "__main__":
    os.umask(0o077)
    generate()

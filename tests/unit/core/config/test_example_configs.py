from __future__ import annotations

import csv
import hashlib
import json
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pytest

from core.config.loader import load_config
from core.preview.capability import FOCUS_PREVIEW_CAPABILITY
from core.preview.focus_metadata import build_requested_focus_metadata_artifact
from core.preview.mapping import (
    FOCUS_1_4_FULL_PROFILE_COLUMNS,
    MAPPING_PROFILE_VERSION,
    PREVIEW_MANIFEST_SCHEMA_VERSION,
    PROFILE_NOT_APPLICABLE_COLUMNS,
    PreviewDataPackageDraft,
    PreviewPackageReconciliation,
    preview_canonical_json,
    preview_manifest_known_gaps,
)
from core.preview.models import (
    PreviewArtifactPayload,
    PreviewCalculationCoverageEntry,
    PreviewRequest,
    PreviewRequestStatus,
    PreviewSourceSnapshot,
)

EXAMPLES_DIR = Path(__file__).parents[4] / "examples"

CCLOUD_ENV = {
    "CCLOUD_TENANT_ID": "ccloud-test",
    "CCLOUD_API_KEY": "TESTKEY",
    "CCLOUD_API_SECRET": "testsecret",
}
PROM_ENV = {"PROMETHEUS_URL": "http://localhost:9090"}


def _load(config_path: Path, env: dict[str, str], monkeypatch: pytest.MonkeyPatch) -> None:
    for k, v in env.items():
        monkeypatch.setenv(k, v)
    config = load_config(config_path)
    assert config.tenants, f"{config_path}: expected at least one tenant"


class TestCCloudExamples:
    def test_ccloud_grafana(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _load(EXAMPLES_DIR / "ccloud-grafana" / "config.yaml", CCLOUD_ENV, monkeypatch)

    def test_ccloud_full(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _load(EXAMPLES_DIR / "ccloud-full" / "config.yaml", CCLOUD_ENV, monkeypatch)

    def test_ccloud_full_documents_optional_focus_preview_contract(self) -> None:
        source = (EXAMPLES_DIR / "ccloud-full" / "config.yaml").read_text(encoding="utf-8")

        assert "focus_preview:" in source
        assert "commercial_profile: direct_payg" in source
        assert "billing_currency: USD" in source
        assert "effective_start_date:" in source
        assert "effective_end_date:" in source
        assert "lookback_days is not retention" in source

    def test_focus_preview_walkthrough(self, monkeypatch: pytest.MonkeyPatch) -> None:
        example = EXAMPLES_DIR / "focus-preview"
        _load(example / "config.yaml", CCLOUD_ENV, monkeypatch)

        request = json.loads((example / "request.json").read_text(encoding="utf-8"))
        manifest = json.loads((example / "sample-output" / "manifest.json").read_text(encoding="utf-8"))
        csv_body = (example / "sample-output" / "cost-and-usage.csv").read_bytes()
        metadata_body = (example / "sample-output" / "focus-metadata.json").read_bytes()
        metadata = json.loads(metadata_body)
        csv_rows = list(csv.reader(csv_body.decode().splitlines()))

        assert request["column_profile"] == manifest["column_profile"] == "custom"
        assert request["grain"] == manifest["grain"] == "daily"
        assert request["start_date"] == manifest["start_date"]
        assert request["end_date"] == manifest["end_date"]
        assert tuple(request["columns"]) == tuple(manifest["effective_columns"]) == tuple(csv_rows[0])
        assert all(column in FOCUS_1_4_FULL_PROFILE_COLUMNS for column in request["columns"])
        assert len(csv_rows) == 2

        assert manifest["schema_version"] == PREVIEW_MANIFEST_SCHEMA_VERSION
        assert manifest["mapping_profile_version"] == MAPPING_PROFILE_VERSION
        assert manifest["target_focus_version"] == FOCUS_PREVIEW_CAPABILITY.target_focus_version
        assert manifest["conformance_status"] == FOCUS_PREVIEW_CAPABILITY.conformance_status
        assert manifest["known_gaps"] == preview_manifest_known_gaps()
        assert manifest["profile_not_applicable_columns"] == list(PROFILE_NOT_APPLICABLE_COLUMNS)
        assert manifest["reconciliation"]["difference"] == "0"
        assert manifest["reconciliation"]["quantity_difference"] == "0"

        [csv_metadata, focus_metadata] = manifest["files"]
        assert csv_metadata["name"] == "cost-and-usage.csv"
        assert csv_metadata["size_bytes"] == len(csv_body)
        assert csv_metadata["sha256"] == hashlib.sha256(csv_body).hexdigest()
        assert focus_metadata["name"] == "focus-metadata.json"
        assert focus_metadata["media_type"] == "application/json"
        assert focus_metadata["order"] == 2
        assert focus_metadata["size_bytes"] == len(metadata_body)
        assert focus_metadata["sha256"] == hashlib.sha256(metadata_body).hexdigest()
        assert metadata_body == (preview_canonical_json(metadata) + "\n").encode()
        focus = metadata["x_ChitraguptaPreviewMetadata"]
        assert focus["metadata_conformance_status"] == "non_conforming_preview_metadata"
        assert focus["conformance_status"] == "non_conforming"
        assert focus["dataset_artifacts"] == [
            {
                "dataset_instance_id": focus["x_ChitraguptaPreviewDatasetInstance"]["dataset_instance_id"],
                "media_type": "text/csv",
                "name": "cost-and-usage.csv",
                "order": 1,
                "schema_id": focus["x_ChitraguptaPreviewSchema"]["schema_id"],
            }
        ]
        assert [column["column_name"] for column in focus["x_ChitraguptaPreviewSchema"]["columns"]] == list(
            request["columns"]
        )
        assert focus["delivery"]["correction_handling"] == "not_a_correction_series"
        assert "Schema" not in metadata

        data_payload = PreviewArtifactPayload("cost-and-usage.csv", "text/csv", 1, csv_body)
        snapshot = PreviewSourceSnapshot(
            calculation_timestamp=datetime(2026, 7, 3, tzinfo=UTC),
            calculation_coverage=(
                PreviewCalculationCoverageEntry(
                    tracking_date=date(2026, 7, 1),
                    calculation_id="calculation-1",
                    calculation_completed_at=datetime(2026, 7, 3, tzinfo=UTC),
                    calculation_run_id=1,
                ),
            ),
            source_through=datetime(2026, 7, 3, tzinfo=UTC),
            effective_coverage_start_date=date(2026, 7, 1),
            effective_coverage_end_date=date(2026, 7, 2),
            availability_cutoff_end_date=None,
            monthly_status=None,
        )
        preview_request = PreviewRequest(
            request_id="request-example-001",
            tenant_name="ccloud-prod",
            ecosystem="confluent_cloud",
            tenant_id="org-1",
            grain="daily",
            start_date=date(2026, 7, 1),
            end_date=date(2026, 7, 2),
            column_profile="custom",
            status=PreviewRequestStatus.RUNNING,
            created_at=datetime(2026, 7, 3, tzinfo=UTC),
            started_at=datetime(2026, 7, 3, tzinfo=UTC),
            completed_at=None,
            expires_at=None,
            source_snapshot=None,
            diagnostic=None,
            storage_key=None,
            package=None,
            effective_columns=tuple(request["columns"]),
        )
        draft = PreviewDataPackageDraft(
            data_files=(data_payload,),
            source_records=1,
            rows=1,
            reconciliation=PreviewPackageReconciliation(
                source_records=1,
                source_cost=Decimal("8"),
                allocated_cost=Decimal("8"),
                source_quantity=Decimal("5"),
                allocated_quantity=Decimal("5"),
            ),
            logical_data_sha256=hashlib.sha256(csv_body).hexdigest(),
        )
        expected_metadata = build_requested_focus_metadata_artifact(
            request=preview_request,
            snapshot=snapshot,
            draft=draft,
            data_files=(data_payload,),
            ready_at=datetime(2026, 7, 4, tzinfo=UTC),
            expires_at=datetime(2026, 7, 11, tzinfo=UTC),
        )
        assert metadata_body == expected_metadata.body


class TestSelfManagedExamples:
    def test_self_managed_full(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _load(EXAMPLES_DIR / "self-managed-full" / "config.yaml", PROM_ENV, monkeypatch)

    def test_self_managed_full_declares_a_prometheus_scope_selector_separate_from_logical_cluster_id(self) -> None:
        source = (EXAMPLES_DIR / "self-managed-full" / "config.yaml").read_text(encoding="utf-8")

        assert "metrics_identifier:" in source
        assert "metrics_identifier_label: kafka_cluster_id" in source
        assert "Prometheus target label" in source

    def test_self_managed_operator_docs_do_not_describe_broker_topic_metrics_as_principal_evidence(self) -> None:
        documents = (
            Path("docs/configuration/self-managed-reference.md").read_text(encoding="utf-8"),
            Path("docs/configuration/guide.md").read_text(encoding="utf-8"),
            (EXAMPLES_DIR / "self-managed-full" / "README.md").read_text(encoding="utf-8"),
        )
        config = (EXAMPLES_DIR / "self-managed-full" / "config.yaml").read_text(encoding="utf-8")

        assert all("metrics_identifier" in source for source in documents)
        assert "metrics_identifier" in config
        assert "metrics_identifier_label: kafka_cluster_id" in config
        assert all("principal label on BrokerTopicMetrics" not in source for source in documents)
        assert all("quota" in source.lower() for source in documents)


class TestSelfManagedKafkaTelemetryLabExample:
    LAB_DIR = EXAMPLES_DIR / "self-managed-kafka-telemetry-lab"

    def test_lab_example_contains_required_assets(self) -> None:
        required_paths = (
            self.LAB_DIR / "README.md",
            self.LAB_DIR / ".gitignore",
            self.LAB_DIR / ".env.example",
            self.LAB_DIR / "docker-compose.yml",
            self.LAB_DIR / "contracts" / "metric-contract.yaml",
            self.LAB_DIR / "jmx" / "kafka-jmx.yml",
            self.LAB_DIR / "prometheus" / "prometheus.yml.template",
            self.LAB_DIR / "workloads" / "workloads.yaml",
            self.LAB_DIR / "scripts" / "lab.sh",
            self.LAB_DIR / "scripts" / "generate_local_config.py",
            self.LAB_DIR / "scripts" / "setup_kafka.sh",
            self.LAB_DIR / "scripts" / "workload.sh",
            self.LAB_DIR / "scripts" / "JmxDump.java",
            self.LAB_DIR / "scripts" / "capture_evidence.py",
            self.LAB_DIR / "scripts" / "validate_evidence.py",
        )

        missing = [str(path.relative_to(self.LAB_DIR)) for path in required_paths if not path.exists()]

        assert not missing, f"Missing lab assets: {missing}"

    def test_lab_readme_documents_standalone_lifecycle_commands(self) -> None:
        readme_path = self.LAB_DIR / "README.md"
        assert readme_path.exists(), f"Expected {readme_path} to exist"

        source = readme_path.read_text(encoding="utf-8")

        assert "local-only" in source or "local only" in source
        assert "docker-compose" in source
        assert "docker compose" not in source

        for command in (
            "./scripts/lab.sh prereq",
            "./scripts/lab.sh start",
            "./scripts/lab.sh ready",
            "./scripts/lab.sh workload start",
            "./scripts/lab.sh workload stop",
            "./scripts/lab.sh workload status",
            "./scripts/lab.sh validate --window 5m",
            "./scripts/lab.sh evidence",
            "./scripts/lab.sh stop",
            "./scripts/lab.sh cleanup",
            "./scripts/lab.sh validate --window 5m --require-recreated-state",
        ):
            assert command in source, f"Expected README to document `{command}`"

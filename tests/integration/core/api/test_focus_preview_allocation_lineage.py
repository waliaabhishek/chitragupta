from __future__ import annotations

import csv
import hashlib
import io
import json
import tempfile
import time
import zipfile
from datetime import UTC, date, datetime
from decimal import Decimal
from importlib import import_module
from pathlib import Path
from typing import TYPE_CHECKING, Any

import anyio.to_thread
import httpx
import respx

from core.api.app import create_app
from core.config.models import ApiConfig, AppSettings, PreviewConfig, StorageConfig, TenantConfig
from core.engine.orchestrator import ChargebackOrchestrator
from core.models.identity import CoreIdentity
from core.models.pipeline import PipelineState
from core.models.resource import CoreResource, ResourceStatus
from core.preview.models import PreviewArtifactMetadata
from core.storage.backends.sqlmodel.repositories import SQLModelEntityTagRepository
from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend
from plugins.confluent_cloud import ConfluentCloudPlugin
from plugins.confluent_cloud.storage.repositories import CCloudBillingRepository, CCloudChargebackRepository
from tests.integration.core.api.preview_pipeline_helpers import (
    calculate_with_lineage,
    gather_billing_with_source_evidence,
)
from tests.integration.core.api.test_focus_preview_pipeline import PipelineApiClient, _request

if TYPE_CHECKING:
    import pytest


def _cost(
    cost_id: str,
    *,
    start: str,
    product: str,
    line_type: str,
    amount: str,
    price: str,
    quantity: str,
    unit: str,
    description: str,
    resource: dict[str, object] | None,
) -> dict[str, object]:
    return {
        "id": cost_id,
        "start_date": start,
        "end_date": date.fromisoformat(start).replace(day=date.fromisoformat(start).day + 1).isoformat(),
        "granularity": "DAILY",
        "product": product,
        "line_type": line_type,
        "amount": amount,
        "original_amount": amount,
        "discount_amount": "0",
        "price": price,
        "quantity": quantity,
        "unit": unit,
        "description": description,
        "resource": resource or {},
        "tier_dimensions": {},
    }


def _settings(tmp_path: Path, connection_string: str) -> tuple[AppSettings, TenantConfig]:
    tenant = TenantConfig(
        ecosystem="confluent_cloud",
        tenant_id="org-1",
        lookback_days=5,
        cutoff_days=1,
        storage=StorageConfig(connection_string=connection_string),
        focus_preview={
            "commercial_profile": "direct_payg",
            "billing_currency": "USD",
            "effective_start_date": "2020-01-01",
            "effective_end_date": "2030-01-01",
        },
        plugin_settings={
            "ccloud_api": {"key": "key", "secret": "secret"},  # pragma: allowlist secret
            "billing_api": {"days_per_query": 30},
        },
    )
    return (
        AppSettings(
            api=ApiConfig(host="127.0.0.1", port=8080),
            preview=PreviewConfig(
                artifact_root=tmp_path / "artifacts",
                max_workers=1,
                max_csv_file_bytes=3200,
            ),
            tenants={"production": tenant},
        ),
        tenant,
    )


def _seed_context(backend: SQLModelBackend) -> tuple[int, int, int]:
    with backend.create_unit_of_work() as uow:
        for resource in (
            CoreResource(
                ecosystem="confluent_cloud",
                tenant_id="org-1",
                resource_id="org-1",
                resource_type="organization",
                display_name="Billing organization",
                status=ResourceStatus.ACTIVE,
                created_at=datetime(2026, 1, 1, tzinfo=UTC),
                metadata={"organization_binding_state": "bound"},
            ),
            CoreResource(
                ecosystem="confluent_cloud",
                tenant_id="org-1",
                resource_id="env-1",
                resource_type="environment",
                display_name="Production",
                status=ResourceStatus.ACTIVE,
                created_at=datetime(2026, 1, 1, tzinfo=UTC),
            ),
            CoreResource(
                ecosystem="confluent_cloud",
                tenant_id="org-1",
                resource_id="lkc-1",
                resource_type="kafka_cluster",
                display_name="Orders",
                parent_id="env-1",
                status=ResourceStatus.ACTIVE,
                created_at=datetime(2026, 1, 1, tzinfo=UTC),
                metadata={"provider_cloud": "AWS", "provider_region": "us-east-1"},
            ),
        ):
            uow.resources.upsert(resource)
        for identity_id, name, identity_type in (
            ("sa-1", "Orders producer", "service_account"),
            ("user-1", "Orders owner", "user"),
        ):
            uow.identities.upsert(
                CoreIdentity(
                    ecosystem="confluent_cloud",
                    tenant_id="org-1",
                    identity_id=identity_id,
                    identity_type=identity_type,
                    display_name=name,
                    created_at=datetime(2026, 1, 1, tzinfo=UTC),
                    deleted_at=datetime(2026, 7, 2, tzinfo=UTC),
                )
            )
        origin_tag = uow.tags.add_tag("org-1", "resource", "lkc-1", "origin", "cluster", "test")
        identity_tag = uow.tags.add_tag("org-1", "identity", "sa-1", "team", "alpha", "test")
        user_tag = uow.tags.add_tag("org-1", "identity", "user-1", "team", "beta", "test")
        uow.commit()
    assert origin_tag.tag_id is not None and identity_tag.tag_id is not None and user_tag.tag_id is not None
    return origin_tag.tag_id, identity_tag.tag_id, user_tag.tag_id


def _csv_rows(client: PipelineApiClient, ready: dict[str, Any]) -> tuple[bytes, list[dict[str, str]]]:
    bodies = []
    rows: list[dict[str, str]] = []
    header: bytes | None = None
    for artifact in ready["package"]["files"]:
        response = client.get(artifact["download_url"])
        assert response.status_code == 200
        body = response.content
        bodies.append(body)
        part_header, _separator, part_rows = body.partition(b"\n")
        if header is None:
            header = part_header
        else:
            assert part_header == header
        rows.extend(csv.DictReader(io.StringIO(body.decode())))
    assert header is not None
    logical_body = header + b"\n" + b"".join(body.partition(b"\n")[2] for body in bodies)
    return logical_body, rows


def _profile_request(client: PipelineApiClient, body: dict[str, object]) -> dict[str, Any]:
    submitted = client.post(
        "/api/v1/tenants/production/focus-preview/requests",
        json=body,
    )
    assert submitted.status_code == 202
    request_id = submitted.json()["request_id"]
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        response = client.get(f"/api/v1/tenants/production/focus-preview/requests/{request_id}")
        assert response.status_code == 200
        if response.json()["status"] in {"ready", "failed"}:
            return response.json()
        time.sleep(0.01)
    raise AssertionError("profile Preview request did not finish")


@respx.mock
def test_real_production_lineage_projects_multiple_origins_actual_portions_and_frozen_separate_tags(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def to_thread_inline(function: Any, *args: object, **kwargs: object) -> object:
        return function(*args, **kwargs)

    async def run_sync_inline(function: Any, *args: object, **_kwargs: object) -> object:
        return function(*args)

    monkeypatch.setattr("core.api.app.asyncio.to_thread", to_thread_inline)
    monkeypatch.setattr(anyio.to_thread, "run_sync", run_sync_inline)
    costs = [
        _cost(
            "cost-kafka",
            start="2026-07-01",
            product="KAFKA",
            line_type="KAFKA_STORAGE",
            amount="8",
            price="2",
            quantity="4",
            unit="GB",
            description="Kafka storage usage",
            resource={"id": "lkc-1", "display_name": "Orders", "environment": {"id": "env-1"}},
        ),
        _cost(
            "cost-link",
            start="2026-07-01",
            product="CLUSTER_LINK",
            line_type="CLUSTER_LINKING_PER_LINK",
            amount="3",
            price="3",
            quantity="1",
            unit="LINK_HOUR",
            description="Cluster Linking per-link usage",
            resource={"id": "lkc-1", "display_name": "Orders", "environment": {"id": "env-1"}},
        ),
        _cost(
            "cost-support",
            start="2026-07-02",
            product="SUPPORT_CLOUD_BASIC",
            line_type="SUPPORT",
            amount="5",
            price="5",
            quantity="1",
            unit="MONTH",
            description="Support subscription",
            resource=None,
        ),
        _cost(
            "cost-audit",
            start="2026-07-03",
            product="AUDIT_LOG",
            line_type="AUDIT_LOG_READ",
            amount="2",
            price="2",
            quantity="1",
            unit="GB",
            description="Audit log read usage",
            resource=None,
        ),
    ]
    cost_route = respx.get("https://api.confluent.cloud/billing/v1/costs").mock(
        return_value=httpx.Response(200, json={"data": costs, "metadata": {}})
    )
    connection_string = f"sqlite:///{tmp_path / 'lineage-api.db'}"
    settings, tenant = _settings(tmp_path, connection_string)
    plugin = ConfluentCloudPlugin()
    plugin.initialize(tenant.plugin_settings.model_dump())
    assert plugin._connection is not None
    plugin._connection.request_interval_seconds = 0
    backend = SQLModelBackend(
        connection_string, plugin.get_storage_module(), use_migrations=False, focus_preview_enabled=True
    )
    backend.create_tables()
    origin_tag_id, identity_tag_id, _user_tag_id = _seed_context(backend)
    orchestrator = ChargebackOrchestrator("production", tenant, plugin, backend)

    assert gather_billing_with_source_evidence(
        orchestrator,
        backend,
        datetime(2026, 7, 5, tzinfo=UTC),
    ) == {
        date(2026, 7, 1),
        date(2026, 7, 2),
        date(2026, 7, 3),
    }
    with backend.create_unit_of_work() as uow:
        for tracking_date in (date(2026, 7, 1), date(2026, 7, 2), date(2026, 7, 3)):
            uow.pipeline_state.upsert(
                PipelineState(
                    ecosystem="confluent_cloud",
                    tenant_id="org-1",
                    tracking_date=tracking_date,
                    billing_gathered=True,
                    resources_gathered=True,
                )
            )
        uow.commit()
    assert calculate_with_lineage(orchestrator, backend, date(2026, 7, 1)) == 3
    assert calculate_with_lineage(orchestrator, backend, date(2026, 7, 2)) == 2
    assert calculate_with_lineage(orchestrator, backend, date(2026, 7, 3)) == 1
    with backend.create_read_only_unit_of_work() as uow:
        produced = [
            *uow.chargebacks.find_by_date("confluent_cloud", "org-1", date(2026, 7, 1)),
            *uow.chargebacks.find_by_date("confluent_cloud", "org-1", date(2026, 7, 2)),
            *uow.chargebacks.find_by_date("confluent_cloud", "org-1", date(2026, 7, 3)),
        ]
    assert sorted((row.identity_id, row.amount, row.allocation_method) for row in produced) == sorted(
        [
            ("lkc-1", 3, "cluster_linking"),
            ("sa-1", 4, "even_split"),
            ("user-1", 4, "even_split"),
            ("sa-1", 2.5, "even_split"),
            ("user-1", 2.5, "even_split"),
            ("UNALLOCATED", 2, "terminal"),
        ]
    )

    tag_calls: list[tuple[str, tuple[str, ...]]] = []
    original_batch_tags = SQLModelEntityTagRepository.find_tags_for_entities

    def spy_batch_tags(
        self: SQLModelEntityTagRepository,
        tenant_id: str,
        entity_type: str,
        entity_ids: list[str],
    ) -> object:
        tag_calls.append((entity_type, tuple(entity_ids)))
        return original_batch_tags(self, tenant_id, entity_type, entity_ids)

    monkeypatch.setattr(SQLModelEntityTagRepository, "find_tags_for_entities", spy_batch_tags)

    def forbidden_overlay(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("Preview must not use the chargeback tag overlay helper")

    monkeypatch.setattr("core.storage.backends.sqlmodel.repositories._overlay_tags", forbidden_overlay)
    provider_calls_before_preview = len(respx.calls)
    app = create_app(settings)
    client = PipelineApiClient(app, use_lifespan=True, backend=backend)
    try:
        ready = _request(client, date(2026, 7, 1), date(2026, 7, 4))
        assert ready["status"] == "ready"
        assert cost_route.call_count == 1
        assert len(respx.calls) == provider_calls_before_preview
        manifest_response = client.get(ready["package"]["manifest"]["download_url"])
        assert manifest_response.status_code == 200
        manifest = manifest_response.json()
        assert manifest["mapping_profile_version"] == "focus-1.4-preview-v1"
        assert manifest["validation"] == {
            "status": "passed",
            "mapping_profile_version": "focus-1.4-preview-v1",
            "source_records": 4,
            "rows": 6,
            "mapping_errors": 0,
            "artifact_integrity": "passed",
        }
        assert manifest["reconciliation"] == {
            "source_cost": "18",
            "allocated_cost": "18",
            "difference": "0",
            "source_quantity": "7",
            "allocated_quantity": "7",
            "quantity_difference": "0",
        }
        files = ready["package"]["files"]
        assert len(files) > 1
        assert [item["order"] for item in files] == list(range(1, len(files) + 1))
        assert [item["name"] for item in files] == [
            f"cost-and-usage-part-{index:05d}-of-{len(files):05d}.csv" for index in range(1, len(files) + 1)
        ]
        assert manifest["files"] == [
            {key: item[key] for key in ("name", "media_type", "size_bytes", "sha256", "order")} for item in files
        ]
        retrieved_parts = [client.get(item["download_url"]).content for item in files]
        assert [len(body) for body in retrieved_parts] == [item["size_bytes"] for item in files]
        assert [hashlib.sha256(body).hexdigest() for body in retrieved_parts] == [item["sha256"] for item in files]
        assert [hashlib.sha256(body).hexdigest() for body in retrieved_parts] == [
            "9fe8007b740666aba032cb5215e34785e8bfa9a1c9563cfb717013b550486cd4",  # pragma: allowlist secret
            "cdc15d1e0cfdbd80ca178f98932e6fc00eb1eb732f820f59cfde46013b618dad",  # pragma: allowlist secret
            "b3769b4f302599dac0fefc62911a41d5020a0469351e3a731202e2f01b942a21",  # pragma: allowlist secret
            "6a1a4e32d5eb5ab999b660111d34a7ee7e480845ce91081f2a28602bc1d3c9a4",  # pragma: allowlist secret
            "2dd63bf4242a5883f1ea111fdb131ef90e14282bce585dbff83d2095e3056a29",  # pragma: allowlist secret
            "042a79a23f9272e93f94fa61e004f5889e5338b5c84a90aa5632f2306145b64f",  # pragma: allowlist secret
        ]
        with backend.create_preview_metadata_read_unit_of_work() as uow:
            persisted = uow.requests.get_for_owner(
                ready["request_id"],
                "confluent_cloud",
                "org-1",
            )
        assert persisted is not None and persisted.package is not None
        assert persisted.package.files == tuple(
            PreviewArtifactMetadata(
                item["name"],
                item["media_type"],
                item["size_bytes"],
                item["sha256"],
                item["order"],
            )
            for item in files
        )
        artifact_module = import_module("core.preview.artifacts")
        real_spooled_temporary_file = tempfile.SpooledTemporaryFile
        server_spools: list[object] = []

        def recording_spool(*args: object, **kwargs: object) -> object:
            spool = real_spooled_temporary_file(*args, **kwargs)
            server_spools.append(spool)
            return spool

        monkeypatch.setattr(artifact_module, "_ARCHIVE_SPOOL_BYTES", 1)
        monkeypatch.setattr(artifact_module.tempfile, "SpooledTemporaryFile", recording_spool)
        archive_response = client.get(ready["package"]["download_all_url"])
        assert archive_response.status_code == 200
        assert len(server_spools) == 1
        assert server_spools[0]._rolled is True  # type: ignore[attr-defined]
        assert server_spools[0].closed is True  # type: ignore[attr-defined]
        with zipfile.ZipFile(io.BytesIO(archive_response.content)) as archive:
            assert archive.namelist() == ["manifest.json", *[item["name"] for item in files]]
            assert archive.read("manifest.json") == manifest_response.content
            assert [archive.read(item["name"]) for item in files] == retrieved_parts
        first_bytes, rows = _csv_rows(client, ready)
        assert len(rows) == 6
        first_row_order = [
            (row["x_ChitraguptaSourceCostId"], row["AllocatedResourceId"], row["BilledCost"]) for row in rows
        ]
        assert {row["x_ChitraguptaSourceCostId"] for row in rows} == {
            "cost-kafka",
            "cost-link",
            "cost-support",
            "cost-audit",
        }
        kafka = [row for row in rows if row["x_ChitraguptaSourceCostId"] == "cost-kafka"]
        assert {row["AllocatedResourceId"] for row in kafka} == {"sa-1", "user-1"}
        assert {row["BilledCost"] for row in kafka} == {"4"}
        assert {row["PricingQuantity"] for row in kafka} == {"2"}
        assert {row["x_ChitraguptaAllocationRatio"] for row in kafka} == {"0.5"}
        assert {row["AllocatedTags"] for row in kafka} == {
            '{"team":"alpha"}',
            '{"team":"beta"}',
        }
        assert {row["Tags"] for row in kafka} == {'{"origin":"cluster"}'}
        resource_row = next(row for row in rows if row["x_ChitraguptaSourceCostId"] == "cost-link")
        assert resource_row["AllocatedResourceId"] == "lkc-1"
        assert resource_row["AllocatedResourceName"] == "Orders"
        assert resource_row["AllocatedTags"] == '{"origin":"cluster"}'
        support = [row for row in rows if row["x_ChitraguptaSourceCostId"] == "cost-support"]
        assert {row["AllocatedResourceId"] for row in support} == {"sa-1", "user-1"}
        assert {row["BilledCost"] for row in support} == {"2.5"}
        expected_allocated_fields = {
            ("cost-link", "lkc-1", "Orders", '{"origin":"cluster"}', '{"origin":"cluster"}'),
            ("cost-kafka", "sa-1", "Orders producer", '{"team":"alpha"}', '{"origin":"cluster"}'),
            ("cost-kafka", "user-1", "Orders owner", '{"team":"beta"}', '{"origin":"cluster"}'),
            ("cost-support", "sa-1", "Orders producer", '{"team":"alpha"}', "{}"),
            ("cost-support", "user-1", "Orders owner", '{"team":"beta"}', "{}"),
        }
        assert {
            (
                row["x_ChitraguptaSourceCostId"],
                row["AllocatedResourceId"],
                row["AllocatedResourceName"],
                row["AllocatedTags"],
                row["Tags"],
            )
            for row in rows
            if row["AllocatedResourceId"]
        } == expected_allocated_fields
        unallocated = next(row for row in rows if row["x_ChitraguptaSourceCostId"] == "cost-audit")
        assert unallocated["AllocatedResourceId"] == ""
        assert unallocated["AllocatedResourceName"] == ""
        assert unallocated["AllocatedTags"] == ""
        assert unallocated["Tags"] == "{}"
        assert "UNALLOCATED" not in first_bytes.decode()
        mapping = __import__("core.preview.mapping", fromlist=["FOCUS_1_4_FULL_PROFILE_COLUMNS"])
        sentinel_columns = (
            "AllocatedResourceId",
            "AllocatedResourceName",
            "AllocatedTags",
            "Tags",
            *mapping.CUSTOM_EVIDENCE_COLUMNS,
        )
        assert all("UNALLOCATED" not in row[column] for row in rows for column in sentinel_columns)
        assert {row["x_ChitraguptaAllocationMethodVersion"] for row in rows} == {"v1"}
        assert all(json.loads(row["AllocatedMethodDetails"])["target_kind"] for row in rows)
        assert tag_calls == [
            ("resource", ("lkc-1",)),
            ("identity", ("sa-1", "user-1")),
        ]

        custom_columns = ["BilledCost", "AllocatedResourceId", "x_ChitraguptaAllocationRatio"]
        profile_columns = {
            "full": list(mapping.FOCUS_1_4_FULL_PROFILE_COLUMNS),
            "summary": list(mapping.FOCUS_1_4_SUMMARY_COLUMNS),
            "custom": custom_columns,
        }
        for profile in ("summary", "custom"):
            body: dict[str, object] = {
                "grain": "daily",
                "start_date": "2026-07-01",
                "end_date": "2026-07-04",
                "column_profile": profile,
            }
            if profile == "custom":
                body["columns"] = custom_columns
            daily_profile = _profile_request(client, body)
            assert daily_profile["status"] == "ready"
            daily_bytes, _daily_rows = _csv_rows(client, daily_profile)
            assert next(csv.reader(io.StringIO(daily_bytes.decode()))) == profile_columns[profile]

        app.state.preview_runtime._clock = lambda: datetime(2026, 7, 4, tzinfo=UTC)
        for profile in ("full", "summary", "custom"):
            body = {"grain": "monthly", "month": "2026-07", "column_profile": profile}
            if profile == "custom":
                body["columns"] = custom_columns
            monthly = _profile_request(client, body)
            assert monthly["status"] == "ready"
            assert monthly["grain"] == "monthly"
            assert monthly["month"] == "2026-07"
            assert monthly["start_date"] == "2026-07-01"
            assert monthly["end_date"] == "2026-08-01"
            assert monthly["column_profile"] == profile
            assert monthly["effective_columns"] == profile_columns[profile]
            assert monthly["source_snapshot"]["monthly_status"] == "provisional"
            assert monthly["source_snapshot"]["effective_coverage_end_date"] == "2026-07-03"
            monthly_bytes, monthly_rows = _csv_rows(client, monthly)
            assert next(csv.reader(io.StringIO(monthly_bytes.decode()))) == profile_columns[profile]
            if profile in {"full", "custom"}:
                assert sum(Decimal(row["BilledCost"]) for row in monthly_rows) == Decimal("16")
        app.state.preview_runtime._clock = lambda: datetime(2026, 7, 5, tzinfo=UTC)
        monthly_full = _profile_request(
            client,
            {"grain": "monthly", "month": "2026-07", "column_profile": "full"},
        )
        assert monthly_full["status"] == "ready"
        assert monthly_full["grain"] == "monthly"
        assert monthly_full["month"] == "2026-07"
        assert monthly_full["start_date"] == "2026-07-01"
        assert monthly_full["end_date"] == "2026-08-01"
        assert monthly_full["column_profile"] == "full"
        assert monthly_full["effective_columns"] == profile_columns["full"]
        assert monthly_full["source_snapshot"]["monthly_status"] == "provisional"
        assert monthly_full["source_snapshot"]["effective_coverage_end_date"] == "2026-07-04"
        monthly_files = monthly_full["package"]["files"]
        assert len(monthly_files) > 1
        assert [item["order"] for item in monthly_files] == list(range(1, len(monthly_files) + 1))
        assert [item["name"] for item in monthly_files] == [
            f"cost-and-usage-part-{index:05d}-of-{len(monthly_files):05d}.csv"
            for index in range(1, len(monthly_files) + 1)
        ]
        monthly_parts = [client.get(item["download_url"]).content for item in monthly_files]
        assert [len(body) for body in monthly_parts] == [item["size_bytes"] for item in monthly_files]
        assert [hashlib.sha256(body).hexdigest() for body in monthly_parts] == [
            item["sha256"] for item in monthly_files
        ]
        assert [hashlib.sha256(body).hexdigest() for body in monthly_parts] == [
            "acb6b31a2d52c2a5e0cba5d406df57a2a19b5355df4e3c1ad7eb357fb35eec7c",  # pragma: allowlist secret
            "5f831989ad3340924a929204f48e2400d402ff2ca682ff98e4ab47497dbe1385",  # pragma: allowlist secret
            "b88baba61a31632d5e24e6506656cc4612a6bc3b1971f258ed3f80e2e087622b",  # pragma: allowlist secret
            "a92943707c521273271ef26a9398d811bfab399a5d567879780d7eef91a2e87e",  # pragma: allowlist secret
            "1a7866d9342b8bdb8648623c3153513d8be3a93ed4ae9e39650785d5d77dd81f",  # pragma: allowlist secret
            "4b338e5b86cad304ad3aeddea3b74465510bba83a872c7bb153f73ed09952785",  # pragma: allowlist secret
        ]
        monthly_full_bytes, monthly_full_rows = _csv_rows(client, monthly_full)
        assert len(monthly_full_rows) == 6
        assert sum(Decimal(row["BilledCost"]) for row in monthly_full_rows) == Decimal("18")
        assert sum(Decimal(row["PricingQuantity"]) for row in monthly_full_rows) == Decimal("7")
        assert [
            (row["x_ChitraguptaSourceCostId"], row["AllocatedResourceId"], row["BilledCost"])
            for row in monthly_full_rows
        ] == first_row_order
        assert {
            (
                row["x_ChitraguptaSourceCostId"],
                row["AllocatedResourceId"],
                row["AllocatedResourceName"],
                row["AllocatedTags"],
                row["Tags"],
            )
            for row in monthly_full_rows
            if row["AllocatedResourceId"]
        } == expected_allocated_fields
        monthly_unallocated = next(row for row in monthly_full_rows if row["x_ChitraguptaSourceCostId"] == "cost-audit")
        assert monthly_unallocated["AllocatedResourceId"] == ""
        assert monthly_unallocated["AllocatedResourceName"] == ""
        assert monthly_unallocated["AllocatedTags"] == ""
        assert monthly_unallocated["Tags"] == "{}"
        assert "UNALLOCATED" not in monthly_full_bytes.decode()
        assert all("UNALLOCATED" not in row[column] for row in monthly_full_rows for column in sentinel_columns)
        assert cost_route.call_count == 1
        assert len(respx.calls) == provider_calls_before_preview

        with backend.create_unit_of_work() as uow:
            uow.tags.update_tag(origin_tag_id, "changed-origin")
            uow.tags.update_tag(identity_tag_id, "changed-team")
            uow.commit()
        repeated_bytes, _ = _csv_rows(client, ready)
        assert repeated_bytes == first_bytes
        repeated_monthly_bytes, _ = _csv_rows(client, monthly_full)
        assert repeated_monthly_bytes == monthly_full_bytes

        original_sources = CCloudBillingRepository.iter_preview_sources
        original_aggregates = CCloudBillingRepository.iter_preview_aggregates
        original_allocations = CCloudChargebackRepository.iter_preview_allocations

        def reversed_sources(self: Any, scope: Any) -> Any:
            return iter(reversed(tuple(original_sources(self, scope))))

        def reversed_aggregates(self: Any, scope: Any) -> Any:
            return iter(reversed(tuple(original_aggregates(self, scope))))

        def reversed_allocations(self: Any, scope: Any, calculation_ids: tuple[str, ...]) -> Any:
            return iter(reversed(tuple(original_allocations(self, scope, calculation_ids))))

        monkeypatch.setattr(CCloudBillingRepository, "iter_preview_sources", reversed_sources)
        monkeypatch.setattr(CCloudBillingRepository, "iter_preview_aggregates", reversed_aggregates)
        monkeypatch.setattr(CCloudChargebackRepository, "iter_preview_allocations", reversed_allocations)
        tag_calls.clear()
        refreshed = _request(client, date(2026, 7, 1), date(2026, 7, 4))
        assert refreshed["status"] == "ready"
        refreshed_bytes, refreshed_rows = _csv_rows(client, refreshed)
        assert refreshed_bytes != first_bytes
        assert any(row["Tags"] == '{"origin":"changed-origin"}' for row in refreshed_rows)
        assert any(row["AllocatedTags"] == '{"team":"changed-team"}' for row in refreshed_rows)
        assert [
            (row["x_ChitraguptaSourceCostId"], row["AllocatedResourceId"], row["BilledCost"]) for row in refreshed_rows
        ] == first_row_order
        assert len(respx.calls) == provider_calls_before_preview
    finally:
        client.close()
        backend.dispose()
        plugin.close()

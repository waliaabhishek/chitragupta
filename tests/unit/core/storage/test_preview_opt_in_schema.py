from __future__ import annotations

import inspect
from importlib import import_module
from pathlib import Path

import pytest
from sqlalchemy import create_engine
from sqlalchemy import inspect as sa_inspect

from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend
from plugins.confluent_cloud.storage.module import CCloudStorageModule


def _tables(connection_string: str) -> set[str]:
    engine = create_engine(connection_string)
    try:
        return set(sa_inspect(engine).get_table_names())
    finally:
        engine.dispose()


def test_disabled_create_all_registers_no_preview_evidence_objects(tmp_path: Path) -> None:
    connection_string = f"sqlite:///{tmp_path / 'disabled.db'}"
    backend = SQLModelBackend(
        connection_string,
        CCloudStorageModule(),
        use_migrations=False,
        focus_preview_enabled=False,
    )
    backend.create_tables()

    ccloud_tables = {name for name in _tables(connection_string) if name.startswith("ccloud_")}

    assert ccloud_tables == {"ccloud_billing"}
    backend.dispose()


def test_enabled_create_all_registers_complete_preview_evidence_objects(tmp_path: Path) -> None:
    preview_tables = import_module("plugins.confluent_cloud.storage.preview_tables")
    connection_string = f"sqlite:///{tmp_path / 'enabled.db'}"
    backend = SQLModelBackend(
        connection_string,
        CCloudStorageModule(),
        use_migrations=False,
        focus_preview_enabled=True,
    )
    backend.create_tables()

    expected = {
        preview_tables.CCloudCostSourceRecordTable.__tablename__,
        preview_tables.CCloudSourceEvidenceAttemptTable.__tablename__,
        preview_tables.CCloudSourceCaptureReadinessTable.__tablename__,
        preview_tables.CCloudOrganizationAuthorityAttemptTable.__tablename__,
        preview_tables.CCloudAllocationLineageRunTable.__tablename__,
        preview_tables.CCloudAllocationLineagePortionTable.__tablename__,
    }

    assert expected <= _tables(connection_string)
    assert backend.preview_evidence_availability.state.value == "ready"
    backend.dispose()


def test_only_ccloud_storage_module_exposes_preview_evidence_capability() -> None:
    protocols = import_module("core.plugin.protocols")
    core_module = import_module("core.storage.backends.sqlmodel.module").CoreStorageModule()
    generic_module = import_module("plugins.generic_metrics_only.storage.module").GenericMetricsOnlyStorageModule()
    ccloud_module = CCloudStorageModule()

    assert isinstance(ccloud_module, protocols.PreviewEvidenceStorageModule)
    assert not isinstance(core_module, protocols.PreviewEvidenceStorageModule)
    assert not isinstance(generic_module, protocols.PreviewEvidenceStorageModule)


def test_core_sqlmodel_backend_does_not_construct_ccloud_evidence_implementations() -> None:
    backend_module = import_module("core.storage.backends.sqlmodel.unit_of_work")
    source = inspect.getsource(backend_module)

    assert "plugins.confluent_cloud" not in source
    assert "CCloudPreview" not in source
    assert "CCloudSource" not in source


def test_disabled_backend_never_checks_optional_preview_module(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    module = CCloudStorageModule()

    def forbidden(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("disabled backend touched Preview evidence capability")

    monkeypatch.setattr(module, "register_preview_evidence_tables", forbidden)
    monkeypatch.setattr(module, "prepare_preview_evidence_migration", forbidden)
    monkeypatch.setattr(module, "create_preview_evidence_unit_of_work", forbidden)
    monkeypatch.setattr(module, "create_preview_generation_read_unit_of_work", forbidden)
    monkeypatch.setattr(module, "create_preview_evidence_bootstrap", forbidden)
    backend = SQLModelBackend(
        f"sqlite:///{tmp_path / 'disabled-no-touch.db'}",
        module,
        use_migrations=False,
        focus_preview_enabled=False,
    )

    backend.create_tables()
    backend.dispose()


def test_disabled_registry_factory_forwards_enablement_without_preview_checks(tmp_path: Path) -> None:
    from core.config.models import StorageConfig
    from core.storage.registry import create_storage_backend

    backend = create_storage_backend(
        StorageConfig(connection_string=f"sqlite:///{tmp_path / 'factory.db'}"),
        storage_module=CCloudStorageModule(),
        use_migrations=False,
        focus_preview_enabled=False,
    )
    backend.create_tables()

    assert {name for name in _tables(f"sqlite:///{tmp_path / 'factory.db'}") if name.startswith("ccloud_")} == {
        "ccloud_billing"
    }
    backend.dispose()


def test_incompatible_evidence_schema_blocks_generation_but_not_metadata_reads(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    availability = import_module("core.preview.storage_availability")
    module = CCloudStorageModule()

    def incompatible(*_args: object, **_kwargs: object) -> None:
        raise availability.PreviewEvidenceSchemaError("incompatible evidence schema")

    monkeypatch.setattr(module, "prepare_preview_evidence_migration", incompatible)
    backend = SQLModelBackend(
        f"sqlite:///{tmp_path / 'incompatible.db'}",
        module,
        use_migrations=False,
        focus_preview_enabled=True,
    )

    backend.create_tables()

    assert backend.preview_evidence_availability.state is availability.PreviewEvidenceAvailabilityState.UNAVAILABLE
    with backend.create_preview_metadata_read_unit_of_work() as metadata_uow:
        assert callable(metadata_uow.requests.list_recent_for_owner)
        assert callable(metadata_uow.revisions.list_for_owner_month)
    with pytest.raises(availability.PreviewEvidenceUnavailableError):
        backend.create_preview_generation_read_unit_of_work()
    backend.dispose()

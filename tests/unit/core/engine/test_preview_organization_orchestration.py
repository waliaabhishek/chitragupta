from __future__ import annotations

import inspect
from datetime import UTC, datetime
from unittest.mock import MagicMock

import pytest

from core.engine.orchestrator import (
    ChargebackOrchestrator,
    GatherPhase,
    PreviewOrganizationBindingConflictError,
)
from core.models.resource import CoreResource
from core.preview.organization_authority import (
    OrganizationAuthorityAttempt,
    OrganizationAuthorityAttemptStatus,
    OrganizationAuthorityFailureReason,
    OrganizationAuthorityFinalStatus,
)
from tests.unit.core.preview.evidence_backend_double import (
    PreviewEvidenceBackendDouble,
    preview_evidence_backend_double,
)

NOW = datetime(2026, 7, 22, tzinfo=UTC)


class _Context:
    def __init__(self, value: MagicMock, events: list[str], name: str) -> None:
        self.value = value
        self.events = events
        self.name = name

    def __enter__(self) -> MagicMock:
        self.events.append(f"{self.name}:enter")
        return self.value

    def __exit__(self, *args: object) -> None:
        del args
        self.events.append(f"{self.name}:exit")


class _Plugin:
    def __init__(self, resources: tuple[CoreResource, ...] = (), error: Exception | None = None) -> None:
        self.resources = resources
        self.error = error
        self.calls = 0

    def gather_preview_organizations(self, tenant_id: str) -> tuple[CoreResource, ...]:
        assert tenant_id == "tenant-1"
        self.calls += 1
        if self.error is not None:
            raise self.error
        return self.resources


def _resource(resource_id: str = "org-1") -> CoreResource:
    return CoreResource(
        ecosystem="confluent_cloud",
        tenant_id="tenant-1",
        resource_id=resource_id,
        resource_type="organization",
        display_name="Provider organization",
    )


def _attempt() -> OrganizationAuthorityAttempt:
    return OrganizationAuthorityAttempt(
        attempt_sequence=9,
        ecosystem="confluent_cloud",
        tenant_id="tenant-1",
        status=OrganizationAuthorityAttemptStatus.PENDING,
        started_at=NOW,
        completed_at=None,
        organization_id=None,
        failure_reason=None,
    )


def _uow(events: list[str], name: str, *, commit_error: Exception | None = None) -> MagicMock:
    uow = MagicMock()
    uow.commit.side_effect = commit_error if commit_error is not None else lambda: events.append(f"{name}:commit")
    return uow


def _orchestrator(
    plugin: object,
    backend: PreviewEvidenceBackendDouble,
    *,
    enabled: bool = True,
) -> ChargebackOrchestrator:
    orchestrator = object.__new__(ChargebackOrchestrator)
    orchestrator._tenant_id = "tenant-1"
    orchestrator._ecosystem = "confluent_cloud"
    orchestrator._tenant_config = MagicMock(focus_preview_enabled=enabled)
    orchestrator._storage_backend = backend
    orchestrator._gather_phase = MagicMock()
    orchestrator._gather_phase._bundle.plugin = plugin
    return orchestrator


def _backend() -> PreviewEvidenceBackendDouble:
    return preview_evidence_backend_double()


def _gather_phase() -> GatherPhase:
    phase = object.__new__(GatherPhase)
    phase._ecosystem = "confluent_cloud"
    phase._tenant_id = "tenant-1"
    return phase


def test_disabled_organization_refresh_has_zero_provider_storage_and_evidence_calls() -> None:
    plugin = _Plugin((_resource(),))
    backend = _backend()
    orchestrator = _orchestrator(plugin, backend, enabled=False)

    orchestrator._refresh_preview_organization_authority()

    assert plugin.calls == 0
    backend.create_unit_of_work.assert_not_called()
    backend.create_preview_evidence_unit_of_work.assert_not_called()


def test_supplemental_gather_has_no_unreachable_organization_reconciliation_branch() -> None:
    source = inspect.getsource(GatherPhase._run_supplemental_gather)

    assert source.count('resource_type == "organization"') == 1


def test_enabled_organization_refresh_calls_provider_once_and_finalizes_after_resource_commit() -> None:
    events: list[str] = []
    plugin = _Plugin((_resource(),))
    backend = _backend()
    begin = _uow(events, "begin")
    begin.organization_authority.begin.return_value = _attempt()
    resource = _uow(events, "resource")
    final = _uow(events, "final")
    backend.create_preview_evidence_unit_of_work.side_effect = [
        _Context(begin, events, "begin"),
        _Context(final, events, "final"),
    ]
    backend.create_unit_of_work.return_value = _Context(resource, events, "resource")
    orchestrator = _orchestrator(plugin, backend)
    orchestrator._gather_phase._reconcile_organization_resources.side_effect = lambda *args: events.append(
        "resource:reconcile"
    )

    orchestrator._refresh_preview_organization_authority()

    assert plugin.calls == 1
    assert events.index("begin:commit") < events.index("resource:reconcile")
    assert events.index("resource:commit") < events.index("final:commit")
    final.organization_authority.finalize.assert_called_once()
    args = final.organization_authority.finalize.call_args
    assert args.args == (9, OrganizationAuthorityFinalStatus.AVAILABLE)
    assert args.kwargs["organization_id"] == "org-1"
    assert args.kwargs["reason"] is None


def test_exact_provider_id_recovers_a_single_unclassified_legacy_organization_binding() -> None:
    plugin = _Plugin((_resource("org-legacy"),))
    phase = _gather_phase()
    uow = MagicMock()
    uow.resources.find_active_at.return_value = ([_resource("org-legacy")], 1)

    phase._reconcile_organization_resources(uow, plugin.resources, NOW)

    persisted = uow.resources.upsert.call_args.args[0]
    assert persisted.resource_id == "org-legacy"
    assert persisted.metadata["organization_binding_state"] == "bound"
    uow.resources.mark_deleted.assert_not_called()


def test_different_provider_id_conflicts_with_a_single_unclassified_legacy_organization() -> None:
    plugin = _Plugin((_resource("org-provider"),))
    phase = _gather_phase()
    uow = MagicMock()
    uow.resources.find_active_at.return_value = ([_resource("org-legacy")], 1)

    with pytest.raises(
        PreviewOrganizationBindingConflictError,
        match="conflicts with legacy organization state",
    ):
        phase._reconcile_organization_resources(uow, plugin.resources, NOW)

    persisted = uow.resources.upsert.call_args.args[0]
    assert persisted.resource_id == "org-provider"
    assert persisted.metadata["organization_binding_state"] == "conflicting_observation"
    uow.resources.mark_deleted.assert_not_called()


@pytest.mark.parametrize(
    ("resources", "error", "status", "reason"),
    [
        (
            (),
            None,
            OrganizationAuthorityFinalStatus.UNAVAILABLE,
            OrganizationAuthorityFailureReason.INVALID_CARDINALITY,
        ),
        (
            (_resource("org-1"), _resource("org-1")),
            None,
            OrganizationAuthorityFinalStatus.CONFLICTING,
            OrganizationAuthorityFailureReason.INVALID_CARDINALITY,
        ),
        (
            (),
            RuntimeError("provider failed"),
            OrganizationAuthorityFinalStatus.UNAVAILABLE,
            OrganizationAuthorityFailureReason.PROVIDER_ERROR,
        ),
    ],
)
def test_invalid_or_failed_provider_call_finalizes_closed_state_without_resource_transaction(
    resources: tuple[CoreResource, ...],
    error: Exception | None,
    status: OrganizationAuthorityFinalStatus,
    reason: OrganizationAuthorityFailureReason,
) -> None:
    events: list[str] = []
    plugin = _Plugin(resources, error)
    backend = _backend()
    begin = _uow(events, "begin")
    begin.organization_authority.begin.return_value = _attempt()
    final = _uow(events, "final")
    backend.create_preview_evidence_unit_of_work.side_effect = [
        _Context(begin, events, "begin"),
        _Context(final, events, "final"),
    ]
    orchestrator = _orchestrator(plugin, backend)

    orchestrator._refresh_preview_organization_authority()

    assert plugin.calls == 1
    backend.create_unit_of_work.assert_not_called()
    args = final.organization_authority.finalize.call_args
    assert args.args == (9, status)
    assert args.kwargs["organization_id"] is None
    assert args.kwargs["reason"] is reason


def test_organization_finalization_double_failure_preserves_pending_attempt_and_generic_resource_commit() -> None:
    events: list[str] = []
    plugin = _Plugin((_resource(),))
    backend = _backend()
    begin = _uow(events, "begin")
    begin.organization_authority.begin.return_value = _attempt()
    resource = _uow(events, "resource")
    first_final = _uow(events, "first-final", commit_error=RuntimeError("first final failed"))
    second_final = _uow(events, "second-final", commit_error=RuntimeError("second final failed"))
    backend.create_preview_evidence_unit_of_work.side_effect = [
        _Context(begin, events, "begin"),
        _Context(first_final, events, "first-final"),
        _Context(second_final, events, "second-final"),
    ]
    backend.create_unit_of_work.return_value = _Context(resource, events, "resource")
    orchestrator = _orchestrator(plugin, backend)

    orchestrator._refresh_preview_organization_authority()

    assert plugin.calls == 1
    begin.commit.assert_called_once_with()
    resource.commit.assert_called_once_with()
    first_final.organization_authority.finalize.assert_called_once()
    second_final.organization_authority.finalize.assert_called_once()


def test_missing_provider_capability_records_unavailable_without_generic_resource_write() -> None:
    events: list[str] = []
    backend = _backend()
    begin = _uow(events, "begin")
    begin.organization_authority.begin.return_value = _attempt()
    final = _uow(events, "final")
    backend.create_preview_evidence_unit_of_work.side_effect = [
        _Context(begin, events, "begin"),
        _Context(final, events, "final"),
    ]
    orchestrator = _orchestrator(object(), backend)

    orchestrator._refresh_preview_organization_authority()

    backend.create_unit_of_work.assert_not_called()
    final.organization_authority.finalize.assert_called_once_with(
        9,
        OrganizationAuthorityFinalStatus.UNAVAILABLE,
        completed_at=final.organization_authority.finalize.call_args.kwargs["completed_at"],
        organization_id=None,
        reason=OrganizationAuthorityFailureReason.CAPABILITY_UNAVAILABLE,
    )


@pytest.mark.parametrize("failure_point", ["reconcile", "commit"])
def test_resource_flush_or_commit_failure_isolated_and_finalized_with_exact_precedence(
    failure_point: str,
) -> None:
    events: list[str] = []
    plugin = _Plugin((_resource(),))
    backend = _backend()
    begin = _uow(events, "begin")
    begin.organization_authority.begin.return_value = _attempt()
    resource = _uow(
        events,
        "resource",
        commit_error=(RuntimeError("resource commit failed") if failure_point == "commit" else None),
    )
    final = _uow(events, "final")
    backend.create_preview_evidence_unit_of_work.side_effect = [
        _Context(begin, events, "begin"),
        _Context(final, events, "final"),
    ]
    backend.create_unit_of_work.return_value = _Context(resource, events, "resource")
    orchestrator = _orchestrator(plugin, backend)
    if failure_point == "reconcile":
        orchestrator._gather_phase._reconcile_organization_resources.side_effect = RuntimeError("resource flush failed")

    orchestrator._refresh_preview_organization_authority()

    assert events.index("resource:exit") < events.index("final:enter")
    final.organization_authority.finalize.assert_called_once()
    call = final.organization_authority.finalize.call_args
    assert call.args == (9, OrganizationAuthorityFinalStatus.UNAVAILABLE)
    assert call.kwargs["organization_id"] is None
    assert call.kwargs["reason"] is OrganizationAuthorityFailureReason.RESOURCE_PERSISTENCE_FAILED


def test_restart_after_double_finalization_failure_creates_and_completes_new_attempt() -> None:
    events: list[str] = []
    plugin = _Plugin((_resource(),))
    backend = _backend()
    first_begin = _uow(events, "first-begin")
    first_begin.organization_authority.begin.return_value = _attempt()
    first_resource = _uow(events, "first-resource")
    first_final = _uow(events, "first-final", commit_error=RuntimeError("first final failed"))
    retry_final = _uow(events, "retry-final", commit_error=RuntimeError("retry final failed"))
    second_begin = _uow(events, "second-begin")
    second_begin.organization_authority.begin.return_value = OrganizationAuthorityAttempt(
        attempt_sequence=10,
        ecosystem="confluent_cloud",
        tenant_id="tenant-1",
        status=OrganizationAuthorityAttemptStatus.PENDING,
        started_at=NOW,
        completed_at=None,
        organization_id=None,
        failure_reason=None,
    )
    second_resource = _uow(events, "second-resource")
    second_final = _uow(events, "second-final")
    backend.create_preview_evidence_unit_of_work.side_effect = [
        _Context(first_begin, events, "first-begin"),
        _Context(first_final, events, "first-final"),
        _Context(retry_final, events, "retry-final"),
        _Context(second_begin, events, "second-begin"),
        _Context(second_final, events, "second-final"),
    ]
    backend.create_unit_of_work.side_effect = [
        _Context(first_resource, events, "first-resource"),
        _Context(second_resource, events, "second-resource"),
    ]
    orchestrator = _orchestrator(plugin, backend)

    orchestrator._refresh_preview_organization_authority()
    orchestrator._refresh_preview_organization_authority()

    second_final.organization_authority.finalize.assert_called_once()
    call = second_final.organization_authority.finalize.call_args
    assert call.args == (10, OrganizationAuthorityFinalStatus.AVAILABLE)
    assert call.kwargs["organization_id"] == "org-1"
    assert call.kwargs["reason"] is None
    second_final.commit.assert_called_once_with()

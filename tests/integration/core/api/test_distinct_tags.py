from __future__ import annotations

from fastapi.testclient import TestClient  # noqa: TC002

from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend  # noqa: TC001


def _seed_tag(
    backend: SQLModelBackend,
    tag_key: str,
    tag_value: str,
    entity_type: str = "resource",
    entity_id: str = "e1",
    tenant_id: str = "test-tenant",
) -> None:
    with backend.create_unit_of_work() as uow:
        uow.tags.add_tag(
            tenant_id=tenant_id,
            entity_type=entity_type,
            entity_id=entity_id,
            tag_key=tag_key,
            tag_value=tag_value,
            created_by="admin",
        )
        uow.commit()


class TestDistinctTagKeys:
    def test_keys_returns_sorted_distinct_keys(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        _seed_tag(in_memory_backend, "env", "prod", entity_id="e1")
        _seed_tag(in_memory_backend, "team", "a", entity_id="e2")
        _seed_tag(in_memory_backend, "env", "staging", entity_id="e3")

        response = app_with_backend.get("/api/v1/tenants/test-tenant/tags/keys")

        assert response.status_code == 200
        assert response.json() == {"keys": ["env", "team"]}

    def test_keys_entity_type_filter_resource(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        _seed_tag(in_memory_backend, "env", "prod", entity_type="resource", entity_id="r1")
        _seed_tag(in_memory_backend, "team", "platform", entity_type="identity", entity_id="u1")

        response = app_with_backend.get("/api/v1/tenants/test-tenant/tags/keys?entity_type=resource")

        assert response.status_code == 200
        assert response.json() == {"keys": ["env"]}

    def test_keys_entity_type_filter_identity(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        _seed_tag(in_memory_backend, "env", "prod", entity_type="resource", entity_id="r1")
        _seed_tag(in_memory_backend, "team", "platform", entity_type="identity", entity_id="u1")

        response = app_with_backend.get("/api/v1/tenants/test-tenant/tags/keys?entity_type=identity")

        assert response.status_code == 200
        assert response.json() == {"keys": ["team"]}

    def test_keys_empty_tenant_returns_200_with_empty_list(self, app_with_backend: TestClient) -> None:
        response = app_with_backend.get("/api/v1/tenants/test-tenant/tags/keys")

        assert response.status_code == 200
        assert response.json() == {"keys": []}

    def test_keys_tenant_isolation(self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend) -> None:
        # Seed tags belonging to another tenant directly via the backend
        _seed_tag(in_memory_backend, "secret", "value", tenant_id="other-tenant", entity_id="x1")

        # test-tenant should see no keys
        response = app_with_backend.get("/api/v1/tenants/test-tenant/tags/keys")

        assert response.status_code == 200
        assert response.json() == {"keys": []}

    def test_keys_invalid_entity_type_returns_422(self, app_with_backend: TestClient) -> None:
        response = app_with_backend.get("/api/v1/tenants/test-tenant/tags/keys?entity_type=dimension")

        assert response.status_code == 422


class TestDistinctTagValues:
    def test_values_returns_sorted_distinct_values(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        _seed_tag(in_memory_backend, "env", "prod", entity_id="e1")
        _seed_tag(in_memory_backend, "env", "staging", entity_id="e2")
        # Duplicate key+value on different entity — should appear once
        _seed_tag(in_memory_backend, "env", "prod", entity_id="e3")

        response = app_with_backend.get("/api/v1/tenants/test-tenant/tags/keys/env/values")

        assert response.status_code == 200
        assert response.json() == {"values": ["prod", "staging"]}

    def test_values_entity_type_filter(self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend) -> None:
        _seed_tag(in_memory_backend, "env", "prod", entity_type="resource", entity_id="r1")
        _seed_tag(in_memory_backend, "env", "staging", entity_type="identity", entity_id="u1")

        response = app_with_backend.get("/api/v1/tenants/test-tenant/tags/keys/env/values?entity_type=resource")

        assert response.status_code == 200
        assert response.json() == {"values": ["prod"]}

    def test_values_q_prefix_filter(self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend) -> None:
        _seed_tag(in_memory_backend, "env", "prod", entity_id="e1")
        _seed_tag(in_memory_backend, "env", "production", entity_id="e2")
        _seed_tag(in_memory_backend, "env", "staging", entity_id="e3")

        response = app_with_backend.get("/api/v1/tenants/test-tenant/tags/keys/env/values?q=pro")

        assert response.status_code == 200
        assert response.json() == {"values": ["prod", "production"]}

    def test_values_q_prefix_case_insensitive(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        _seed_tag(in_memory_backend, "env", "Production", entity_id="e1")
        _seed_tag(in_memory_backend, "env", "staging", entity_id="e2")

        response = app_with_backend.get("/api/v1/tenants/test-tenant/tags/keys/env/values?q=pro")

        assert response.status_code == 200
        assert response.json() == {"values": ["Production"]}

    def test_values_unknown_key_returns_empty_list(self, app_with_backend: TestClient) -> None:
        response = app_with_backend.get("/api/v1/tenants/test-tenant/tags/keys/nonexistent/values")

        assert response.status_code == 200
        assert response.json() == {"values": []}

    def test_values_tenant_isolation(self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend) -> None:
        # Seed env=secret for another tenant
        _seed_tag(in_memory_backend, "env", "secret", tenant_id="other-tenant", entity_id="x1")

        # test-tenant should see no values for env
        response = app_with_backend.get("/api/v1/tenants/test-tenant/tags/keys/env/values")

        assert response.status_code == 200
        assert response.json() == {"values": []}

    def test_values_invalid_tag_key_returns_400(self, app_with_backend: TestClient) -> None:
        response = app_with_backend.get("/api/v1/tenants/test-tenant/tags/keys/!!bad!!/values")

        assert response.status_code == 400

    def test_values_invalid_entity_type_returns_422(self, app_with_backend: TestClient) -> None:
        response = app_with_backend.get("/api/v1/tenants/test-tenant/tags/keys/env/values?entity_type=dimension")

        assert response.status_code == 422


class TestDistinctTagResponseSchema:
    def test_keys_response_has_keys_field(self, app_with_backend: TestClient) -> None:
        response = app_with_backend.get("/api/v1/tenants/test-tenant/tags/keys")

        assert response.status_code == 200
        data = response.json()
        assert "keys" in data
        assert isinstance(data["keys"], list)

    def test_values_response_has_values_field(self, app_with_backend: TestClient) -> None:
        response = app_with_backend.get("/api/v1/tenants/test-tenant/tags/keys/env/values")

        assert response.status_code == 200
        data = response.json()
        assert "values" in data
        assert isinstance(data["values"], list)

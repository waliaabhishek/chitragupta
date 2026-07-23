from __future__ import annotations

import httpx
import respx

from plugins.confluent_cloud.plugin import ConfluentCloudPlugin


def _plugin() -> ConfluentCloudPlugin:
    plugin = ConfluentCloudPlugin()
    plugin.initialize(
        {
            "ccloud_api": {"key": "key", "secret": "secret"},  # pragma: allowlist secret
            "request_interval_seconds": 0,
        }
    )
    return plugin


@respx.mock
def test_ccloud_preview_organization_capability_calls_provider_once_and_returns_tuple() -> None:
    route = respx.get("https://api.confluent.cloud/org/v2/organizations").mock(
        return_value=httpx.Response(
            200,
            json={
                "data": [
                    {
                        "id": "11111111-2222-4333-8444-555555555555",
                        "display_name": "Provider billing organization",
                    }
                ],
                "metadata": {},
            },
        )
    )
    plugin = _plugin()

    resources = plugin.gather_preview_organizations("tenant-1")

    assert route.call_count == 1
    assert isinstance(resources, tuple)
    assert len(resources) == 1
    assert resources[0].resource_id == "11111111-2222-4333-8444-555555555555"
    assert resources[0].tenant_id == "tenant-1"
    assert resources[0].metadata == {}


def test_ccloud_organization_is_not_part_of_generic_supplemental_gathering() -> None:
    from core.plugin import protocols

    plugin = _plugin()

    assert isinstance(plugin, protocols.PreviewOrganizationGatherer)
    assert not isinstance(plugin, protocols.SupplementalResourceGatherer)

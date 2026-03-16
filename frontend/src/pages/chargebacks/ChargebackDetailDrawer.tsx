import type React from "react";
import { Descriptions, Divider, Drawer, Spin, notification } from "antd";
import { useEffect, useState } from "react";
import { TagEditor } from "../../components/chargebacks/TagEditor";
import { API_URL } from "../../config";
import { useTenant } from "../../providers/TenantContext";
import type { ChargebackDimensionResponse } from "../../types/api";

interface ChargebackDetailDrawerProps {
  dimensionId: number | null;
  onClose: () => void;
  onTagsChanged: () => void;
}

async function fetchDimension(
  tenantName: string,
  dimensionId: number,
  signal?: AbortSignal,
): Promise<ChargebackDimensionResponse | null> {
  const resp = await fetch(
    `${API_URL}/tenants/${tenantName}/chargebacks/${dimensionId}`,
    signal ? { signal } : {},
  );
  if (!resp.ok) return null;
  return (await resp.json()) as ChargebackDimensionResponse;
}

async function patchDimension(
  tenantName: string,
  dimensionId: number,
  body: {
    add_tags?: { tag_key: string; display_name: string; created_by: string }[];
    tags?: { tag_key: string; display_name: string; created_by: string }[];
    remove_tag_ids?: number[];
  },
): Promise<void> {
  const resp = await fetch(
    `${API_URL}/tenants/${tenantName}/chargebacks/${dimensionId}`,
    {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    },
  );
  if (!resp.ok) {
    const detail = await resp.text();
    throw new Error(detail);
  }
}

export function ChargebackDetailDrawer({
  dimensionId,
  onClose,
  onTagsChanged,
}: ChargebackDetailDrawerProps): React.JSX.Element | null {
  const { currentTenant, isReadOnly } = useTenant();
  const [dimension, setDimension] =
    useState<ChargebackDimensionResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [notFound, setNotFound] = useState(false);

  useEffect(() => {
    if (dimensionId === null || !currentTenant) return;
    const controller = new AbortController();
    setLoading(true);
    setNotFound(false);
    setDimension(null);

    fetchDimension(currentTenant.tenant_name, dimensionId, controller.signal)
      .then((data) => {
        if (data === null) {
          setNotFound(true);
        } else {
          setDimension(data);
        }
      })
      .catch((err: unknown) => {
        if (err instanceof Error && err.name === "AbortError") return;
        setNotFound(true);
      })
      .finally(() => {
        if (!controller.signal.aborted) setLoading(false);
      });

    return () => {
      controller.abort();
    };
  }, [dimensionId, currentTenant]);

  const handleAddTag = async (key: string, displayName: string): Promise<void> => {
    if (!currentTenant || dimensionId === null) return;
    try {
      await patchDimension(currentTenant.tenant_name, dimensionId, {
        add_tags: [{ tag_key: key, display_name: displayName, created_by: "ui" }],
      });
      onTagsChanged();
      const updated = await fetchDimension(
        currentTenant.tenant_name,
        dimensionId,
      );
      if (updated !== null) setDimension(updated);
    } catch (err) {
      notification.error({
        message: "Failed to add tag",
        description: err instanceof Error ? err.message : "Unknown error",
      });
    }
  };

  const handleRemoveTag = async (tagId: number): Promise<void> => {
    if (!currentTenant || dimensionId === null) return;
    try {
      await patchDimension(currentTenant.tenant_name, dimensionId, {
        remove_tag_ids: [tagId],
      });
      onTagsChanged();
      const updated = await fetchDimension(
        currentTenant.tenant_name,
        dimensionId,
      );
      if (updated !== null) setDimension(updated);
    } catch (err) {
      notification.error({
        message: "Failed to remove tag",
        description: err instanceof Error ? err.message : "Unknown error",
      });
    }
  };

  if (dimensionId === null) return null;

  return (
    <Drawer
      open
      onClose={onClose}
      title="Chargeback Details"
      width={480}
    >
      {loading && (
        <div style={{ textAlign: "center", padding: 40 }}>
          <Spin />
        </div>
      )}
      {!loading && notFound && (
        <p>Dimension not found.</p>
      )}
      {!loading && dimension && (
        <>
          <Descriptions column={1} size="small" bordered>
            <Descriptions.Item label="Product Type">
              {dimension.product_type}
            </Descriptions.Item>
            <Descriptions.Item label="Product Category">
              {dimension.product_category}
            </Descriptions.Item>
            <Descriptions.Item label="Identity">
              {dimension.identity_id}
            </Descriptions.Item>
            <Descriptions.Item label="Resource">
              {dimension.resource_id ?? "—"}
            </Descriptions.Item>
            <Descriptions.Item label="Cost Type">
              {dimension.cost_type}
            </Descriptions.Item>
            <Descriptions.Item label="Allocation Method">
              {dimension.allocation_method ?? "—"}
            </Descriptions.Item>
          </Descriptions>
          <Divider />
          <TagEditor
            tags={dimension.tags}
            onAdd={handleAddTag}
            onRemove={handleRemoveTag}
            readOnly={isReadOnly}
          />
        </>
      )}
    </Drawer>
  );
}

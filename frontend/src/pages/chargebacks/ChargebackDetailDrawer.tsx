import type React from "react";
import {
  Descriptions,
  Divider,
  Drawer,
  Space,
  Spin,
  Tag,
  Typography,
} from "antd";
import { useEffect, useState } from "react";
import { API_URL } from "../../config";
import { useTenant } from "../../providers/TenantContext";
import type { ChargebackDimensionResponse } from "../../types/api";

interface ChargebackDetailDrawerProps {
  dimensionId: number | null;
  inheritedTags: Record<string, string>;
  onClose: () => void;
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

export function ChargebackDetailDrawer({
  dimensionId,
  inheritedTags,
  onClose,
}: ChargebackDetailDrawerProps): React.JSX.Element | null {
  const { currentTenant } = useTenant();
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

  if (dimensionId === null) return null;

  return (
    <Drawer open onClose={onClose} title="Chargeback Details" width={480}>
      {loading && (
        <div style={{ textAlign: "center", padding: 40 }}>
          <Spin />
        </div>
      )}
      {!loading && notFound && <p>Dimension not found.</p>}
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
          <Typography.Title level={5}>Inherited Tags</Typography.Title>
          {(() => {
            const tagEntries = Object.entries(inheritedTags);
            return tagEntries.length === 0 ? (
              <Typography.Text type="secondary">No tags</Typography.Text>
            ) : (
              <Space wrap>
                {tagEntries.map(([k, v]) => (
                  <Tag key={k}>
                    {k}: {v}
                  </Tag>
                ))}
              </Space>
            );
          })()}
        </>
      )}
    </Drawer>
  );
}

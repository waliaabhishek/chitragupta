import type React from "react";
import { Button, Descriptions, Divider, Drawer, Table, Typography, notification } from "antd";
import type { ColumnsType } from "antd/es/table";
import { useCallback, useEffect, useRef, useState } from "react";
import { EntityTagEditor } from "../../components/entities/EntityTagEditor";
import { API_URL } from "../../config";
import { useTenant } from "../../providers/TenantContext";
import type { PaginatedResponse, ResourceResponse } from "../../types/api";

const PAGE_SIZE = 100;

function ResourceDetailDrawer({
  resource,
  tenantName,
  onClose,
}: {
  resource: ResourceResponse;
  tenantName: string;
  onClose: () => void;
}): React.JSX.Element {
  return (
    <Drawer open onClose={onClose} title="Resource Details" width={480}>
      <Descriptions column={1} size="small" bordered>
        <Descriptions.Item label="Resource ID">{resource.resource_id}</Descriptions.Item>
        <Descriptions.Item label="Type">{resource.resource_type}</Descriptions.Item>
        <Descriptions.Item label="Display Name">{resource.display_name ?? "—"}</Descriptions.Item>
        <Descriptions.Item label="Owner">{resource.owner_id ?? "—"}</Descriptions.Item>
        <Descriptions.Item label="Status">{resource.status}</Descriptions.Item>
      </Descriptions>
      <Divider />
      <EntityTagEditor
        tenantName={tenantName}
        entityType="resource"
        entityId={resource.resource_id}
      />
    </Drawer>
  );
}

export function ResourceListPage(): React.JSX.Element {
  const { currentTenant } = useTenant();
  const [resources, setResources] = useState<ResourceResponse[]>([]);
  const [total, setTotal] = useState(0);
  const [page, setPage] = useState(1);
  const [loading, setLoading] = useState(false);
  const [selected, setSelected] = useState<ResourceResponse | null>(null);
  const abortRef = useRef<AbortController | null>(null);

  const fetchResources = useCallback(async (p: number) => {
    if (!currentTenant) return;
    abortRef.current?.abort();
    const controller = new AbortController();
    abortRef.current = controller;
    setLoading(true);
    try {
      const url = new URL(`${window.location.origin}${API_URL}/tenants/${currentTenant.tenant_name}/resources`);
      url.searchParams.set("page", String(p));
      url.searchParams.set("page_size", String(PAGE_SIZE));
      const resp = await fetch(url.toString(), { signal: controller.signal });
      if (!resp.ok) throw new Error("Failed to fetch resources");
      const data = (await resp.json()) as PaginatedResponse<ResourceResponse>;
      setResources(data.items);
      setTotal(data.total);
    } catch (err) {
      if (err instanceof Error && err.name === "AbortError") return;
      notification.error({ message: "Failed to load resources", description: err instanceof Error ? err.message : "Unknown error" });
    } finally {
      if (!controller.signal.aborted) setLoading(false);
    }
  }, [currentTenant]);

  useEffect(() => { void fetchResources(page); }, [fetchResources, page]);

  const columns: ColumnsType<ResourceResponse> = [
    { title: "Resource ID", dataIndex: "resource_id", key: "resource_id", ellipsis: true },
    { title: "Type", dataIndex: "resource_type", key: "resource_type" },
    { title: "Display Name", dataIndex: "display_name", key: "display_name" },
    { title: "Status", dataIndex: "status", key: "status" },
    {
      title: "Actions",
      key: "actions",
      render: (_: unknown, record: ResourceResponse) => (
        <Button type="link" size="small" onClick={() => setSelected(record)}>Details</Button>
      ),
    },
  ];

  if (!currentTenant) {
    return (
      <div>
        <Typography.Title level={3}>Resources</Typography.Title>
        <Typography.Text type="secondary">Select a tenant to begin.</Typography.Text>
      </div>
    );
  }

  return (
    <div>
      <Typography.Title level={3} style={{ margin: "0 0 16px 0" }}>Resources</Typography.Title>
      <Table
        dataSource={resources}
        columns={columns}
        rowKey="resource_id"
        loading={loading}
        pagination={{ total, pageSize: PAGE_SIZE, current: page, onChange: setPage, showTotal: (t) => `${t} resources` }}
      />
      {selected && (
        <ResourceDetailDrawer
          resource={selected}
          tenantName={currentTenant.tenant_name}
          onClose={() => setSelected(null)}
        />
      )}
    </div>
  );
}

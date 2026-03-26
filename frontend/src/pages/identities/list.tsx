import type React from "react";
import { Button, Descriptions, Divider, Drawer, Table, Typography, notification } from "antd";
import type { ColumnsType } from "antd/es/table";
import { useCallback, useEffect, useRef, useState } from "react";
import { EntityTagEditor } from "../../components/entities/EntityTagEditor";
import { API_URL } from "../../config";
import { useTenant } from "../../providers/TenantContext";
import type { IdentityResponse, PaginatedResponse } from "../../types/api";

const PAGE_SIZE = 100;

function IdentityDetailDrawer({
  identity,
  tenantName,
  onClose,
}: {
  identity: IdentityResponse;
  tenantName: string;
  onClose: () => void;
}): React.JSX.Element {
  return (
    <Drawer open onClose={onClose} title="Identity Details" width={480}>
      <Descriptions column={1} size="small" bordered>
        <Descriptions.Item label="Identity ID">{identity.identity_id}</Descriptions.Item>
        <Descriptions.Item label="Type">{identity.identity_type}</Descriptions.Item>
        <Descriptions.Item label="Display Name">{identity.display_name ?? "—"}</Descriptions.Item>
        <Descriptions.Item label="Status">{identity.deleted_at ? "Deleted" : "Active"}</Descriptions.Item>
      </Descriptions>
      <Divider />
      <EntityTagEditor
        tenantName={tenantName}
        entityType="identity"
        entityId={identity.identity_id}
      />
    </Drawer>
  );
}

export function IdentityListPage(): React.JSX.Element {
  const { currentTenant } = useTenant();
  const [identities, setIdentities] = useState<IdentityResponse[]>([]);
  const [total, setTotal] = useState(0);
  const [page, setPage] = useState(1);
  const [loading, setLoading] = useState(false);
  const [selected, setSelected] = useState<IdentityResponse | null>(null);
  const abortRef = useRef<AbortController | null>(null);

  const fetchIdentities = useCallback(async (p: number) => {
    if (!currentTenant) return;
    abortRef.current?.abort();
    const controller = new AbortController();
    abortRef.current = controller;
    setLoading(true);
    try {
      const url = new URL(`${window.location.origin}${API_URL}/tenants/${currentTenant.tenant_name}/identities`);
      url.searchParams.set("page", String(p));
      url.searchParams.set("page_size", String(PAGE_SIZE));
      const resp = await fetch(url.toString(), { signal: controller.signal });
      if (!resp.ok) throw new Error("Failed to fetch identities");
      const data = (await resp.json()) as PaginatedResponse<IdentityResponse>;
      setIdentities(data.items);
      setTotal(data.total);
    } catch (err) {
      if (err instanceof Error && err.name === "AbortError") return;
      notification.error({ message: "Failed to load identities", description: err instanceof Error ? err.message : "Unknown error" });
    } finally {
      if (!controller.signal.aborted) setLoading(false);
    }
  }, [currentTenant]);

  useEffect(() => { void fetchIdentities(page); }, [fetchIdentities, page]);

  const columns: ColumnsType<IdentityResponse> = [
    { title: "Identity ID", dataIndex: "identity_id", key: "identity_id", ellipsis: true },
    { title: "Type", dataIndex: "identity_type", key: "identity_type" },
    { title: "Display Name", dataIndex: "display_name", key: "display_name" },
    {
      title: "Actions",
      key: "actions",
      render: (_: unknown, record: IdentityResponse) => (
        <Button type="link" size="small" onClick={() => setSelected(record)}>Details</Button>
      ),
    },
  ];

  if (!currentTenant) {
    return (
      <div>
        <Typography.Title level={3}>Identities</Typography.Title>
        <Typography.Text type="secondary">Select a tenant to begin.</Typography.Text>
      </div>
    );
  }

  return (
    <div>
      <Typography.Title level={3} style={{ margin: "0 0 16px 0" }}>Identities</Typography.Title>
      <Table
        dataSource={identities}
        columns={columns}
        rowKey="identity_id"
        loading={loading}
        pagination={{ total, pageSize: PAGE_SIZE, current: page, onChange: setPage, showTotal: (t) => `${t} identities` }}
      />
      {selected && (
        <IdentityDetailDrawer
          identity={selected}
          tenantName={currentTenant.tenant_name}
          onClose={() => setSelected(null)}
        />
      )}
    </div>
  );
}

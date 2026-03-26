import type React from "react";
import { Button, Input, Popconfirm, Space, Table, Typography, notification } from "antd";
import type { ColumnsType } from "antd/es/table";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { API_URL } from "../../config";
import { useTenant } from "../../providers/TenantContext";
import type { EntityTagResponse, PaginatedResponse } from "../../types/api";

const { Title, Text } = Typography;
const PAGE_SIZE = 100;

export function TagManagementPage(): React.JSX.Element {
  const { currentTenant, isReadOnly } = useTenant();

  const [tags, setTags] = useState<EntityTagResponse[]>([]);
  const [total, setTotal] = useState(0);
  const [page, setPage] = useState(1);
  const [loading, setLoading] = useState(false);
  const [editingTag, setEditingTag] = useState<EntityTagResponse | null>(null);
  const [editValue, setEditValue] = useState("");
  const [savingTagId, setSavingTagId] = useState<number | null>(null);
  const savingRef = useRef(false);
  const abortRef = useRef<AbortController | null>(null);

  const fetchTags = useCallback(
    async (p: number) => {
      if (!currentTenant) return;
      abortRef.current?.abort();
      const controller = new AbortController();
      abortRef.current = controller;
      setLoading(true);
      try {
        const url = new URL(
          `${window.location.origin}${API_URL}/tenants/${currentTenant.tenant_name}/tags`,
        );
        url.searchParams.set("page", String(p));
        url.searchParams.set("page_size", String(PAGE_SIZE));

        const resp = await fetch(url.toString(), { signal: controller.signal });
        if (!resp.ok) throw new Error("Failed to fetch tags");
        const data = (await resp.json()) as PaginatedResponse<EntityTagResponse>;
        setTags(data.items);
        setTotal(data.total);
      } catch (err) {
        if (err instanceof Error && err.name === "AbortError") return;
        notification.error({
          message: "Failed to load tags",
          description: err instanceof Error ? err.message : "Unknown error",
        });
      } finally {
        if (!controller.signal.aborted) setLoading(false);
      }
    },
    [currentTenant],
  );

  useEffect(() => {
    void fetchTags(page);
  }, [fetchTags, page]);

  const handleSaveEdit = useCallback(
    async (tag: EntityTagResponse) => {
      if (savingRef.current) return;
      savingRef.current = true;
      if (!currentTenant) { savingRef.current = false; return; }
      setSavingTagId(tag.tag_id);
      try {
        const resp = await fetch(
          `${API_URL}/tenants/${currentTenant.tenant_name}/entities/${tag.entity_type}/${tag.entity_id}/tags/${tag.tag_key}`,
          {
            method: "PUT",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ tag_value: editValue }),
          },
        );
        if (!resp.ok) throw new Error("Failed to update tag");
        setEditingTag(null);
        setEditValue("");
        await fetchTags(page);
      } catch (err) {
        notification.error({
          message: "Failed to update tag",
          description: err instanceof Error ? err.message : "Unknown error",
        });
      } finally {
        setSavingTagId(null);
        savingRef.current = false;
      }
    },
    [currentTenant, editValue, fetchTags, page],
  );

  const handleDelete = useCallback(
    async (tag: EntityTagResponse) => {
      if (!currentTenant) return;
      try {
        const resp = await fetch(
          `${API_URL}/tenants/${currentTenant.tenant_name}/entities/${tag.entity_type}/${tag.entity_id}/tags/${tag.tag_key}`,
          { method: "DELETE" },
        );
        if (!resp.ok) throw new Error("Failed to delete tag");
        await fetchTags(page);
      } catch (err) {
        notification.error({
          message: "Failed to delete tag",
          description: err instanceof Error ? err.message : "Unknown error",
        });
      }
    },
    [currentTenant, fetchTags, page],
  );

  const columns: ColumnsType<EntityTagResponse> = useMemo(
    () => [
      {
        title: "Entity Type",
        dataIndex: "entity_type",
        key: "entity_type",
      },
      {
        title: "Entity ID",
        dataIndex: "entity_id",
        key: "entity_id",
        ellipsis: true,
      },
      {
        title: "Key",
        dataIndex: "tag_key",
        key: "tag_key",
      },
      {
        title: "Value",
        dataIndex: "tag_value",
        key: "tag_value",
        render: (value: string, record: EntityTagResponse) => {
          if (editingTag?.tag_id === record.tag_id && !isReadOnly) {
            return (
              <Space>
                <Input
                  value={editValue}
                  onChange={(e) => setEditValue(e.target.value)}
                  onPressEnter={() => void handleSaveEdit(record)}
                  onBlur={() => void handleSaveEdit(record)}
                  autoFocus
                  maxLength={500}
                  style={{ width: 200 }}
                />
                <Button size="small" onClick={() => { setEditingTag(null); setEditValue(""); }}>
                  Cancel
                </Button>
              </Space>
            );
          }
          if (isReadOnly) return <span>{value}</span>;
          return (
            <Button
              type="link"
              style={{ padding: 0 }}
              loading={savingTagId === record.tag_id}
              onClick={() => { setEditingTag(record); setEditValue(value); }}
            >
              {value || <Text type="secondary">(click to set)</Text>}
            </Button>
          );
        },
      },
      {
        title: "Created By",
        dataIndex: "created_by",
        key: "created_by",
      },
      {
        title: "Actions",
        key: "actions",
        render: (_: unknown, record: EntityTagResponse) => (
          !isReadOnly && (
            <Popconfirm
              title="Delete this tag?"
              onConfirm={() => void handleDelete(record)}
              okText="Delete"
              cancelText="Cancel"
            >
              <Button type="link" danger size="small">Delete</Button>
            </Popconfirm>
          )
        ),
      },
    ],
    [editingTag, editValue, isReadOnly, savingTagId, handleSaveEdit, handleDelete],
  );

  if (!currentTenant) {
    return (
      <div>
        <Title level={3}>Tags</Title>
        <Text type="secondary">Select a tenant to view tags.</Text>
      </div>
    );
  }

  return (
    <div>
      <Title level={3} style={{ margin: "0 0 16px 0" }}>
        Tag Management
      </Title>
      <Table
        dataSource={tags}
        columns={columns}
        rowKey="tag_id"
        loading={loading}
        pagination={{
          total,
          pageSize: PAGE_SIZE,
          current: page,
          onChange: (p) => setPage(p),
          showTotal: (t) => `${t} tags`,
        }}
      />
    </div>
  );
}

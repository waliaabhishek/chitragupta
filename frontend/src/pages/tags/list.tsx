import { Button, Input, Popconfirm, Space, Table, Typography, notification } from "antd";
import type { ColumnsType } from "antd/es/table";
import { useCallback, useEffect, useRef, useState } from "react";
import { useNavigate } from "react-router-dom";
import { API_URL } from "../../config";
import { useTenant } from "../../providers/TenantContext";
import type { PaginatedResponse, TagWithDimensionResponse } from "../../types/api";

const { Title, Text } = Typography;
const PAGE_SIZE = 100;

export function TagManagementPage(): JSX.Element {
  const { currentTenant, isReadOnly } = useTenant();
  const navigate = useNavigate();

  const [tags, setTags] = useState<TagWithDimensionResponse[]>([]);
  const [total, setTotal] = useState(0);
  const [page, setPage] = useState(1);
  const [search, setSearch] = useState("");
  const [loading, setLoading] = useState(false);
  const [editingTagId, setEditingTagId] = useState<number | null>(null);
  const [editValue, setEditValue] = useState("");
  const [savingTagId, setSavingTagId] = useState<number | null>(null);
  const searchTimeout = useRef<ReturnType<typeof setTimeout> | null>(null);
  const abortRef = useRef<AbortController | null>(null);

  const fetchTags = useCallback(
    async (p: number, s: string) => {
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
        if (s) url.searchParams.set("search", s);

        const resp = await fetch(url.toString(), { signal: controller.signal });
        if (!resp.ok) throw new Error("Failed to fetch tags");
        const data = (await resp.json()) as PaginatedResponse<TagWithDimensionResponse>;
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
    void fetchTags(page, search);
  }, [fetchTags, page, search]);

  const handleSearchChange = (value: string) => {
    if (searchTimeout.current) clearTimeout(searchTimeout.current);
    searchTimeout.current = setTimeout(() => {
      setPage(1);
      setSearch(value);
    }, 300);
  };

  const handleStartEdit = (tag: TagWithDimensionResponse) => {
    setEditingTagId(tag.tag_id);
    setEditValue(tag.display_name);
  };

  const handleCancelEdit = () => {
    setEditingTagId(null);
    setEditValue("");
  };

  const handleSaveEdit = async (tagId: number) => {
    if (!currentTenant) return;
    setSavingTagId(tagId);
    try {
      const resp = await fetch(
        `${API_URL}/tenants/${currentTenant.tenant_name}/tags/${tagId}`,
        {
          method: "PATCH",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ display_name: editValue }),
        },
      );
      if (!resp.ok) throw new Error("Failed to update tag");
      setEditingTagId(null);
      await fetchTags(page, search);
    } catch (err) {
      notification.error({
        message: "Failed to update tag",
        description: err instanceof Error ? err.message : "Unknown error",
      });
    } finally {
      setSavingTagId(null);
    }
  };

  const handleDelete = async (tagId: number) => {
    if (!currentTenant) return;
    try {
      const resp = await fetch(
        `${API_URL}/tenants/${currentTenant.tenant_name}/tags/${tagId}`,
        { method: "DELETE" },
      );
      if (!resp.ok) throw new Error("Failed to delete tag");
      await fetchTags(page, search);
    } catch (err) {
      notification.error({
        message: "Failed to delete tag",
        description: err instanceof Error ? err.message : "Unknown error",
      });
    }
  };

  const columns: ColumnsType<TagWithDimensionResponse> = [
    {
      title: "Display Name",
      dataIndex: "display_name",
      key: "display_name",
      render: (value: string, record: TagWithDimensionResponse) => {
        if (editingTagId === record.tag_id && !isReadOnly) {
          return (
            <Space>
              <Input
                value={editValue}
                onChange={(e) => setEditValue(e.target.value)}
                onPressEnter={() => void handleSaveEdit(record.tag_id)}
                onBlur={() => void handleSaveEdit(record.tag_id)}
                autoFocus
                maxLength={500}
                style={{ width: 200 }}
              />
              <Button size="small" onClick={handleCancelEdit}>
                Cancel
              </Button>
            </Space>
          );
        }
        if (isReadOnly) {
          return <span>{value}</span>;
        }
        return (
          <Button
            type="link"
            style={{ padding: 0 }}
            loading={savingTagId === record.tag_id}
            onClick={() => handleStartEdit(record)}
          >
            {value || <Text type="secondary">(click to set)</Text>}
          </Button>
        );
      },
    },
    {
      title: "Key",
      dataIndex: "tag_key",
      key: "tag_key",
    },
    {
      title: "Identity",
      dataIndex: "identity_id",
      key: "identity_id",
      ellipsis: true,
    },
    {
      title: "Product Type",
      dataIndex: "product_type",
      key: "product_type",
    },
    {
      title: "Actions",
      key: "actions",
      render: (_: unknown, record: TagWithDimensionResponse) => (
        <Space>
          <Button
            type="link"
            size="small"
            onClick={() => navigate(`/chargebacks?selected=${record.dimension_id}`)}
          >
            View
          </Button>
          {!isReadOnly && (
            <Popconfirm
              title="Delete this tag?"
              onConfirm={() => void handleDelete(record.tag_id)}
              okText="Delete"
              cancelText="Cancel"
            >
              <Button type="link" danger size="small">
                Delete
              </Button>
            </Popconfirm>
          )}
        </Space>
      ),
    },
  ];

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
      <div style={{ marginBottom: 16 }}>
        <Input.Search
          placeholder="Search tags..."
          allowClear
          style={{ width: 320 }}
          onChange={(e) => handleSearchChange(e.target.value)}
          onSearch={(value) => {
            setPage(1);
            setSearch(value);
          }}
        />
      </div>
      {!loading && tags.length === 0 && search && (
        <Text type="secondary">No tags match your search.</Text>
      )}
      {!loading && tags.length === 0 && !search && (
        <Text type="secondary">No tags yet. Add tags from the Chargebacks page.</Text>
      )}
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

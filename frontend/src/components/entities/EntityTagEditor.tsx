import type React from "react";
import { Button, Form, Input, Space, Spin, Tag, Typography, notification } from "antd";
import { useCallback, useEffect, useState } from "react";
import { API_URL } from "../../config";
import { useTenant } from "../../providers/TenantContext";
import type { EntityTagCreateRequest, EntityTagResponse } from "../../types/api";

interface EntityTagEditorProps {
  tenantName: string;
  entityType: "resource" | "identity";
  entityId: string;
}

interface AddFormValues {
  tag_key: string;
  tag_value: string;
}

function entityTagsUrl(tenantName: string, entityType: string, entityId: string): string {
  return `${API_URL}/tenants/${tenantName}/entities/${entityType}/${entityId}/tags`;
}

export function EntityTagEditor({
  tenantName,
  entityType,
  entityId,
}: EntityTagEditorProps): React.JSX.Element {
  const { isReadOnly } = useTenant();
  const [tags, setTags] = useState<EntityTagResponse[]>([]);
  const [loading, setLoading] = useState(false);
  const [form] = Form.useForm<AddFormValues>();
  const [submitting, setSubmitting] = useState(false);

  const fetchTags = useCallback(async () => {
    setLoading(true);
    try {
      const resp = await fetch(entityTagsUrl(tenantName, entityType, entityId));
      if (!resp.ok) throw new Error("Failed to fetch tags");
      setTags((await resp.json()) as EntityTagResponse[]);
    } catch (err) {
      notification.error({
        message: "Failed to load tags",
        description: err instanceof Error ? err.message : "Unknown error",
      });
    } finally {
      setLoading(false);
    }
  }, [tenantName, entityType, entityId]);

  useEffect(() => { void fetchTags(); }, [fetchTags]);

  const handleAdd = useCallback(async (values: AddFormValues): Promise<void> => {
    setSubmitting(true);
    try {
      const body: EntityTagCreateRequest = {
        tag_key: values.tag_key,
        tag_value: values.tag_value,
        created_by: "ui",
      };
      const resp = await fetch(entityTagsUrl(tenantName, entityType, entityId), {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });
      if (!resp.ok) {
        const text = await resp.text();
        throw new Error(text);
      }
      form.resetFields();
      await fetchTags();
    } catch (err) {
      notification.error({
        message: "Failed to add tag",
        description: err instanceof Error ? err.message : "Unknown error",
      });
    } finally {
      setSubmitting(false);
    }
  }, [tenantName, entityType, entityId, form, fetchTags]);

  const handleRemove = useCallback(async (tagKey: string): Promise<void> => {
    try {
      const resp = await fetch(
        `${entityTagsUrl(tenantName, entityType, entityId)}/${tagKey}`,
        { method: "DELETE" },
      );
      if (!resp.ok) throw new Error("Failed to delete tag");
      await fetchTags();
    } catch (err) {
      notification.error({
        message: "Failed to remove tag",
        description: err instanceof Error ? err.message : "Unknown error",
      });
    }
  }, [tenantName, entityType, entityId, fetchTags]);

  return (
    <div>
      <Typography.Title level={5}>Tags</Typography.Title>
      {loading ? (
        <Spin size="small" />
      ) : (
        <Space wrap style={{ marginBottom: 12 }}>
          {tags.length === 0 && (
            <Typography.Text type="secondary">No tags</Typography.Text>
          )}
          {tags.map((tag) => (
            <Tag
              key={tag.tag_key}
              closable={!isReadOnly}
              onClose={() => { void handleRemove(tag.tag_key); }}
            >
              {tag.tag_key}: {tag.tag_value}
            </Tag>
          ))}
        </Space>
      )}
      {!isReadOnly && (
        <Form form={form} layout="inline" onFinish={(v) => void handleAdd(v)}>
          <Form.Item name="tag_key" rules={[{ required: true, message: "Key required" }]}>
            <Input placeholder="Key" maxLength={100} style={{ width: 140 }} />
          </Form.Item>
          <Form.Item name="tag_value" rules={[{ required: true, message: "Value required" }]}>
            <Input placeholder="Value" maxLength={500} style={{ width: 180 }} />
          </Form.Item>
          <Form.Item>
            <Button type="primary" htmlType="submit" loading={submitting}>Add</Button>
          </Form.Item>
        </Form>
      )}
    </div>
  );
}

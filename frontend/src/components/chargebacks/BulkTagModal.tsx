import { Button, Checkbox, Form, Input, Modal, notification, Typography } from "antd";
import { useState } from "react";
import { API_URL } from "../../config";
import type { BulkTagByFilterRequest, BulkTagRequest, BulkTagResponse } from "../../types/api";

interface BulkTagModalProps {
  tenantName: string;
  /** Provide selectedIds for per-row bulk, or filters for "tag all filtered" mode. */
  selectedIds: number[] | null;
  filters: Record<string, string> | null;
  disabled?: boolean;
  totalCount: number;
  onClose: () => void;
  onSuccess: () => void;
}

interface FormValues {
  tag_key: string;
  display_name: string;
  override_existing: boolean;
}

export function BulkTagModal({
  tenantName,
  selectedIds,
  filters,
  totalCount,
  onClose,
  onSuccess,
  disabled,
}: BulkTagModalProps): JSX.Element {
  const [form] = Form.useForm<FormValues>();
  const [submitting, setSubmitting] = useState(false);

  const isByFilter = selectedIds === null;
  const title = isByFilter
    ? `Tag All ${totalCount} Filtered Rows`
    : `Add Tags to ${totalCount} Selected Row${totalCount !== 1 ? "s" : ""}`;

  const handleSubmit = async (values: FormValues): Promise<void> => {
    setSubmitting(true);
    try {
      let url: string;
      let body: BulkTagRequest | BulkTagByFilterRequest;

      if (isByFilter && filters !== null) {
        url = `${API_URL}/tenants/${tenantName}/tags/bulk-by-filter`;
        body = {
          ...Object.fromEntries(
            Object.entries(filters).map(([k, v]) => [k, v]),
          ),
          tag_key: values.tag_key,
          display_name: values.display_name,
          created_by: "ui",
          override_existing: values.override_existing,
        } as BulkTagByFilterRequest;
      } else {
        url = `${API_URL}/tenants/${tenantName}/tags/bulk`;
        body = {
          dimension_ids: selectedIds ?? [],
          tag_key: values.tag_key,
          display_name: values.display_name,
          created_by: "ui",
          override_existing: values.override_existing,
        } as BulkTagRequest;
      }

      const resp = await fetch(url, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });

      if (!resp.ok) {
        const text = await resp.text();
        throw new Error(text);
      }

      const result = (await resp.json()) as BulkTagResponse;
      notification.success({
        message: "Tags applied",
        description: `Created: ${result.created_count}, Updated: ${result.updated_count}, Skipped: ${result.skipped_count}`,
      });
      onSuccess();
    } catch (err) {
      notification.error({
        message: "Failed to apply tags",
        description: err instanceof Error ? err.message : "Unknown error",
      });
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <Modal open title={title} onCancel={onClose} footer={null}>
      <Typography.Paragraph type="secondary">
        {isByFilter
          ? "This will apply the tag to all rows matching the current filters."
          : `This will apply the tag to ${totalCount} selected row${totalCount !== 1 ? "s" : ""}.`}
      </Typography.Paragraph>
      <Form
        form={form}
        layout="vertical"
        onFinish={(values) => void handleSubmit(values)}
        initialValues={{ override_existing: false }}
      >
        <Form.Item
          name="tag_key"
          label="Key"
          rules={[{ required: true, message: "Key is required" }]}
        >
          <Input placeholder="e.g. cost_center" maxLength={100} />
        </Form.Item>
        <Form.Item
          name="display_name"
          label="Display Name"
          rules={[{ required: true, message: "Display name is required" }]}
        >
          <Input placeholder="e.g. Engineering" maxLength={500} />
        </Form.Item>
        <Form.Item name="override_existing" valuePropName="checked">
          <Checkbox>Override existing tags with the same key</Checkbox>
        </Form.Item>
        <Form.Item>
          <Button type="primary" htmlType="submit" loading={submitting} disabled={disabled} block>
            Apply Tags
          </Button>
        </Form.Item>
      </Form>
    </Modal>
  );
}

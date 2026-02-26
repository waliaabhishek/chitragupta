import { Button, Form, Input, Space, Tag, Typography } from "antd";
import { useState } from "react";
import type { TagResponse } from "../../types/api";

interface TagEditorProps {
  tags: TagResponse[];
  onAdd: (key: string, displayName: string) => Promise<void>;
  onRemove: (tagId: number) => Promise<void>;
}

interface TagFormValues {
  key: string;
  displayName: string;
}

export function TagEditor({ tags, onAdd, onRemove }: TagEditorProps): JSX.Element {
  const [form] = Form.useForm<TagFormValues>();
  const [loading, setLoading] = useState(false);

  const handleSubmit = async (values: TagFormValues): Promise<void> => {
    setLoading(true);
    try {
      await onAdd(values.key, values.displayName);
      form.resetFields();
    } finally {
      setLoading(false);
    }
  };

  return (
    <div>
      <Typography.Title level={5}>Tags</Typography.Title>
      <Space wrap style={{ marginBottom: 12 }}>
        {tags.map((tag) => (
          <Tag
            key={tag.tag_id}
            closable
            onClose={() => {
              void onRemove(tag.tag_id);
            }}
          >
            {tag.display_name}
          </Tag>
        ))}
      </Space>
      <Form form={form} layout="inline" onFinish={handleSubmit}>
        <Form.Item name="key" rules={[{ required: true, message: "Key required" }]}>
          <Input placeholder="Key" maxLength={100} style={{ width: 140 }} />
        </Form.Item>
        <Form.Item
          name="displayName"
          rules={[{ required: true, message: "Display name required" }]}
        >
          <Input placeholder="Display Name" maxLength={500} style={{ width: 180 }} />
        </Form.Item>
        <Form.Item>
          <Button type="primary" htmlType="submit" loading={loading}>
            Add
          </Button>
        </Form.Item>
      </Form>
    </div>
  );
}

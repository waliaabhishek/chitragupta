import type React from "react";
import { ExportOutlined } from "@ant-design/icons";
import { Button, notification, Tooltip } from "antd";
import { useState } from "react";
import { exportTopicAttributions } from "../../api/topicAttributions";

interface TopicAttributionExportButtonProps {
  tenantName: string;
  filters: Record<string, string>;
  disabled?: boolean;
}

export function TopicAttributionExportButton({
  tenantName,
  filters,
  disabled,
}: TopicAttributionExportButtonProps): React.JSX.Element {
  const [loading, setLoading] = useState(false);

  const handleExport = async (): Promise<void> => {
    setLoading(true);
    try {
      const blob = await exportTopicAttributions(tenantName, {
        start_date: filters.start_date,
        end_date: filters.end_date,
        timezone: filters.timezone,
      });
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = "topic-attributions.csv";
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);
      URL.revokeObjectURL(url);
    } catch (err) {
      notification.error({
        message: "Export failed",
        description: err instanceof Error ? err.message : "Unknown error",
      });
    } finally {
      setLoading(false);
    }
  };

  const btn = (
    <Button
      icon={<ExportOutlined />}
      loading={loading}
      disabled={disabled}
      onClick={() => void handleExport()}
    >
      Export CSV
    </Button>
  );

  return disabled ? (
    <Tooltip title="Read-only while pipeline is running">{btn}</Tooltip>
  ) : (
    btn
  );
}

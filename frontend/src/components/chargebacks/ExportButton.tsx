import type React from "react";
import { DownloadOutlined } from "@ant-design/icons";
import { Button, notification, Tooltip } from "antd";
import { useState } from "react";
import { API_URL } from "../../config";

interface ExportButtonProps {
  tenantName: string;
  filters: Record<string, string>;
  disabled?: boolean;
}

export function ExportButton({ tenantName, filters, disabled }: ExportButtonProps): React.JSX.Element {
  const [loading, setLoading] = useState(false);

  const handleExport = async (): Promise<void> => {
    setLoading(true);
    try {
      // Build ExportRequest from current filters
      const body: Record<string, unknown> = {};
      if (filters.start_date) body.start_date = filters.start_date;
      if (filters.end_date) body.end_date = filters.end_date;

      const filterKeys = ["identity_id", "product_type", "resource_id", "cost_type"] as const;
      const activeFilters: Record<string, string> = {};
      for (const key of filterKeys) {
        if (filters[key]) {
          activeFilters[key] = filters[key];
        }
      }
      if (Object.keys(activeFilters).length > 0) {
        body.filters = activeFilters;
      }

      const resp = await fetch(`${API_URL}/tenants/${tenantName}/export`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });

      if (!resp.ok) {
        throw new Error(`Export failed: ${resp.status}`);
      }

      const blob = await resp.blob();
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = "chargebacks.csv";
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
      icon={<DownloadOutlined />}
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

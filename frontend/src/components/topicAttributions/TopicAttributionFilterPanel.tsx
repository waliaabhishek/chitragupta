import type React from "react";
import { useMemo } from "react";
import { DatePicker, Form, Input, Select, Button, Space, Tooltip } from "antd";
import { FilterOutlined, ReloadOutlined } from "@ant-design/icons";
import { useQuery } from "@tanstack/react-query";
import dayjs from "dayjs";
import { fetchTopicAttributionDates } from "../../api/topicAttributions";
import type { TopicAttributionFilters } from "../../types/filters";

const ATTRIBUTION_METHOD_OPTIONS = [
  { label: "Bytes Ratio", value: "bytes_ratio" },
  { label: "Retained Bytes Ratio", value: "retained_bytes_ratio" },
  { label: "Even Split", value: "even_split" },
];

// Common timezones — mirrors pattern in chargebacks FilterPanel
const TIMEZONE_OPTIONS = [
  { label: "UTC", value: "UTC" },
  { label: "America/New_York", value: "America/New_York" },
  { label: "America/Chicago", value: "America/Chicago" },
  { label: "America/Denver", value: "America/Denver" },
  { label: "America/Los_Angeles", value: "America/Los_Angeles" },
  { label: "Europe/London", value: "Europe/London" },
  { label: "Europe/Paris", value: "Europe/Paris" },
  { label: "Asia/Tokyo", value: "Asia/Tokyo" },
  { label: "Asia/Shanghai", value: "Asia/Shanghai" },
  { label: "Australia/Sydney", value: "Australia/Sydney" },
];

interface TopicAttributionFilterPanelProps {
  tenantName: string;
  filters: TopicAttributionFilters;
  onChange: (key: keyof TopicAttributionFilters, value: string | null) => void;
  onBatchChange?: (updates: Partial<TopicAttributionFilters>) => void;
  onReset: () => void;
  onRefresh?: () => void;
  activeTab?: "table" | "analytics";
}

export function TopicAttributionFilterPanel({
  tenantName,
  filters,
  onChange,
  onBatchChange,
  onReset,
  onRefresh,
  activeTab,
}: TopicAttributionFilterPanelProps): React.JSX.Element {
  const { data: availableDates } = useQuery({
    queryKey: ["topic-attribution-dates", tenantName],
    queryFn: ({ signal }) => fetchTopicAttributionDates(tenantName, signal),
    enabled: !!tenantName,
  });

  const availableDatesSet = useMemo(
    () => new Set(availableDates?.dates ?? []),
    [availableDates?.dates],
  );

  const disabledDate = (current: dayjs.Dayjs): boolean => {
    if (!availableDates?.dates.length) return false;
    const dateStr = current.format("YYYY-MM-DD");
    return !availableDatesSet.has(dateStr);
  };

  const startValue = filters.start_date ? dayjs(filters.start_date) : null;
  const endValue = filters.end_date ? dayjs(filters.end_date) : null;

  return (
    <Form layout="inline" style={{ marginBottom: 8, flexWrap: "wrap", gap: 8 }}>
      <Form.Item label="Date Range">
        <DatePicker.RangePicker
          value={[startValue, endValue]}
          disabledDate={disabledDate}
          onChange={(dates) => {
            if (onBatchChange) {
              onBatchChange({
                start_date: dates?.[0]?.format("YYYY-MM-DD") ?? null,
                end_date: dates?.[1]?.format("YYYY-MM-DD") ?? null,
              });
            }
          }}
        />
      </Form.Item>
      <Form.Item label="Cluster">
        <Input
          placeholder="Any cluster"
          value={filters.cluster_resource_id ?? ""}
          onChange={(e) =>
            onChange("cluster_resource_id", e.target.value || null)
          }
          style={{ width: 180 }}
        />
      </Form.Item>
      <Form.Item label="Topic Name">
        <Input
          placeholder="Any topic"
          value={filters.topic_name ?? ""}
          onChange={(e) => onChange("topic_name", e.target.value || null)}
          style={{ width: 180 }}
        />
      </Form.Item>
      <Form.Item label="Product Type">
        <Select
          allowClear
          placeholder="Any"
          value={filters.product_type ?? undefined}
          onChange={(v) => onChange("product_type", v ?? null)}
          style={{ width: 180 }}
          options={[
            { label: "Storage", value: "KAFKA_STORAGE" },
            { label: "Network Read", value: "KAFKA_NETWORK_READ" },
            { label: "Network Write", value: "KAFKA_NETWORK_WRITE" },
          ]}
        />
      </Form.Item>
      <Form.Item label="Attribution Method">
        <Tooltip
          title={
            activeTab === "analytics"
              ? "Applies to table view only"
              : undefined
          }
        >
          <Select
            allowClear
            placeholder="Any"
            value={filters.attribution_method ?? undefined}
            onChange={(v) => onChange("attribution_method", v ?? null)}
            style={{ width: 200 }}
            options={ATTRIBUTION_METHOD_OPTIONS}
          />
        </Tooltip>
      </Form.Item>
      <Form.Item label="Timezone">
        <Select
          value={filters.timezone ?? undefined}
          onChange={(v) => onChange("timezone", v ?? null)}
          style={{ width: 200 }}
          options={TIMEZONE_OPTIONS}
        />
      </Form.Item>
      <Form.Item label="Tag Key">
        <Input
          placeholder="e.g. cost_center"
          value={filters.tag_key ?? ""}
          onChange={(e) => onChange("tag_key", e.target.value || null)}
          style={{ width: 160 }}
        />
      </Form.Item>
      <Form.Item label="Tag Value">
        <Input
          placeholder="e.g. engineering"
          value={filters.tag_value ?? ""}
          onChange={(e) => onChange("tag_value", e.target.value || null)}
          style={{ width: 160 }}
        />
      </Form.Item>
      <Form.Item>
        <Space>
          <Button icon={<FilterOutlined />} onClick={onReset}>
            Reset
          </Button>
          {onRefresh && (
            <Tooltip title="Refresh">
              <Button icon={<ReloadOutlined />} onClick={onRefresh} />
            </Tooltip>
          )}
        </Space>
      </Form.Item>
    </Form>
  );
}

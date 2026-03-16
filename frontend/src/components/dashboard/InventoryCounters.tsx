import type React from "react";
import { Card, Col, Collapse, Empty, Row, Skeleton, Statistic, Typography } from "antd";
import { useMemo } from "react";
import type { InventorySummaryResponse } from "../../types/api";

const SKELETON_INDICES = [0, 1, 2];
const EMPTY_COUNTS: Record<string, number> = {};

interface InventoryCountersProps {
  data: InventorySummaryResponse | null;
  isLoading: boolean;
  error: string | null;
}

/** Convert snake_case API key to Title Case display name. */
function toTitleCase(key: string): string {
  return key.replace(/_/g, " ").replace(/\b\w/g, (c) => c.toUpperCase());
}

interface CounterRowProps {
  label: string;
  counts: Record<string, number>;
  isLoading: boolean;
  error: string | null;
}

function CounterRow({ label, counts, isLoading, error }: CounterRowProps): React.JSX.Element {
  const entries = Object.entries(counts);

  if (!isLoading && !error && entries.length === 0) {
    return (
      <div style={{ marginBottom: 8 }}>
        <Typography.Text type="secondary" style={{ fontSize: 12 }}>
          {label}
        </Typography.Text>
        <Empty image={Empty.PRESENTED_IMAGE_SIMPLE} description="No inventory data" style={{ margin: "8px 0" }} />
      </div>
    );
  }

  return (
    <div style={{ marginBottom: 16 }}>
      <Typography.Text type="secondary" style={{ fontSize: 12, display: "block", marginBottom: 8 }}>
        {label}
      </Typography.Text>
      <Row gutter={[12, 12]}>
        {isLoading
          ? SKELETON_INDICES.map((i) => (
              <Col xs={12} sm={8} md={6} key={i}>
                <Card size="small">
                  <Skeleton active paragraph={false} />
                </Card>
              </Col>
            ))
          : entries.length === 0
            ? (
              <Col span={24}>
                <Typography.Text type="secondary">—</Typography.Text>
              </Col>
            )
            : entries.map(([key, value]) => (
              <Col xs={12} sm={8} md={6} key={key}>
                <Card size="small">
                  <Statistic title={toTitleCase(key)} value={error ? "—" : value} />
                </Card>
              </Col>
            ))}
      </Row>
    </div>
  );
}

export function InventoryCounters({ data, isLoading, error }: InventoryCountersProps): React.JSX.Element {
  const resourceCounts = data?.resource_counts ?? EMPTY_COUNTS;
  const identityCounts = data?.identity_counts ?? EMPTY_COUNTS;

  const collapseItems = useMemo(
    () => [
      {
        key: "inventory",
        label: "Inventory",
        children: (
          <>
            <CounterRow
              label="Resources"
              counts={resourceCounts}
              isLoading={isLoading}
              error={error}
            />
            <CounterRow
              label="Identities"
              counts={identityCounts}
              isLoading={isLoading}
              error={error}
            />
          </>
        ),
      },
    ],
    [resourceCounts, identityCounts, isLoading, error],
  );

  return <Collapse items={collapseItems} />;
}

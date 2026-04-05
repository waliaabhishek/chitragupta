import type React from "react";
import { Button, Card, Result, Spin, Typography } from "antd";
import type { ReactNode } from "react";

interface ChartCardProps {
  title: string;
  subtitle?: string;
  children: ReactNode;
  loading?: boolean;
  error?: string | null;
  onRetry?: () => void;
  extra?: ReactNode;
}

export function ChartCard({
  title,
  subtitle,
  children,
  loading,
  error,
  onRetry,
  extra,
}: ChartCardProps): React.JSX.Element {
  const cardTitle = (
    <div>
      <div>{title}</div>
      {subtitle && (
        <Typography.Text
          type="secondary"
          style={{ fontSize: 12, fontWeight: "normal" }}
        >
          {subtitle}
        </Typography.Text>
      )}
    </div>
  );
  return (
    <Card title={cardTitle} extra={extra} style={{ height: "100%" }}>
      {loading ? (
        <div
          style={{
            display: "flex",
            justifyContent: "center",
            alignItems: "center",
            minHeight: 200,
          }}
        >
          <Spin />
        </div>
      ) : error ? (
        <Result
          status="error"
          title="Failed to load chart"
          subTitle={error}
          extra={
            onRetry ? (
              <Button type="primary" onClick={onRetry}>
                Retry
              </Button>
            ) : undefined
          }
        />
      ) : (
        children
      )}
    </Card>
  );
}

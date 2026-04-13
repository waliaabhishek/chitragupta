import type React from "react";
import { Button, Space } from "antd";
import { HomeOutlined } from "@ant-design/icons";

interface Breadcrumb {
  id: string;
  label: string;
  type: string;
}

interface BreadcrumbTrailProps {
  breadcrumbs: Breadcrumb[];
  onNavigate: (index: number) => void;
  onGoBack: () => void;
  onGoToRoot: () => void;
}

export function BreadcrumbTrail({
  breadcrumbs,
  onNavigate,
  onGoBack,
  onGoToRoot,
}: BreadcrumbTrailProps): React.JSX.Element {
  return (
    <div
      style={{
        padding: "8px 12px",
        display: "flex",
        alignItems: "center",
        gap: 4,
        background: "rgba(0,0,0,0.03)",
        borderBottom: "1px solid rgba(0,0,0,0.06)",
        flexWrap: "wrap",
      }}
    >
      <Space size={4}>
        <Button
          type="text"
          size="small"
          icon={<HomeOutlined />}
          onClick={onGoToRoot}
          style={{ padding: "0 4px" }}
        />
        {breadcrumbs.map((crumb, i) => (
          <span key={crumb.id} style={{ display: "flex", alignItems: "center", gap: 4 }}>
            <span style={{ opacity: 0.4 }}>/</span>
            <Button
              type="text"
              size="small"
              onClick={() => onNavigate(i)}
              style={{
                padding: "0 4px",
                fontWeight: i === breadcrumbs.length - 1 ? 600 : 400,
                opacity: i === breadcrumbs.length - 1 ? 1 : 0.7,
              }}
            >
              {crumb.label}
            </Button>
          </span>
        ))}
      </Space>
      {breadcrumbs.length > 0 && (
        <Button
          type="text"
          size="small"
          onClick={onGoBack}
          style={{ marginLeft: "auto", opacity: 0.6 }}
        >
          ← Back
        </Button>
      )}
    </div>
  );
}

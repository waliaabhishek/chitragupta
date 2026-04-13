import type React from "react";
import { Button, Space } from "antd";

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
      data-testid="breadcrumb-trail"
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
          onClick={onGoToRoot}
          style={{ padding: "0 4px" }}
        >
          Tenant
        </Button>
        {breadcrumbs.map((crumb, i) => (
          <span key={crumb.id} style={{ display: "flex", alignItems: "center", gap: 4 }}>
            <span style={{ opacity: 0.4 }}>/</span>
            {i === breadcrumbs.length - 1 ? (
              <span
                style={{
                  padding: "0 4px",
                  fontWeight: 600,
                  fontSize: 14,
                }}
              >
                {crumb.label}
              </span>
            ) : (
              <Button
                type="text"
                size="small"
                onClick={() => onNavigate(i)}
                style={{
                  padding: "0 4px",
                  fontWeight: 400,
                  opacity: 0.7,
                }}
              >
                {crumb.label}
              </Button>
            )}
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

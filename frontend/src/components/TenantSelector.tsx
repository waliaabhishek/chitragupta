import type React from "react";
import { Button, Select, Space, Spin, Typography } from "antd";
import { useTenant } from "../providers/TenantContext";

const { Text } = Typography;

export function TenantSelector(): React.JSX.Element {
  const {
    tenants,
    currentTenant,
    setCurrentTenant,
    isLoading,
    error,
    refetch,
  } = useTenant();

  if (error) {
    return (
      <Space>
        <Text type="danger">Tenant load failed</Text>
        <Button size="small" onClick={refetch}>
          Retry
        </Button>
      </Space>
    );
  }

  if (isLoading) {
    return <Spin size="small" />;
  }

  return (
    <Select
      style={{ minWidth: 200 }}
      placeholder="Select tenant"
      value={currentTenant?.tenant_name ?? undefined}
      onChange={(value: string) => {
        const found = tenants.find((t) => t.tenant_name === value);
        setCurrentTenant(found ?? null);
      }}
      options={tenants.map((t) => ({
        label: `${t.tenant_name} (${t.ecosystem})`,
        value: t.tenant_name,
      }))}
    />
  );
}

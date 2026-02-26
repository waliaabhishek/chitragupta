import { Typography } from "antd";
import { useTenant } from "../providers/TenantContext";

const { Title, Text } = Typography;

export function DashboardPage(): JSX.Element {
  const { currentTenant } = useTenant();

  return (
    <div>
      <Title level={3}>Dashboard</Title>
      {currentTenant ? (
        <Text>
          Tenant: <strong>{currentTenant.tenant_name}</strong> (
          {currentTenant.ecosystem})
        </Text>
      ) : (
        <Text type="secondary">Select a tenant to get started.</Text>
      )}
    </div>
  );
}

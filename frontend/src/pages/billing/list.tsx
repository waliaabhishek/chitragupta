import type React from "react";
import { Typography } from "antd";
import { useTenant } from "../../providers/TenantContext";

const { Title, Text } = Typography;

export function BillingListPage(): React.JSX.Element {
  const { currentTenant } = useTenant();

  if (!currentTenant) {
    return (
      <div>
        <Title level={3}>Billing</Title>
        <Text type="secondary">Select a tenant to begin.</Text>
      </div>
    );
  }

  return (
    <div>
      <Title level={3}>Billing</Title>
      <Text type="secondary">Coming soon.</Text>
    </div>
  );
}

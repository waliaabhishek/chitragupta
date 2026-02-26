import { Typography } from "antd";
import { useTenant } from "../../providers/TenantContext";

const { Title, Text } = Typography;

export function ResourceListPage(): JSX.Element {
  const { currentTenant } = useTenant();

  if (!currentTenant) {
    return (
      <div>
        <Title level={3}>Resources</Title>
        <Text type="secondary">Select a tenant to begin.</Text>
      </div>
    );
  }

  return (
    <div>
      <Title level={3}>Resources</Title>
      <Text type="secondary">Coming soon.</Text>
    </div>
  );
}

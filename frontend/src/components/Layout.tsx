import {
  BarChartOutlined,
  CloudServerOutlined,
  DashboardOutlined,
  DollarOutlined,
  TeamOutlined,
  ThunderboltOutlined,
} from "@ant-design/icons";
import { Layout as AntLayout, Menu, theme, Typography } from "antd";
import { useState } from "react";
import { useNavigate, useLocation } from "react-router-dom";
import { useTenant } from "../providers/TenantContext";
import { TenantSelector } from "./TenantSelector";

const { Header, Sider, Content } = AntLayout;
const { Title } = Typography;

interface AppLayoutProps {
  children: React.ReactNode;
}

export function AppLayout({ children }: AppLayoutProps): JSX.Element {
  const [collapsed, setCollapsed] = useState(false);
  const navigate = useNavigate();
  const location = useLocation();
  const { currentTenant } = useTenant();
  const {
    token: { colorBgContainer },
  } = theme.useToken();

  // Tenant-scoped pages are disabled/greyed when no tenant is selected.
  const tenantRequired = !currentTenant;
  const menuItems = [
    {
      key: "/dashboard",
      icon: <DashboardOutlined />,
      label: "Dashboard",
      disabled: tenantRequired,
    },
    {
      key: "/chargebacks",
      icon: <DollarOutlined />,
      label: "Chargebacks",
      disabled: tenantRequired,
    },
    {
      key: "/billing",
      icon: <BarChartOutlined />,
      label: "Billing",
      disabled: tenantRequired,
    },
    {
      key: "/resources",
      icon: <CloudServerOutlined />,
      label: "Resources",
      disabled: tenantRequired,
    },
    {
      key: "/identities",
      icon: <TeamOutlined />,
      label: "Identities",
      disabled: tenantRequired,
    },
    {
      key: "/pipeline",
      icon: <ThunderboltOutlined />,
      label: "Pipeline",
      disabled: tenantRequired,
    },
  ];

  return (
    <AntLayout style={{ minHeight: "100vh" }}>
      <Sider
        collapsible
        collapsed={collapsed}
        onCollapse={setCollapsed}
        style={{ background: colorBgContainer }}
      >
        <div
          style={{
            height: 32,
            margin: 16,
            display: "flex",
            alignItems: "center",
            justifyContent: "center",
          }}
        >
          {!collapsed && (
            <Title level={5} style={{ margin: 0, color: "#1677ff" }}>
              Chargeback
            </Title>
          )}
        </div>
        <Menu
          mode="inline"
          selectedKeys={[location.pathname]}
          items={menuItems}
          onClick={({ key }) => navigate(key)}
        />
      </Sider>
      <AntLayout>
        <Header
          style={{
            padding: "0 24px",
            background: colorBgContainer,
            display: "flex",
            alignItems: "center",
            justifyContent: "flex-end",
            gap: 16,
          }}
        >
          <TenantSelector />
        </Header>
        <Content
          style={{
            margin: "24px 16px",
            padding: 24,
            background: colorBgContainer,
            borderRadius: 8,
          }}
        >
          {children}
        </Content>
      </AntLayout>
    </AntLayout>
  );
}

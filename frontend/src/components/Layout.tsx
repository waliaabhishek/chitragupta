import type React from "react";
import {
  BarChartOutlined,
  BulbFilled,
  BulbOutlined,
  CloudServerOutlined,
  DashboardOutlined,
  DollarOutlined,
  LineChartOutlined,
  TagsOutlined,
  TeamOutlined,
  ThunderboltOutlined,
} from "@ant-design/icons";
import {
  Button,
  Layout as AntLayout,
  Menu,
  Switch,
  theme,
  Typography,
} from "antd";
import { useState } from "react";
import { useNavigate, useLocation } from "react-router";
import { useTenant } from "../providers/TenantContext";
import { useResourceLinks } from "../providers/ResourceLinkContext";
import { PipelineStatusBanner } from "./PipelineStatusBanner";
import { TenantSelector } from "./TenantSelector";

const { Header, Sider, Content } = AntLayout;
const { Title } = Typography;

interface AppLayoutProps {
  children: React.ReactNode;
  isDark: boolean;
  onToggleTheme: () => void;
}

export function AppLayout({
  children,
  isDark,
  onToggleTheme,
}: AppLayoutProps): React.JSX.Element {
  const [collapsed, setCollapsed] = useState(false);
  const navigate = useNavigate();
  const location = useLocation();
  const { currentTenant } = useTenant();
  const { enabled: deepLinksEnabled, setEnabled: setDeepLinksEnabled } =
    useResourceLinks();
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
      key: "/topic-attributions",
      icon: <LineChartOutlined />,
      label: "Topic Attribution",
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
      key: "/tags",
      icon: <TagsOutlined />,
      label: "Tags",
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
          <Button
            type="text"
            icon={isDark ? <BulbFilled /> : <BulbOutlined />}
            onClick={onToggleTheme}
            title={isDark ? "Switch to light mode" : "Switch to dark mode"}
          />
          <Switch
            checked={deepLinksEnabled}
            onChange={setDeepLinksEnabled}
            checkedChildren="Links"
            unCheckedChildren="Links"
            size="small"
            title={
              deepLinksEnabled ? "Disable deep links" : "Enable deep links"
            }
          />
          <TenantSelector />
        </Header>
        <PipelineStatusBanner />
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

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
  Badge,
  Button,
  Layout as AntLayout,
  Menu,
  Switch,
  theme,
  Tooltip,
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
  const taStatus = currentTenant?.topic_attribution_status ?? "disabled";
  const topicAttributionEnabled = taStatus === "enabled";
  const topicAttributionConfigError = taStatus === "config_error";
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
      icon: (
        <LineChartOutlined
          style={!tenantRequired && !topicAttributionEnabled ? { color: "#bfbfbf" } : undefined}
        />
      ),
      label: !tenantRequired && !topicAttributionEnabled ? (
        <span>
          Topic Attribution{" "}
          {topicAttributionConfigError ? (
            <Badge
              count="Config error"
              style={{ backgroundColor: "#fff1f0", color: "#cf1322", fontSize: 10, boxShadow: "none" }}
            />
          ) : (
            <Badge
              count="Not configured"
              style={{ backgroundColor: "#f0f0f0", color: "#8c8c8c", fontSize: 10, boxShadow: "none" }}
            />
          )}
        </span>
      ) : (
        "Topic Attribution"
      ),
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
          <Tooltip
            title="Toggles clickable deep links from resource and identity IDs to their Confluent Cloud console pages. Connectors and identity pools are not supported — Confluent does not expose stable URLs for these. Deleted resources and identities will not have clickable links as they are excluded from the lookup index."
          >
            <Switch
              checked={deepLinksEnabled}
              onChange={setDeepLinksEnabled}
              checkedChildren="Links"
              unCheckedChildren="Links"
              size="small"
            />
          </Tooltip>
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

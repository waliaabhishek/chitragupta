import type React from "react";
import { Refine } from "@refinedev/core";
import { useNotificationProvider } from "@refinedev/antd";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { ReactQueryDevtools } from "@tanstack/react-query-devtools";
import { BrowserRouter, Route, Routes } from "react-router";
import { App as AntApp, ConfigProvider } from "antd";
import "@refinedev/antd/dist/reset.css";

import { dataProvider } from "./providers/dataProvider";
import { TenantProvider } from "./providers/TenantContext";
import { ResourceLinkProvider } from "./providers/ResourceLinkContext";
import { AppLayout } from "./components/Layout";
import { useTheme } from "./hooks/useTheme";
import { DashboardPage } from "./pages/index";
import { CostDashboardPage } from "./pages/dashboard/index";
import { ChargebackListPage } from "./pages/chargebacks/list";
import { BillingListPage } from "./pages/billing/list";
import { ResourceListPage } from "./pages/resources/list";
import { IdentityListPage } from "./pages/identities/list";
import { PipelineStatusPage } from "./pages/pipeline/status";
import { TagManagementPage } from "./pages/tags/list";
import { TopicAttributionPage } from "./pages/topicAttributions/list";

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      staleTime: 5 * 60 * 1000,
      gcTime: 10 * 60 * 1000,
      refetchOnWindowFocus: false,
      retry: 1,
    },
  },
});

export function App(): React.JSX.Element {
  const { algorithm, isDark, toggleTheme } = useTheme();

  return (
    <BrowserRouter>
      <ConfigProvider theme={{ algorithm }}>
        <AntApp>
          <QueryClientProvider client={queryClient}>
            <TenantProvider>
              <ResourceLinkProvider>
                <Refine
                  dataProvider={dataProvider}
                  notificationProvider={useNotificationProvider}
                  resources={[
                    { name: "topic-attributions", list: "/topic-attributions" },
                    { name: "chargebacks", list: "/chargebacks" },
                    { name: "billing", list: "/billing" },
                    { name: "resources", list: "/resources" },
                    { name: "identities", list: "/identities" },
                    { name: "pipeline", list: "/pipeline" },
                    { name: "tags", list: "/tags" },
                  ]}
                  options={{ syncWithLocation: true }}
                >
                  <AppLayout isDark={isDark} onToggleTheme={toggleTheme}>
                    <Routes>
                      <Route path="/" element={<DashboardPage />} />
                      <Route
                        path="/dashboard"
                        element={<CostDashboardPage />}
                      />
                      <Route
                        path="/topic-attributions"
                        element={<TopicAttributionPage />}
                      />
                      <Route
                        path="/chargebacks"
                        element={<ChargebackListPage />}
                      />
                      <Route path="/billing" element={<BillingListPage />} />
                      <Route path="/resources" element={<ResourceListPage />} />
                      <Route
                        path="/identities"
                        element={<IdentityListPage />}
                      />
                      <Route
                        path="/pipeline"
                        element={<PipelineStatusPage />}
                      />
                      <Route path="/tags" element={<TagManagementPage />} />
                    </Routes>
                  </AppLayout>
                </Refine>
              </ResourceLinkProvider>
            </TenantProvider>
            {import.meta.env.DEV && (
              <ReactQueryDevtools initialIsOpen={false} />
            )}
          </QueryClientProvider>
        </AntApp>
      </ConfigProvider>
    </BrowserRouter>
  );
}

import { Refine } from "@refinedev/core";
import { useNotificationProvider } from "@refinedev/antd";
import { BrowserRouter, Route, Routes } from "react-router-dom";
import { App as AntApp, ConfigProvider } from "antd";
import "@refinedev/antd/dist/reset.css";

import { dataProvider } from "./providers/dataProvider";
import { TenantProvider } from "./providers/TenantContext";
import { AppLayout } from "./components/Layout";
import { DashboardPage } from "./pages/index";
import { ChargebackListPage } from "./pages/chargebacks/list";
import { BillingListPage } from "./pages/billing/list";
import { ResourceListPage } from "./pages/resources/list";
import { IdentityListPage } from "./pages/identities/list";
import { PipelineStatusPage } from "./pages/pipeline/status";

export function App(): JSX.Element {
  return (
    <BrowserRouter>
      <ConfigProvider>
        <AntApp>
          <TenantProvider>
            <Refine
              dataProvider={dataProvider}
              notificationProvider={useNotificationProvider}
              resources={[
                { name: "chargebacks", list: "/chargebacks" },
                { name: "billing", list: "/billing" },
                { name: "resources", list: "/resources" },
                { name: "identities", list: "/identities" },
                { name: "pipeline", list: "/pipeline" },
              ]}
              options={{ syncWithLocation: true }}
            >
              <AppLayout>
                <Routes>
                  <Route path="/" element={<DashboardPage />} />
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
                </Routes>
              </AppLayout>
            </Refine>
          </TenantProvider>
        </AntApp>
      </ConfigProvider>
    </BrowserRouter>
  );
}

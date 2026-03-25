import React from "react";
import ReactDOM from "react-dom/client";
import { ModuleRegistry, AllCommunityModule } from "ag-grid-community";
import { App } from "./App";

ModuleRegistry.registerModules([AllCommunityModule]);

ReactDOM.createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>,
);

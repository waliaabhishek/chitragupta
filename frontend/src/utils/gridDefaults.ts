import type { ColDef } from "ag-grid-community";
import { themeAlpine } from "ag-grid-community";

export const defaultColDef: ColDef = {
  sortable: true,
  resizable: true,
};

const systemFont = [
  "-apple-system",
  "BlinkMacSystemFont",
  "Segoe UI",
  "Roboto",
  "Helvetica Neue",
  "Arial",
  "sans-serif",
];

export const gridTheme = themeAlpine
  .withParams({
    fontFamily: systemFont,
    fontSize: 14,
    headerHeight: 48,
    rowHeight: 48,
    cellHorizontalPadding: 16,
    headerFontWeight: 500 as const,
    wrapperBorder: false,
    columnBorder: false,
    backgroundColor: "#ffffff",
    headerBackgroundColor: "#fafafa",
    oddRowBackgroundColor: "#fafafa",
    rowHoverColor: "#f0f0f0",
    selectedRowBackgroundColor: "#e6f4ff",
    foregroundColor: "rgba(0, 0, 0, 0.88)",
    headerTextColor: "rgba(0, 0, 0, 0.88)",
    borderColor: "#f0f0f0",
    rowBorder: { color: "#f0f0f0", width: 1, style: "solid" as const },
  })
  .withParams(
    {
      backgroundColor: "#141414",
      headerBackgroundColor: "#1d1d1d",
      oddRowBackgroundColor: "#1a1a1a",
      rowHoverColor: "#262626",
      selectedRowBackgroundColor: "#111d2c",
      foregroundColor: "rgba(255, 255, 255, 0.85)",
      headerTextColor: "rgba(255, 255, 255, 0.85)",
      borderColor: "#303030",
      rowBorder: { color: "#303030", width: 1, style: "solid" as const },
    },
    "dark",
  );

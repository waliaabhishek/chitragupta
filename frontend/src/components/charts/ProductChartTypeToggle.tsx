import { Segmented } from "antd";

interface ProductChartTypeToggleProps {
  value: "pie" | "treemap";
  onChange: (value: "pie" | "treemap") => void;
}

const CHART_TYPE_OPTIONS: Array<{ label: string; value: "pie" | "treemap" }> = [
  { label: "Pie", value: "pie" },
  { label: "Treemap", value: "treemap" },
];

export function ProductChartTypeToggle({ value, onChange }: ProductChartTypeToggleProps): JSX.Element {
  return (
    <Segmented
      size="small"
      options={CHART_TYPE_OPTIONS}
      value={value}
      onChange={(v) => onChange(v as "pie" | "treemap")}
    />
  );
}

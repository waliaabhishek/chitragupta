import type React from "react";
import { useMemo } from "react";
import ReactECharts from "echarts-for-react";
import type { TooltipComponentFormatterCallbackParams, EChartsOption } from "echarts";
import type { TopicAttributionAggregationBucket } from "../../../types/api";
import { aggregateByDimension, formatCurrency } from "../../../utils/aggregation";

interface AttributionMethodDonutProps {
  data: TopicAttributionAggregationBucket[];
  height?: number;
}

const TIER_CONFIG: Record<string, { label: string; color: string }> = {
  bytes_ratio: { label: "High Confidence", color: "#52c41a" },
  retained_bytes_ratio: { label: "Medium Confidence", color: "#faad14" },
  even_split: { label: "Low Confidence", color: "#f5222d" },
};

const DEFAULT_TIER = TIER_CONFIG.even_split;

const headlineStyle: React.CSSProperties = {
  fontSize: 32,
  fontWeight: 700,
  lineHeight: 1.1,
  color: "var(--ant-color-text, inherit)",
};

const subtitleStyle: React.CSSProperties = {
  fontSize: 12,
  color: "var(--ant-color-text-secondary, #8c8c8c)",
  marginTop: 2,
};

const headerWrapStyle: React.CSSProperties = {
  textAlign: "center",
  marginBottom: 4,
};

export function AttributionMethodDonut({
  data,
  height = 300,
}: AttributionMethodDonutProps): React.JSX.Element {
  const slices = useMemo(
    () => aggregateByDimension(data, "attribution_method"),
    [data],
  );

  const { confidencePct, totalCost } = useMemo(() => {
    const total = slices.reduce((sum, s) => sum + s.amount, 0);
    const evenSplit = slices.find((s) => s.key === "even_split")?.amount ?? 0;
    const pct = total === 0 ? 0 : ((total - evenSplit) / total) * 100;
    return { confidencePct: pct, totalCost: total };
  }, [slices]);

  const chartData = useMemo(
    () =>
      slices.map((s) => {
        const tier = TIER_CONFIG[s.key] ?? DEFAULT_TIER;
        return {
          name: tier.label,
          value: s.amount,
          itemStyle: { color: tier.color },
          methodKey: s.key,
        };
      }),
    [slices],
  );

  const option: EChartsOption = useMemo(
    () => ({
      tooltip: {
        trigger: "item",
        formatter: (raw: TooltipComponentFormatterCallbackParams) => {
          const params = Array.isArray(raw) ? raw[0] : raw;
          const itemData = params.data as { methodKey: string };
          const pct = (params.percent as number).toFixed(1);
          return `${params.name} (${itemData.methodKey}): ${formatCurrency(params.value as number)} (${pct}%)`;
        },
      },
      legend: { orient: "horizontal", bottom: 0 },
      series: [
        {
          type: "pie",
          radius: ["35%", "65%"],
          data: chartData,
          label: { show: false },
          emphasis: { label: { show: true, formatter: "{b}: {d}%" } },
        },
      ],
    }),
    [chartData],
  );

  return (
    <div>
      <div style={headerWrapStyle}>
        <div data-testid="confidence-headline" style={headlineStyle}>
          {totalCost === 0 ? "—" : `${Math.round(confidencePct)}%`}
        </div>
        <div style={subtitleStyle}>of cost attributed with metrics data</div>
      </div>
      <ReactECharts option={option} notMerge style={{ height }} />
    </div>
  );
}

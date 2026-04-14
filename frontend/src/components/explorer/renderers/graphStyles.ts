import type cytoscape from "cytoscape";

function baseStylesheet(): cytoscape.StylesheetStyle[] {
  return [
    {
      selector: "node",
      style: {
        label: "data(display_name)",
        "text-valign": "bottom",
        "text-halign": "center",
        "font-size": 11,
        shape: "data(shape)" as unknown as cytoscape.Css.NodeShape,
        width: "data(size)" as unknown as number,
        height: "data(size)" as unknown as number,
        "border-width": 2,
        "transition-property":
          "width, height, opacity, border-width, border-color" as unknown as string,
        "transition-duration": 300,
      },
    },
    {
      selector: 'node[status = "phantom"]',
      style: {
        opacity: 0.15,
        "border-style": "dashed" as const,
      },
    },
    {
      selector: "node.faded",
      style: {
        opacity: 0.2,
        "text-opacity": 0.3,
      },
    },
    {
      selector: "edge",
      style: {
        "curve-style": "bezier" as const,
        "target-arrow-shape": "triangle" as const,
        width: 1.5,
      },
    },
    {
      selector: 'edge[relationship_type = "parent"]',
      style: {
        "line-style": "solid" as const,
        "line-color": "#8c8c8c",
        width: 1.5,
      },
    },
    {
      selector: 'edge[relationship_type = "charge"]',
      style: {
        "line-style": "dashed" as const,
        "line-color": "#1890ff",
        width: 2,
      },
    },
    {
      selector: 'edge[relationship_type = "attribution"]',
      style: {
        "line-style": "dotted" as const,
        "line-color": "#52c41a",
        width: 1.5,
      },
    },
    {
      selector: "edge.faded",
      style: {
        opacity: 0.15,
      },
    },
    {
      selector: "node.diff-increase",
      style: {
        "border-width": 4,
        "border-color": "#ff4d4f",
      },
    },
    {
      selector: "node.diff-decrease",
      style: {
        "border-width": 4,
        "border-color": "#52c41a",
      },
    },
    {
      selector: "node.diff-new",
      style: {
        "border-width": 3,
        "border-color": "#1890ff",
      },
    },
    {
      selector: "node.diff-deleted",
      style: {
        opacity: 0.35,
        "border-style": "dashed" as const,
        "border-color": "#8c8c8c",
      },
    },
  ];
}

export function getStylesheet(isDark: boolean): cytoscape.StylesheetStyle[] {
  const base = baseStylesheet();
  if (isDark) {
    return [
      ...base,
      {
        selector: "node",
        style: {
          color: "#e0e0e0",
          "border-color": "#555",
          "background-color": "#3a3a5c",
        },
      },
      {
        selector: "core",
        style: {
          "active-bg-color": "#444",
        },
      } as cytoscape.StylesheetStyle,
    ];
  }
  return [
    ...base,
    {
      selector: "node",
      style: {
        color: "#262626",
        "border-color": "#d9d9d9",
        "background-color": "#e6f4ff",
      },
    },
  ];
}

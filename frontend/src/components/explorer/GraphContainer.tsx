import type React from "react";
import { useEffect, useRef, useState } from "react";
import { CytoscapeRenderer } from "./renderers/CytoscapeRenderer";
import type { GraphRendererProps } from "./renderers/types";

type GraphContainerProps = Omit<GraphRendererProps, "width" | "height">;

export function GraphContainer(props: GraphContainerProps): React.JSX.Element {
  const containerRef = useRef<HTMLDivElement>(null);
  const [dimensions, setDimensions] = useState({ width: 800, height: 600 });

  useEffect(() => {
    const el = containerRef.current;
    if (!el || typeof ResizeObserver === "undefined") return;

    const observer = new ResizeObserver((entries) => {
      for (const entry of entries) {
        const { width, height } = entry.contentRect;
        setDimensions({ width, height });
      }
    });
    observer.observe(el);
    return () => observer.disconnect();
  }, []);

  return (
    <div
      ref={containerRef}
      data-testid="graph-container"
      style={{ width: "100%", height: "100%", overflow: "hidden" }}
    >
      <CytoscapeRenderer
        {...props}
        width={dimensions.width}
        height={dimensions.height}
      />
    </div>
  );
}

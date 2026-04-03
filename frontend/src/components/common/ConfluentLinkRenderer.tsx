import type React from "react";
import { useResourceLinks } from "../../providers/ResourceLinkContext";

interface ConfluentLinkRendererProps {
  value: string | null;
  url?: string | null;
}

export function ConfluentLinkRenderer({
  value,
  url,
}: ConfluentLinkRendererProps): React.JSX.Element {
  const { resolveUrl, enabled } = useResourceLinks();
  const finalUrl = enabled ? (url ?? resolveUrl(value ?? "")) : null;
  if (!value) return <span>—</span>;
  if (!finalUrl) return <span>{value}</span>;
  return (
    <a href={finalUrl} target="_blank" rel="noopener noreferrer">
      {value}
    </a>
  );
}

// Icon SVG inner content sourced from Lucide Icons (MIT license)
// https://lucide.dev — commit-stable paths, 24x24 viewBox

const svgSuffix = "</svg>";

function svgPrefix(stroke: string): string {
  return `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="${stroke}" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">`;
}

function makeSvg(content: string, stroke: string): string {
  return `data:image/svg+xml,${encodeURIComponent(svgPrefix(stroke) + content + svgSuffix)}`;
}

// Lucide: Building2
const TENANT =
  '<path d="M10 12h4"/><path d="M10 8h4"/><path d="M14 21v-3a2 2 0 0 0-4 0v3"/><path d="M6 10H4a2 2 0 0 0-2 2v7a2 2 0 0 0 2 2h16a2 2 0 0 0 2-2V9a2 2 0 0 0-2-2h-2"/><path d="M6 21V5a2 2 0 0 1 2-2h8a2 2 0 0 1 2 2v16"/>';

// Lucide: FolderOpen
const ENVIRONMENT =
  '<path d="m6 14 1.5-2.9A2 2 0 0 1 9.24 10H20a2 2 0 0 1 1.94 2.5l-1.54 6a2 2 0 0 1-1.95 1.5H4a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h3.9a2 2 0 0 1 1.69.9l.81 1.2a2 2 0 0 0 1.67.9H18a2 2 0 0 1 2 2v2"/>';

// Lucide: Database
const CLUSTER =
  '<ellipse cx="12" cy="5" rx="9" ry="3"/><path d="M3 5V19A9 3 0 0 0 21 19V5"/><path d="M3 12A9 3 0 0 0 21 12"/>';

// Lucide: FileText
const TOPIC =
  '<path d="M6 22a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h8a2.4 2.4 0 0 1 1.704.706l3.588 3.588A2.4 2.4 0 0 1 20 8v12a2 2 0 0 1-2 2z"/><path d="M14 2v5a1 1 0 0 0 1 1h5"/><path d="M10 9H8"/><path d="M16 13H8"/><path d="M16 17H8"/>';

// Lucide: User
const IDENTITY =
  '<path d="M19 21v-2a4 4 0 0 0-4-4H9a4 4 0 0 0-4 4v2"/><circle cx="12" cy="7" r="4"/>';

// Lucide: KeyRound
const API_KEY = // pragma: allowlist secret
  '<path d="M2.586 17.414A2 2 0 0 0 2 18.828V21a1 1 0 0 0 1 1h3a1 1 0 0 0 1-1v-1a1 1 0 0 1 1-1h1a1 1 0 0 0 1-1v-1a1 1 0 0 1 1-1h.172a2 2 0 0 0 1.414-.586l.814-.814a6.5 6.5 0 1 0-4-4z"/><circle cx="16.5" cy="7.5" r=".5" fill="currentColor"/>';

// Lucide: Plug
const CONNECTOR =
  '<path d="M12 22v-5"/><path d="M15 8V2"/><path d="M17 8a1 1 0 0 1 1 1v4a4 4 0 0 1-4 4h-4a4 4 0 0 1-4-4V9a1 1 0 0 1 1-1z"/><path d="M9 8V2"/>';

// Lucide: Zap
const FLINK =
  '<path d="M4 14a1 1 0 0 1-.78-1.63l9.9-10.2a.5.5 0 0 1 .86.46l-1.92 6.02A1 1 0 0 0 13 10h7a1 1 0 0 1 .78 1.63l-9.9 10.2a.5.5 0 0 1-.86-.46l1.92-6.02A1 1 0 0 0 11 14z"/>';

// Lucide: BookOpen
const SCHEMA_REGISTRY =
  '<path d="M12 7v14"/><path d="M3 18a1 1 0 0 1-1-1V4a1 1 0 0 1 1-1h5a4 4 0 0 1 4 4 4 4 0 0 1 4-4h5a1 1 0 0 1 1 1v13a1 1 0 0 1-1 1h-6a3 3 0 0 0-3 3 3 3 0 0 0-3-3z"/>';

// Lucide: TerminalSquare
const KSQLDB =
  '<path d="m7 11 2-2-2-2"/><path d="M11 13h4"/><rect width="18" height="18" x="3" y="3" rx="2" ry="2"/>';

const ICON_PATHS: Record<string, string> = {
  tenant: TENANT,
  environment: ENVIRONMENT,
  kafka_cluster: CLUSTER,
  dedicated_cluster: CLUSTER,
  kafka_topic: TOPIC,
  service_account: IDENTITY,
  identity: IDENTITY,
  api_key: API_KEY,
  connector: CONNECTOR,
  flink_compute_pool: FLINK,
  schema_registry: SCHEMA_REGISTRY,
  ksqldb_cluster: KSQLDB,
};

// Pre-build both light and dark icon maps at module load
const DARK_STROKE = "#1a1a2e";
const LIGHT_STROKE = "#fff";

const darkIcons: Record<string, string> = {};
const lightIcons: Record<string, string> = {};
for (const [type, paths] of Object.entries(ICON_PATHS)) {
  darkIcons[type] = makeSvg(paths, DARK_STROKE);
  lightIcons[type] = makeSvg(paths, LIGHT_STROKE);
}

export function getNodeIcon(resourceType: string, isDark: boolean): string | null {
  const map = isDark ? lightIcons : darkIcons;
  return map[resourceType] ?? null;
}

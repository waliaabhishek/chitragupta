const intlAny = Intl as { supportedValuesOf?: (key: string) => string[] };

const rawZones: string[] =
  typeof intlAny.supportedValuesOf === "function"
    ? intlAny.supportedValuesOf("timeZone")
    : [
        "UTC",
        "America/New_York",
        "America/Chicago",
        "America/Denver",
        "America/Los_Angeles",
        "Europe/London",
        "Europe/Paris",
        "Asia/Tokyo",
        "Asia/Shanghai",
        "Australia/Sydney",
      ];

// Ensure UTC is always present — some environments omit it from Intl.supportedValuesOf
const zones = rawZones.includes("UTC") ? rawZones : ["UTC", ...rawZones];

export const TIMEZONE_OPTIONS = zones.map((tz) => ({ label: tz, value: tz }));

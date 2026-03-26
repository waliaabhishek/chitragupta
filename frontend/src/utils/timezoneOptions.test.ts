import { describe, expect, it } from "vitest";
import { TIMEZONE_OPTIONS } from "./timezoneOptions";

describe("TIMEZONE_OPTIONS", () => {
  it("contains objects with label and value fields", () => {
    expect(TIMEZONE_OPTIONS.length).toBeGreaterThan(0);
    const first = TIMEZONE_OPTIONS[0];
    expect(first).toHaveProperty("label");
    expect(first).toHaveProperty("value");
    expect(first.label).toBe(first.value);
  });

  it("contains UTC in all environments", () => {
    const values = TIMEZONE_OPTIONS.map((o) => o.value);
    expect(values).toContain("UTC");
  });

  it("contains America/New_York in all environments (fallback coverage)", () => {
    const values = TIMEZONE_OPTIONS.map((o) => o.value);
    expect(values).toContain("America/New_York");
  });
});

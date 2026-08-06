import { describe, expect, it } from "vitest";
import {
  getCurrentUtcMonth,
  getCurrentUtcMonthRange,
  getUtcMonthRange,
} from "./dateRange";

describe("FOCUS Preview UTC month helpers", () => {
  it.each([
    ["2026-08-01T00:30:00-07:00", "2026-08", "2026-08-01", "2026-09-01"],
    ["2026-12-31T23:30:00-08:00", "2027-01", "2027-01-01", "2027-02-01"],
  ])("uses UTC fields for %s", (instant, month, startDate, endDate) => {
    const now = new Date(instant);

    expect(getCurrentUtcMonth(now)).toBe(month);
    expect(getCurrentUtcMonthRange(now)).toEqual({ startDate, endDate });
    expect(getUtcMonthRange(month)).toEqual({ startDate, endDate });
  });
});

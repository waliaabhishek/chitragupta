import { afterEach, describe, expect, it, vi } from "vitest";
import {
  clearTimezoneFromStorage,
  loadTimezoneFromStorage,
  saveTimezoneToStorage,
  thirtyDaysAgoStr,
  todayStr,
} from "./dateFilterStorage";

afterEach(() => {
  vi.useRealTimers();
  localStorage.clear();
});

describe("timezone storage helpers", () => {
  it("loadTimezoneFromStorage returns null when nothing stored", () => {
    expect(loadTimezoneFromStorage()).toBeNull();
  });

  it("saveTimezoneToStorage writes to user_timezone key", () => {
    saveTimezoneToStorage("America/Chicago");
    expect(localStorage.getItem("user_timezone")).toBe("America/Chicago");
  });

  it("loadTimezoneFromStorage returns the stored timezone", () => {
    saveTimezoneToStorage("Europe/London");
    expect(loadTimezoneFromStorage()).toBe("Europe/London");
  });

  it("clearTimezoneFromStorage removes user_timezone key", () => {
    saveTimezoneToStorage("America/Chicago");
    clearTimezoneFromStorage();
    expect(localStorage.getItem("user_timezone")).toBeNull();
  });

  it("loadTimezoneFromStorage returns null after clearTimezoneFromStorage", () => {
    saveTimezoneToStorage("Asia/Tokyo");
    clearTimezoneFromStorage();
    expect(loadTimezoneFromStorage()).toBeNull();
  });
});

describe("date default helpers", () => {
  it("todayStr returns the current UTC ISO date at a year boundary", () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2035-01-01T00:30:00.000Z"));

    expect(todayStr()).toBe("2035-01-01");
  });

  it("thirtyDaysAgoStr subtracts thirty days across a UTC month and year boundary", () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2035-01-01T12:00:00.000Z"));

    expect(thirtyDaysAgoStr()).toBe("2034-12-02");
  });
});

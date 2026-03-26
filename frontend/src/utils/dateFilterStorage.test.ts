import { afterEach, describe, expect, it } from "vitest";
import {
  clearTimezoneFromStorage,
  loadTimezoneFromStorage,
  saveTimezoneToStorage,
} from "./dateFilterStorage";

afterEach(() => {
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

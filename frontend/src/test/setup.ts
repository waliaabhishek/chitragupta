import "@testing-library/jest-dom";
import { afterAll, afterEach, beforeAll, vi } from "vitest";
import { server } from "./mocks/server";

// Ant Design uses window.matchMedia for responsive breakpoints — jsdom doesn't implement it.
Object.defineProperty(window, "matchMedia", {
  writable: true,
  value: vi.fn().mockImplementation((query: string) => ({
    matches: false,
    media: query,
    onchange: null,
    addListener: vi.fn(),
    removeListener: vi.fn(),
    addEventListener: vi.fn(),
    removeEventListener: vi.fn(),
    dispatchEvent: vi.fn(),
  })),
});

// Ant Design uses getComputedStyle for scroll-bar size detection which jsdom doesn't support.
// Stub it with a minimal implementation that returns empty styles.
Object.defineProperty(window, "getComputedStyle", {
  writable: true,
  value: () =>
    ({
      overflow: "",
      paddingRight: "0px",
      getPropertyValue: () => "",
      setProperty: () => undefined,
    }) as unknown as CSSStyleDeclaration,
});

// Provide a functional localStorage mock for jsdom environments
// that don't initialize it properly.
function makeLocalStorageMock(): Storage {
  let store: Record<string, string> = {};
  return {
    get length() {
      return Object.keys(store).length;
    },
    key(index: number) {
      return Object.keys(store)[index] ?? null;
    },
    getItem(key: string) {
      return store[key] ?? null;
    },
    setItem(key: string, value: string) {
      store[key] = value;
    },
    removeItem(key: string) {
      delete store[key];
    },
    clear() {
      store = {};
    },
  };
}

vi.stubGlobal("localStorage", makeLocalStorageMock());

beforeAll(() => server.listen({ onUnhandledRequest: "error" }));
afterEach(() => server.resetHandlers());
afterAll(() => server.close());

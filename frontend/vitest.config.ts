import { defineConfig } from "vitest/config";
import react from "@vitejs/plugin-react";

export default defineConfig({
  plugins: [react()],
  test: {
    globals: true,
    environment: "./src/test/vitest-env-jsdom-compat.ts",
    setupFiles: ["./src/test/setup.ts"],
    dangerouslyForceExit: true,
    // React 19's async act() uses setTimeout(resolve, 0) internally.
    // Without shouldAdvanceTime, vi.useFakeTimers() prevents that timeout
    // from ever firing, causing await userEvent.click/type to hang.
    // advanceTimeDelta:5 advances fake time 5ms per 5ms real time (1:1),
    // so React's internal 0ms timeouts fire within ~5ms of real time,
    // while the 300ms debounce timer still requires explicit advance.
    fakeTimers: {
      shouldAdvanceTime: true,
      advanceTimeDelta: 5,
    },
    coverage: {
      provider: "v8",
      reporter: ["text", "json", "html"],
      include: [
        "src/config/**/*.{ts,tsx}",
        "src/components/common/**/*.{ts,tsx}",
        "src/providers/**/*.{ts,tsx}",
        "src/hooks/**/*.{ts,tsx}",
        "src/components/chargebacks/**/*.{ts,tsx}",
        "src/components/charts/**/*.{ts,tsx}",
        "src/components/dashboard/**/*.{ts,tsx}",
        "src/components/entities/**/*.{ts,tsx}",
        "src/pages/chargebacks/**/*.{ts,tsx}",
        "src/pages/dashboard/**/*.{ts,tsx}",
        "src/pages/resources/**/*.{ts,tsx}",
        "src/pages/identities/**/*.{ts,tsx}",
        "src/pages/tags/**/*.{ts,tsx}",
        "src/utils/aggregation.ts",
      ],
      thresholds: {
        lines: 85,
        functions: 85,
        branches: 85,
        statements: 85,
      },
    },
  },
});

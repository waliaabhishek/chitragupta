import { defineConfig } from "vitest/config";
import react from "@vitejs/plugin-react";

export default defineConfig({
  plugins: [react()],
  test: {
    globals: true,
    environment: "./src/test/vitest-env-jsdom-compat.ts",
    setupFiles: ["./src/test/setup.ts"],
    dangerouslyForceExit: true,
    coverage: {
      provider: "v8",
      reporter: ["text", "json", "html"],
      include: [
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

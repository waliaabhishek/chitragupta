import { defineConfig } from "vitest/config";
import react from "@vitejs/plugin-react";

export default defineConfig({
  plugins: [react()],
  test: {
    globals: true,
    environment: "jsdom",
    setupFiles: ["./src/test/setup.ts"],
    dangerouslyForceExit: true,
    coverage: {
      provider: "v8",
      reporter: ["text", "json", "html"],
      include: [
        "src/providers/**/*.{ts,tsx}",
        "src/hooks/**/*.{ts,tsx}",
        "src/components/chargebacks/**/*.{ts,tsx}",
        "src/pages/chargebacks/**/*.{ts,tsx}",
      ],
      thresholds: {
        lines: 80,
        functions: 80,
        branches: 80,
        statements: 80,
      },
    },
  },
});

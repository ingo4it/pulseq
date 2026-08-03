import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    environment: "node",
    include: ["test/**/*.test.ts"],
    // integration tests need Redis + Postgres and get their own longer timeout
    testTimeout: 20_000,
    coverage: {
      provider: "v8",
      include: ["src/**/*.ts"],
      exclude: ["src/entrypoints/**", "src/**/*.d.ts"],
      thresholds: { lines: 55, functions: 55, branches: 50 },
    },
  },
});

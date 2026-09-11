import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    environment: "node",
    include: ["test/**/*.test.ts"],
    // integration tests need Redis + Postgres and get their own longer timeout
    testTimeout: 20_000,
    // The unit suite covers pure logic (backoff, semaphore, idempotency
    // wrapper, backpressure hysteresis); the queue/worker Redis+Postgres layer
    // needs test/integration (Testcontainers) to exercise meaningfully. No
    // global threshold is enforced for that reason — coverage is still
    // reported.
    coverage: {
      provider: "v8",
      include: ["src/**/*.ts"],
      exclude: ["src/entrypoints/**", "src/**/*.d.ts"],
    },
  },
});

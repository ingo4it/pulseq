import { describe, expect, it } from "vitest";
import { backoffDelayMs, nextRunAt } from "../../src/core/backoff.js";

const opts = { baseMs: 1000, capMs: 60_000, jitter: "none" } as const;

describe("backoffDelayMs", () => {
  it("is exponential and deterministic with jitter=none", () => {
    expect(backoffDelayMs(0, opts)).toBe(1000);
    expect(backoffDelayMs(1, opts)).toBe(2000);
    expect(backoffDelayMs(3, opts)).toBe(8000);
  });

  it("clamps to capMs", () => {
    expect(backoffDelayMs(20, opts)).toBe(60_000);
  });

  it("full jitter stays within [0, exp]", () => {
    const exp = 8000;
    for (const r of [0, 0.5, 0.999]) {
      const d = backoffDelayMs(3, { ...opts, jitter: "full" }, () => r);
      expect(d).toBeGreaterThanOrEqual(0);
      expect(d).toBeLessThanOrEqual(exp);
    }
  });

  it("equal jitter stays within [exp/2, exp]", () => {
    const exp = 8000;
    for (const r of [0, 0.5, 1]) {
      const d = backoffDelayMs(3, { ...opts, jitter: "equal" }, () => r);
      expect(d).toBeGreaterThanOrEqual(exp / 2);
      expect(d).toBeLessThanOrEqual(exp);
    }
  });

  it("nextRunAt adds the delay to now", () => {
    expect(nextRunAt(1, 10_000, opts)).toBe(12_000);
  });
});

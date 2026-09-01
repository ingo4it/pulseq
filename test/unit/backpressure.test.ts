import { describe, expect, it } from "vitest";
import { BackpressureGate } from "../../src/worker/backpressure.js";
import type { Queue } from "../../src/queue/queue.js";
import type { QueueDepth } from "../../src/core/types.js";

/** Fake queue whose depth we drive from the test. */
function queueWithDepth(ref: { ready: number }): Queue {
  return {
    depth: async (namespace: string): Promise<QueueDepth> => ({
      namespace,
      ready: ref.ready,
      delayed: 0,
      inFlight: 0,
      dlq: 0,
    }),
  } as unknown as Queue;
}

describe("BackpressureGate hysteresis", () => {
  it("engages at the high watermark and only releases below the low watermark", async () => {
    const depth = { ready: 0 };
    const gate = new BackpressureGate(queueWithDepth(depth), 1000, 200);

    depth.ready = 500;
    expect((await gate.check("default")).throttled).toBe(false);

    depth.ready = 1000; // hit high
    expect((await gate.check("default")).throttled).toBe(true);

    depth.ready = 400; // below high, above low — still throttled
    expect((await gate.check("default")).throttled).toBe(true);

    depth.ready = 200; // hit low
    expect((await gate.check("default")).throttled).toBe(false);

    depth.ready = 900; // below high again — not throttled until it hits high
    expect((await gate.check("default")).throttled).toBe(false);
  });
});

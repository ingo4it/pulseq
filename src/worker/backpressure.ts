import type { Queue } from "../queue/queue.js";

/**
 * Producer-side backpressure. When a namespace's depth (ready + delayed)
 * crosses the high watermark the gate engages and `check()` reports
 * `throttled: true`; it disengages only once depth falls back below the low
 * watermark. The hysteresis gap stops the gate from flapping on every enqueue
 * while depth hovers around a single threshold.
 *
 * Producers call `waitForCapacity()` before enqueueing a batch; the admin API
 * exposes the current state so upstream systems can also react.
 */
export type BackpressureState = { throttled: boolean; depth: number; watermark: "high" | "low" | "mid" };

export class BackpressureGate {
  private engaged = false;

  constructor(
    private readonly queue: Queue,
    private readonly high: number,
    private readonly low: number,
  ) {}

  async check(namespace: string): Promise<BackpressureState> {
    const d = await this.queue.depth(namespace);
    const depth = d.ready + d.delayed;

    if (!this.engaged && depth >= this.high) this.engaged = true;
    else if (this.engaged && depth <= this.low) this.engaged = false;

    return {
      throttled: this.engaged,
      depth,
      watermark: depth >= this.high ? "high" : depth <= this.low ? "low" : "mid",
    };
  }

  async waitForCapacity(namespace: string, opts: { pollMs: number; timeoutMs?: number } = { pollMs: 250 }): Promise<void> {
    const deadline = opts.timeoutMs ? Date.now() + opts.timeoutMs : Infinity;
    for (;;) {
      const state = await this.check(namespace);
      if (!state.throttled) return;
      if (Date.now() >= deadline) throw new Error(`backpressure: ${namespace} still throttled after ${opts.timeoutMs}ms`);
      await new Promise((r) => setTimeout(r, opts.pollMs));
    }
  }
}

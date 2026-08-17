import type { NewJob } from "./core/types.js";
import type { Queue } from "./queue/queue.js";
import type { BackpressureGate } from "./worker/backpressure.js";

/**
 * The producer-facing API. `enqueue` is a straight passthrough to the queue;
 * `enqueueRespectingBackpressure` blocks the caller while a namespace is over
 * its high watermark, so a fast producer can't run consumers into the ground.
 * Library consumers can also just call `queue.enqueue` directly if they manage
 * flow control themselves.
 */
export class Producer {
  constructor(
    private readonly queue: Queue,
    private readonly backpressure: BackpressureGate,
  ) {}

  enqueue(job: NewJob): Promise<{ id: string; state: "READY" | "DELAYED" }> {
    return this.queue.enqueue(job);
  }

  async enqueueRespectingBackpressure(
    job: NewJob,
    opts: { pollMs?: number; timeoutMs?: number } = {},
  ): Promise<{ id: string; state: "READY" | "DELAYED"; waitedMs: number }> {
    const ns = job.namespace ?? "default";
    const started = Date.now();
    await this.backpressure.waitForCapacity(ns, { pollMs: opts.pollMs ?? 250, timeoutMs: opts.timeoutMs });
    const res = await this.queue.enqueue(job);
    return { ...res, waitedMs: Date.now() - started };
  }
}

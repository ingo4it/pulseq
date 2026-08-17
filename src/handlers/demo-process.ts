import type { JobHandler } from "../worker/handler.js";

/**
 * Simulated unit of work for the load harness. The payload controls how it
 * behaves so a single job type can exercise every path:
 *
 *   { workMs?: number,   // simulated processing time
 *     failRate?: number, // 0..1 chance this attempt throws (transient)
 *     poison?: boolean }  // always throws → will reach the DLQ
 *
 * It honours `ctx.signal` so graceful shutdown can interrupt a long sleep.
 */
export const demoProcessHandler: JobHandler = async (payload, ctx) => {
  const p = (payload ?? {}) as { workMs?: number; failRate?: number; poison?: boolean };

  await interruptibleSleep(p.workMs ?? 25, ctx.signal);

  if (p.poison) throw new Error("poison job: always fails");
  if (p.failRate && Math.random() < p.failRate) {
    throw new Error("downstream unavailable (simulated transient failure)");
  }
  return { ok: true, processedAt: Date.now() };
};

function interruptibleSleep(ms: number, signal: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
    if (signal.aborted) return reject(new Error("aborted"));
    const t = setTimeout(resolve, ms);
    signal.addEventListener(
      "abort",
      () => {
        clearTimeout(t);
        reject(new Error("aborted"));
      },
      { once: true },
    );
  });
}

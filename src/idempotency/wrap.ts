import type { JsonValue } from "../core/types.js";
import type { IdempotencyStore } from "./store.js";

/** Thrown when another delivery of the same key is still running. The worker
 * treats it as a transient failure and retries with backoff; by the time the
 * retry lands the other delivery has usually completed and its result is
 * returned. */
export class ConcurrentDeliveryError extends Error {
  constructor(key: string) {
    super(`idempotency key ${key} is already in progress`);
    this.name = "ConcurrentDeliveryError";
  }
}

export type IdempotentContext = { key: string; namespace: string; jobId: string; ttlMs?: number };

/**
 * Run `fn` at most once per idempotency key.
 *
 *   - first delivery  → run `fn`, record the result, return it
 *   - redelivery after success → skip `fn`, return the recorded result
 *   - redelivery while another is running → throw `ConcurrentDeliveryError`
 *   - `fn` throws → drop the ledger row so the retry starts fresh, rethrow
 */
export async function withIdempotency<T extends JsonValue>(
  store: IdempotencyStore,
  ctx: IdempotentContext,
  fn: () => Promise<T>,
): Promise<T> {
  const begun = await store.begin({
    key: ctx.key,
    namespace: ctx.namespace,
    jobId: ctx.jobId,
    ttlMs: ctx.ttlMs ?? 24 * 60 * 60 * 1000,
  });

  if (begun.status === "completed") return begun.result as T;
  if (begun.status === "in_progress") throw new ConcurrentDeliveryError(ctx.key);

  try {
    const result = await fn();
    await store.complete(ctx.key, result);
    return result;
  } catch (err) {
    await store.abort(ctx.key);
    throw err;
  }
}

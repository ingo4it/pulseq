import { describe, expect, it, vi } from "vitest";
import { withIdempotency, ConcurrentDeliveryError } from "../../src/idempotency/wrap.js";
import type { IdempotencyStore, BeginResult } from "../../src/idempotency/store.js";

function fakeStore(begin: BeginResult) {
  return {
    begin: vi.fn(async () => begin),
    complete: vi.fn(async () => {}),
    abort: vi.fn(async () => {}),
    sweep: vi.fn(async () => 0),
  } as unknown as IdempotencyStore & {
    begin: ReturnType<typeof vi.fn>;
    complete: ReturnType<typeof vi.fn>;
    abort: ReturnType<typeof vi.fn>;
  };
}

const ctx = { key: "k1", namespace: "default", jobId: "job-1" };

describe("withIdempotency", () => {
  it("runs the handler once on first delivery and records the result", async () => {
    const store = fakeStore({ status: "fresh" });
    const fn = vi.fn(async () => ({ charged: true }));

    const out = await withIdempotency(store, ctx, fn);

    expect(fn).toHaveBeenCalledOnce();
    expect(out).toEqual({ charged: true });
    expect(store.complete).toHaveBeenCalledWith("k1", { charged: true });
  });

  it("skips the handler on redelivery after success and returns the stored result", async () => {
    const store = fakeStore({ status: "completed", result: { charged: true } });
    const fn = vi.fn(async () => ({ charged: true }));

    const out = await withIdempotency(store, ctx, fn);

    expect(fn).not.toHaveBeenCalled();
    expect(out).toEqual({ charged: true });
  });

  it("throws ConcurrentDeliveryError when another delivery holds the key", async () => {
    const store = fakeStore({ status: "in_progress" });
    await expect(withIdempotency(store, ctx, vi.fn())).rejects.toBeInstanceOf(ConcurrentDeliveryError);
  });

  it("drops the ledger row and rethrows when the handler fails", async () => {
    const store = fakeStore({ status: "fresh" });
    const boom = new Error("downstream 500");

    await expect(
      withIdempotency(store, ctx, async () => {
        throw boom;
      }),
    ).rejects.toBe(boom);

    expect(store.abort).toHaveBeenCalledWith("k1");
    expect(store.complete).not.toHaveBeenCalled();
  });
});

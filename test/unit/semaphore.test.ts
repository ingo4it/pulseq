import { describe, expect, it } from "vitest";
import { Semaphore } from "../../src/worker/semaphore.js";

describe("Semaphore", () => {
  it("hands out up to `size` permits without blocking", async () => {
    const s = new Semaphore(2);
    await s.acquire();
    await s.acquire();
    expect(s.available).toBe(0);
  });

  it("blocks the (size+1)th acquire until a permit is released", async () => {
    const s = new Semaphore(1);
    const r1 = await s.acquire();

    let acquired = false;
    const pending = s.acquire().then((r) => {
      acquired = true;
      return r;
    });

    await Promise.resolve();
    expect(acquired).toBe(false);

    r1();
    await pending;
    expect(acquired).toBe(true);
  });

  it("ignores a double release", async () => {
    const s = new Semaphore(1);
    const r = await s.acquire();
    r();
    r();
    expect(s.available).toBe(1);
  });
});

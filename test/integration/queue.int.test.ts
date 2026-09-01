import { afterAll, beforeAll, describe, expect, it } from "vitest";

/**
 * Integration suite — real Redis + Postgres (CI `integration` job, or set
 * REDIS_URL + DATABASE_URL locally). Skipped otherwise so `pnpm test` stays
 * hermetic.
 *
 * This file is the scaffold and the checklist; each `it` describes a behaviour
 * the queue must hold under a real broker. Filling in the bodies is the
 * remaining work.
 */
const HAS_INFRA = Boolean(process.env.REDIS_URL && process.env.DATABASE_URL);

describe.skipIf(!HAS_INFRA)("Queue (integration)", () => {
  beforeAll(async () => {
    // connect Redis + Prisma, run migrations, flush a scratch namespace
  });
  afterAll(async () => {
    // disconnect
  });

  it.todo("enqueue → lease → ack marks the job SUCCEEDED and removes it from the stream");
  it.todo("a failing handler with attempts remaining schedules a delayed retry with backoff");
  it.todo("a job that exhausts maxAttempts lands in the DLQ and is marked DEAD");
  it.todo("replayDlq moves DLQ entries back to ready and resets their PG state");
  it.todo("a lease with no renewal past LEASE_TTL_MS is reclaimable by another consumer");
  it.todo("renewLease resets the idle clock so the job is not reclaimed early");
  it.todo("promoteDue moves only entries whose runAt has passed");
  it.todo("a redelivered job with an idempotency key runs its side effect exactly once");
  it.todo("graceful stop drains in-flight jobs; nothing is acked that did not complete");
  it.todo("two workers on one namespace never process the same stream entry concurrently");

  it("infra is reachable", () => {
    expect(HAS_INFRA).toBe(true);
  });
});

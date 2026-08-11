import type { Prisma, PrismaClient } from "@prisma/client";
import type { JsonValue } from "../core/types.js";

/**
 * The idempotency ledger. It's what makes at-least-once delivery safe for a
 * handler with side effects (charge a card, send an email): the handler is
 * wrapped so the effect runs once per key even if the job is delivered twice.
 *
 * States: a row is inserted `IN_PROGRESS` on first delivery and flipped to
 * `COMPLETED` with the recorded result when the handler returns. A redelivery
 * that finds `COMPLETED` skips the handler and returns the stored result. A row
 * left `IN_PROGRESS` past `staleAfterMs` is assumed to belong to a worker that
 * crashed and is reclaimable.
 */
export type BeginResult =
  | { status: "fresh" }
  | { status: "completed"; result: JsonValue }
  | { status: "in_progress" };

export class IdempotencyStore {
  constructor(
    private readonly prisma: PrismaClient,
    private readonly staleAfterMs = 60_000,
  ) {}

  async begin(args: { key: string; namespace: string; jobId: string; ttlMs: number }): Promise<BeginResult> {
    const now = Date.now();
    try {
      await this.prisma.idempotencyKey.create({
        data: {
          key: args.key,
          namespace: args.namespace,
          jobId: args.jobId,
          status: "IN_PROGRESS",
          expiresAt: new Date(now + args.ttlMs),
        },
      });
      return { status: "fresh" };
    } catch {
      // unique violation — a row already exists
    }

    const existing = await this.prisma.idempotencyKey.findUnique({ where: { key: args.key } });
    if (!existing) return { status: "fresh" }; // raced with a delete; caller retries

    if (existing.status === "COMPLETED") {
      return { status: "completed", result: (existing.result ?? null) as JsonValue };
    }

    const age = now - existing.createdAt.getTime();
    if (age > this.staleAfterMs) {
      // previous holder almost certainly crashed; take the row over
      await this.prisma.idempotencyKey.update({
        where: { key: args.key },
        data: { jobId: args.jobId, createdAt: new Date(now), expiresAt: new Date(now + args.ttlMs) },
      });
      return { status: "fresh" };
    }
    return { status: "in_progress" };
  }

  async complete(key: string, result: JsonValue): Promise<void> {
    await this.prisma.idempotencyKey.update({
      where: { key },
      data: {
        status: "COMPLETED",
        result: (result ?? undefined) as Prisma.InputJsonValue,
        completedAt: new Date(),
      },
    });
  }

  /** Handler threw: drop the row so the retry re-enters cleanly. */
  async abort(key: string): Promise<void> {
    await this.prisma.idempotencyKey.deleteMany({ where: { key, status: "IN_PROGRESS" } });
  }

  async sweep(): Promise<number> {
    const { count } = await this.prisma.idempotencyKey.deleteMany({ where: { expiresAt: { lt: new Date() } } });
    return count;
  }
}

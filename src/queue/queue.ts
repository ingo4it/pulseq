import type { Prisma, PrismaClient } from "@prisma/client";
import type { Redis } from "ioredis";
import type { Logger } from "../logger.js";
import type { Clock } from "../core/clock.js";
import { systemClock } from "../core/clock.js";
import { nextRunAt, type BackoffOptions } from "../core/backoff.js";
import { newJobId } from "../core/ids.js";
import type { AckOutcome, JobEnvelope, LeasedJob, NewJob, QueueDepth } from "../core/types.js";
import { CONSUMER_GROUP, delayedZset, dlqStream, pausedFlag, readyStream } from "./keys.js";
import { registerScripts, type QueueScripts } from "./scripts.js";

export type QueueOptions = {
  redis: Redis;
  /**
   * Dedicated connection for the blocking `XREADGROUP` in `leaseBatch`. A
   * blocked connection can't serve other commands, so acks/fails/renewals must
   * not share it. Falls back to `redis` when omitted (fine for the admin
   * process and tests, which never block).
   */
  blockingRedis?: Redis;
  prisma: PrismaClient;
  logger: Logger;
  defaultNamespace?: string;
  defaultMaxAttempts: number;
  leaseTtlMs: number;
  backoff: BackoffOptions;
  clock?: Clock;
};

/**
 * The queue: enqueue, lease, ack, fail, DLQ, replay, depth. Redis is
 * authoritative for what runs next; Postgres is a mirror for durability and the
 * admin API. Redis writes go through the Lua scripts (`scripts.ts`) so each
 * transition is atomic; the PG mirror is updated right after and is allowed to
 * lag — a worker crash between the two leaves Redis correct and PG catches up
 * on the next transition or the reconciler.
 */
export class Queue {
  private readonly scripts: QueueScripts;
  private readonly clock: Clock;
  private readonly groupsEnsured = new Set<string>();

  constructor(private readonly opts: QueueOptions) {
    this.scripts = registerScripts(opts.redis);
    this.clock = opts.clock ?? systemClock;
  }

  private ns(explicit?: string): string {
    return explicit ?? this.opts.defaultNamespace ?? "default";
  }

  /** Idempotent: create the consumer group (and the stream) if missing. */
  async ensureGroup(namespace: string): Promise<void> {
    if (this.groupsEnsured.has(namespace)) return;
    try {
      await this.opts.redis.xgroup("CREATE", readyStream(namespace), CONSUMER_GROUP, "$", "MKSTREAM");
    } catch (err) {
      if (!(err instanceof Error) || !err.message.includes("BUSYGROUP")) throw err;
    }
    this.groupsEnsured.add(namespace);
  }

  async enqueue(job: NewJob): Promise<{ id: string; state: "READY" | "DELAYED" }> {
    const namespace = this.ns(job.namespace);
    await this.ensureGroup(namespace);

    const now = this.clock.now();
    const runAt = now + Math.max(0, job.delayMs ?? 0);
    const envelope: JobEnvelope = {
      id: newJobId(),
      namespace,
      type: job.type,
      payload: job.payload,
      attempt: 0,
      maxAttempts: job.maxAttempts ?? this.opts.defaultMaxAttempts,
      idempotencyKey: job.idempotencyKey,
      enqueuedAt: now,
    };

    const placed = await this.scripts.enqueue(
      readyStream(namespace),
      delayedZset(namespace),
      runAt,
      now,
      JSON.stringify(envelope),
    );

    await this.opts.prisma.job.create({
      data: {
        id: envelope.id,
        namespace,
        type: envelope.type,
        payload: envelope.payload as Prisma.InputJsonValue,
        state: placed === "ready" ? "READY" : "DELAYED",
        maxAttempts: envelope.maxAttempts,
        idempotencyKey: envelope.idempotencyKey ?? null,
        runAt: new Date(runAt),
        enqueuedAt: new Date(now),
      },
    });

    return { id: envelope.id, state: placed === "ready" ? "READY" : "DELAYED" };
  }

  /** Move up to `limit` due jobs from `delayed` into `ready`. */
  async promoteDue(namespace: string, limit: number): Promise<number> {
    const moved = await this.scripts.promoteDelayed(
      readyStream(namespace),
      delayedZset(namespace),
      this.clock.now(),
      limit,
    );
    if (moved > 0) {
      await this.opts.prisma.job.updateMany({
        where: { namespace, state: "DELAYED", runAt: { lte: new Date(this.clock.now()) } },
        data: { state: "READY" },
      });
    }
    return moved;
  }

  async isPaused(namespace: string): Promise<boolean> {
    return (await this.opts.redis.exists(pausedFlag(namespace))) === 1;
  }

  /**
   * Lease up to `count` new jobs, blocking up to `blockMs` for one to arrive.
   * Each returned job's attempt is incremented and its lease clock started.
   */
  async leaseBatch(namespace: string, consumer: string, count: number, blockMs: number): Promise<LeasedJob[]> {
    await this.ensureGroup(namespace);
    const conn = this.opts.blockingRedis ?? this.opts.redis;
    const res = (await conn.xreadgroup(
      "GROUP",
      CONSUMER_GROUP,
      consumer,
      "COUNT",
      count,
      "BLOCK",
      blockMs,
      "STREAMS",
      readyStream(namespace),
      ">",
    )) as Array<[string, Array<[string, string[]]>]> | null;

    if (!res || res.length === 0) return [];
    const entries = res[0]?.[1] ?? [];
    return this.materialise(namespace, consumer, entries);
  }

  /**
   * Reclaim entries whose lease has expired (no renewal for `leaseTtlMs`) so a
   * crashed worker's jobs don't sit stuck. This is at-least-once in action: the
   * new owner re-runs the job, and the consumer-side idempotency key is what
   * keeps that safe.
   */
  async reclaimExpired(namespace: string, consumer: string, count: number): Promise<LeasedJob[]> {
    const [, entries] = (await this.opts.redis.xautoclaim(
      readyStream(namespace),
      CONSUMER_GROUP,
      consumer,
      this.opts.leaseTtlMs,
      "0",
      "COUNT",
      count,
    )) as [string, Array<[string, string[]]>, string[]];

    return this.materialise(namespace, consumer, entries ?? []);
  }

  /** Renew the lease on a still-running job: reset its idle clock to zero. */
  async renewLease(namespace: string, consumer: string, streamId: string): Promise<number> {
    await this.opts.redis.xclaim(readyStream(namespace), CONSUMER_GROUP, consumer, 0, streamId, "JUSTID");
    const leaseExpiresAt = this.clock.now() + this.opts.leaseTtlMs;
    await this.opts.prisma.job
      .updateMany({ where: { streamId, namespace }, data: { leaseExpiresAt: new Date(leaseExpiresAt) } })
      .catch(() => undefined);
    return leaseExpiresAt;
  }

  async ack(job: LeasedJob, durationMs: number): Promise<void> {
    await this.scripts.ackJob(readyStream(job.namespace), CONSUMER_GROUP, job.streamId);
    await this.opts.prisma.$transaction([
      this.opts.prisma.job.update({
        where: { id: job.id },
        data: { state: "SUCCEEDED", finishedAt: new Date(this.clock.now()), streamId: null, leaseOwner: null },
      }),
      this.opts.prisma.jobAttempt.create({
        data: {
          jobId: job.id,
          attempt: job.attempt,
          worker: job.leaseOwner,
          outcome: "succeeded",
          finishedAt: new Date(this.clock.now()),
          durationMs,
        },
      }),
    ]);
  }

  /**
   * Fail a job. If its attempt budget is spent it's dead-lettered; otherwise a
   * retry is scheduled at `now + backoff(attempt)`. Both branches ack the ready
   * entry in the same Lua call, so the job is never left un-acked.
   */
  async fail(
    job: LeasedJob,
    error: Error,
    durationMs: number,
    opts: { permanent?: boolean } = {},
  ): Promise<AckOutcome> {
    // a permanent error (e.g. no handler for this type) skips the retry budget
    const isDead = opts.permanent === true || job.attempt >= job.maxAttempts;
    const runAt = isDead ? this.clock.now() : nextRunAt(job.attempt, this.clock.now(), this.opts.backoff);

    const outcome = (await this.scripts.failJob(
      readyStream(job.namespace),
      delayedZset(job.namespace),
      dlqStream(job.namespace),
      CONSUMER_GROUP,
      job.streamId,
      runAt,
      isDead,
      JSON.stringify(this.toEnvelope(job)),
      error.message.slice(0, 500),
    )) as AckOutcome;

    await this.opts.prisma.$transaction([
      this.opts.prisma.job.update({
        where: { id: job.id },
        data: {
          state: isDead ? "DEAD" : "DELAYED",
          runAt: new Date(runAt),
          lastError: error.message.slice(0, 500),
          streamId: null,
          leaseOwner: null,
          leaseExpiresAt: null,
          finishedAt: isDead ? new Date(this.clock.now()) : null,
        },
      }),
      this.opts.prisma.jobAttempt.create({
        data: {
          jobId: job.id,
          attempt: job.attempt,
          worker: job.leaseOwner,
          outcome: isDead ? "dead" : "retryable_error",
          error: error.message.slice(0, 500),
          finishedAt: new Date(this.clock.now()),
          durationMs,
        },
      }),
    ]);

    return outcome;
  }

  async depth(namespace: string): Promise<QueueDepth> {
    const [ready, delayed, dlq, pending] = await Promise.all([
      this.opts.redis.xlen(readyStream(namespace)),
      this.opts.redis.zcard(delayedZset(namespace)),
      this.opts.redis.xlen(dlqStream(namespace)),
      this.opts.redis
        .xpending(readyStream(namespace), CONSUMER_GROUP)
        .then((p) => (Array.isArray(p) ? Number(p[0] ?? 0) : 0))
        .catch(() => 0),
    ]);
    return { namespace, ready, delayed, inFlight: pending, dlq };
  }

  async replayDlq(namespace: string, count: number): Promise<number> {
    const ids = await this.scripts.replayDlq(dlqStream(namespace), readyStream(namespace), count);
    if (ids.length > 0) {
      await this.opts.prisma.job.updateMany({
        where: { id: { in: ids } },
        data: { state: "READY", attempt: 0, lastError: null, finishedAt: null },
      });
    }
    return ids.length;
  }

  async pause(namespace: string): Promise<void> {
    await this.opts.redis.set(pausedFlag(namespace), "1");
  }
  async resume(namespace: string): Promise<void> {
    await this.opts.redis.del(pausedFlag(namespace));
  }

  // ---- internals ----

  private async materialise(
    namespace: string,
    consumer: string,
    entries: Array<[string, string[]]>,
  ): Promise<LeasedJob[]> {
    const leased: LeasedJob[] = [];
    for (const [streamId, fields] of entries) {
      const envJson = fieldValue(fields, "env");
      if (!envJson) {
        // malformed entry — ack it away so it doesn't wedge the group
        await this.scripts.ackJob(readyStream(namespace), CONSUMER_GROUP, streamId);
        continue;
      }
      const env = JSON.parse(envJson) as JobEnvelope;
      const attempt = env.attempt + 1;
      leased.push({
        ...env,
        attempt,
        streamId,
        leaseOwner: consumer,
        leaseExpiresAt: this.clock.now() + this.opts.leaseTtlMs,
      });
    }

    if (leased.length > 0) {
      await this.opts.prisma.$transaction(
        leased.map((j) =>
          this.opts.prisma.job.update({
            where: { id: j.id },
            data: {
              state: "LEASED",
              attempt: j.attempt,
              streamId: j.streamId,
              leaseOwner: consumer,
              leaseExpiresAt: new Date(j.leaseExpiresAt),
              startedAt: new Date(this.clock.now()),
            },
          }),
        ),
      );
    }
    return leased;
  }

  private toEnvelope(job: LeasedJob): JobEnvelope {
    return {
      id: job.id,
      namespace: job.namespace,
      type: job.type,
      payload: job.payload,
      attempt: job.attempt,
      maxAttempts: job.maxAttempts,
      idempotencyKey: job.idempotencyKey,
      enqueuedAt: job.enqueuedAt,
    };
  }
}

function fieldValue(fields: string[], name: string): string | undefined {
  for (let i = 0; i < fields.length; i += 2) {
    if (fields[i] === name) return fields[i + 1];
  }
  return undefined;
}

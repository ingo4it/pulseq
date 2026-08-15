import type { Logger } from "../logger.js";
import type { Config } from "../config.js";
import type { JsonValue, LeasedJob } from "../core/types.js";
import type { Queue } from "../queue/queue.js";
import type { Metrics } from "../metrics/collectors.js";
import { HandlerRegistry, NoHandlerError, type JobContext } from "./handler.js";
import { Semaphore } from "./semaphore.js";
import { IdempotencyStore } from "../idempotency/store.js";
import { withIdempotency } from "../idempotency/wrap.js";
import { workerId } from "../core/ids.js";

export type WorkerDeps = {
  queue: Queue;
  registry: HandlerRegistry;
  idempotency: IdempotencyStore;
  metrics: Metrics;
  config: Config;
  logger: Logger;
};

/**
 * The worker run loop.
 *
 * Per namespace it runs a lease loop; process-wide it runs a delayed→ready
 * promoter and an expired-lease reclaimer. Concurrency is bounded by a
 * semaphore sized to `WORKER_CONCURRENCY` — the worker only leases as many jobs
 * as it has free permits, which is the consumer side of backpressure.
 *
 * Each in-flight job gets a lease-renewal timer firing at
 * `LEASE_TTL_MS * LEASE_RENEW_AT`; if the worker dies, renewals stop, the lease
 * expires, and the reclaimer on another worker picks the job up. That
 * redelivery is safe because handlers with side effects run under
 * `withIdempotency`.
 */
export class Worker {
  private readonly id = workerId();
  private readonly sem: Semaphore;
  private readonly inFlight = new Set<string>();
  private readonly shutdownController = new AbortController();
  private running = false;
  private timers: NodeJS.Timeout[] = [];
  private loops: Promise<void>[] = [];

  constructor(private readonly d: WorkerDeps) {
    this.sem = new Semaphore(d.config.worker.concurrency);
  }

  get consumerName(): string {
    return this.id;
  }

  async start(): Promise<void> {
    this.running = true;
    const { config, logger, queue, metrics } = this.d;

    for (const ns of config.worker.namespaces) {
      await queue.ensureGroup(ns);
      this.loops.push(this.leaseLoop(ns));
    }

    this.timers.push(
      setInterval(() => void this.promoteAll(), config.worker.promoteIntervalMs),
      setInterval(() => void this.reclaimAll(), config.worker.leaseTtlMs),
    );

    metrics.bindDepth(queue, [...config.worker.namespaces]);
    logger.info({ worker: this.id, namespaces: config.worker.namespaces }, "worker started");
  }

  /**
   * Graceful stop: stop leasing new work, wait for in-flight jobs to finish up
   * to `deadlineMs`. Anything still running past the deadline is left alone —
   * its lease will expire and another worker reclaims it. Nothing is acked that
   * didn't complete, so nothing is lost.
   */
  async stop(deadlineMs = 25_000): Promise<void> {
    if (!this.running) return;
    this.running = false;
    this.d.logger.info({ inFlight: this.inFlight.size }, "worker draining");
    this.shutdownController.abort();
    this.timers.forEach(clearInterval);

    await Promise.race([
      Promise.allSettled(this.loops),
      new Promise((r) => setTimeout(r, deadlineMs)),
    ]);

    const start = Date.now();
    while (this.inFlight.size > 0 && Date.now() - start < deadlineMs) {
      await new Promise((r) => setTimeout(r, 100));
    }
    if (this.inFlight.size > 0) {
      this.d.logger.warn({ stuck: this.inFlight.size }, "worker stopped with jobs still in flight; leases will expire");
    } else {
      this.d.logger.info("worker drained cleanly");
    }
  }

  // ---- loops ----

  private async leaseLoop(namespace: string): Promise<void> {
    const { queue, config, logger } = this.d;
    while (this.running) {
      try {
        if (await queue.isPaused(namespace)) {
          await this.sleep(config.worker.idlePollMs);
          continue;
        }
        const capacity = this.sem.available;
        if (capacity === 0) {
          await this.sleep(50);
          continue;
        }
        const batch = await queue.leaseBatch(
          namespace,
          this.id,
          Math.min(capacity, config.worker.batchSize),
          config.worker.idlePollMs,
        );
        for (const job of batch) void this.dispatch(job);
      } catch (err) {
        logger.error({ err, namespace }, "lease loop error");
        await this.sleep(config.worker.idlePollMs);
      }
    }
  }

  private async promoteAll(): Promise<void> {
    if (!this.running) return;
    for (const ns of this.d.config.worker.namespaces) {
      await this.d.queue.promoteDue(ns, this.d.config.worker.batchSize * 8).catch((err) => {
        this.d.logger.error({ err, ns }, "promote failed");
      });
    }
  }

  private async reclaimAll(): Promise<void> {
    if (!this.running) return;
    for (const ns of this.d.config.worker.namespaces) {
      try {
        const reclaimed = await this.d.queue.reclaimExpired(ns, this.id, this.d.config.worker.batchSize);
        for (const job of reclaimed) {
          this.d.metrics.reclaimed.inc({ namespace: ns });
          void this.dispatch(job);
        }
      } catch (err) {
        this.d.logger.error({ err, ns }, "reclaim failed");
      }
    }
  }

  // ---- one job ----

  private async dispatch(job: LeasedJob): Promise<void> {
    const release = await this.sem.acquire();
    this.inFlight.add(job.id);
    this.d.metrics.inFlight.inc({ namespace: job.namespace });
    this.d.metrics.attempts.inc({ namespace: job.namespace, type: job.type });

    this.startLeaseRenewal(job);
    const startedAt = performance.now();
    const log = this.d.logger.child({ jobId: job.id, type: job.type, attempt: job.attempt });

    try {
      await this.runHandler(job, log);
      const durationMs = Math.round(performance.now() - startedAt);
      await this.d.queue.ack(job, durationMs);
      this.record(job, "succeeded", durationMs);
      log.debug({ durationMs }, "job succeeded");
    } catch (err) {
      const durationMs = Math.round(performance.now() - startedAt);
      const error = err instanceof Error ? err : new Error(String(err));
      const permanent = error instanceof NoHandlerError;
      const outcome = await this.d.queue.fail(job, error, durationMs, { permanent });
      this.record(job, outcome, durationMs);
      log.warn({ err: error.message, outcome, durationMs, permanent }, "job failed");
    } finally {
      this.inFlight.delete(job.id);
      const timer = this.renewTimers.get(job.id);
      if (timer) clearTimeout(timer);
      this.renewTimers.delete(job.id);
      this.d.metrics.inFlight.dec({ namespace: job.namespace });
      release();
    }
  }

  private async runHandler(job: LeasedJob, logger: Logger): Promise<void> {
    const handler = this.d.registry.get(job.type);
    if (!handler) throw new NoHandlerError(job.type);

    const ctx: JobContext = { job, attempt: job.attempt, logger, signal: this.shutdownController.signal };

    if (job.idempotencyKey) {
      await withIdempotency(
        this.d.idempotency,
        { key: job.idempotencyKey, namespace: job.namespace, jobId: job.id },
        async () => ((await handler(job.payload, ctx)) ?? null) as JsonValue,
      );
      return;
    }
    await handler(job.payload, ctx);
  }

  private readonly renewTimers = new Map<string, NodeJS.Timeout>();

  private startLeaseRenewal(job: LeasedJob): void {
    const { config, queue, metrics, logger } = this.d;
    const interval = Math.max(1000, Math.floor(config.worker.leaseTtlMs * config.worker.leaseRenewAt));
    const tick = async (): Promise<void> => {
      if (!this.inFlight.has(job.id)) return;
      try {
        await queue.renewLease(job.namespace, this.id, job.streamId);
        metrics.leaseRenewals.inc({ namespace: job.namespace });
      } catch (err) {
        logger.warn({ err, jobId: job.id }, "lease renewal failed");
      }
      if (this.inFlight.has(job.id)) this.renewTimers.set(job.id, setTimeout(() => void tick(), interval));
    };
    this.renewTimers.set(job.id, setTimeout(() => void tick(), interval));
  }

  private record(job: LeasedJob, outcome: string, durationMs: number): void {
    this.d.metrics.processed.inc({ namespace: job.namespace, type: job.type, outcome });
    this.d.metrics.processingSeconds.observe(
      { namespace: job.namespace, type: job.type, outcome },
      durationMs / 1000,
    );
  }

  private sleep(ms: number): Promise<void> {
    return new Promise((r) => setTimeout(r, ms));
  }
}

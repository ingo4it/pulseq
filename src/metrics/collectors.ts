import { Counter, Gauge, Histogram, Registry, collectDefaultMetrics } from "prom-client";
import type { Queue } from "../queue/queue.js";

/**
 * All pulseq metrics. Instantiated once per process against a private registry
 * so tests don't collide on the default global one. The queue-depth gauges are
 * populated by a `collect` callback that queries Redis at scrape time, so they
 * cost nothing between scrapes.
 */
export class Metrics {
  readonly registry = new Registry();

  readonly enqueued = new Counter({
    name: "pulseq_jobs_enqueued_total",
    help: "Jobs enqueued",
    labelNames: ["namespace", "type", "placement"], // placement: ready | delayed
    registers: [this.registry],
  });

  readonly processed = new Counter({
    name: "pulseq_jobs_processed_total",
    help: "Jobs that reached a terminal outcome",
    labelNames: ["namespace", "type", "outcome"], // succeeded | retry_scheduled | dead_lettered | lease_lost
    registers: [this.registry],
  });

  readonly attempts = new Counter({
    name: "pulseq_job_attempts_total",
    help: "Delivery attempts (first tries + retries)",
    labelNames: ["namespace", "type"],
    registers: [this.registry],
  });

  readonly processingSeconds = new Histogram({
    name: "pulseq_job_processing_seconds",
    help: "Handler wall-clock per attempt",
    labelNames: ["namespace", "type", "outcome"],
    buckets: [0.01, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30],
    registers: [this.registry],
  });

  readonly leaseRenewals = new Counter({
    name: "pulseq_lease_renewals_total",
    help: "Lease renewals issued for in-flight jobs",
    labelNames: ["namespace"],
    registers: [this.registry],
  });

  readonly reclaimed = new Counter({
    name: "pulseq_jobs_reclaimed_total",
    help: "Jobs reclaimed from an expired lease (crashed/stalled worker)",
    labelNames: ["namespace"],
    registers: [this.registry],
  });

  readonly inFlight = new Gauge({
    name: "pulseq_worker_in_flight",
    help: "Jobs currently being processed by this worker",
    labelNames: ["namespace"],
    registers: [this.registry],
  });

  readonly depthDelayed = new Gauge({
    name: "pulseq_queue_delayed",
    help: "Delayed set size",
    labelNames: ["namespace"],
    registers: [this.registry],
  });
  readonly depthDlq = new Gauge({
    name: "pulseq_queue_dlq",
    help: "DLQ stream length",
    labelNames: ["namespace"],
    registers: [this.registry],
  });
  readonly depthInFlight = new Gauge({
    name: "pulseq_queue_in_flight",
    help: "Pending (leased, un-acked) entries",
    labelNames: ["namespace"],
    registers: [this.registry],
  });

  /**
   * `bindDepth()` fills this in once the queue + namespace list are known
   * (they aren't yet when the gauges above are constructed). `depthReady`'s
   * `collect` callback, registered below, reads it at scrape time.
   */
  private depthSource: { queue: Queue; namespaces: string[] } | null = null;

  /**
   * Populated by a scrape-time Redis query rather than a background poller —
   * prom-client invokes a gauge's `collect` callback on every `/metrics`
   * request, so depth is always current and costs nothing between scrapes.
   */
  readonly depthReady = new Gauge({
    name: "pulseq_queue_ready",
    help: "Ready stream length",
    labelNames: ["namespace"],
    registers: [this.registry],
    collect: async () => {
      if (!this.depthSource) return;
      const { queue, namespaces } = this.depthSource;
      for (const ns of namespaces) {
        const d = await queue.depth(ns).catch(() => null);
        if (!d) continue;
        this.depthReady.set({ namespace: ns }, d.ready);
        this.depthDelayed.set({ namespace: ns }, d.delayed);
        this.depthDlq.set({ namespace: ns }, d.dlq);
        this.depthInFlight.set({ namespace: ns }, d.inFlight);
      }
    },
  });

  constructor() {
    collectDefaultMetrics({ register: this.registry, prefix: "pulseq_" });
  }

  /** Wire the depth gauges to a queue + namespace list. See `depthSource`. */
  bindDepth(queue: Queue, namespaces: string[]): void {
    this.depthSource = { queue, namespaces };
  }

  async scrape(): Promise<string> {
    return this.registry.metrics();
  }
}

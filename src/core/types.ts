/** Dependency-free domain types shared across queue, worker, admin, metrics. */

export type JsonValue = string | number | boolean | null | JsonValue[] | { [k: string]: JsonValue };

export type EnqueueOptions = {
  /** logical queue; workers subscribe to a set of namespaces */
  namespace?: string;
  /** delay before the job becomes eligible to run */
  delayMs?: number;
  /** per-job retry budget; falls back to the configured default */
  maxAttempts?: number;
  /**
   * If set, the handler runs under `withIdempotency(key, ...)`: a redelivery
   * with the same key returns the first run's result instead of repeating the
   * side effect.
   */
  idempotencyKey?: string;
};

export type NewJob = {
  type: string;
  payload: JsonValue;
} & EnqueueOptions;

/** A job as it travels through Redis (stream entry fields are all strings). */
export type JobEnvelope = {
  id: string;
  namespace: string;
  type: string;
  payload: JsonValue;
  attempt: number;
  maxAttempts: number;
  idempotencyKey?: string;
  enqueuedAt: number;
};

/** A job handed to a worker, plus the handle it needs to ack / fail / renew. */
export type LeasedJob = JobEnvelope & {
  /** Redis stream entry id in the ready stream (the ack target) */
  streamId: string;
  /** consumer name that holds the lease */
  leaseOwner: string;
  /** epoch-ms the lease is currently good until */
  leaseExpiresAt: number;
};

export type AckOutcome = "succeeded" | "retry_scheduled" | "dead_lettered" | "lease_lost";

export type QueueDepth = {
  namespace: string;
  ready: number;
  delayed: number;
  /** entries leased but not yet acked (pending list size) */
  inFlight: number;
  dlq: number;
};

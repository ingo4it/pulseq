# ADR-0001 — Redis streams over a dedicated broker

- Status: Accepted
- Date: 2026-08-06

## Context

pulseq needs a transport with consumer groups, per-message ack, redelivery of
unacked messages, and delayed delivery. Candidates:

1. **Redis streams** (`XADD` / `XREADGROUP` / `XACK` / `XAUTOCLAIM`) plus a ZSET
   for the delayed tier.
2. **A dedicated broker** — RabbitMQ, NATS JetStream, Kafka, SQS.

## Decision

Use **Redis streams**. `ready` is a stream with a `workers` consumer group;
`delayed` is a ZSET scored by `runAt` that a promoter drains into `ready`; `dlq`
is a second stream. Postgres holds the durable job record and the idempotency
ledger.

## Rationale

- **Consumer groups give us the primitives directly.** Pending-entries list =
  in-flight tracking; `min-idle-time` on `XAUTOCLAIM` = visibility timeout;
  re-`XCLAIM` = lease renewal. No feature is simulated.
- **One infrastructure dependency.** The services already need Redis and
  Postgres. A broker is a third system to run, secure, and reason about for a
  queue that targets moderate throughput, not a firehose.
- **Atomicity where it matters via Lua.** "ack the ready entry AND schedule the
  retry (or DLQ it)" is one script — a job is never left both un-acked and
  re-scheduled.
- **Inspectable.** `XLEN`, `XRANGE`, `XPENDING`, `XINFO` make queue state
  trivially observable, which is half of what the admin API and dashboard need.

## Consequences

- Redis is a single point of failure for the hot path. Mitigations: AOF
  persistence, and the Postgres mirror lets state be reconstructed after a Redis
  loss (a reconciler is on the roadmap).
- No built-in exactly-once, no transactions across Redis + Postgres — hence
  ADR-0002 (at-least-once + idempotency).
- Very high fan-out or multi-MB payloads would outgrow this design; payloads are
  kept small (a reference, not a blob) and Kafka is the escape hatch if
  throughput demands it.
- Stream trimming is our responsibility — acked entries are `XDEL`'d
  immediately; the DLQ is bounded by operator replay/purge.

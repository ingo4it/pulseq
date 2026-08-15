# ADR-0003 — Visibility timeout via lease renewal

- Status: Accepted
- Date: 2026-08-10

## Context

When a worker leases a job, other workers must not also run it. But if that
worker dies, the job has to become available again. The classic tension: a
**fixed visibility timeout** must be longer than the slowest job (or a slow job
gets double-run) *and* short enough that a crash is recovered quickly — and
those pull in opposite directions.

## Decision

A **short base lease** (`LEASE_TTL_MS`, default 30s) that the owning worker
**renews** while the job is still running. Renewal fires at
`LEASE_TTL_MS * LEASE_RENEW_AT` (default 15s) and resets the entry's idle clock
(`XCLAIM ... JUSTID`). Reclaim is `XAUTOCLAIM` with `min-idle-time =
LEASE_TTL_MS`: an entry only gets reclaimed if it has gone a full TTL with no
renewal — i.e. its worker stopped renewing, i.e. it crashed or stalled.

## Rationale

- **Decouples "how long can a job run" from "how fast do we detect a crash".** A
  10-minute job renews 40 times; a crashed worker's jobs are recoverable within
  ~30s regardless.
- Uses Redis stream primitives directly — the PEL idle time *is* the lease
  clock, `XCLAIM` *is* renewal, `XAUTOCLAIM` *is* reclaim. Nothing is
  reimplemented.
- Renewal failure is a useful signal: if a worker can't reach Redis to renew, it
  has probably also lost the ability to finish safely, and letting the lease
  lapse is the right outcome.

## Consequences

- Every in-flight job carries a renewal timer. At high concurrency that's many
  timers and periodic `XCLAIM` calls — cheap, but non-zero, and it's why the
  worker keeps a non-blocking Redis connection separate from the blocking lease
  read.
- A worker that is alive but wedged (event loop blocked) keeps its lease as long
  as the timer somehow fires; a true hang stops renewals and the job is
  reclaimed. Pathological partial hangs are not fully covered.
- Reclaimed jobs are redelivered, so this design only works alongside ADR-0002.
- Clock skew between workers matters only within one `LEASE_TTL_MS` window;
  renewal at half-TTL leaves ample margin.

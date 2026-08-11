# ADR-0002 — At-least-once + idempotency, not exactly-once

- Status: Accepted
- Date: 2026-08-08

## Context

"Exactly-once delivery" is the feature everyone asks for and nobody can give
you: the worker can always crash in the gap between "side effect committed" and
"ack sent", and no amount of protocol removes that window when the side effect
lands in a system the queue doesn't control.

## Decision

pulseq is **at-least-once**. A job may be delivered more than once (crash before
ack, lease expiry, reclaim). Consumers that have side effects make themselves
safe by wrapping the handler in `withIdempotency(key, fn)`, backed by a Postgres
ledger:

- first delivery → row inserted `IN_PROGRESS`, handler runs, row flipped to
  `COMPLETED` with the recorded result
- redelivery after success → handler skipped, stored result returned
- redelivery while another is still running → `ConcurrentDeliveryError`, treated
  as a transient failure and retried
- handler throws → ledger row deleted so the retry starts clean

## Rationale

- Moves the correctness boundary to where it can actually be enforced: the
  consumer's own database, in the same transaction as the side effect if the
  consumer wants that.
- The queue stays simple and fast — no two-phase commit, no dedup window to
  size, no distributed transaction across Redis and Postgres.
- Idempotency is opt-in per job. A naturally idempotent handler (`PUT`, "set
  state to X") pays nothing; only handlers that need it carry the ledger write.

## Consequences

- Handlers without a key **will** double-execute on redelivery. That's
  documented loudly; the `demo.process` handler and the README make the
  contract explicit.
- The ledger is another table to sweep — entries carry a TTL and a periodic
  `sweep()` deletes expired ones.
- The "in progress" branch can bounce a redelivery a few times against a slow
  first run; backoff absorbs it, and a crashed first run is taken over after
  `staleAfterMs`.

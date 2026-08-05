/**
 * Exponential backoff with jitter for retry scheduling.
 *
 * base = `baseMs * 2^attempt`, clamped to `capMs`. Jitter then spreads retries
 * so a batch of jobs that failed together against a downed dependency don't all
 * retry in lockstep and hammer it the instant it recovers (the "thundering
 * herd"). Strategies follow the AWS Architecture Blog's "Exponential Backoff
 * And Jitter":
 *
 *   none  : exactly `base`            (deterministic; only for tests / demos)
 *   full  : random in [0, base]       (max spread, lowest contention)
 *   equal : base/2 + random(0, base/2) (spread, but keeps a sane minimum wait)
 */
export type JitterStrategy = "full" | "equal" | "none";

export type BackoffOptions = {
  baseMs: number;
  capMs: number;
  jitter: JitterStrategy;
};

export function backoffDelayMs(
  attempt: number,
  opts: BackoffOptions,
  rand: () => number = Math.random,
): number {
  const exp = Math.min(opts.capMs, opts.baseMs * 2 ** Math.max(0, attempt));
  switch (opts.jitter) {
    case "none":
      return Math.round(exp);
    case "full":
      return Math.round(rand() * exp);
    case "equal":
      return Math.round(exp / 2 + rand() * (exp / 2));
  }
}

/** Absolute epoch-ms at which the next attempt should run. */
export function nextRunAt(attempt: number, now: number, opts: BackoffOptions, rand?: () => number): number {
  return now + backoffDelayMs(attempt, opts, rand);
}

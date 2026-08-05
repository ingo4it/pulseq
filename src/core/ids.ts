import { randomUUID } from "node:crypto";
import { hostname } from "node:os";

export const newJobId = (): string => randomUUID();

/**
 * Stable-ish consumer name for a worker process:  host:pid:short-random.
 * Used as the Redis consumer-group consumer name and as the lease owner, so
 * XAUTOCLAIM can tell "my own stale entry" from "another worker's".
 */
export function workerId(): string {
  return `${hostname()}:${process.pid}:${randomUUID().slice(0, 8)}`;
}

import { Redis } from "ioredis";

/**
 * A worker needs two connections: one dedicated to the blocking `XREADGROUP`
 * call (which parks the connection until a job arrives or the timeout fires)
 * and one for everything else — acks, Lua scripts, metrics reads. Sharing one
 * connection would stall all commands behind the block.
 */
export function createRedis(url: string): Redis {
  return new Redis(url, { maxRetriesPerRequest: null, enableAutoPipelining: true });
}

export type { Redis };

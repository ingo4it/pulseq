import type { Redis } from "ioredis";

/**
 * The queue's atomic operations. Each is a single Lua script so a step can't be
 * half-applied if the worker or Redis dies mid-operation — e.g. `fail_job` acks
 * the ready entry AND schedules the retry (or DLQs it) in one round trip, so a
 * job can never be both un-acked and re-scheduled.
 *
 * Envelopes are stored as a single JSON string: in the `env` field of a ready
 * stream entry, and as the member of the delayed ZSET.
 */

// KEYS[1]=ready  KEYS[2]=delayed
// ARGV[1]=runAt  ARGV[2]=now  ARGV[3]=envelopeJson
const ENQUEUE = `
local runAt = tonumber(ARGV[1])
local now   = tonumber(ARGV[2])
if runAt <= now then
  redis.call('XADD', KEYS[1], '*', 'env', ARGV[3])
  return 'ready'
else
  redis.call('ZADD', KEYS[2], runAt, ARGV[3])
  return 'delayed'
end
`;

// KEYS[1]=ready  KEYS[2]=delayed
// ARGV[1]=now  ARGV[2]=limit
// Moves up to `limit` due jobs from delayed -> ready. Returns count moved.
const PROMOTE_DELAYED = `
local now  = tonumber(ARGV[1])
local lim  = tonumber(ARGV[2])
local due  = redis.call('ZRANGEBYSCORE', KEYS[2], '-inf', now, 'LIMIT', 0, lim)
for _, env in ipairs(due) do
  redis.call('XADD', KEYS[1], '*', 'env', env)
  redis.call('ZREM', KEYS[2], env)
end
return #due
`;

// KEYS[1]=ready  KEYS[2]=delayed  KEYS[3]=dlq
// ARGV[1]=group  ARGV[2]=streamId  ARGV[3]=nextRunAt  ARGV[4]=isDead(0|1)
// ARGV[5]=envelopeJson  ARGV[6]=reason
const FAIL_JOB = `
redis.call('XACK', KEYS[1], ARGV[1], ARGV[2])
redis.call('XDEL', KEYS[1], ARGV[2])
if ARGV[4] == '1' then
  redis.call('XADD', KEYS[3], '*', 'env', ARGV[5], 'reason', ARGV[6], 'deadAt', ARGV[3])
  return 'dead_lettered'
else
  redis.call('ZADD', KEYS[2], tonumber(ARGV[3]), ARGV[5])
  return 'retry_scheduled'
end
`;

// KEYS[1]=ready  ARGV[1]=group  ARGV[2]=streamId
const ACK_JOB = `
redis.call('XACK', KEYS[1], ARGV[1], ARGV[2])
redis.call('XDEL', KEYS[1], ARGV[2])
return 1
`;

// KEYS[1]=dlq  KEYS[2]=ready
// ARGV[1]=count  — pop up to N oldest DLQ entries back onto ready.
// Returns the list of job ids moved, so the caller can flip their PG state.
const REPLAY_DLQ = `
local n = tonumber(ARGV[1])
local entries = redis.call('XRANGE', KEYS[1], '-', '+', 'COUNT', n)
local moved = {}
for _, e in ipairs(entries) do
  local id = e[1]
  local fields = e[2]
  local env = nil
  for i = 1, #fields, 2 do
    if fields[i] == 'env' then env = fields[i+1] end
  end
  if env ~= nil then
    redis.call('XADD', KEYS[2], '*', 'env', env)
    redis.call('XDEL', KEYS[1], id)
    local ok, decoded = pcall(cjson.decode, env)
    if ok and decoded.id then moved[#moved + 1] = decoded.id end
  end
end
return moved
`;

export type QueueScripts = {
  enqueue(ready: string, delayed: string, runAt: number, now: number, env: string): Promise<string>;
  promoteDelayed(ready: string, delayed: string, now: number, limit: number): Promise<number>;
  failJob(
    ready: string,
    delayed: string,
    dlq: string,
    group: string,
    streamId: string,
    nextRunAt: number,
    isDead: boolean,
    env: string,
    reason: string,
  ): Promise<string>;
  ackJob(ready: string, group: string, streamId: string): Promise<number>;
  replayDlq(dlq: string, ready: string, count: number): Promise<string[]>;
};

type WithScripts = Redis & {
  pq_enqueue(k1: string, k2: string, a1: string, a2: string, a3: string): Promise<string>;
  pq_promote(k1: string, k2: string, a1: string, a2: string): Promise<number>;
  pq_fail(
    k1: string,
    k2: string,
    k3: string,
    a1: string,
    a2: string,
    a3: string,
    a4: string,
    a5: string,
    a6: string,
  ): Promise<string>;
  pq_ack(k1: string, a1: string, a2: string): Promise<number>;
  pq_replay(k1: string, k2: string, a1: string): Promise<string[]>;
};

export function registerScripts(redis: Redis): QueueScripts {
  const r = redis as WithScripts;
  r.defineCommand("pq_enqueue", { numberOfKeys: 2, lua: ENQUEUE });
  r.defineCommand("pq_promote", { numberOfKeys: 2, lua: PROMOTE_DELAYED });
  r.defineCommand("pq_fail", { numberOfKeys: 3, lua: FAIL_JOB });
  r.defineCommand("pq_ack", { numberOfKeys: 1, lua: ACK_JOB });
  r.defineCommand("pq_replay", { numberOfKeys: 2, lua: REPLAY_DLQ });

  return {
    enqueue: (ready, delayed, runAt, now, env) =>
      r.pq_enqueue(ready, delayed, String(runAt), String(now), env),
    promoteDelayed: (ready, delayed, now, limit) =>
      r.pq_promote(ready, delayed, String(now), String(limit)),
    failJob: (ready, delayed, dlq, group, streamId, nextRunAt, isDead, env, reason) =>
      r.pq_fail(ready, delayed, dlq, group, streamId, String(nextRunAt), isDead ? "1" : "0", env, reason),
    ackJob: (ready, group, streamId) => r.pq_ack(ready, group, streamId),
    replayDlq: (dlq, ready, count) => r.pq_replay(dlq, ready, String(count)),
  };
}

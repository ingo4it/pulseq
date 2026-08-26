import { loadConfig } from "../config.js";
import { createLogger } from "../logger.js";
import { createRedis } from "../db/redis.js";
import { createPrisma } from "../db/client.js";
import { Queue } from "../queue/queue.js";
import { HandlerRegistry } from "../worker/handler.js";
import { registerDefaultHandlers } from "../handlers/index.js";
import { IdempotencyStore } from "../idempotency/store.js";
import { Metrics } from "../metrics/collectors.js";
import { startMetricsServer } from "../metrics/server.js";
import { Worker } from "../worker/worker.js";
import { installShutdown } from "../worker/shutdown.js";

const config = loadConfig();
const logger = createLogger(config, { role: "worker" });

// two Redis connections: one parked on the blocking lease read, one for
// acks / Lua scripts / metrics
const redis = createRedis(config.redisUrl);
const blockingRedis = createRedis(config.redisUrl);
const prisma = createPrisma(config.databaseUrl);

const queue = new Queue({
  redis,
  blockingRedis,
  prisma,
  logger,
  defaultMaxAttempts: config.retry.defaultMaxAttempts,
  leaseTtlMs: config.worker.leaseTtlMs,
  backoff: {
    baseMs: config.retry.backoffBaseMs,
    capMs: config.retry.backoffCapMs,
    jitter: config.retry.jitter,
  },
});

const registry = registerDefaultHandlers(new HandlerRegistry());
const idempotency = new IdempotencyStore(prisma);
const metrics = new Metrics();

const worker = new Worker({ queue, registry, idempotency, metrics, config, logger });
const metricsServer = startMetricsServer(config.metricsPort, metrics, logger);

const idemSweep = setInterval(() => {
  void idempotency.sweep().then((n) => n > 0 && logger.debug({ removed: n }, "idempotency sweep"));
}, 60_000);
idemSweep.unref();

installShutdown(logger, [
  () => worker.stop(),
  async () => clearInterval(idemSweep),
  async () => void metricsServer.close(),
  async () => void blockingRedis.disconnect(),
  async () => void redis.disconnect(),
  () => prisma.$disconnect(),
]);

await worker.start();
logger.info("worker up");

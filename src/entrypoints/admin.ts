import { loadConfig } from "../config.js";
import { createLogger } from "../logger.js";
import { createRedis } from "../db/redis.js";
import { createPrisma } from "../db/client.js";
import { Queue } from "../queue/queue.js";
import { BackpressureGate } from "../worker/backpressure.js";
import { buildAdminServer } from "../admin/server.js";
import { installShutdown } from "../worker/shutdown.js";

const config = loadConfig();
const logger = createLogger(config, { role: "admin" });

const redis = createRedis(config.redisUrl);
const prisma = createPrisma(config.databaseUrl);

const queue = new Queue({
  redis,
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

const backpressure = new BackpressureGate(
  queue,
  config.backpressure.depthHighWatermark,
  config.backpressure.depthLowWatermark,
);

const app = await buildAdminServer(logger, {
  prisma,
  queue,
  backpressure,
  namespaces: [...config.worker.namespaces],
});

installShutdown(logger, [() => app.close(), async () => void redis.disconnect(), () => prisma.$disconnect()]);

await app.listen({ host: "0.0.0.0", port: config.adminPort });
logger.info({ port: config.adminPort }, "admin API up");

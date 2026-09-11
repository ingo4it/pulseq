import { z } from "zod";

/** Parsed once at boot, frozen. Both entrypoints (worker, admin) load this. */
const schema = z.object({
  nodeEnv: z.enum(["development", "test", "production"]).default("development"),
  logLevel: z.enum(["fatal", "error", "warn", "info", "debug", "trace"]).default("info"),

  redisUrl: z.string().url(),
  databaseUrl: z.string().url(),

  worker: z.object({
    namespaces: z
      .string()
      .default("default")
      .transform((s) =>
        s
          .split(",")
          .map((v) => v.trim())
          .filter(Boolean),
      ),
    concurrency: z.coerce.number().int().positive().default(16),
    batchSize: z.coerce.number().int().positive().default(8),
    leaseTtlMs: z.coerce.number().int().positive().default(30_000),
    leaseRenewAt: z.coerce.number().min(0.1).max(0.9).default(0.5),
    idlePollMs: z.coerce.number().int().positive().default(1000),
    promoteIntervalMs: z.coerce.number().int().positive().default(1000),
  }),

  retry: z.object({
    defaultMaxAttempts: z.coerce.number().int().positive().default(5),
    backoffBaseMs: z.coerce.number().int().positive().default(1000),
    backoffCapMs: z.coerce.number().int().positive().default(300_000),
    jitter: z.enum(["full", "equal", "none"]).default("equal"),
  }),

  backpressure: z.object({
    depthHighWatermark: z.coerce.number().int().positive().default(10_000),
    depthLowWatermark: z.coerce.number().int().positive().default(2000),
  }),

  adminPort: z.coerce.number().int().positive().default(7411),
  metricsPort: z.coerce.number().int().positive().default(9464),
});

export type Config = z.infer<typeof schema>;

export function loadConfig(env: NodeJS.ProcessEnv = process.env): Config {
  const parsed = schema.safeParse({
    nodeEnv: env.NODE_ENV,
    logLevel: env.LOG_LEVEL,
    redisUrl: env.REDIS_URL,
    databaseUrl: env.DATABASE_URL,
    worker: {
      namespaces: env.WORKER_NAMESPACES,
      concurrency: env.WORKER_CONCURRENCY,
      batchSize: env.WORKER_BATCH_SIZE,
      leaseTtlMs: env.LEASE_TTL_MS,
      leaseRenewAt: env.LEASE_RENEW_AT,
      idlePollMs: env.IDLE_POLL_MS,
      promoteIntervalMs: env.PROMOTE_INTERVAL_MS,
    },
    retry: {
      defaultMaxAttempts: env.DEFAULT_MAX_ATTEMPTS,
      backoffBaseMs: env.BACKOFF_BASE_MS,
      backoffCapMs: env.BACKOFF_CAP_MS,
      jitter: env.BACKOFF_JITTER,
    },
    backpressure: {
      depthHighWatermark: env.DEPTH_HIGH_WATERMARK,
      depthLowWatermark: env.DEPTH_LOW_WATERMARK,
    },
    adminPort: env.ADMIN_PORT,
    metricsPort: env.METRICS_PORT,
  });

  if (!parsed.success) {
    const issues = parsed.error.issues
      .map((i) => `  - ${i.path.join(".") || "(root)"}: ${i.message}`)
      .join("\n");
    throw new Error(`Invalid configuration:\n${issues}`);
  }
  return Object.freeze(parsed.data);
}

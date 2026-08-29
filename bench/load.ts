/**
 * Load harness.  pnpm bench [--preset demo|bench] [--jobs N] [--fail-rate 0.2]
 *                           [--poison-rate 0.02] [--namespace default] [--out file]
 *
 * Enqueues a burst of `demo.process` jobs with an injected downstream failure
 * rate (so the retry and DLQ paths are actually exercised), waits for the queue
 * to drain, then reports throughput, latency percentiles, retry rate and DLQ
 * rate from the Postgres job/attempt records.
 *
 * Needs Redis, Postgres, and at least one worker running (`pnpm dev:worker`).
 * Results are NOT committed until measured on real hardware — `results/latest.json`
 * ships as a placeholder.
 */
import { writeFile } from "node:fs/promises";
import { loadConfig } from "../src/config.js";
import { createLogger } from "../src/logger.js";
import { createRedis } from "../src/db/redis.js";
import { createPrisma } from "../src/db/client.js";
import { Queue } from "../src/queue/queue.js";
import type { JsonValue } from "../src/core/types.js";

type Preset = { jobs: number; failRate: number; poisonRate: number; workMs: number };
const PRESETS: Record<string, Preset> = {
  demo: { jobs: 500, failRate: 0.15, poisonRate: 0.01, workMs: 20 },
  bench: { jobs: 20_000, failRate: 0.2, poisonRate: 0.02, workMs: 15 },
};

async function main(): Promise<void> {
  const args = process.argv.slice(2);
  const preset = PRESETS[flag(args, "--preset") ?? "demo"] ?? PRESETS.demo!;
  const jobs = Number(flag(args, "--jobs") ?? preset.jobs);
  const failRate = Number(flag(args, "--fail-rate") ?? preset.failRate);
  const poisonRate = Number(flag(args, "--poison-rate") ?? preset.poisonRate);
  const namespace = flag(args, "--namespace") ?? "default";
  const outPath = flag(args, "--out");

  const config = loadConfig();
  const logger = createLogger({ ...config, logLevel: "warn" });
  const redis = createRedis(config.redisUrl);
  const prisma = createPrisma(config.databaseUrl);
  const queue = new Queue({
    redis,
    prisma,
    logger,
    defaultMaxAttempts: config.retry.defaultMaxAttempts,
    leaseTtlMs: config.worker.leaseTtlMs,
    backoff: { baseMs: config.retry.backoffBaseMs, capMs: config.retry.backoffCapMs, jitter: config.retry.jitter },
  });

  console.log(`enqueuing ${jobs} jobs (failRate=${failRate}, poisonRate=${poisonRate}) into "${namespace}"…`);
  const runTag = `bench-${Date.now()}`;
  const enqueueStart = performance.now();

  for (let i = 0; i < jobs; i++) {
    const payload: JsonValue = {
      runTag,
      workMs: preset.workMs,
      failRate,
      poison: Math.random() < poisonRate,
    };
    await queue.enqueue({ namespace, type: "demo.process", payload });
  }
  const enqueueMs = performance.now() - enqueueStart;
  console.log(`enqueued in ${Math.round(enqueueMs)}ms; waiting for drain…`);

  const drainStart = performance.now();
  await waitForDrain(queue, namespace);
  const drainMs = performance.now() - drainStart;

  // pull the settled records for this run
  const settled = await prisma.job.findMany({
    where: { namespace, payload: { path: ["runTag"], equals: runTag } },
    select: { state: true, enqueuedAt: true, finishedAt: true, attempt: true },
  });
  const succeeded = settled.filter((j) => j.state === "SUCCEEDED" && j.finishedAt);
  const dead = settled.filter((j) => j.state === "DEAD");
  const latencies = succeeded
    .map((j) => j.finishedAt!.getTime() - j.enqueuedAt.getTime())
    .sort((a, b) => a - b);
  const totalAttempts = settled.reduce((s, j) => s + j.attempt, 0);

  const report = {
    generatedAt: new Date().toISOString(),
    preset: flag(args, "--preset") ?? "demo",
    input: { jobs, failRate, poisonRate, namespace },
    throughputJobsPerSec: round((succeeded.length / drainMs) * 1000),
    enqueueRateJobsPerSec: round((jobs / enqueueMs) * 1000),
    latencyMs: {
      p50: percentile(latencies, 0.5),
      p95: percentile(latencies, 0.95),
      p99: percentile(latencies, 0.99),
      max: latencies.at(-1) ?? 0,
    },
    retryRate: round((totalAttempts - settled.length) / Math.max(1, settled.length)),
    dlqRate: round(dead.length / Math.max(1, settled.length)),
    settled: settled.length,
    succeeded: succeeded.length,
    dead: dead.length,
  };

  console.table({
    throughput: `${report.throughputJobsPerSec} jobs/s`,
    "p50 / p95 / p99": `${report.latencyMs.p50} / ${report.latencyMs.p95} / ${report.latencyMs.p99} ms`,
    "retry rate": report.retryRate,
    "dlq rate": report.dlqRate,
  });

  if (outPath) {
    await writeFile(outPath, JSON.stringify(report, null, 2) + "\n");
    console.log(`wrote ${outPath}`);
  }

  await redis.quit();
  await prisma.$disconnect();
}

async function waitForDrain(queue: Queue, ns: string): Promise<void> {
  let quietTicks = 0;
  for (;;) {
    const d = await queue.depth(ns);
    const outstanding = d.ready + d.delayed + d.inFlight;
    quietTicks = outstanding === 0 ? quietTicks + 1 : 0;
    if (quietTicks >= 3) return; // stable-empty for ~3s
    await new Promise((r) => setTimeout(r, 1000));
  }
}

const flag = (args: string[], name: string): string | undefined => {
  const i = args.indexOf(name);
  return i >= 0 ? args[i + 1] : undefined;
};
const round = (n: number) => Math.round(n * 1e4) / 1e4;
function percentile(sortedAsc: number[], p: number): number {
  if (sortedAsc.length === 0) return 0;
  return sortedAsc[Math.min(sortedAsc.length - 1, Math.floor(p * sortedAsc.length))]!;
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});

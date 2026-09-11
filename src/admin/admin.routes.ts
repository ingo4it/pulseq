import type { FastifyInstance } from "fastify";
import type { PrismaClient } from "@prisma/client";
import type { Queue } from "../queue/queue.js";
import type { BackpressureGate } from "../worker/backpressure.js";
import type { JsonValue } from "../core/types.js";
import { jobIdParam, namespaceParam, replayBody } from "./admin.schema.js";

/**
 * Operator API: inspect depth, pause/resume a namespace, replay the DLQ, and
 * chase a single job's history. Read paths hit Postgres (durable, queryable);
 * mutating paths go through the `Queue` so Redis stays authoritative.
 */
export type AdminDeps = {
  prisma: PrismaClient;
  queue: Queue;
  backpressure: BackpressureGate;
  namespaces: string[];
};

export async function adminRoutes(app: FastifyInstance, deps: AdminDeps): Promise<void> {
  const { prisma, queue, backpressure, namespaces } = deps;

  app.get("/healthz", async () => ({ status: "ok" }));

  app.get("/v1/stats", async () => {
    const perNamespace = await Promise.all(
      namespaces.map(async (ns) => ({
        namespace: ns,
        depth: await queue.depth(ns),
        backpressure: await backpressure.check(ns),
        paused: await queue.isPaused(ns),
      })),
    );
    const byState = await prisma.job.groupBy({ by: ["state"], _count: { _all: true } });
    return {
      namespaces: perNamespace,
      jobsByState: Object.fromEntries(byState.map((r) => [r.state, r._count._all])),
    };
  });

  app.get("/v1/namespaces/:ns/depth", async (request) => {
    const { ns } = namespaceParam.parse(request.params);
    return queue.depth(ns);
  });

  app.post("/v1/namespaces/:ns/pause", async (request) => {
    const { ns } = namespaceParam.parse(request.params);
    await queue.pause(ns);
    return { namespace: ns, paused: true };
  });

  app.post("/v1/namespaces/:ns/resume", async (request) => {
    const { ns } = namespaceParam.parse(request.params);
    await queue.resume(ns);
    return { namespace: ns, paused: false };
  });

  app.post("/v1/namespaces/:ns/dlq/replay", async (request) => {
    const { ns } = namespaceParam.parse(request.params);
    const { count } = replayBody.parse(request.body ?? {});
    const moved = await queue.replayDlq(ns, count);
    return { namespace: ns, replayed: moved };
  });

  app.get("/v1/jobs/:id", async (request, reply) => {
    const { id } = jobIdParam.parse(request.params);
    const job = await prisma.job.findUnique({
      where: { id },
      include: { attempts: { orderBy: { attempt: "asc" } } },
    });
    if (!job) return reply.status(404).send({ code: "not_found", title: "job not found" });
    return job;
  });

  app.post("/v1/jobs/:id/retry", async (request, reply) => {
    const { id } = jobIdParam.parse(request.params);
    const job = await prisma.job.findUnique({ where: { id } });
    if (!job) return reply.status(404).send({ code: "not_found", title: "job not found" });
    if (job.state !== "DEAD" && job.state !== "FAILED") {
      return reply.status(409).send({ code: "not_retryable", title: `job is ${job.state}` });
    }
    const { id: newId } = await queue.enqueue({
      namespace: job.namespace,
      type: job.type,
      payload: job.payload as unknown as JsonValue,
      maxAttempts: job.maxAttempts,
      idempotencyKey: job.idempotencyKey ?? undefined,
    });
    await prisma.job.update({
      where: { id },
      data: { state: "SUCCEEDED", lastError: `superseded by ${newId}` },
    });
    return { retriedAs: newId };
  });
}

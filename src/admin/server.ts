import Fastify, { type FastifyInstance } from "fastify";
import cors from "@fastify/cors";
import { ZodError } from "zod";
import type { Logger } from "../logger.js";
import { adminRoutes, type AdminDeps } from "./admin.routes.js";

/** Builds the admin HTTP app. Pure — caller owns `listen` and shutdown. */
export async function buildAdminServer(logger: Logger, deps: AdminDeps): Promise<FastifyInstance> {
  const app = Fastify({ logger, trustProxy: true });
  await app.register(cors, { origin: false });

  app.setErrorHandler((err, _req, reply) => {
    if (err instanceof ZodError) {
      return reply.status(422).send({
        code: "validation_failed",
        title: "bad request",
        detail: err.issues.map((i) => `${i.path.join(".")}: ${i.message}`).join("; "),
      });
    }
    reply.log.error({ err }, "admin error");
    return reply.status(500).send({ code: "internal", title: "Internal Server Error" });
  });

  await app.register(async (instance) => {
    await adminRoutes(instance, deps);
  });
  await app.ready();
  return app;
}

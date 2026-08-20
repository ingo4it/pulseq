import { createServer, type Server } from "node:http";
import type { Logger } from "../logger.js";
import type { Metrics } from "./collectors.js";

/**
 * Tiny dedicated HTTP server for `/metrics` (and `/healthz`). Kept separate
 * from the admin API so Prometheus scraping and operator traffic don't share a
 * port, and so a worker with no admin API still exposes metrics. Port 9464
 * matches the container probe in the `groundwork` infra repo.
 */
export function startMetricsServer(port: number, metrics: Metrics, logger: Logger): Server {
  const server = createServer((req, res) => {
    if (req.url === "/healthz") {
      res.writeHead(200, { "content-type": "text/plain" }).end("ok");
      return;
    }
    if (req.url === "/metrics") {
      metrics
        .scrape()
        .then((body) => res.writeHead(200, { "content-type": metrics.registry.contentType }).end(body))
        .catch((err) => {
          logger.error({ err }, "metrics scrape failed");
          res.writeHead(500).end();
        });
      return;
    }
    res.writeHead(404).end();
  });

  server.listen(port, () => logger.info({ port }, "metrics server listening"));
  return server;
}

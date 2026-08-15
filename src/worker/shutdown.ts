import type { Logger } from "../logger.js";

/**
 * SIGTERM/SIGINT → run `steps` in order (stop the worker, then close Redis and
 * Postgres), with a hard deadline so the process always exits. The worker's own
 * `stop()` does the job draining; this just sequences the teardown.
 */
export function installShutdown(logger: Logger, steps: Array<() => Promise<void>>, deadlineMs = 30_000): void {
  let shuttingDown = false;

  const run = async (signal: string): Promise<void> => {
    if (shuttingDown) return;
    shuttingDown = true;
    logger.info({ signal }, "shutdown starting");

    const kill = setTimeout(() => {
      logger.error("shutdown deadline exceeded — forcing exit");
      process.exit(1);
    }, deadlineMs);
    kill.unref();

    try {
      for (const step of steps) await step();
      logger.info("shutdown complete");
      process.exit(0);
    } catch (err) {
      logger.error({ err }, "shutdown error");
      process.exit(1);
    }
  };

  for (const sig of ["SIGTERM", "SIGINT"] as const) process.once(sig, () => void run(sig));
  process.on("unhandledRejection", (reason) => logger.error({ reason }, "unhandledRejection"));
}

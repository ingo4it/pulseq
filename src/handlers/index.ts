import type { HandlerRegistry } from "../worker/handler.js";
import { echoHandler } from "./echo.js";
import { demoProcessHandler } from "./demo-process.js";

/**
 * The default handler set. A real deployment registers its own domain handlers
 * here (or builds its own registry). `echo` and `demo.process` exist for the
 * getting-started walkthrough and the load harness.
 */
export function registerDefaultHandlers(registry: HandlerRegistry): HandlerRegistry {
  return registry.register("echo", echoHandler).register("demo.process", demoProcessHandler);
}

import { pino, type Logger } from "pino";
import type { Config } from "./config.js";

export function createLogger(config: Config, bindings: Record<string, unknown> = {}): Logger {
  return pino({
    level: config.logLevel,
    formatters: { level: (label) => ({ level: label }) },
    base: { service: "pulseq", env: config.nodeEnv, ...bindings },
    timestamp: pino.stdTimeFunctions.isoTime,
  });
}

export type { Logger };

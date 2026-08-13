import type { Logger } from "../logger.js";
import type { JsonValue, LeasedJob } from "../core/types.js";

export type JobContext = {
  job: LeasedJob;
  attempt: number;
  logger: Logger;
  /** aborted when the worker is shutting down — long handlers should check it */
  signal: AbortSignal;
};

/** A handler returns a JSON result (recorded for idempotent jobs) or nothing. */
export type JobHandler = (payload: JsonValue, ctx: JobContext) => Promise<JsonValue | void>;

export class HandlerRegistry {
  private readonly handlers = new Map<string, JobHandler>();

  register(type: string, handler: JobHandler): this {
    if (this.handlers.has(type)) throw new Error(`handler for "${type}" already registered`);
    this.handlers.set(type, handler);
    return this;
  }

  get(type: string): JobHandler | undefined {
    return this.handlers.get(type);
  }

  types(): string[] {
    return [...this.handlers.keys()];
  }
}

/** A missing handler is a permanent failure — retrying won't conjure one. */
export class NoHandlerError extends Error {
  constructor(type: string) {
    super(`no handler registered for job type "${type}"`);
    this.name = "NoHandlerError";
  }
}

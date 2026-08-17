import type { JobHandler } from "../worker/handler.js";

/** Returns its payload. Useful for smoke-testing the pipeline end to end. */
export const echoHandler: JobHandler = async (payload) => payload;

import { z } from "zod";

export const namespaceParam = z.object({ ns: z.string().min(1) });
export const jobIdParam = z.object({ id: z.string().uuid() });
export const replayBody = z.object({ count: z.coerce.number().int().min(1).max(1000).default(100) });

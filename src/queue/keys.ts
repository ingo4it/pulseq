/**
 * Redis key layout, one set per namespace:
 *
 *   pulseq:{ns}:ready     XSTREAM  — jobs eligible to run now; consumer group
 *                                    `workers` leases from here
 *   pulseq:{ns}:delayed   ZSET     — member = job envelope JSON, score = runAt
 *                                    epoch-ms; the promoter moves due entries
 *                                    into `ready`
 *   pulseq:{ns}:dlq       XSTREAM  — jobs that exhausted their attempt budget
 *   pulseq:{ns}:paused    STRING   — presence means "stop leasing this ns"
 */
export const CONSUMER_GROUP = "workers";

const prefix = (ns: string) => `pulseq:${ns}`;

export const readyStream = (ns: string) => `${prefix(ns)}:ready`;
export const delayedZset = (ns: string) => `${prefix(ns)}:delayed`;
export const dlqStream = (ns: string) => `${prefix(ns)}:dlq`;
export const pausedFlag = (ns: string) => `${prefix(ns)}:paused`;

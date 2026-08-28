import { dual } from "effect/Function";

/**
 * Stable engine-owned names written by the Absurd workflow adapter.
 *
 * These identifiers are a durable compatibility commitment: workflows may
 * resume under a newer worker long after an older worker persisted them.
 * Change names by adding backward-compatible readers, never by silently
 * renaming an existing identifier.
 */
const namespace = "$absurd:effect:v1";
const identifier = (...segments: ReadonlyArray<string | number>): string =>
  [namespace, ...segments].join(":");

export const activityCheckpointName: {
  (attempt: number): (name: string) => string;
  (name: string, attempt: number): string;
} = dual(2, (name: string, attempt: number): string => identifier("activity", name, attempt));

export const deferredCheckpointName = (name: string): string => identifier("deferred", name);

export const clockDeadlineCheckpointName = (deferredName: string): string =>
  identifier("clock", deferredName);

export const interruptCheckpointName = identifier("interrupt");

export const parentExecutionHeaderKey = identifier("parent");

export const deferredEventName: {
  (executionId: string, deferredName: string): (workflowTag: string) => string;
  (workflowTag: string, executionId: string, deferredName: string): string;
} = dual(3, (workflowTag: string, executionId: string, deferredName: string): string =>
  identifier("deferred-event", workflowTag, executionId, deferredName),
);

export interface DoorbellNames {
  readonly checkpointName: string;
  readonly eventName: string;
}

export const doorbellNames: {
  (id: string): (executionId: string) => DoorbellNames;
  (executionId: string, id: string): DoorbellNames;
} = dual(2, (executionId: string, id: string): DoorbellNames => ({
  checkpointName: identifier("wake", id),
  eventName: identifier("wake-event", executionId, id),
}));

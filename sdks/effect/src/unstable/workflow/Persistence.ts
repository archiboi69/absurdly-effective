import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import { dual } from "effect/Function";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import { Workflow } from "effect/unstable/workflow";

// These helpers implement the erased persistence boundaries required by
// WorkflowEngine.makeUnsafe. Workflow schemas recover the concrete types at
// the engine boundary.
// oxlint-disable anti-slop/no-unknown-parameters

/**
 * Stable names and shapes written by the Absurd workflow adapter.
 *
 * These identifiers are a durable compatibility commitment: workflows may
 * resume under a newer worker long after an older worker persisted them.
 * Change representations by adding backward-compatible readers, never by
 * silently renaming an existing identifier.
 */
export const activityCheckpointName: {
  (attempt: number): (name: string) => string;
  (name: string, attempt: number): string;
} = dual(2, (name: string, attempt: number): string => `$activity:${name}:${attempt}`);
export const deferredCheckpointName = (name: string): string => `$defer:${name}`;
export const clockDeadlineCheckpointName = (deferredName: string): string =>
  `$clock:${deferredName}`;
export const interruptCheckpointName = "$effect:interrupt";
export const parentExecutionHeaderKey = "$effect:parent";

export const deferredEventName: {
  (executionId: string, deferredName: string): (workflowTag: string) => string;
  (workflowTag: string, executionId: string, deferredName: string): string;
} = dual(
  3,
  (workflowTag: string, executionId: string, deferredName: string): string =>
    `absurd-effect:deferred:${workflowTag}:${executionId}:${deferredName}`,
);

interface DoorbellNames {
  readonly checkpointName: string;
  readonly eventName: string;
}

export const doorbellNames: {
  (id: string): (executionId: string) => DoorbellNames;
  (executionId: string, id: string): DoorbellNames;
} = dual(2, (executionId: string, id: string): DoorbellNames => ({
  checkpointName: `$effect:wake:${id}`,
  eventName: `absurd-effect:wake:${executionId}:${id}`,
}));

export const ExecutionReference = Schema.Struct({
  queue: Schema.String,
  executionId: Schema.String,
});
export type ExecutionReference = typeof ExecutionReference.Type;

const WorkflowHeaders = Schema.NullOr(
  Schema.Struct({
    [parentExecutionHeaderKey]: Schema.optionalKey(ExecutionReference),
  }),
);

export const parentExecutionHeaders = (parent: ExecutionReference) => ({
  [parentExecutionHeaderKey]: parent,
});

export const decodeParentExecution = Effect.fnUntraced(function* (headers: unknown) {
  const decoded = yield* Schema.decodeUnknownEffect(WorkflowHeaders)(headers);
  if (decoded === null) return Option.none();
  return Option.fromNullishOr(decoded[parentExecutionHeaderKey]);
});

/** Canonical JSON codec pair for one workflow's Type-side persistence. */
export interface Codecs {
  readonly payload: ReturnType<typeof Schema.toCodecJson>;
  readonly result: ReturnType<typeof Schema.toCodecJson>;
}

export const makeCodecs = (workflow: Workflow.Any): Codecs => ({
  payload: Schema.toCodecJson(workflow.payloadSchema),
  result: Schema.toCodecJson(
    Workflow.Result({
      success: workflow.successSchema,
      error: workflow.errorSchema,
    }),
  ),
});

/**
 * Structural Exit envelope for already-encoded activity and deferred values.
 * Unknown slots preserve those values; Defect handles defect serialization.
 */
const structuralExitCodec = Schema.toCodecJson(
  Schema.Exit(Schema.Unknown, Schema.Unknown, Schema.Defect()),
);

/** Normalizes nullish successes, mirroring WorkflowEngine's JSON exit shape. */
export const exitWithNullishValues = (
  exit: Exit.Exit<unknown, unknown>,
): Exit.Exit<unknown, unknown> => Exit.map(exit, (value) => value ?? null);

type StructuralExitEncoded = Schema.Codec.Encoded<typeof structuralExitCodec>;

export const encodeStructuralExit = (exit: Exit.Exit<unknown, unknown>): StructuralExitEncoded =>
  exit.pipe(exitWithNullishValues, Schema.encodeSync(structuralExitCodec));

export const decodeStructuralExit = (stored: unknown): Exit.Exit<unknown, unknown> =>
  Schema.decodeUnknownSync(structuralExitCodec)(stored);

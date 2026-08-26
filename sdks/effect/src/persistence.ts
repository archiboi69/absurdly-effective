import * as Exit from "effect/Exit";
import * as Schema from "effect/Schema";
import { Workflow } from "effect/unstable/workflow";

// These helpers implement the erased persistence boundaries required by
// WorkflowEngine.makeUnsafe. Workflow schemas recover the concrete types at
// the engine boundary.
// oxlint-disable anti-slop/no-unknown-parameters

/** Canonical JSON codec pair for one workflow's Type-side persistence. */
export interface WorkflowPersistenceCodecs {
  readonly payload: ReturnType<typeof Schema.toCodecJson>;
  readonly result: ReturnType<typeof Schema.toCodecJson>;
}

export const workflowPersistenceCodecs = (workflow: Workflow.Any): WorkflowPersistenceCodecs => ({
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
export const structuralExitCodec = Schema.toCodecJson(
  Schema.Exit(Schema.Unknown, Schema.Unknown, Schema.Defect()),
);

/** Normalizes nullish successes, mirroring WorkflowEngine's JSON exit shape. */
export const exitWithNullishValues = (
  exit: Exit.Exit<unknown, unknown>,
): Exit.Exit<unknown, unknown> => Exit.map(exit, (value) => value ?? null);

export type StructuralExitEncoded = Schema.Codec.Encoded<typeof structuralExitCodec>;

export const encodeStructuralExit = (exit: Exit.Exit<unknown, unknown>): StructuralExitEncoded =>
  exit.pipe(exitWithNullishValues, Schema.encodeSync(structuralExitCodec));

export const decodeStructuralExit = (stored: unknown): Exit.Exit<unknown, unknown> =>
  Schema.decodeUnknownSync(structuralExitCodec)(stored);

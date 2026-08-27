import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { CurrentTask, currentTaskStep } from "./CurrentTask.ts";

export class StepEncodeError extends Schema.TaggedError<StepEncodeError>()("StepEncodeError", {
  stepName: Schema.String,
  cause: Schema.Defect(),
}) {}

export class StepDecodeError extends Schema.TaggedError<StepDecodeError>()("StepDecodeError", {
  stepName: Schema.String,
  cause: Schema.Defect(),
}) {}

export class StepPersistenceError extends Schema.TaggedError<StepPersistenceError>()(
  "StepPersistenceError",
  {
    stepName: Schema.String,
    operation: Schema.Literals(["begin", "complete"]),
    cause: Schema.Defect(),
  },
) {}

export type StepError = StepDecodeError | StepEncodeError | StepPersistenceError;

export interface Options<Success extends Schema.Top, Error, Requirements> {
  readonly name: string;
  readonly success: Success;
  readonly execute: Effect.Effect<Success["Type"], Error, Requirements>;
}

export type Make = <Success extends Schema.Top, Error, Requirements>(
  options: Options<Success, Error, Requirements>,
) => Effect.Effect<
  Success["Type"],
  Error | StepError,
  CurrentTask | Requirements | Success["EncodingServices"] | Success["DecodingServices"]
>;

export const make: Make = Effect.fn("Step.make")(function* <
  Success extends Schema.Top,
  Error,
  Requirements,
>(options: Options<Success, Error, Requirements>) {
  const execute = currentTaskStep(yield* CurrentTask);
  return yield* execute(options);
});

import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";
import type * as Schema from "effect/Schema";
import type { Options as StepOptions, StepError } from "./Step.ts";

export class CurrentTask extends Context.Service<
  CurrentTask,
  {
    /** Stable Absurd task UUID. */
    readonly id: string;
    /** Headers attached when the task was spawned. */
    readonly headers: Schema.JsonObject;
  }
>()("absurdly-effective/CurrentTask") {}

type ExecuteStep = <Success extends Schema.Top, Error, Requirements>(
  options: StepOptions<Success, Error, Requirements>,
) => Effect.Effect<
  Success["Type"],
  Error | StepError,
  Requirements | Success["EncodingServices"] | Success["DecodingServices"]
>;

// CurrentTask exposes only stable task metadata. Worker privately associates
// each value with the durable Step implementation for that task execution.
const stepImplementations = new WeakMap<CurrentTask["Service"], ExecuteStep>();

/** @internal */
export const currentTaskFromRuntime = (options: {
  readonly id: string;
  readonly headers: Schema.JsonObject;
  readonly executeStep: ExecuteStep;
}): CurrentTask["Service"] => {
  const task = CurrentTask.of({ id: options.id, headers: options.headers });
  stepImplementations.set(task, options.executeStep);
  return task;
};

/** @internal */
export const currentTaskStep = (task: CurrentTask["Service"]): ExecuteStep => {
  const execute = stepImplementations.get(task);
  if (execute === undefined) {
    throw new Error(
      "CurrentTask cannot execute Steps; provide it with Worker.layer or TestCurrentTask",
    );
  }
  return execute;
};

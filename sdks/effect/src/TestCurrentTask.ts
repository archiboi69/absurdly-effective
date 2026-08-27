import * as Effect from "effect/Effect";
import type * as Schema from "effect/Schema";

import { CurrentTask, currentTaskWithStep } from "./CurrentTask.ts";
import * as StepStorage from "./StepStorage.ts";

// Test checkpoints intentionally exercise the same untyped JSON boundary as storage.
// oxlint-disable anti-slop/no-unknown-parameters

export interface TestCurrentTask {
  readonly run: <Success, Error, Requirements>(
    effect: Effect.Effect<Success, Error, Requirements>,
  ) => Effect.Effect<Success, Error, Exclude<Requirements, CurrentTask>>;
}

export interface Options {
  readonly id?: string | undefined;
  readonly headers?: Schema.JsonObject | undefined;
  readonly onStep?: ((name: string) => void) | undefined;
}

/** Creates an isolated task runtime with in-memory durable checkpoints. */
export const make = (options: Options = {}): TestCurrentTask => {
  const checkpoints = new Map<string, unknown>();

  return {
    run: (effect) =>
      effect.pipe(
        Effect.provideService(
          CurrentTask,
          currentTaskWithStep({
            id: options.id ?? "test-task",
            headers: options.headers ?? {},
            executeStep: StepStorage.make(
              StepStorage.inMemory({ checkpoints, onStep: options.onStep }),
            ),
          }),
        ),
      ),
  };
};

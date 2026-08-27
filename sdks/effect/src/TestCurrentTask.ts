import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import type * as Schema from "effect/Schema";

import { CurrentTask, currentTaskFromRuntime } from "./CurrentTask.ts";
import { fromStorage, type BeginStep } from "./StepStorage.ts";

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

  const beginRun = (): BeginStep => {
    const counts = new Map<string, number>();
    return (name) =>
      Effect.sync(() => {
        options.onStep?.(name);
        const count = (counts.get(name) ?? 0) + 1;
        counts.set(name, count);
        const checkpointName = count === 1 ? name : `${name}#${count}`;
        return {
          value: checkpoints.has(checkpointName)
            ? Option.some(checkpoints.get(checkpointName))
            : Option.none(),
          complete: (value: unknown) =>
            Effect.sync(() => {
              checkpoints.set(checkpointName, value);
            }),
        };
      });
  };

  return {
    run: (effect) =>
      effect.pipe(
        Effect.provideService(
          CurrentTask,
          currentTaskFromRuntime({
            id: options.id ?? "test-task",
            headers: options.headers ?? {},
            executeStep: fromStorage(beginRun()),
          }),
        ),
      ),
  };
};

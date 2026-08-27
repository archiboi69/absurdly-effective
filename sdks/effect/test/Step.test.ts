import { assert, describe, it } from "@effect/vitest";
import { CurrentTask, Step, TestCurrentTask } from "../src/index.ts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

describe("Step", () => {
  it.effect("replays a successful checkpoint in the same test task", () => {
    let executions = 0;
    const checkpoint = (): Effect.Effect<string, Step.StepError, CurrentTask> =>
      Step.make({
        name: "provider/mutate",
        success: Schema.String,
        execute: Effect.sync(() => {
          executions += 1;
          return "completed";
        }),
      });
    const task = TestCurrentTask.make();

    return Effect.gen(function* () {
      assert.strictEqual(yield* task.run(checkpoint()), "completed");
      assert.strictEqual(yield* task.run(checkpoint()), "completed");
      assert.strictEqual(executions, 1);
    });
  });
});

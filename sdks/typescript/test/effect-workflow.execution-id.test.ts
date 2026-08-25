import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { Workflow } from "effect/unstable/workflow";

const CrossSdkWorkflow = Workflow.make("ShippingBroker/Finance/IssueSalesInvoice", {
  payload: { attemptId: Schema.Int },
  idempotencyKey: ({ attemptId }) => String(attemptId),
});

it.effect("keeps Effect Workflow execution IDs compatible across SDKs", () =>
  Effect.gen(function* () {
    const executionId = yield* CrossSdkWorkflow.executionId({ attemptId: 123 });

    assert.strictEqual(executionId, "3cf216794c6e5c9e7422efff15806610");
  }),
);

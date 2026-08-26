import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { Workflow } from "effect/unstable/workflow";
import { effectWorkflowExecutionId } from "absurd-sdk";

const CrossSdkWorkflow = Workflow.make("ShippingBroker/Finance/IssueSalesInvoice", {
  payload: { attemptId: Schema.Int },
  idempotencyKey: ({ attemptId }) => String(attemptId),
});

it.effect("keeps Effect Workflow execution IDs compatible across SDKs", () =>
  Effect.gen(function* () {
    const executionId = yield* CrossSdkWorkflow.executionId({ attemptId: 123 });

    assert.strictEqual(executionId, effectWorkflowExecutionId(CrossSdkWorkflow._tag, "123"));
  }),
);

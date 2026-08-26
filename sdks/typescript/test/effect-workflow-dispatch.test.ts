import { afterEach, beforeAll, describe, expect, test } from "./testlib.ts";
import { createTestAbsurd, randomName, type TestContext } from "./setup.ts";
import { effectWorkflowExecutionId, type Absurd, type JsonValue } from "../src/index.ts";

describe("Effect workflow dispatch", () => {
  let context: TestContext;
  let absurd: Absurd;

  beforeAll(async () => {
    context = await createTestAbsurd(randomName("effect_workflow"));
    absurd = context.absurd;
  });

  afterEach(async () => {
    await context.cleanupTasks();
  });

  test("matches Effect execution IDs for ASCII and UTF-8 keys", () => {
    expect(effectWorkflowExecutionId("ShippingBroker/Finance/IssueSalesInvoice", "123")).toBe(
      "3cf216794c6e5c9e7422efff15806610",
    );
    expect(effectWorkflowExecutionId("workflow-zażółć", "klucz-🚚")).toBe(
      "444a0466cfbeccfe2cdc7953618be84b",
    );
  });

  test("sets Effect infrastructure defaults and dispatches idempotently", async () => {
    const first = await absurd.spawnWorkflow(
      "ShippingBroker/Finance/IssueSalesInvoice",
      { attemptId: 123 },
      "invoice-123",
    );
    const second = await absurd.spawnWorkflow(
      "ShippingBroker/Finance/IssueSalesInvoice",
      { attemptId: 456 },
      "invoice-123",
    );

    expect(second).toEqual(first);

    const result = await context.pool.query<{
      task_name: string;
      params: JsonValue;
      idempotency_key: string;
      retry_strategy: JsonValue;
      max_attempts: number;
    }>(
      `SELECT task_name, params, idempotency_key, retry_strategy, max_attempts
       FROM absurd.t_${context.queueName}
       WHERE task_id = $1`,
      [first.taskID],
    );

    expect(result.rows[0]).toEqual({
      task_name: "ShippingBroker/Finance/IssueSalesInvoice",
      params: { attemptId: 123 },
      idempotency_key: first.executionID,
      retry_strategy: { kind: "fixed", base_seconds: 1 },
      max_attempts: 5,
    });
  });
});

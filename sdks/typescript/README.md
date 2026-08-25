# Absurd SDK for TypeScript

TypeScript SDK for [Absurd](https://github.com/earendil-works/absurd): a PostgreSQL-based durable task execution system.

Absurd is the simplest durable execution workflow system you can think of. It's entirely based on Postgres and nothing else. It's almost as easy to use as a queue, but it handles scheduling and retries, and it does all of that without needing any other services to run in addition to Postgres.

**Warning:** _this is an early experiment and should not be used in production._

## What is Durable Execution?

Durable execution (or durable workflows) is a way to run long-lived, reliable functions that can survive crashes, restarts, and network failures without losing state or duplicating work. Instead of running your logic in memory, a durable execution system decomposes a task into smaller pieces (step functions) and records every step and decision.

## Installation

```bash
npm install absurd-sdk
```

Examples in this README are intended to run directly on modern Node.js with
native TypeScript type stripping. No transpilation step is required.

## Prerequisites

Before using the SDK, you need to initialize Absurd in your PostgreSQL database:

```bash
# One-off usage
uvx absurdctl init -d your-database-name
uvx absurdctl create-queue -d your-database-name default

# Or install it once
uv tool install absurdctl
absurdctl init -d your-database-name
absurdctl create-queue -d your-database-name default
```

See the [absurdctl docs](https://earendil-works.github.io/absurd/tools/absurdctl/) for installation details and
the full CLI reference, including
[`uvx`](https://docs.astral.sh/uv/guides/tools/) usage.

## Effect workflows

`AbsurdWorkflowEngine` implements Effect's native
`effect/unstable/workflow` engine. Bind each workflow definition to its
physical Absurd queue, register its handler with `Workflow.toLayer`, and
provide one engine layer for the worker process.

### Ensure an execution exists, then check its status

`execute(payload, { discard: true })` is the idempotent ensure operation. It
creates the execution if necessary and always returns Effect's deterministic
workflow execution ID. Pass that returned ID directly to `poll`:

```typescript
const executionId = yield* IssueInvoiceWorkflow.execute(payload, { discard: true });
const result = yield* IssueInvoiceWorkflow.poll(executionId);
```

This is the recommended reconciliation pattern. It repairs a missing backing
task before checking status without exposing Absurd task lookup or UUIDs.

Effect's native `poll` deliberately has a small portable contract: `None` can
mean either “not found” or “pending/running”, and a sleeping workflow is a
`Some(Suspended)`. For operational tooling that must distinguish Absurd's
storage states, use the additive API:

```typescript
const status = yield* AbsurdWorkflowEngine.executionStatus(
  IssueInvoiceWorkflow,
  executionId,
);
// NotFound | Pending | Running | Sleeping
// Completed { exit } | Failed { failure } | Cancelled
```

`Completed.exit` is the schema-decoded typed workflow exit, including typed
business failures. `Failed` is different: it means the backing Absurd task
failed at the infrastructure or protocol level. `executionStatus` is for
reconciliation, diagnostics, and operational tooling; normal workflow control
stays on Effect's native `Workflow` methods. The workflow argument supplies
the queue binding and result schemas needed to locate and decode that execution;
the engine reads the storage status.

### Execution identity

Persist the workflow execution ID returned by `execute`, not the backing
Absurd task UUID. Native `poll`, `resume`, and `interrupt` all address an
execution by that execution ID. Absurd maps it to the task idempotency key; task
and run UUIDs remain storage and operations details.

Non-Effect producers must follow the same protocol. Prefer the Python SDK's
`spawnWorkflow(...)`, which derives the compatible execution ID, uses it as the
Absurd idempotency key, and returns both IDs so application data can retain only
`execution_id`.

### Retry ownership

Typed workflow failures are terminal workflow outcomes. Put business retries
around the fallible `Activity`, and use `DurableClock` when time between
attempts must survive process restarts. The backing Absurd task has a bounded
fixed retry policy only to recover infrastructure-level execution failures such
as a lost worker claim; it is not a second business retry policy.

### Malformed external tasks and rolling deployments

An externally spawned workflow must include its execution ID as Absurd's
idempotency key. A missing ID fails with an
`AbsurdEffectWorkflowProtocolError` through Absurd's ordinary `fail_run`
policy, so the task's configured retry budget remains authoritative. The
Effect and Python workflow spawn helpers set a bounded five-attempt fixed
retry policy.

An unregistered workflow name is different: workers defer it and log a warning
indefinitely. This preserves Absurd's pull-worker and rolling-deployment model;
deploying a worker that registers the workflow and resuming the execution lets
the same task continue.

## Quick Start

If you omit `db`, the client uses `ABSURD_DATABASE_URL`, then `PGDATABASE`,
then `postgresql://localhost/absurd`.

```typescript
import { Absurd } from "absurd-sdk";

const app = new Absurd({ db: "postgresql://localhost/mydb" });

// Register a task
app.registerTask({ name: "order-fulfillment" }, async (params, ctx) => {
  // Each step is checkpointed, so if the process crashes, we resume
  // from the last completed step
  const payment = await ctx.step("process-payment", async () => {
    return { paymentId: `pay-${params.orderId}`, amount: params.amount };
  });

  const inventory = await ctx.step("reserve-inventory", async () => {
    return { reservedItems: params.items };
  });

  // Wait for an event - the task suspends until the event arrives
  const shipment = await ctx.awaitEvent(`shipment.packed:${params.orderId}`);

  await ctx.step("send-notification", async () => {
    return { sentTo: params.email, trackingNumber: shipment.trackingNumber };
  });

  return {
    orderId: params.orderId,
    payment,
    inventory,
    trackingNumber: shipment.trackingNumber,
  };
});

// Start a worker that pulls tasks from Postgres
await app.startWorker();
```

## Spawning Tasks

```typescript
// Spawn a task - it will be executed durably with automatic retries
await app.spawn("order-fulfillment", {
  orderId: "42",
  amount: 9999,
  items: ["widget-1", "gadget-2"],
  email: "customer@example.com",
});
```

If the task is not registered in this process, pass `options.queue` explicitly.
For unregistered tasks, defaults from `registerTask(...)` are unavailable;
spawn options (or client defaults) are used.

## Task Result Snapshots

You can inspect or wait for a task's terminal result:

```typescript
const snapshot = await app.fetchTaskResult(taskID);
// { state: "pending" } | { state: "running" } | { state: "sleeping" }
// { state: "completed", result: ... }
// { state: "failed", failure: ... }
// { state: "cancelled" }

const final = await app.awaitTaskResult(taskID, { timeout: 30 });
```

Inside a task, you can also wait for child tasks durably:

```typescript
const child = await app.spawn("child-task", {}, { queue: "child-workers" });
const childResult = await ctx.awaitTaskResult(child.taskID, {
  queue: "child-workers",
  timeout: 30,
});
```

## Emitting Events

```typescript
// Emit an event that a suspended task might be waiting for
await app.emitEvent("shipment.packed:42", {
  trackingNumber: "TRACK123",
});
```

## Idempotency Keys

Use the task ID to derive idempotency keys for external APIs:

```typescript
const payment = await ctx.step("process-payment", async () => {
  const idempotencyKey = `${ctx.taskID}:payment`;
  return { idempotencyKey, amount: params.amount };
});
```

## Decomposed Steps

If you need explicit before/after control, split `step()` into two calls:

```typescript
const handle = await ctx.beginStep<{ messages: any[] }>("agent-turn");

let messages: any[];
if (handle.done) {
  // `handle.state` is fully typed when done=true.
  messages = handle.state.messages;
} else {
  messages = [{ role: "assistant", content: "hello" }];
  await ctx.completeStep(handle, { messages });
}
```

This is useful when integrating with event-driven loops (for example agent
runtimes) where the checkpoint boundary is not a single inline callback.

## License and Links

- [Examples](https://github.com/earendil-works/absurd/tree/main/sdks/typescript/examples)
- [Issue Tracker](https://github.com/earendil-works/absurd/issues)
- License: [Apache-2.0](https://github.com/earendil-works/absurd/blob/main/LICENSE)

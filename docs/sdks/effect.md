# Effect SDK

Build durable Effect programs with Postgres and one `absurd.sql` file.

The SDK gives you two ways to work:

| Start with | Choose it when |
| --- | --- |
| `Task` | You need a background job, webhook, provider call, or fan-out worker |
| `Workflow` | The business program sleeps, waits for signals, runs children, or compensates |

Most applications should start with `Task`. Add a `Workflow` when the business
logic—not merely the worker process—spans time.

Both APIs use a plain Postgres database with Absurd's tables. You do not need Redis, a scheduler service, or a coordinator.

New here? Follow [your first durable task](#tutorial-your-first-durable-task).
If you already know you need sleeps, signals, or compensation, jump to
[your first Effect workflow](#tutorial-your-first-effect-workflow).

## Before you start

Install the Effect SDK:

```bash
pnpm add absurd-effect effect @effect/platform-node
```

Then:

1. Install the Absurd SQL contract by following
   [Database Setup and Migrations](../database.md).
2. Create the queues used by your application.
3. Set `DATABASE_URL`.

Keep `absurd-effect` and the Absurd SQL contract on the same `0.x` release. For
example, `absurd-effect@0.5.x` targets Absurd SQL `0.5.x`.

## Tutorial: your first durable task

We will create an email task, run a worker, spawn the task, and inspect its
result.

### Step 1: define the task

```typescript
import { Schema } from "effect";
import { Task } from "absurd-effect";

export const SendEmail = Task.make("send-email", {
  queue: "email",
  payload: {
    emailId: Schema.String,
    to: Schema.String,
    subject: Schema.String,
  },
  success: Schema.Struct({ messageId: Schema.String }),
  idempotencyKey: ({ emailId }) => emailId,
});
```

That one value is the task's wire contract:

- `"send-email"` is the stable task name.
- `queue` selects the physical Absurd queue.
- `payload` is validated before spawn and after claim.
- `success` is encoded by the worker and decoded when inspected.
- `idempotencyKey` prevents duplicate logical work.

Keep task names and payload schemas compatible after deployment. Other SDKs
can spawn the same task name and JSON payload.

### Step 2: write the handler

```typescript
import { Effect } from "effect";
import { SendEmail } from "./SendEmail.js";

export const SendEmailHandler = SendEmail.handler(
  Effect.fn("SendEmail.handler")(function* ({ to, subject }) {
    yield* Effect.logInfo(`Sending "${subject}" to ${to}`);
    return { messageId: crypto.randomUUID() };
  }),
);
```

The handler is an ordinary `Effect`:

- typed errors stay in its error channel;
- required services stay visible in the Layer type;
- the return value must match the task's success Schema.

### Step 3: build the worker Layer

```typescript
import { NodeRuntime } from "@effect/platform-node";
import { Config, Layer } from "effect";
import { Absurd, Worker } from "absurd-effect";
import { SendEmailHandler } from "./SendEmailHandler.js";

export const DatabaseLayer = Absurd.layerConfig({
  url: Config.redacted("DATABASE_URL"),
});

const WorkerLayer = Worker.layer({
  handlers: [SendEmailHandler],
  concurrency: 10,
  pollInterval: "250 millis",
}).pipe(Layer.provide(DatabaseLayer));

Layer.launch(WorkerLayer).pipe(NodeRuntime.runMain);
```

`Worker.layer` discovers its queues from the handlers. It starts scoped polling
fibers and closes them with the Layer's Scope. There is no separate Absurd
worker bootstrap.

Using Bun? Replace `NodeRuntime` with `BunRuntime`; the Layers do not change.

### Step 4: spawn the task

```typescript
const taskId = yield* SendEmail.spawn({
  emailId: "email-123",
  to: "person@example.com",
  subject: "Your invoice",
});
```

Provide `DatabaseLayer` to this producer program just like any other Effect
infrastructure Layer.

The returned task ID is branded by the task name. Store it if the application
will inspect or reconcile the task later.

### Step 5: inspect the result

```typescript
const status = yield* SendEmail.status(taskId);

switch (status._tag) {
  case "Completed":
    console.log(status.value.messageId);
    break;
  case "Failed":
    console.error(status.failure);
    break;
}
```

`Task.status` is a snapshot, not a waiter. It returns one of:

```text
NotFound | Pending | Running | Sleeping | Completed | Failed | Cancelled
```

Compose repeated inspection, schedules, and timeouts with normal Effect
operators when you need to wait.

`Failed.failure` is operational JSON from Absurd, not a typed Effect error. The
SQL contract does not preserve the original TypeScript error type.

If a task ID came from application storage, restore its task-name brand before
using it:

```typescript
const taskId = yield* Schema.decodeEffect(SendEmail.idSchema)(storedTaskId);
const status = yield* SendEmail.status(taskId);
```

### What just happened?

```text
Task.make       -> typed wire contract
Task.handler    -> ordinary Effect handler
Absurd.layer    -> Postgres-backed task service
Worker.layer    -> scoped queue workers
Task.spawn      -> durable task ID
Task.status     -> typed result snapshot
```

That is the complete background-task model. Continue only when you need one of
the capabilities below.

## Add a durable step when retries must not repeat work

Suppose the provider accepted an email, but the worker died before completing
the task. Absurd will retry the task. Wrap the external mutation in `Step.make`
so the retry can restore its successful result. Here, `mailer.send` represents
an Effect-based provider client:

```typescript
import { Effect, Schema } from "effect";
import { CurrentTask, Step } from "absurd-effect";

export const SendEmailHandler = SendEmail.handler(
  Effect.fn("SendEmail.handler")(function* ({ to, subject }) {
    const task = yield* CurrentTask;

    return yield* Step.make({
      name: "provider/send-email",
      success: Schema.Struct({ messageId: Schema.String }),
      execute: mailer.send({
        to,
        subject,
        idempotencyKey: `${task.id}:provider/send-email`,
      }),
    });
  }),
);
```

Once the step succeeds, Absurd stores its Schema-encoded result. A later task
run returns that result without executing the provider call again.

Two rules matter:

1. Keep step names stable after deployment; they are durable checkpoint names.
2. Use `CurrentTask.id` as the provider idempotency key whenever possible.

A checkpoint narrows the duplicate-work window, but it cannot make a remote
provider call and a Postgres write atomic. Provider idempotency closes that
last gap.

## Add Effect services normally

Handlers can require any Effect service. Provide those services to the worker
Layer:

```typescript
const WorkerLayer = Worker.layer({
  handlers: [SendEmailHandler],
}).pipe(
  Layer.provide(DatabaseLayer),
  Layer.provide(MailerLayer),
);
```

`Worker.layer` supplies only task-local services such as `CurrentTask` and the
step executor. Your domain dependencies remain explicit.

## Tutorial: your first Effect workflow

Choose a workflow when the program itself must survive sleeps, signals,
children, or compensation. The API is Effect's native `Workflow`; Absurd only
provides its `WorkflowEngine`.

Install the PostgreSQL client if your application does not already use it:

```bash
npm install @effect/sql-pg pg
```

### Step 1: define and route the workflow

```typescript
import { AbsurdWorkflowEngine } from "absurd-effect/unstable/workflow";
import { Schema } from "effect";
import { Workflow } from "effect/unstable/workflow";

export const IssueInvoice = Workflow.make("Finance/IssueInvoice", {
  payload: { invoiceId: Schema.String },
  success: Schema.String,
  idempotencyKey: ({ invoiceId }) => invoiceId,
}).annotate(AbsurdWorkflowEngine.Queue, "finance");
```

`AbsurdWorkflowEngine.Queue` is a typed Effect annotation. It maps this
workflow to the physical `finance` queue.

If you prefer a helper, `inQueue` writes the same annotation and validates the
queue name immediately:

```typescript
const IssueInvoice = AbsurdWorkflowEngine.inQueue("finance")(
  Workflow.make("Finance/IssueInvoice", {
    payload: { invoiceId: Schema.String },
    success: Schema.String,
    idempotencyKey: ({ invoiceId }) => invoiceId,
  }),
);
```

There is one routing mechanism; choose the syntax that reads best.

### Step 2: write the workflow handler

```typescript
import { Effect } from "effect";

export const IssueInvoiceLayer = IssueInvoice.toLayer(
  Effect.fn("IssueInvoice.handler")(function* ({ invoiceId }, executionId) {
    yield* Effect.logInfo("Issuing invoice", { invoiceId, executionId });
    return `issued:${invoiceId}`;
  }),
);
```

`Workflow.toLayer` is native Effect. There is no Absurd-specific handler type.

### Step 3: provide the engine

```typescript
import { PgClient } from "@effect/sql-pg";
import { Config, Layer } from "effect";

const DatabaseLayer = PgClient.layerConfig({
  url: Config.redacted("DATABASE_URL"),
});

const EngineLayer = AbsurdWorkflowEngine.layer({
  queues: { finance: { concurrency: 4 } },
}).pipe(Layer.provide(DatabaseLayer));

export const WorkerLayer = IssueInvoiceLayer.pipe(
  Layer.provide(EngineLayer),
);
```

The whole worker is ordinary Layer composition:

```text
PgClient.layerConfig          -> SqlClient
AbsurdWorkflowEngine.layer    -> WorkflowEngine
Workflow.toLayer              -> registered handler
Layer.provide                 -> runnable worker Layer
```

Launch `WorkerLayer` with the Node, Bun, or application runtime you already
use. The Layer's Scope owns the Postgres resources and polling fibers.

### Step 4: execute the workflow

To start the execution and wait for its typed result:

```typescript
const result = yield* IssueInvoice.execute({
  invoiceId: "invoice-123",
});
```

`Workflow.execute` is lazy like every Effect value. Nothing happens until the
Effect is yielded or run.

### Add more workflows

Merge handler Layers, then provide one shared engine:

```typescript
const WorkflowHandlersLayer = Layer.mergeAll(
  IssueInvoiceLayer,
  ReconcilePaymentLayer,
  SendReceiptLayer,
);

export const WorkerLayer = WorkflowHandlersLayer.pipe(
  Layer.provide(EngineLayer),
);
```

## Start now, inspect later

Sometimes an HTTP request or dispatcher should start work without waiting for
the result. Use Effect's native `discard` option:

```typescript
const executionId = yield* IssueInvoice.execute(
  { invoiceId: "invoice-123" },
  { discard: true },
);
```

`discard` means “discard the result at this call site.” It does not delete or
cancel the result. The workflow continues durably, and the returned execution
ID is its stable application handle.

Later, poll it:

```typescript
const result = yield* IssueInvoice.poll(executionId);
```

Repeating `execute(payload, { discard: true })` is also the recommended
“ensure execution exists, then inspect it” pattern. It returns the same
deterministic execution ID and repairs a missing backing task without creating
a second logical execution.

## Understand `poll` and operational status

`Workflow.poll` is the portable Effect API. It returns Effect workflow results,
not Absurd task states:

| Absurd state | `Workflow.poll` |
| --- | --- |
| Missing, pending, or running | `None` |
| Sleeping | `Some(Workflow.Suspended)` |
| Completed successfully | `Some(Complete(Exit.Success))` |
| Completed with a typed workflow failure | `Some(Complete(Exit.Failure))` |
| Backing task failed or cancelled | Defects |

A typed business failure is a completed workflow result. A failed Absurd task
means infrastructure or protocol execution exhausted its attempts.

Operational tools can ask the Absurd engine for its richer storage view without
defecting:

```typescript
const status = yield* AbsurdWorkflowEngine.executionStatus(
  IssueInvoice,
  executionId,
);
```

The result is:

```text
NotFound | Pending | Running | Sleeping
| Completed { exit } | Failed { failure } | Cancelled
```

Use `Workflow.poll` in portable application logic. Use `executionStatus` in
reconciliation, support, and operational tooling.

## Understand workflow identity

You can use the SDK without knowing Absurd's internal IDs. Persist only the
execution ID returned by Effect.

When you need the deeper model, it is:

| Identity | Meaning | Lifetime |
| --- | --- | --- |
| Workflow name | Reusable Effect program and wire contract | Constant |
| Execution ID | One logical workflow invocation | Stable for that invocation |
| Task ID | Physical Absurd task backing the execution | Storage detail |
| Run ID | One claim or infrastructure attempt | Changes on retry or reclaim |
| Checkpoint | Persisted Activity, clock, deferred, or step result | Reused during replay |

Effect derives the execution ID from the workflow name and business
idempotency key:

```text
Finance/IssueInvoice + invoice-123 -> stable execution ID
```

The engine stores that execution ID as Absurd's idempotency key. Absurd task and
run UUIDs stay internal. `poll`, `resume`, and `interrupt` all use the Effect
execution ID.

## Durable workflow behavior

### Activities and retries

Typed workflow failures are terminal outcomes. Put business and provider
retries around the fallible `Activity`. Use a `DurableClock` when the retry
delay itself must survive worker restarts.

Absurd's backing-task retry policy has a narrower job: recover from
infrastructure-level execution failures.

### Safe interruption and compensation

```typescript
yield* IssueInvoice.interrupt(executionId);
```

Safe interruption is durable and cooperative. The engine records the request,
wakes a sleeping execution, replays through Effect, and runs registered
compensations. Repeated interruption and interruption of a terminal execution
are no-ops.

The wake event is only a scheduler doorbell. The durable interruption marker is
the source of truth, so an event emitted before or during wait registration is
not a lost wakeup.

An operation already running in another process cannot be remotely killed.
Model indefinitely blocking work as bounded or retryable Activities. The
engine rechecks interruption before committing workflow success.

### Clocks, deferreds, and child workflows

Durable clocks become timed Absurd event waits. Durable deferreds use cached
events, so a signal arriving before its waiter is retained. Child workflow
links are persisted so parents can resume after process replacement and safe
interruption can propagate.

These are standard Effect workflow APIs; the adapter adds no parallel versions.

## Use the same Absurd deployment from other SDKs

The task API interoperates directly through task name, JSON payload, and
idempotency key.

A workflow adds one Effect concept: the stable execution ID. A TypeScript or
Python producer that already has this ID can spawn the workflow task with the
execution ID as Absurd's idempotency key:

```typescript
await app.spawn("Finance/IssueInvoice", payload, {
  idempotencyKey: executionId,
});
```

Persist the execution ID, not the backing task UUID.

Version-matched Effect, TypeScript, and Python SDKs can share the same Absurd
schema, queues, task names, and operational tooling. Upgrade the SQL contract
and SDKs together within the same release line.

Unknown workflow names are deferred so rolling deployments remain safe.
Externally spawned workflow tasks without an execution ID exhaust Absurd's
bounded infrastructure retry policy and become visible as protocol failures.

## API at a glance

### Durable tasks

- `Task.make`, `Task.spawn`, `Task.status`, and `Task.handler`
- `Absurd.layer`, `Absurd.layerConfig`, and `Absurd.layerPool`
- `Worker.layer({ handlers })`
- `CurrentTask` for stable task metadata
- `Step.make` for durable success checkpoints
- `TestTaskStore.layer({ handlers })`

### Effect workflows

- `AbsurdWorkflowEngine.Queue` and `AbsurdWorkflowEngine.inQueue`
- `AbsurdWorkflowEngine.layer` and `AbsurdWorkflowEngine.make`
- `AbsurdWorkflowEngine.executionStatus`
- Native `Workflow.execute`, `poll`, `resume`, `interrupt`, and `toLayer`
- Native Activities, durable clocks, deferreds, children, and compensation

## Test the same task without Postgres

Use `TestTaskStore` for fast handler and checkpoint tests:

```typescript
import { assert, expect, it } from "@effect/vitest";
import { Effect } from "effect";
import { TestTaskStore } from "absurd-effect";
import { SendEmail } from "./SendEmail.js";
import { SendEmailHandler } from "./SendEmailHandler.js";

const TestLayer = TestTaskStore.layer({
  handlers: [SendEmailHandler],
});

it.effect("sends an email", () =>
  Effect.gen(function* () {
    const taskId = yield* SendEmail.spawn({
      emailId: "email-123",
      to: "person@example.com",
      subject: "Your invoice",
    });

    const status = yield* SendEmail.status(taskId);
    assert(status._tag === "Completed");
    expect(status.value.messageId).toBeDefined();
  }).pipe(Effect.provide(TestLayer)),
);
```

`TestTaskStore` uses the same definitions, handlers, and Schemas. It also keeps
successful step checkpoints across explicit reruns.

Use `TestTaskStore` for fast domain and checkpoint tests. Use PostgreSQL
integration tests for database claims, leases, automatic retries, concurrency,
and SQL compatibility.

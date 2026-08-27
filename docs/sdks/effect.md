# Effect SDK

`absurd-effect` is the native Effect SDK for Absurd. It gives Effect applications
durable tasks and an implementation of Effect's `WorkflowEngine`, while Absurd
keeps the durability machinery in Postgres.

The important part is what you do **not** need: no scheduler service, Redis,
coordinator, or Effect-specific database schema. Absurd's queues, claims,
checkpoints, sleeps, events, and retries are installed from one `absurd.sql`
file. The SDK is the typed Effect interface to that existing SQL contract.

This SDK is maintained in this repository and targets Absurd deliberately; it
is not part of Effect itself and does not introduce a second persistence model.
Application code gets Schemas, Effects, services, scoped Layers, and Effect's
workflow API. Postgres remains the durable runtime.

## One SQL contract, multiple SDKs

`absurd-effect` mirrors Absurd's `0.x` release version. For example,
`absurd-effect@0.5.x` targets the Absurd SQL `0.5.x` contract and interoperates
with the TypeScript and Python `absurd-sdk@0.5.x` packages.

That means an existing Absurd database does not become an Effect-only
deployment. Version-matched SDKs can use the same schema, queues, task names,
JSON payloads, idempotency keys, and operational tooling. An Effect worker can
consume work produced by a TypeScript or Python service, and non-Effect workers
can continue alongside it. Upgrade the SQL contract and SDKs together within
the same release line.

The two Effect surfaces serve different shapes of work:

- Use the primary `Task` and `Step` API for background jobs and mutation fan-out.
- Use `absurd-effect/unstable/workflow` when business logic spans time and needs
  Effect-native durable clocks, deferreds, interruption, typed outcomes, or
  compensation.

```bash
npm install absurd-effect effect
```

Install the single Absurd SQL contract as described in
[Database Setup and Migrations](../database.md), then create the queues used by
your application.

## Durable tasks

```typescript
import { Config, Effect, Layer, Schema } from "effect";
import { Absurd, Step, Task, Worker } from "absurd-effect";
import { rebuildProjection } from "./projections.js";

const RebuildProjection = Task.make("rebuild-projection", {
  queue: "projections",
  payload: { accountId: Schema.String },
  success: Schema.Struct({ rows: Schema.Number }),
  idempotencyKey: ({ accountId }) => accountId,
});

const RebuildProjectionHandler = RebuildProjection.handler(
  Effect.fn("RebuildProjection.handler")(function* ({ accountId }) {
    return yield* Step.make({
      name: "projection/rebuild",
      success: Schema.Struct({ rows: Schema.Number }),
      execute: rebuildProjection(accountId),
    });
  }),
);

const AbsurdLayer = Absurd.layerConfig({
  url: Config.redacted("DATABASE_URL"),
});

export const WorkerLayer = Worker.layer({
  handlers: [RebuildProjectionHandler],
  pollInterval: "250 millis",
}).pipe(Layer.provide(AbsurdLayer));
```

The task definition owns its queue and payload/result encoding. `Worker.layer`
derives the required queues from its handlers, starts one scoped worker per
queue, and closes them with the Layer's Scope. Payload construction input is
accepted directly, including for `Schema.Class`; successful tasks default to
`Schema.Void` when no result Schema is declared.

Producer programs provide the same `AbsurdLayer` Layer around
`RebuildProjection.enqueue(...)` or `RebuildProjection.status(...)`. Persisted
task IDs can be safely rehydrated with the definition's `idSchema`.

`Step.make` stores only successful results and rehydrates them through its Schema
after a retry. `CurrentTask.id` supplies the stable identifier used for logs,
application persistence, and provider idempotency.

For unit tests, `TestTaskStore.layer({ handlers })` executes the same definitions
and handlers in memory and retains successful step checkpoints across explicit
reruns. PostgreSQL integration tests remain the authority for claims, automatic
retries, leases, concurrency, and database behavior.

## Effect-native WorkflowEngine

`AbsurdWorkflowEngine` implements Effect's public `WorkflowEngine` service using
stock Absurd primitives. Your domain defines ordinary Effect `Workflow` values;
the runtime supplies the Absurd-backed engine as a Layer. Workflow definitions,
handlers, Activities, durable clocks, deferreds, typed outcomes, interruption,
and compensation therefore remain Effect-native.

Define a workflow in domain code, bind it to a physical Absurd queue at the
runtime edge, and provide the engine to its handler Layer:

```typescript
import { PgClient } from "@effect/sql-pg";
import { AbsurdWorkflowEngine } from "absurd-effect/unstable/workflow";
import { Effect, Layer, Redacted, Schema } from "effect";
import { Workflow } from "effect/unstable/workflow";

const IssueInvoice = Workflow.make("Finance/IssueInvoice", {
  payload: { invoiceId: Schema.String },
  success: Schema.Void,
  idempotencyKey: ({ invoiceId }) => invoiceId,
});

const FinanceIssueInvoice = AbsurdWorkflowEngine.inQueue("finance")(IssueInvoice);

const WorkflowLayer = FinanceIssueInvoice.toLayer(
  Effect.fn("FinanceIssueInvoice.handler")(function* ({ invoiceId }, executionId) {
    yield* Effect.logInfo("Issuing invoice", { invoiceId, executionId });
  }),
);

const RuntimeLayer = WorkflowLayer.pipe(
  Layer.provideMerge(
    AbsurdWorkflowEngine.layer({
      queues: { finance: { concurrency: 4 } },
    }),
  ),
  Layer.provide(
    PgClient.layer({
      url: Redacted.make(process.env.DATABASE_URL ?? "postgresql://localhost/absurd"),
    }),
  ),
);
```

The Layer provides Effect's ordinary `WorkflowEngine.WorkflowEngine` service.
The workflow itself does not know that Absurd is underneath it, so application
code keeps using established Effect APIs rather than an Absurd-flavored copy of
the workflow abstraction.

### Mental model: workflow, engine, execution, task, and run

A `Workflow` is a reusable definition: its stable name, payload and result
Schemas, typed error, and idempotency-key function. `AbsurdWorkflowEngine` is
the runtime service that makes executions of that definition durable.

Effect derives the stable execution ID from the workflow name and the business
key returned by its idempotency-key function. One definition can therefore have
many logical executions:

```text
Finance/IssueInvoice + invoice-123 -> execution A
Finance/IssueInvoice + invoice-456 -> execution B
```

Repeating `invoice-123` resolves to execution A instead of creating another
logical execution. Absurd then supplies the physical task and run records that
carry out that execution:

| Identity | Meaning | Lifetime |
| --- | --- | --- |
| Workflow name | Reusable Effect program and wire contract | Constant |
| Execution ID | One logical workflow invocation | Stable for that invocation |
| Task ID | Absurd task currently backing the execution | May change during repair or migration |
| Run ID | One claim or infrastructure attempt | Changes on retry or reclaim |
| Checkpoint | Persisted Activity, clock, deferred, or step result | Reused during replay |

The engine stores Effect's execution ID as Absurd's idempotency key. Effect's
`poll`, `resume`, and `interrupt` APIs therefore use the stable execution ID;
Absurd task and run UUIDs remain operational details. Run IDs change so a stale
worker cannot commit after losing its claim.

Conceptually, the relationship is:

```text
Workflow definition
  -> WorkflowEngine service selected by the runtime
    -> stable execution ID
      -> Absurd task
        -> one or more runs
          -> durable checkpoints and event waits in Postgres
```

On replay, the workflow runs again, but completed durable operations are
restored from checkpoints. Activities can register compensations, durable clocks
become timed Absurd event waits, and durable deferreds use Absurd's cached events
so completing a deferred before its waiter starts is not a lost wake-up.

### Start, wait, or inspect

`Workflow.execute` is lazy like every Effect value. Once yielded, its default
form starts or attaches to the durable execution and waits for its typed result:

```typescript
const result = yield* FinanceIssueInvoice.execute(payload);
```

To start or ensure an execution without waiting for its result, use Effect's
upstream discard option and retain the returned execution ID:

```typescript
const executionId = yield* FinanceIssueInvoice.execute(payload, { discard: true });
const result = yield* FinanceIssueInvoice.poll(executionId);
```

`discard` means “discard the result at this call site.” It does not delete the
result or make the workflow ephemeral. The execution continues durably, and the
returned ID is the handle to persist for later polling, resumption, or
interruption. Repeating the call also repairs a missing backing task without
creating a second logical execution.

Effect's portable `Workflow.poll` contract intentionally treats “not found” and
“not finished” alike. Absurd-specific operational tooling can ask the engine for
the richer storage state:

```typescript
const status = yield* AbsurdWorkflowEngine.executionStatus(
  FinanceIssueInvoice,
  executionId,
);
// NotFound | Pending | Running | Sleeping
// Completed { exit } | Failed { failure } | Cancelled
```

`Completed.exit` contains the Schema-decoded typed workflow outcome. `Failed`
means the backing Absurd task failed at the infrastructure or protocol level,
not that the workflow returned a typed business failure.

### Interruption and retries

`Workflow.interrupt(executionId)` is durable and cooperative. The adapter
persists the interruption request before waking an event wait, and replay runs
registered compensations. Repeated interruption and interruption of a terminal
execution are no-ops. The wake event is only a scheduler doorbell; the durable
interruption marker remains the source of truth.

Typed workflow failures are terminal outcomes. Put business retries around the
fallible `Activity`, using `DurableClock` when retry delays must survive worker
restarts. Absurd's task retry policy protects infrastructure-level execution.

### Interoperability for workflows

The ordinary task API interoperates directly through task name, JSON payload,
and idempotency key. A workflow execution adds one Effect concept: its stable
execution ID. Once that ID is known, a TypeScript or Python producer can spawn
the same workflow task using the execution ID as Absurd's idempotency key.
Persist the execution ID in application data; do not expose the backing Absurd
task or run UUID as the workflow handle.

See the package
[README](https://github.com/earendil-works/absurd/tree/main/sdks/effect)
for the complete task API, workflow identity model, interruption semantics,
testing patterns, and retry ownership.

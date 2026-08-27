# Absurd SDK for Effect

`absurd-effect` implements Effect's native `WorkflowEngine` on Absurd's
PostgreSQL durable execution primitives. Workflow definitions, handlers,
activities, durable clocks, deferreds, typed failures, and interruption remain
Effect-owned; Absurd owns persistence, claims, retries, events, and waits.

```bash
npm install absurd-effect effect @effect/sql-pg pg
```

## Define and run a workflow

Define the workflow in domain code, bind it to a physical queue at the runtime
edge, and provide one engine layer to the worker process:

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

## Mental model: definition, execution, task, and run

A workflow definition is a reusable program. This is the constant definition
and its stable name:

```typescript
const IssueInvoice = Workflow.make("Finance/IssueInvoice", {
  // ...
});
```

One definition can have many logical executions. Effect derives a stable
execution ID from the workflow name and the business idempotency key returned
by the definition:

```text
Finance/IssueInvoice + invoice-123 -> execution A
Finance/IssueInvoice + invoice-456 -> execution B
```

Repeatedly executing `invoice-123` resolves to execution A; it does not create
another logical execution. Different business keys produce different execution
IDs.

Absurd then gives that logical execution a physical task and one or more runs:

| Identity | Represents | Lifetime |
| --- | --- | --- |
| Workflow name | The reusable workflow definition | Constant |
| Execution ID | One logical invocation/idempotency key | Stable across its entire execution |
| Task ID | The physical Absurd task backing the execution | May be replaced during repair or migration |
| Run ID | One claim or infrastructure attempt | Changes on retry or reclaim |

Effect's `poll`, `resume`, and `interrupt` operations use the execution ID.
Absurd task and run UUIDs remain operational storage identifiers. Keeping the
execution and task IDs separate allows Absurd to repair or migrate physical
storage without changing application identity; run IDs must change so stale
workers can be fenced from committing.

The execution ID is stored as Absurd's idempotency key, which provides the
stable mapping to the current backing task.

## Execute now or start and return

`Workflow.execute` is lazy, like every Effect value. Neither form does anything
until it is yielded or run. Once evaluated, the default form starts or attaches
to the execution and waits for its typed business result:

```typescript
const invoice = yield* FinanceIssueInvoice.execute(payload);
// Effect<Invoice, IssueInvoiceError>
```

Effect's upstream `{ discard: true }` option means “discard the result at this
call site.” It starts or attaches to the durable execution, ensures its backing
task exists, and returns the deterministic execution ID without waiting for the
workflow to finish:

```typescript
const executionId = yield* FinanceIssueInvoice.execute(payload, {
  discard: true,
});
// Effect<string, never>
```

`discard` does **not** delete, cancel, or make the workflow result ephemeral.
The workflow continues durably and its result remains available for polling.
With the Absurd engine, the call returns only after the backing execution has
been accepted and persisted.

This form is useful when an HTTP request, dispatcher, or parent process must
start work that may take minutes, days, or years without keeping an in-memory
fiber or connection open. Persist the returned execution ID as the handle for
later control and observation.

## Ensure an execution exists, then inspect it

Because execution IDs and Absurd task creation are idempotent, the discard form
also acts as an ensure operation:

```typescript
const executionId = yield* FinanceIssueInvoice.execute(payload, { discard: true });
const result = yield* FinanceIssueInvoice.poll(executionId);
```

If the execution already has a backing task, the call returns the existing
execution ID. If the task is missing, the engine recreates it without creating
a second logical execution. This makes the pattern especially useful for
reconciliation loops.

Effect's portable `poll` contract intentionally treats missing and active work
the same. Operational tooling can request Absurd's richer storage status:

```typescript
const status = yield* AbsurdWorkflowEngine.executionStatus(
  FinanceIssueInvoice,
  executionId,
);
// NotFound | Pending | Running | Sleeping
// Completed { exit } | Failed { failure } | Cancelled
```

`Completed.exit` contains the schema-decoded typed workflow outcome. `Failed`
means the backing Absurd task failed at the infrastructure or protocol level.

## Safe interruption

`Workflow.interrupt(executionId)` is durable and cooperative. The engine first
records Effect's reserved interruption marker, then rings the execution's
current one-shot Absurd event wait. Replaying the workflow observes the marker,
interrupts through Effect, and runs registered compensations. Repeated calls
and calls against a terminal execution are no-ops.

The event is only a scheduler doorbell; it never carries the meaning or result
of a clock, deferred, or interruption. Those facts are persisted separately
and re-read during replay. This makes event emission races safe: an event sent
before the wait is registered is cached by Absurd, and a fact written during
registration is found by the engine's post-registration check.

An operation already running in another worker process cannot be remotely
killed. It may reach its next Effect boundary, but the engine rechecks the
durable marker before committing handler success, so a completed interrupt
request is not silently lost. Code that can block indefinitely should be
modeled as bounded or retryable `Activity` work rather than an unbounded
in-process call.

This protocol uses stock Absurd `await_event` and `emit_event`; it requires no
push worker or additional scheduler-control stored procedure. Workers remain
pull-only and claim a run after its event wait becomes pending.

## Producing work outside Effect

The promise-based TypeScript and Python SDKs remain generic. A producer that
already has an Effect execution ID passes it to their ordinary `spawn` API as
the Absurd idempotency key:

```typescript
const executionId = "execution-id-from-the-workflow-system";
await app.spawn("Finance/IssueInvoice", payload, {
  idempotencyKey: executionId,
});
```

Absurd treats the value as opaque and returns the existing task when the key is
reused. Persist the execution ID, not the backing task UUID.

## Retry ownership

Typed workflow failures are terminal outcomes. Business retries belong around
the fallible `Activity`; use `DurableClock` when retry delays must survive
worker restarts. Absurd's backing-task retry policy only protects
infrastructure-level execution.

Unknown workflow names are deferred so pull workers and rolling deployments
remain safe. Externally spawned workflow tasks that omit their execution ID
fail through Absurd's ordinary bounded retry policy as protocol errors.

# Effect SDK

`absurd-effect` provides Effect-native durable tasks backed directly by stock
Absurd SQL through `@effect/sql-pg`.

The package version mirrors Absurd's release version: `absurd-effect@0.5.x`
targets the SQL `0.5.x` contract and contains the corresponding native driver.

```bash
npm install absurd-effect effect
```

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

## Optional WorkflowEngine adapter

Applications that need durable clocks, deferreds, typed workflow failures,
interruption, and compensation can use the optional adapter for Effect's native
`WorkflowEngine`:

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

The adapter uses stock Absurd SQL and requires no Effect-specific migration.
Effect's stable execution ID is stored as Absurd's idempotency key; Absurd task
and run UUIDs remain internal operational identifiers.

To start or ensure an execution without waiting for its result, use Effect's
upstream discard option and retain the returned execution ID:

```typescript
const executionId = yield* FinanceIssueInvoice.execute(payload, { discard: true });
const result = yield* FinanceIssueInvoice.poll(executionId);
```

`Workflow.interrupt(executionId)` is durable and cooperative. The adapter
persists the interruption request before waking an event wait, and replay runs
registered compensations. Operational tooling can distinguish `NotFound`,
`Pending`, `Running`, `Sleeping`, `Completed`, `Failed`, and `Cancelled` through
`AbsurdWorkflowEngine.executionStatus` without changing Effect's portable
`Workflow.poll` contract.

See the package
[README](https://github.com/earendil-works/absurd/tree/main/sdks/effect)
for the complete task API, workflow identity model, interruption semantics,
testing patterns, and retry ownership.

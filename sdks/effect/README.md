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
import { AbsurdWorkflowEngine } from "absurd-effect";
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

## Ensure an execution exists, then inspect it

`execute(payload, { discard: true })` is the idempotent ensure operation. It
repairs a missing backing task and returns Effect's deterministic execution ID:

```typescript
const executionId = yield* FinanceIssueInvoice.execute(payload, { discard: true });
const result = yield* FinanceIssueInvoice.poll(executionId);
```

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

## Identity and retries

Persist the workflow execution ID, not Absurd's task or run UUID. Effect's
`poll`, `resume`, and `interrupt` all address the execution ID.

Use `absurd-sdk`'s `spawnWorkflow` when promise-based TypeScript code produces
work for an Effect worker. It derives the compatible execution ID and applies
the bounded infrastructure retry policy.

Typed workflow failures are terminal outcomes. Business retries belong around
the fallible `Activity`; use `DurableClock` when retry delays must survive
worker restarts. Absurd's backing-task retry policy only protects
infrastructure-level execution.

Unknown workflow names are deferred so pull workers and rolling deployments
remain safe. Externally spawned workflow tasks that omit their execution ID
fail through Absurd's ordinary bounded retry policy as protocol errors.

# Absurd in two TypeScript idioms

This note compares the mental model of Absurd's promise-based TypeScript SDK
with the Effect SDK. It is written for an Absurd maintainer evaluating one
question: does the Effect integration preserve the small Postgres system, or
does it quietly turn Absurd into the kind of runtime platform it was designed
to avoid?

The short answer is that the primary Effect `Task` API is the existing Absurd
model expressed in Effect's idioms. The optional `WorkflowEngine` adapter goes
further: it translates Effect's workflow semantics into stock Absurd tasks,
checkpoints, events, and waits. That adapter adds a durable convention, but it
does not add a database schema, coordinator, scheduler service, broker, or push
worker model.

## The invariants come from Absurd

The relevant constraints are not Effect preferences. They are Absurd's design:

- Postgres is the queue and durable state store.
- `absurd.sql` owns state transitions and concurrency policy.
- Workers pull work when they have capacity.
- A Task is the stable logical unit of work; a Run is one attempt to execute it.
- Explicit Steps are the replay boundary. Completed checkpoint values are
  loaded instead of repeating successful work.
- SDKs should stay understandable and expose those primitives in the idiom of
  their host language.

These are the points emphasized in [Absurd Workflows: Durable Execution With
Just Postgres](https://lucumr.pocoo.org/2025/11/3/absurd-workflows/), revisited
after production use in [Absurd In
Production](https://lucumr.pocoo.org/2026/4/4/absurd-in-production/), and made
concrete in [Absurd's concepts](../concepts.md) and
[`CONTRIBUTING.md`](../../CONTRIBUTING.md).

The Effect SDK should therefore be judged by a fairly strict test: if it needs
a second durable store, an always-on coordinator, pushed execution, or state
transitions outside Absurd's stored procedures, it has stopped being an Absurd
SDK.

It does not do any of those things.

## The shared storage model

Both SDKs ultimately operate the same five durable nouns:

| Stored concept | Meaning |
| --- | --- |
| Task | One stable logical invocation |
| Run | One physical attempt to execute that Task |
| Checkpoint | The persisted result of a completed durable operation |
| Event | A cached signal, with first emission winning |
| Wait registration | A suspended Task's durable interest in an Event |

A worker in either SDK claims a Run, executes ordinary application code, and
completes, fails, or suspends that Run through the functions in
[`absurd.sql`](../../sql/absurd.sql). A later Run belongs to the same Task and
sees the same checkpoints.

Neither SDK provides exactly-once external side effects. A checkpoint prevents
a successful operation from being deliberately repeated after its result was
stored, but there is still a failure window between an external mutation and
the checkpoint write. Stable Task identity should be used as an external
idempotency input whenever the provider supports it. This is the same
constraint in both APIs.

## The promise SDK: one contextual object

The TypeScript SDK presents Absurd as an object-oriented, promise-based client:

```typescript
app.registerTask({ name: "send-email" }, async (params, ctx) => {
  return await ctx.step("provider/send-email", async () => {
    return await sendEmail(params, {
      idempotencyKey: `${ctx.taskID}:provider/send-email`,
    });
  });
});

const { taskID } = await app.spawn("send-email", payload);
```

The important object inside a handler is
[`TaskContext`](../../sdks/typescript/src/index.ts). It combines three kinds of
things because that is convenient in a promise API:

1. Stable application-visible metadata: Task ID and headers.
2. Durable operations: Step, sleep, await Event, await child result, and emit
   Event.
3. Attempt-local machinery: the claimed Run, database connection, checkpoint
   cache, step occurrence counter, claim timeout, and lease extension callback.

`TaskContext` is not itself the durable state. The database remains the source
of truth. It is the in-process façade through which the current handler reads
and changes that state.

The physical Run ID is needed internally to fence writes. If a worker loses its
lease and another worker takes over, the stale Run must not be allowed to write
a checkpoint under the replacement. That does not make Run identity a useful
business identifier. The Task ID remains stable across retries; the Run ID
does not.

Registration is mutable and direct: `Absurd.registerTask` adds a handler to the
client's registry, and `startWorker` starts polling. Dependencies are normally
captured by closures. Errors are thrown promises; a handler rejection fails the
Run and lets Absurd apply the Task retry policy.

That is a small and coherent model for ordinary TypeScript.

## The Effect Task API: the same model split by capability

The Effect SDK starts from the same Task rather than trying to make every piece
of background work an Effect Workflow:

```typescript
const SendEmail = Task.make("send-email", {
  queue: "email",
  payload: {
    emailId: Schema.String,
    to: Schema.String,
  },
  success: Schema.Struct({ messageId: Schema.String }),
  idempotencyKey: ({ emailId }) => emailId,
});

const SendEmailHandler = SendEmail.handler(
  Effect.fn("SendEmail.handler")(function* (payload) {
    const task = yield* CurrentTask;

    return yield* Step.make({
      name: "provider/send-email",
      success: Schema.Struct({ messageId: Schema.String }),
      execute: sendEmail(payload, {
        idempotencyKey: `${task.id}:provider/send-email`,
      }),
    });
  }),
);
```

This is not a different persistence model. It is a different decomposition of
the promise SDK's surface:

| TypeScript SDK | Effect Task API | Reason for the difference |
| --- | --- | --- |
| Task name passed separately to `registerTask` and `spawn` | `Task.make` returns one reusable definition | The definition is a typed value shared by producers, workers, and tests. |
| Unchecked JSON parameters and result | Payload and success `Schema` | JSON is still stored by Absurd; schemas validate and transform at that existing boundary. |
| `app.registerTask(..., handler)` | `Task.handler(effect)` returns a registration value | Registration stays declarative until a worker Layer consumes it. |
| Dependencies captured in closures | Requirements in the Effect environment | Handler requirements remain visible in its type and are provided with Layers. |
| `ctx.taskID` and `ctx.headers` | `yield* CurrentTask` | Stable metadata is an ambient execution capability. |
| `ctx.step(name, fn)` | `Step.make({ name, success, execute })` | The checkpoint operation is independently typed and its persisted result has a Schema. |
| `app.startWorker()` | `Worker.layer({ handlers })` | A Layer acquires polling fibers and releases them with its Scope. |
| Promise rejection | Effect error channel | A Task handler failure still fails the Absurd Run and uses Absurd's retry policy. |
| `{ taskID, ... }` | Branded `TaskId<Name>` | Application code gets the stable logical identity, not an attempt identity. |

The implementation follows this table directly in
[`Task.ts`](../../sdks/effect/src/Task.ts),
[`CurrentTask.ts`](../../sdks/effect/src/CurrentTask.ts),
[`Step.ts`](../../sdks/effect/src/Step.ts), and
[`Worker.ts`](../../sdks/effect/src/Worker.ts).

### What happened to `TaskContext`?

It was decomposed, not rejected:

```text
TypeScript TaskContext
├── stable Task ID and headers ───── CurrentTask
├── durable checkpoint operation ─── Step.make
└── Run, lease, SQL, cache ───────── Worker-owned private runtime
```

`CurrentTask` is intentionally not the whole execution object. It exposes the
small part application code should safely depend upon: stable Task identity and
headers. `Step.make` obtains the current checkpoint capability through that
execution environment, while the worker keeps the current Run and lease
private.

This is a design judgment, not a claim that Effect forbids contextual objects.
Effect makes dependencies and lifetime visible through services, Effect values,
Layers, and Scopes. Splitting the public capabilities lets a handler say that it
needs `CurrentTask` only when it reads Task metadata, while every `Step` makes
its durable dependency explicit in the Effect type.

Keeping the Run private is also a deliberate pit-of-success decision. Provider
idempotency and reconciliation should normally use the stable Task ID. Exposing
the attempt ID alongside it would make the unstable value too easy to choose.

### Why `Task.handler` returns a value

The TypeScript client owns a mutable registry. The Effect worker instead takes
an immutable collection of handler values and builds the polling runtime as a
Layer. This lets the Layer's input type retain the union of services required
by its handlers.

The word `handler` has the same meaning in both SDKs: application logic for a
claimed Task. The different call shape exists because registration is an
imperative operation in the promise SDK and a composable value in the Effect
SDK.

### Why `Step.make` is not `CurrentTask.step`

Putting `step` back on `CurrentTask` would recreate `TaskContext` under another
name. The separate `Step` module makes the durable boundary visible at the call
site and gives it its own Schema and typed persistence failures.

There is a real ergonomic trade-off. `ctx.step(name, fn)` is shorter and keeps
all current-execution operations discoverable on one object. `Step.make` is
more explicit, schema-backed, and testable as an Effect capability. The Effect
SDK chooses the latter because its audience already expects operations and
dependencies to compose as Effects.

## Where the primary Task APIs intentionally differ

The promise SDK lets a single Task grow progressively from one background job
into a long-lived program by calling `sleepFor`, `awaitEvent`, or
`awaitTaskResult` on `TaskContext`.

The primary Effect `Task` API is currently narrower. It covers typed Task
definition, spawning, status, workers, and explicit Steps. When logic itself
must sleep, wait for signals, run children, or compensate, the SDK directs the
user to Effect's native `Workflow` API.

That is a product boundary, not a limitation in Absurd and not a requirement
imposed by Effect. It avoids inventing Effect-specific copies of clocks,
deferreds, interruption, child workflows, and compensation when Effect already
has those concepts. The cost is that a small sleeping Task is conceptually
lighter in the promise SDK than in the Effect SDK.

This difference should remain explicit. Claiming one-for-one API parity would
hide it.

## The optional Workflow API

Effect separates the reusable workflow definition, one logical execution, the
engine that runs executions, and the in-memory state of a currently replayed
execution:

| Effect concept | Meaning |
| --- | --- |
| `Workflow` | Typed reusable program and wire contract |
| Execution ID | Stable identity of one logical invocation |
| `WorkflowEngine` | Service implementing execute, poll, resume, interrupt, Activities, Deferreds, and durable clocks |
| `WorkflowInstance` | Engine-internal, in-memory state for the execution currently being replayed |
| `Workflow.toLayer` | Registration of the workflow handler into an engine |

These are Effect's public and internal contracts, defined in the vendored
[`Workflow.ts`](../../.context/effect/packages/effect/src/unstable/workflow/Workflow.ts)
and
[`WorkflowEngine.ts`](../../.context/effect/packages/effect/src/unstable/workflow/WorkflowEngine.ts).

`WorkflowEngine` is a service because workflow operations depend on an
implementation. `AbsurdWorkflowEngine.layer` is the recipe that constructs and
provides that service from a PostgreSQL `SqlClient`. The engine is not “a Layer”
in the domain model; the Layer owns the implementation's resources and
lifetime.

`WorkflowInstance` is closer to the runtime portion of TypeScript
`TaskContext` than to Effect `CurrentTask`. It contains execution identity,
workflow definition, Scope, suspension and interruption flags, failure state,
and Activity/Deferred coordination. Application workflow handlers receive the
stable execution ID directly; `WorkflowInstance` is engine machinery.

## How a Workflow maps to Absurd

The adapter in
[`AbsurdWorkflowEngine.ts`](../../sdks/effect/src/unstable/workflow/AbsurdWorkflowEngine.ts)
implements Effect's `WorkflowEngine` contract with stock Absurd primitives:

| Effect Workflow concept | Absurd representation |
| --- | --- |
| One Workflow execution | One logical Absurd Task |
| Execution ID | Absurd idempotency key for that Task |
| One replay or infrastructure attempt | One Absurd Run |
| Activity result | Named checkpoint |
| Durable clock | Checkpointed deadline plus timed Event wait |
| Durable Deferred | Cached Event plus checkpointed result |
| Suspended execution | Sleeping Task with a wait registration |
| Child Workflow link | Parent metadata in existing Task headers |
| Child completion | Cached Event used as a scheduler doorbell |
| Interrupt request | Durable checkpoint marker plus a doorbell Event |
| Typed Workflow result | Schema-encoded result in the completed Absurd Task |

The execution ID and backing Task ID are intentionally different. Effect owns
the logical workflow identity and derives it from the Workflow name and its
business idempotency key. Absurd assigns its normal Task UUID. Storing the
execution ID as Absurd's idempotency key makes repeated Effect execution locate
or recreate the same logical execution without replacing Absurd's own identity
model.

The Run remains a physical attempt. The adapter uses its ID to fence writes and
extend the claim, but `poll`, `resume`, and `interrupt` take the stable Effect
execution ID.

### `WorkflowInstance` and `TaskContext` are only partially symmetrical

Both are execution-scoped runtime objects used by nested durable operations,
but they sit at different API boundaries:

```text
TypeScript TaskContext                 Effect WorkflowInstance
├── public handler argument            ├── engine-internal service
├── stable Task identity               ├── stable execution identity
├── Step/sleep/Event operations        ├── suspension/interruption state
└── private Run and lease machinery    └── Scope and operation coordination
```

`TaskContext` is the promise SDK's public façade over Absurd execution.
`WorkflowInstance` is Effect engine state and is not the normal application
API. `CurrentTask` is smaller still: it is only the application-facing stable
projection needed by the primary Effect Task API.

The useful identity correspondence is therefore:

```text
CurrentTask.id       <-> stable Absurd Task ID
Workflow executionId <-> stable logical Effect Workflow invocation
Absurd Run ID        <-> physical retry/reclaim attempt
```

None of those stable application handles should be replaced by a Run ID.

## Replay remains checkpoint replay, not a deterministic runtime

The adapter does not introduce a compiler or capture every operation performed
by an Effect. Ordinary handler code can run again. Durable operations consult
their Absurd checkpoint or cached Event before performing work, exactly as
`ctx.step` does in the promise SDK.

Effect's workflow handler is replayed to reconstruct process-local state such
as Scopes and compensation registration, but durable Activity, Deferred, and
clock outcomes come from Absurd storage. Absurd remains a checkpoint system,
not a deterministic history-replay virtual machine.

This preserves one of the properties Armin called out after production use:
application code outside explicit durable boundaries does not need to satisfy a
global deterministic-runtime contract. It may execute more than once, so
side-effects still belong inside explicit durable operations.

## A doorbell is not pushed execution

The Workflow adapter uses cached Absurd Events to wake a sleeping execution.
That can sound like push, but the worker model does not change:

1. An interrupt, Deferred completion, or child completion records durable
   state.
2. An Event atomically makes a sleeping Task eligible to run.
3. A worker later pulls and claims the pending Run when it has capacity.

No coordinator invokes a worker, no HTTP callback enters a worker process, and
the producer does not select a worker. The Event is a durable scheduler
doorbell, not delivery of execution.

The checkpoint is the source of truth for interruption. The Event only avoids
waiting for the next timeout. Because Absurd caches Events and registers waits
atomically, an Event arriving before its waiter is not lost. This uses the
pull-oriented Event model Absurd already exposes rather than adding the push
model deliberately excluded from the core.

## Failure and retry ownership

The two Effect entry points preserve different contracts:

| Situation | Effect Task | Effect Workflow |
| --- | --- | --- |
| Application failure | Fails the Absurd Run; Absurd may create another Run | Typed failure is a completed Workflow result |
| Infrastructure failure | Fails the Absurd Run; Absurd retries | Fails the backing Absurd Run; Absurd retries |
| Provider retry | Usually Task retry, optionally protected by a Step | Explicit around the Activity, with a durable clock when needed |
| Completed durable operation | Step checkpoint | Activity/Deferred/clock checkpoint |

The Workflow distinction is required by Effect's contract: a typed business
failure belongs in `Exit` and must remain inspectable as the workflow's terminal
result. Treating it as a failed backing Task would erase the typed failure and
accidentally hand business retry policy to Absurd's infrastructure retry loop.

This does not redefine Absurd failure. Habitat and `absurdctl` still see the
truth about the backing Task. A completed Task can contain a failed typed
Workflow result; a failed Task means the adapter itself could not complete the
Workflow protocol within its infrastructure attempts.

## The two Absurd-specific Workflow extensions

Most application code uses Effect's native Workflow API. The adapter adds only
the pieces that belong specifically to an Absurd deployment:

- `AbsurdWorkflowEngine.Queue` associates a Workflow with a physical Absurd
  queue. `inQueue` is a convenience form of the same annotation.
- `AbsurdWorkflowEngine.executionStatus` exposes Absurd's operational states
  for reconciliation and support tooling.

Queue routing is annotation metadata because it is a backend placement concern,
not a new Workflow semantic. A different `WorkflowEngine` may interpret a
different annotation or need none.

`Workflow.poll` intentionally retains Effect's portable result model. It cannot
represent every Absurd storage state without changing the upstream abstraction.
`executionStatus` is therefore additive rather than making application code
mistake `pending`, `sleeping`, a typed Workflow failure, and an exhausted
backing Task for the same kind of result.

## What the Workflow adapter really adds

It would be misleading to call the optional adapter merely a thin syntax
wrapper. It implements orchestration semantics on top of Absurd's primitives
and is consequently much larger than the primary Task API.

It also writes stable Effect-owned names and values into existing Absurd
headers, checkpoints, and Events. Those names are centralized and versioned in
[`Persistence.ts`](../../sdks/effect/src/unstable/workflow/Persistence.ts).
They are not an additional SQL schema, but they are a durable protocol: an old
execution may resume under a newer worker, so existing names and encodings
cannot be silently changed.

This is why the adapter lives under `unstable/workflow`. The primary Task API
can remain a direct, language-idiomatic SDK over Absurd. The optional adapter
has a wider responsibility: conform to Effect's evolving Workflow contract
while maintaining compatibility with data persisted by earlier adapter
versions.

## Cross-SDK interoperability

At the Task boundary, interoperability remains ordinary Absurd:

- queue and Task name;
- JSON payload and result;
- idempotency key;
- Task states, Runs, checkpoints, Events, and waits;
- the matching `0.x` `absurd.sql` contract.

Schemas are local to Effect. They validate the same JSON that a TypeScript or
Python producer can write; they do not change the database representation.

Effect Workflows require one additional convention: another SDK spawning a
Workflow Task must use the Effect execution ID as Absurd's idempotency key.
Advanced Workflow state also uses the adapter's reserved persisted names. A
promise SDK can observe and operate the backing Absurd Task, but it does not
automatically acquire Effect Workflow semantics merely by sharing the queue.

That is honest interoperability: one storage engine and one operational model,
with richer semantics understood by the adapter that owns their durable
encoding.

## What this SDK deliberately does not do

The boundary can be summarized by its non-goals:

- It does not modify the promise TypeScript or Python SDK to know about Effect.
- It does not add Effect-only tables or stored procedures.
- It does not make Absurd push workers or add a coordinator.
- It does not wrap the promise SDK; the native Effect driver calls the same
  stored functions through Effect's `SqlClient`.
- It does not expose a public Run identity for application logic.
- It does not invent Absurd-specific versions of Workflow, Activity, Deferred,
  durable clock, child execution, compensation, resume, or interrupt.
- It does not make all Effect programs durable. Durability still occurs at
  explicit Task, Step, Activity, Deferred, and clock boundaries.

These constraints are what keep the project recognizable as Absurd.

## Bottom line

The TypeScript SDK and the primary Effect Task API are two façades over the same
mental model:

```text
stable Task
  -> one or more Runs
    -> ordinary handler execution
      -> explicit checkpointed Steps
        -> Postgres remains the source of truth
```

The promise SDK groups the current execution behind `TaskContext`. The Effect
SDK separates stable metadata, durable operations, dependencies, and runtime
lifetime into `CurrentTask`, `Step`, Effect requirements, and `Worker.layer`.
That changes how the model is expressed, not what Absurd stores or how workers
make progress.

The optional Workflow adapter is a more ambitious layer. It maps Effect's
native Workflow mental model onto Absurd rather than cloning that API under
Absurd-specific names. Its acceptability depends on continuing to honor a hard
line: compile richer semantics into stock Absurd primitives, keep workers
pull-based, keep state transitions in `absurd.sql`, and treat adapter-owned
persisted metadata as a versioned compatibility contract.

Under that line, Effect is not a second engine beside Absurd. It is an
Effect-idiomatic SDK, plus an optional interpreter that demonstrates how far
Absurd's small set of durable primitives can go.

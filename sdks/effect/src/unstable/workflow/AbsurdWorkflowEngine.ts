/**
 * Idiomatic Effect v4 `WorkflowEngine` adapter backed by Absurd queues.
 *
 * `AbsurdWorkflowEngine.Queue` annotates an Effect `Workflow` with the physical
 * Absurd queue it runs on; `AbsurdWorkflowEngine.inQueue` is its validated
 * convenience form. `AbsurdWorkflowEngine.layer` provides
 * `WorkflowEngine.WorkflowEngine` from a generic `effect/unstable/sql`
 * `SqlClient`. Definitions, execution, polling, handler registration,
 * activities, and durable deferreds remain Effect-owned; Absurd persists the
 * tasks, checkpoints, events, and waits that make them durable.
 *
 * Workflow execution identity is Effect's deterministic execution ID, mapped
 * to Absurd's idempotency key. Absurd UUID task/run IDs stay internal.
 *
 * Infrastructure failures surface as defects, matching Effect `WorkflowEngine`
 * semantics; workflow typed failures are preserved through encoded results.
 *
 * Requires PostgreSQL: statements target the `absurd` schema functions and
 * queue-prefixed tables with `$n` placeholders.
 */
import * as Cause from "effect/Cause";
import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Random from "effect/Random";
import * as Schedule from "effect/Schedule";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { Activity, DurableDeferred, Workflow, WorkflowEngine } from "effect/unstable/workflow";
import * as os from "os";

import * as AbsurdSql from "../../internal/AbsurdSql.ts";
import * as Reserved from "./Reserved.ts";

// The upstream `WorkflowEngine.Encoded` contract mandates `object`/`unknown`
// at its persistence seam (payloads, checkpoints, exits), and Effect's own
// reference implementations of that contract are lint-excluded vendored code.
// These signatures are the contract speaking, not design failures; everything
// outside them passes all rules.
// oxlint-disable anti-slop/no-object-parameters
// oxlint-disable anti-slop/no-unknown-parameters
// oxlint-disable anti-slop/no-chained-type-assertions
// oxlint-disable anti-slop/require-safety-comment-for-type-assertion
// oxlint-disable anti-slop/no-runtime-typeof
// oxlint-disable effecttsgo/any-unknown-in-error-context

const ExecutionReference = Schema.Struct({
  queue: Schema.String,
  executionId: Schema.String,
});
type ExecutionReference = typeof ExecutionReference.Type;

const WorkflowHeaders = Schema.NullOr(
  Schema.Struct({
    [Reserved.parentExecutionHeaderKey]: Schema.optionalKey(ExecutionReference),
  }),
);

const parentExecutionHeaders = (parent: ExecutionReference) => ({
  [Reserved.parentExecutionHeaderKey]: parent,
});

const decodeParentExecution = Effect.fnUntraced(function* (headers: unknown) {
  const decoded = yield* Schema.decodeUnknownEffect(WorkflowHeaders)(headers);
  if (decoded === null) return Option.none();
  return Option.fromNullishOr(decoded[Reserved.parentExecutionHeaderKey]);
});

/** @internal Canonical JSON codec pair for one workflow's persistence. */
interface WorkflowCodecs {
  readonly payload: ReturnType<typeof Schema.toCodecJson>;
  readonly result: ReturnType<typeof Schema.toCodecJson>;
}

const makeWorkflowCodecs = (workflow: Workflow.Any): WorkflowCodecs => ({
  payload: Schema.toCodecJson(workflow.payloadSchema),
  result: Schema.toCodecJson(
    Workflow.Result({
      success: workflow.successSchema,
      error: workflow.errorSchema,
    }),
  ),
});

/**
 * Structural Exit envelope for already-encoded activity and deferred values.
 * Unknown slots preserve those values; Defect handles defect serialization.
 */
const structuralExitCodec = Schema.toCodecJson(
  Schema.Exit(Schema.Unknown, Schema.Unknown, Schema.Defect()),
);

/** Normalizes nullish successes for the engine's JSON exit shape. */
const exitWithNullishValues = (exit: Exit.Exit<unknown, unknown>): Exit.Exit<unknown, unknown> =>
  Exit.map(exit, (value) => value ?? null);

type StructuralExitEncoded = Schema.Codec.Encoded<typeof structuralExitCodec>;

const encodeStructuralExit = (exit: Exit.Exit<unknown, unknown>): StructuralExitEncoded =>
  exit.pipe(exitWithNullishValues, Schema.encodeSync(structuralExitCodec));

const decodeStructuralExit = (stored: unknown): Exit.Exit<unknown, unknown> =>
  Schema.decodeUnknownSync(structuralExitCodec)(stored);

/**
 * Annotation holding the physical Absurd queue for a workflow.
 *
 * Use directly with `Workflow.annotate`, or use
 * `AbsurdWorkflowEngine.inQueue` for validated convenience.
 */
const Queue = Context.Reference<string>(
  "absurdly-effective/unstable/workflow/AbsurdWorkflowEngine/Queue",
  { defaultValue: () => "" },
);

interface ActiveClaim {
  readonly queue: string;
  readonly taskId: string;
  /**
   * The exact run ID this worker claimed.
   *
   * Persistence operations fence themselves to this run: checkpoints are
   * written under its ID, so a stale worker whose lease was lost cannot
   * commit state underneath its replacement (Absurd rejects writes owned by
   * superseded runs).
   */
  readonly runId: string;
  /** Child executions observed while replaying this parent run. */
  readonly awaitedChildren: Map<string, ExecutionReference>;
  /** Workflow-level deferreds that returned pending during this replay. */
  readonly pendingDeferreds: Set<string>;
  /** Root instance; activity bodies receive a distinct awaited-deferred set. */
  readonly instance: WorkflowEngine.WorkflowInstance["Service"];
  /** Fallback cadence for observing externally terminated children. */
  readonly childStatusPollInterval: Duration.Duration;
}

/**
 * The claim a worker fiber is currently executing.
 *
 * Provided around handler replay; persistence helpers read it to target the
 * claimed queue/task/run triple without rereading mutable ownership columns.
 */
class ClaimContext extends Context.Service<ClaimContext, ActiveClaim>()(
  "absurdly-effective/unstable/workflow/AbsurdWorkflowEngine/ClaimContext",
) {}

const MAX_QUEUE_NAME_LENGTH = 57;
const DEFAULT_CLAIM_TIMEOUT = Duration.seconds(120);
const DEFAULT_CHILD_STATUS_POLL_INTERVAL = Duration.seconds(30);
const WORKFLOW_INFRASTRUCTURE_MAX_ATTEMPTS = 5;
const UNKNOWN_TASK_DEFER_BASE_SECONDS = 15;
const UNKNOWN_TASK_DEFER_JITTER_SECONDS = 15;

/**
 * Converts a duration at the Absurd SQL boundary, which accepts whole seconds.
 */
const toPositiveWholeSeconds = (duration: Duration.Duration): number =>
  Math.max(1, Math.round(Duration.toSeconds(duration)));

/**
 * Heartbeat cadence derived from the lease: renew at half the lease so a
 * killed worker holds a stale claim for at most one extra interval.
 */
const heartbeatInterval = (claimTimeout: Duration.Duration): Duration.Duration =>
  claimTimeout.pipe(
    toPositiveWholeSeconds,
    (seconds) => Duration.seconds(seconds / 2),
    Duration.max(Duration.millis(500)),
  );

const minimumDefined = (left: number | undefined, right: number | undefined) =>
  left === undefined ? right : right === undefined ? left : Math.min(left, right);

interface QueueConfig {
  readonly name: string;
  readonly concurrency: number;
  readonly pollInterval: Duration.Duration;
  readonly claimTimeout: Duration.Duration;
  readonly childStatusPollInterval: Duration.Duration;
}

export interface QueueOptions {
  /**
   * Maximum workflow executions processed in parallel for this queue.
   *
   * @default 1
   */
  readonly concurrency?: number | undefined;
  /**
   * Idle delay between claim polls.
   *
   * @default 250ms
   */
  readonly pollInterval?: Duration.Input | undefined;
  /**
   * Claim lease granted to this worker when it picks up a run, in Effect
   * `Duration`. The lease is renewed by a heartbeat at half this interval
   * while a handler is in flight; losing the heartbeat interrupts the
   * handler, and if the worker dies another worker may reclaim the run once
   * the lease expires. Absurd leases whole seconds, so values are rounded
   * and clamped to a minimum of one second.
   *
   * @default 120s
   */
  readonly claimTimeout?: Duration.Input | undefined;
  /**
   * Fallback cadence for observing a nested child whose underlying Absurd
   * task was failed or cancelled outside this workflow engine. Normal child
   * completion rings the parent immediately; this pull-based check covers
   * external terminal transitions without requiring an Absurd push primitive.
   * Absurd accepts whole seconds, so values are rounded and clamped to one.
   *
   * @default 30s
   */
  readonly childStatusPollInterval?: Duration.Input | undefined;
}

export interface LayerOptions {
  readonly queues: Readonly<Record<string, QueueOptions>>;
}

/**
 * Absurd's operational view of one Effect workflow execution.
 *
 * Typed workflow failures are represented by `Completed.exit`; `Failed` is
 * reserved for failures of the backing Absurd task itself.
 */
export type AbsurdWorkflowExecutionStatus<Success, Error> =
  | { readonly _tag: "NotFound" }
  | { readonly _tag: "Pending" }
  | { readonly _tag: "Running" }
  | { readonly _tag: "Sleeping" }
  | {
      readonly _tag: "Completed";
      readonly exit: Exit.Exit<Success, Error>;
    }
  | { readonly _tag: "Failed"; readonly failure: unknown }
  | { readonly _tag: "Cancelled" };

/** Internal capability paired with the public Absurd workflow-engine layer. */
class ExecutionStatusService extends Context.Service<
  ExecutionStatusService,
  {
    readonly get: (
      workflow: Workflow.Any,
      executionId: string,
    ) => Effect.Effect<AbsurdWorkflowExecutionStatus<unknown, unknown>>;
  }
>()("absurdly-effective/unstable/workflow/AbsurdWorkflowEngine/ExecutionStatusService") {}

interface Registration {
  readonly workflow: Workflow.Any;
  readonly queue: string;
  /** Persistence codecs derived once from the workflow's own schemas. */
  readonly codecs: WorkflowCodecs;
  /** Registration-time context; provides schema encoding/decoding services. */
  readonly services: Context.Context<never>;
  readonly execute: (
    payload: object,
    executionId: string,
  ) => Effect.Effect<
    unknown,
    unknown,
    WorkflowEngine.WorkflowInstance | WorkflowEngine.WorkflowEngine
  >;
}

const validateQueueName = (queueName: string): string => {
  if (queueName === "" || !/^[A-Za-z0-9_]+$/.test(queueName)) {
    throw new Error(`Invalid Absurd queue name "${queueName}": expected non-empty [A-Za-z0-9_].`);
  }
  if (queueName.length > MAX_QUEUE_NAME_LENGTH) {
    throw new Error(
      `Queue name "${queueName}" is too long (max ${MAX_QUEUE_NAME_LENGTH} characters).`,
    );
  }
  return queueName;
};

const makeDoorbell = (executionId: string) =>
  Effect.map(Effect.all([Random.nextInt, Random.nextInt]), ([high, low]) => {
    const id = `${high.toString(36)}:${low.toString(36)}`;
    return Reserved.doorbellNames(executionId, id);
  });

const isTerminalTaskState = (state: AbsurdSql.TaskState): boolean =>
  state === "completed" || state === "failed" || state === "cancelled";

const deterministicJitterSeconds = (seed: string, maxJitterSeconds: number): number => {
  let hash = 2166136261;
  for (let index = 0; index < seed.length; index += 1) {
    hash ^= seed.charCodeAt(index);
    hash = Math.imul(hash, 16777619);
  }
  return Math.abs(hash) % (maxJitterSeconds + 1);
};

const inQueue =
  (queueName: string) =>
  <W extends Workflow.Any>(workflow: W): W => {
    validateQueueName(queueName);
    // SAFETY: Effect's erased `Workflow.Any` omits `annotate`, while every
    // concrete workflow value has it and annotation preserves the exact W.
    return (workflow as unknown as Workflow.Workflow<string, never, never, never>).annotate(
      Queue,
      queueName,
    ) as unknown as W;
  };

/**
 * Read Absurd's operational state without changing Effect's portable
 * `Workflow.poll` contract.
 */
const executionStatus = <
  Name extends string,
  Payload extends Workflow.AnyStructSchema,
  Success extends Schema.Top,
  Error extends Schema.Top,
>(
  workflow: Workflow.Workflow<Name, Payload, Success, Error>,
  executionId: string,
): Effect.Effect<
  AbsurdWorkflowExecutionStatus<Success["Type"], Error["Type"]>,
  never,
  ExecutionStatusService | Success["DecodingServices"] | Error["DecodingServices"]
> =>
  Effect.flatMap(
    ExecutionStatusService,
    (service) =>
      // SAFETY: the service decodes completed values with the schemas carried
      // by this exact workflow; its erased storage method cannot express that
      // relationship, while this public generic signature can.
      service.get(workflow, executionId) as Effect.Effect<
        AbsurdWorkflowExecutionStatus<Success["Type"], Error["Type"]>,
        never,
        Success["DecodingServices"] | Error["DecodingServices"]
      >,
  );

/** Creates an Absurd-backed workflow engine and its execution-status capability. */
const makeServices = (options: LayerOptions) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const store = AbsurdSql.make(sql);

    const queueConfigs = Object.entries(options.queues).map(([name, queueOptions]) => ({
      name: validateQueueName(name),
      concurrency: Math.max(1, Math.floor(queueOptions.concurrency ?? 1)),
      pollInterval: Duration.fromInputUnsafe(queueOptions.pollInterval ?? Duration.millis(250)),
      claimTimeout: Duration.fromInputUnsafe(queueOptions.claimTimeout ?? DEFAULT_CLAIM_TIMEOUT),
      childStatusPollInterval: Duration.fromInputUnsafe(
        queueOptions.childStatusPollInterval ?? DEFAULT_CHILD_STATUS_POLL_INTERVAL,
      ),
    }));
    const queueConfigByName = new Map(queueConfigs.map((config) => [config.name, config]));
    const queueReady = new Map<string, Deferred.Deferred<void>>();
    for (const config of queueConfigs) {
      queueReady.set(config.name, yield* Deferred.make<void>());
    }

    const workerId = `absurdly-effective:${os.hostname?.() ?? "host"}:${process.pid}`;
    const registrations = new Map<string, Registration>();
    const codecCache = new Map<string, WorkflowCodecs>();

    const queueFor = (workflow: Workflow.Any): string => {
      const annotation = Context.getOption(workflow.annotations, Queue);
      if (Option.isNone(annotation) || annotation.value === "") {
        throw new Error(
          `Workflow "${workflow._tag}" has no Absurd queue annotation; annotate it with AbsurdWorkflowEngine.Queue or use AbsurdWorkflowEngine.inQueue.`,
        );
      }
      if (!queueConfigByName.has(annotation.value)) {
        throw new Error(
          `Workflow "${workflow._tag}" is bound to queue "${annotation.value}", but that queue is absent from AbsurdWorkflowEngine.layer.`,
        );
      }
      return annotation.value;
    };

    const codecsFor = (workflow: Workflow.Any): WorkflowCodecs => {
      const cached = codecCache.get(workflow._tag);
      if (cached !== undefined) return cached;
      const codecs = makeWorkflowCodecs(workflow);
      codecCache.set(workflow._tag, codecs);
      return codecs;
    };

    /**
     * `Workflow.Any` necessarily erases schema service requirements. The
     * context captured at registration (or at the public call site) is the
     * exact context from which those schemas came, so restoring it here is
     * the single type-erasure boundary for schema operations.
     */
    const runCodecWith = <A>(
      effect: Effect.Effect<A, unknown, unknown>,
      services: Context.Context<never>,
    ): Effect.Effect<A> =>
      Effect.provideContext(effect, services as Context.Context<unknown>).pipe(Effect.orDie);

    const runCodec = <A>(effect: Effect.Effect<A, unknown, unknown>): Effect.Effect<A> =>
      Effect.flatMap(Effect.context<never>(), (services) => runCodecWith(effect, services));

    const requireClaim = Effect.fnUntraced(function* (): Effect.fn.Return<ActiveClaim> {
      const claim = yield* Effect.serviceOption(ClaimContext);
      if (Option.isNone(claim)) {
        return yield* Effect.die(
          "This operation is only valid while executing a claimed Absurd workflow run.",
        );
      }
      return claim.value;
    });

    const registerWorkflow = Effect.fnUntraced(function* (
      workflow: Workflow.Any,
      execute: Registration["execute"],
    ): Effect.fn.Return<void, never, Scope.Scope> {
      const queue = queueFor(workflow);
      const existing = registrations.get(workflow._tag);
      if (existing !== undefined) {
        return yield* Effect.die(
          `Workflow "${workflow._tag}" is already registered in queue "${existing.queue}".`,
        );
      }

      const registration: Registration = {
        workflow,
        queue,
        codecs: codecsFor(workflow),
        services: yield* Effect.context<never>(),
        execute,
      };
      registrations.set(workflow._tag, registration);

      yield* Scope.addFinalizer(
        yield* Effect.scope,
        Effect.sync(() => {
          if (registrations.get(workflow._tag) === registration) {
            registrations.delete(workflow._tag);
          }
        }),
      );
      yield* Deferred.succeed(queueReady.get(queue)!, undefined);
    });

    const failMissingExecutionId = Effect.fnUntraced(function* (
      queue: string,
      claimed: AbsurdSql.ClaimedRow,
    ): Effect.fn.Return<void> {
      yield* Effect.orDie(
        store.failRun(queue, claimed.run_id, {
          name: "AbsurdEffectWorkflowProtocolError",
          reason: "MissingExecutionId",
          workflowName: claimed.task_name,
        }),
      );
      yield* Effect.logError(
        `[absurdly-effective] failed malformed workflow task ${claimed.task_id}: MissingExecutionId (${claimed.task_name})`,
      );
    });

    const deferUnknownRun = Effect.fnUntraced(function* (
      queue: string,
      claimed: AbsurdSql.ClaimedRow,
    ): Effect.fn.Return<void> {
      const doorbell = yield* makeDoorbell(claimed.task_id);
      yield* Effect.orDie(
        store.awaitEvent(
          queue,
          claimed.task_id,
          claimed.run_id,
          doorbell.checkpointName,
          doorbell.eventName,
          UNKNOWN_TASK_DEFER_BASE_SECONDS +
            deterministicJitterSeconds(claimed.run_id, UNKNOWN_TASK_DEFER_JITTER_SECONDS),
        ),
      );
      yield* Effect.logWarning(
        `[absurdly-effective] deferred unregistered workflow ${claimed.task_name} (${claimed.task_id}); it remains replayable when a worker with that handler is deployed`,
      );
    });

    const heartbeat = (claim: ActiveClaim, claimTimeout: Duration.Duration) => {
      const claimTimeoutSeconds = claimTimeout.pipe(toPositiveWholeSeconds);
      return claimTimeout.pipe(
        heartbeatInterval,
        Effect.sleep,
        Effect.andThen(
          Effect.suspend(() =>
            Effect.orDie(store.extendClaim(claim.queue, claim.runId, claimTimeoutSeconds)),
          ),
        ),
        Effect.forever,
      );
    };

    /**
     * Rings the one-shot event currently used to park a run, if it has
     * reached that suspension point. The durable state that explains the
     * wake is always written first; the event is only a scheduler doorbell.
     */
    const ringCurrentDoorbell = Effect.fnUntraced(function* (queue: string, runId: string) {
      const eventName = yield* store.currentWakeEvent(queue, runId);
      if (Option.isSome(eventName)) {
        yield* store.emitEvent(queue, eventName.value, null);
      }
    }, Effect.orDie);

    const ringExecutionDoorbell = Effect.fnUntraced(function* (
      execution: ExecutionReference,
    ): Effect.fn.Return<void> {
      const task = yield* Effect.orDie(
        store.taskByExecutionId(execution.queue, execution.executionId),
      );
      if (Option.isSome(task) && task.value.last_attempt_run !== null) {
        yield* ringCurrentDoorbell(execution.queue, task.value.last_attempt_run);
      }
    });

    const deferredAvailable = Effect.fnUntraced(function* (
      claim: ActiveClaim,
      deferredName: string,
    ) {
      const checkpoint = yield* store.checkpointState(
        claim.queue,
        claim.taskId,
        Reserved.deferredCheckpointName(deferredName),
      );
      if (Option.isSome(checkpoint)) return true;
      const event = yield* store.eventPayload(
        claim.queue,
        Reserved.deferredEventName(
          claim.instance.workflow._tag,
          claim.instance.executionId,
          deferredName,
        ),
      );
      return Option.isSome(event);
    }, Effect.orDie);

    const childIsTerminal = Effect.fnUntraced(function* (
      child: ExecutionReference,
    ): Effect.fn.Return<boolean> {
      const task = yield* Effect.orDie(store.taskByExecutionId(child.queue, child.executionId));
      return Option.isSome(task) && isTerminalTaskState(task.value.state);
    });

    const activeWakeAvailable = Effect.fnUntraced(function* (
      claim: ActiveClaim,
    ): Effect.fn.Return<boolean> {
      const now = yield* Clock.currentTimeMillis;
      for (const deferredName of claim.pendingDeferreds) {
        if (yield* deferredAvailable(claim, deferredName)) return true;

        const deadline = yield* Effect.orDie(
          store.checkpointState(
            claim.queue,
            claim.taskId,
            Reserved.clockDeadlineCheckpointName(deferredName),
          ),
        );
        if (
          Option.isSome(deadline) &&
          typeof deadline.value === "number" &&
          deadline.value <= now
        ) {
          return true;
        }
      }
      return false;
    });

    const waitForActiveWake = (claim: ActiveClaim, pollInterval: Duration.Duration) =>
      activeWakeAvailable(claim).pipe(
        Effect.repeat({
          schedule: Schedule.spaced(pollInterval),
          until: (available) => available,
        }),
        Effect.as({ _tag: "ActiveWake" } as const),
      );

    const parkSuspended = Effect.fnUntraced(function* (claim: ActiveClaim): Effect.fn.Return<void> {
      const interruptRequested = yield* Effect.orDie(
        store.checkpointState(claim.queue, claim.taskId, Reserved.interruptCheckpointName),
      );
      if (Option.isSome(interruptRequested)) {
        yield* Effect.orDie(store.scheduleRunInSeconds(claim.queue, claim.runId, 0));
        return;
      }

      for (const child of claim.awaitedChildren.values()) {
        if (yield* childIsTerminal(child)) {
          yield* Effect.orDie(store.scheduleRunInSeconds(claim.queue, claim.runId, 0));
          return;
        }
      }

      let earliestDeadline: number | undefined;
      const unresolvedDeferreds: Array<string> = [];
      for (const deferredName of claim.instance.awaitedDeferreds) {
        if (yield* deferredAvailable(claim, deferredName)) continue;
        unresolvedDeferreds.push(deferredName);

        const deadline = yield* Effect.orDie(
          store.checkpointState(
            claim.queue,
            claim.taskId,
            Reserved.clockDeadlineCheckpointName(deferredName),
          ),
        );
        if (Option.isSome(deadline) && typeof deadline.value === "number") {
          earliestDeadline =
            earliestDeadline === undefined
              ? deadline.value
              : Math.min(earliestDeadline, deadline.value);
        }
      }

      const clockTimeoutSeconds =
        earliestDeadline === undefined
          ? undefined
          : Math.max(0, Math.ceil((earliestDeadline - (yield* Clock.currentTimeMillis)) / 1000));
      const childPollTimeoutSeconds =
        claim.awaitedChildren.size === 0
          ? undefined
          : toPositiveWholeSeconds(claim.childStatusPollInterval);
      const timeoutSeconds = minimumDefined(clockTimeoutSeconds, childPollTimeoutSeconds);
      const doorbell = yield* makeDoorbell(claim.instance.executionId);
      const wait = yield* Effect.orDie(
        store.awaitEvent(
          claim.queue,
          claim.taskId,
          claim.runId,
          doorbell.checkpointName,
          doorbell.eventName,
          timeoutSeconds,
        ),
      );
      if (!wait.should_suspend) {
        yield* Effect.orDie(store.scheduleRunInSeconds(claim.queue, claim.runId, 0));
        return;
      }

      // Recheck every durable wake source after registering. Producers write
      // their durable fact before ringing; this closes both sides of the
      // check/register race without requiring a scheduler-control primitive.
      const racedInterrupt = yield* Effect.orDie(
        store.checkpointState(claim.queue, claim.taskId, Reserved.interruptCheckpointName),
      );
      if (Option.isSome(racedInterrupt)) {
        yield* Effect.orDie(store.emitEvent(claim.queue, doorbell.eventName, null));
        return;
      }
      for (const deferredName of unresolvedDeferreds) {
        if (yield* deferredAvailable(claim, deferredName)) {
          yield* Effect.orDie(store.emitEvent(claim.queue, doorbell.eventName, null));
          return;
        }
      }
      for (const child of claim.awaitedChildren.values()) {
        if (yield* childIsTerminal(child)) {
          yield* Effect.orDie(store.emitEvent(claim.queue, doorbell.eventName, null));
          return;
        }
      }
    });

    let engine!: WorkflowEngine.WorkflowEngine["Service"];

    const processClaim = Effect.fnUntraced(function* (
      config: QueueConfig,
      claimed: AbsurdSql.ClaimedRow,
    ): Effect.fn.Return<void> {
      const executionId = yield* Effect.orDie(
        store.executionIdForTask(config.name, claimed.task_id),
      );
      if (Option.isNone(executionId)) {
        return yield* failMissingExecutionId(config.name, claimed);
      }

      const registration = registrations.get(claimed.task_name);
      if (registration === undefined) {
        return yield* deferUnknownRun(config.name, claimed);
      }

      const payload = (yield* runCodecWith(
        Schema.decodeUnknownEffect(registration.codecs.payload)(claimed.params),
        registration.services,
      )) as object;
      const instance = WorkflowEngine.WorkflowInstance.initial(
        registration.workflow,
        executionId.value,
      );
      const claim: ActiveClaim = {
        queue: config.name,
        taskId: claimed.task_id,
        runId: claimed.run_id,
        awaitedChildren: new Map(),
        pendingDeferreds: new Set(),
        instance,
        childStatusPollInterval: config.childStatusPollInterval,
      };

      // Replay must rebuild the workflow scope before interruption is
      // applied: checkpointed activities register their compensations while
      // replaying. The commit-boundary check below then closes that rebuilt
      // scope with the durable interrupt cause.
      const handler = registration.execute(payload, executionId.value).pipe(
        Effect.onInterrupt(() =>
          Effect.sync(() => {
            // Process shutdown or lease loss is replay, not workflow
            // failure: do not run compensations for infrastructure churn.
            if (!instance.interrupted) instance.suspended = true;
          }),
        ),
        Effect.onExit(() =>
          Effect.gen(function* () {
            // Re-read at the commit boundary as well as on replay. A safe
            // interrupt cannot preempt arbitrary provider code in another
            // process, but once requested it must win over a later handler
            // success instead of being silently committed past.
            const interruptRequested = yield* Effect.orDie(
              store.checkpointState(config.name, claimed.task_id, Reserved.interruptCheckpointName),
            );
            if (Option.isNone(interruptRequested)) return;
            instance.interrupted = true;
            instance.suspended = false;
            return yield* Effect.withFiber((fiber) =>
              fiber.pipe(Fiber.interrupt, Effect.interruptible),
            );
          }),
        ),
        Workflow.intoResult,
        Effect.provideService(WorkflowEngine.WorkflowEngine, engine),
        Effect.provideService(WorkflowEngine.WorkflowInstance, instance),
        Effect.provideService(ClaimContext, ClaimContext.of(claim)),
      );
      const lease = heartbeat(claim, config.claimTimeout).pipe(
        Effect.tapCause((cause) =>
          Cause.hasInterruptsOnly(cause)
            ? Effect.void
            : Effect.sync(() => {
                instance.suspended = true;
              }),
        ),
      );

      const activeWake = waitForActiveWake(claim, config.pollInterval);

      const raced = yield* Effect.raceFirst(
        handler.pipe(Effect.map((outcome) => ({ _tag: "Handler", outcome }) as const)),
        Effect.raceFirst(lease, activeWake),
      );
      if (raced._tag === "ActiveWake") {
        yield* Effect.orDie(store.scheduleRunInSeconds(claim.queue, claim.runId, 0));
        yield* Scope.close(instance.scope, Exit.void);
        return;
      }

      const outcome = raced.outcome;
      if (outcome._tag === "Complete") {
        const stored = yield* runCodecWith(
          Schema.encodeEffect(registration.codecs.result)(outcome),
          registration.services,
        );
        const parent = yield* decodeParentExecution(claimed.headers).pipe(Effect.orDie);
        yield* Effect.orDie(
          sql.withTransaction(
            Effect.gen(function* () {
              yield* Effect.orDie(store.completeRun(config.name, claimed.run_id, stored));
              if (Option.isSome(parent)) {
                yield* ringExecutionDoorbell(parent.value);
              }
            }),
          ),
        );
        return;
      }

      yield* parkSuspended(claim);
      // A suspended run is reconstructed by replay. Close this process-local
      // scope successfully to release resources without firing compensation.
      yield* Scope.close(instance.scope, Exit.void);
    });

    const workerLoop = (config: QueueConfig) =>
      Effect.forever(
        Effect.suspend(() =>
          Effect.orDie(
            store.claimTask(config.name, workerId, toPositiveWholeSeconds(config.claimTimeout)),
          ),
        ).pipe(
          Effect.flatMap(
            Option.match({
              onNone: () => Effect.sleep(config.pollInterval),
              onSome: (claimed) =>
                processClaim(config, claimed).pipe(
                  Effect.catchCause((cause) =>
                    Effect.logError(
                      `[absurdly-effective] workflow run ${claimed.task_name} (${claimed.task_id}/${claimed.run_id}) failed`,
                      cause,
                    ),
                  ),
                ),
            }),
          ),
        ),
      );

    const decodeCompleted = Effect.fnUntraced(function* (
      workflow: Workflow.Any,
      snapshot: AbsurdSql.TaskSnapshotRow,
    ): Effect.fn.Return<Workflow.Result<unknown, unknown>> {
      return (yield* runCodec(
        Schema.decodeUnknownEffect(codecsFor(workflow).result)(snapshot.result),
      )) as Workflow.Result<unknown, unknown>;
    });

    const resultFromSnapshot = (
      workflow: Workflow.Any,
      snapshot: AbsurdSql.TaskSnapshotRow,
    ): Effect.Effect<Workflow.Result<unknown, unknown>> => {
      if (snapshot.state === "completed") return decodeCompleted(workflow, snapshot);
      if (snapshot.state === "failed" || snapshot.state === "cancelled") {
        return Effect.die(
          new Error(`Workflow "${workflow._tag}" ended in state "${snapshot.state}".`, {
            cause: snapshot.failure_reason,
          }),
        );
      }
      return Effect.succeed(Workflow.Suspended.make({}));
    };

    const getExecutionStatus = Effect.fnUntraced(function* (
      workflow: Workflow.Any,
      executionId: string,
    ): Effect.fn.Return<AbsurdWorkflowExecutionStatus<unknown, unknown>> {
      const queue = queueFor(workflow);
      const task = yield* Effect.orDie(store.taskByExecutionId(queue, executionId));
      if (Option.isNone(task)) return { _tag: "NotFound" };

      const snapshot = yield* Effect.orDie(store.taskResult(queue, task.value.task_id));
      if (Option.isNone(snapshot)) return { _tag: "NotFound" };

      switch (snapshot.value.state) {
        case "pending":
          return { _tag: "Pending" };
        case "running":
          return { _tag: "Running" };
        case "sleeping":
          return { _tag: "Sleeping" };
        case "completed": {
          const result = yield* decodeCompleted(workflow, snapshot.value);
          if (result._tag !== "Complete") {
            return yield* Effect.die(
              `Completed Absurd task for execution "${executionId}" contained a suspended Effect workflow result.`,
            );
          }
          return { _tag: "Completed", exit: result.exit };
        }
        case "failed":
          return { _tag: "Failed", failure: snapshot.value.failure_reason };
        case "cancelled":
          return { _tag: "Cancelled" };
      }
    });

    const cancelByExecutionId = Effect.fnUntraced(function* (
      workflow: Workflow.Any,
      executionId: string,
    ) {
      const queue = queueFor(workflow);
      const task = yield* store.taskByExecutionId(queue, executionId);
      if (Option.isSome(task)) {
        yield* store.cancelTask(queue, task.value.task_id);
      }
    }, Effect.orDie);

    const interruptByExecutionId = Effect.fnUntraced(function* (
      workflow: Workflow.Any,
      executionId: string,
    ): Effect.fn.Return<void> {
      const queue = queueFor(workflow);
      yield* Effect.orDie(
        sql.withTransaction(
          Effect.gen(function* () {
            const task = yield* Effect.orDie(store.taskByExecutionId(queue, executionId));
            if (Option.isNone(task)) return;
            if (isTerminalTaskState(task.value.state)) return;

            const runId = task.value.last_attempt_run;
            if (runId === null) {
              return yield* Effect.die(
                `Active Absurd task for execution "${executionId}" has no run.`,
              );
            }

            // The marker is the durable interrupt request. The event emitted
            // afterwards is only a best-effort doorbell; parkSuspended's
            // post-registration recheck closes the lost-wakeup race.
            yield* Effect.orDie(
              store.setCheckpointState(
                queue,
                task.value.task_id,
                Reserved.interruptCheckpointName,
                true,
                runId,
              ),
            );
            yield* ringCurrentDoorbell(queue, runId);
          }),
        ),
      );
    });

    const queueForDeferred = Effect.fnUntraced(function* (
      workflowName: string,
      executionId: string,
    ): Effect.fn.Return<string> {
      const registered = registrations.get(workflowName);
      if (registered !== undefined) return registered.queue;

      const matches: Array<string> = [];
      for (const config of queueConfigs) {
        const task = yield* Effect.orDie(store.taskByExecutionId(config.name, executionId));
        if (Option.isSome(task)) matches.push(config.name);
      }
      if (matches.length !== 1) {
        return yield* Effect.die(
          `Expected exactly one Absurd queue for workflow "${workflowName}" execution "${executionId}", found ${matches.length}.`,
        );
      }
      return matches[0]!;
    });

    engine = WorkflowEngine.makeUnsafe({
      register: registerWorkflow,

      execute: (workflow, opts) =>
        Effect.gen(function* () {
          const queue = queueFor(workflow);
          const parent =
            opts.parent === undefined
              ? undefined
              : ({
                  queue: queueFor(opts.parent.workflow),
                  executionId: opts.parent.executionId,
                } satisfies ExecutionReference);
          const storedPayload = yield* runCodec(
            Schema.encodeEffect(codecsFor(workflow).payload)(opts.payload),
          );
          const commonSpawnOptions = {
            idempotency_key: opts.executionId,
            max_attempts: WORKFLOW_INFRASTRUCTURE_MAX_ATTEMPTS,
            retry_strategy: { kind: "fixed", base_seconds: 1 },
          };
          const spawnOptions =
            parent === undefined
              ? commonSpawnOptions
              : {
                  ...commonSpawnOptions,
                  headers: parentExecutionHeaders(parent),
                };
          // SAFETY: workflow payload schemas are structs, so their encoded
          // persistence representation is an object.
          const spawned = yield* Effect.orDie(
            store.spawnTask(queue, workflow._tag, storedPayload as object, spawnOptions),
          );
          if (parent !== undefined) {
            const parentClaim = yield* Effect.serviceOption(ClaimContext);
            if (Option.isSome(parentClaim)) {
              const child = {
                queue,
                executionId: opts.executionId,
              } satisfies ExecutionReference;
              parentClaim.value.awaitedChildren.set(`${queue}:${opts.executionId}`, child);
            }
          }
          if (opts.discard) return;

          const snapshot = yield* Effect.orDie(store.taskResult(queue, spawned.task_id));
          if (Option.isNone(snapshot)) {
            return yield* Effect.die(
              `Absurd task for execution "${opts.executionId}" vanished from queue "${queue}".`,
            );
          }
          return yield* resultFromSnapshot(workflow, snapshot.value);
        }) as Effect.Effect<
          typeof opts.discard extends true ? void : Workflow.Result<unknown, unknown>
        >,

      poll: Effect.fnUntraced(function* (
        workflow: Workflow.Any,
        executionId: string,
      ): Effect.fn.Return<Option.Option<Workflow.Result<unknown, unknown>>> {
        const queue = queueFor(workflow);
        const task = yield* Effect.orDie(store.taskByExecutionId(queue, executionId));
        if (Option.isNone(task)) return Option.none();
        const snapshot = yield* Effect.orDie(store.taskResult(queue, task.value.task_id));
        if (Option.isNone(snapshot)) return Option.none();
        if (snapshot.value.state === "pending" || snapshot.value.state === "running") {
          return Option.none();
        }
        return Option.some(yield* resultFromSnapshot(workflow, snapshot.value));
      }),

      interrupt: interruptByExecutionId,
      interruptUnsafe: cancelByExecutionId,

      resume: Effect.fnUntraced(function* (workflow, executionId) {
        const queue = queueFor(workflow);
        const task = yield* Effect.orDie(store.taskByExecutionId(queue, executionId));
        if (Option.isSome(task) && task.value.last_attempt_run !== null) {
          yield* ringCurrentDoorbell(queue, task.value.last_attempt_run);
        }
      }),

      activityExecute: Effect.fnUntraced(function* (activity: Activity.Any, attempt: number) {
        const parent = yield* WorkflowEngine.WorkflowInstance;
        const claim = yield* requireClaim();
        const checkpoint = Reserved.activityCheckpointName(activity.name, attempt);
        const stored = yield* Effect.orDie(
          store.checkpointState(claim.queue, claim.taskId, checkpoint),
        );
        if (Option.isSome(stored)) {
          return new Workflow.Complete({
            exit: decodeStructuralExit(stored.value),
          });
        }

        // Replays must restore earlier checkpointed activities so their
        // compensation finalizers are registered, but an acknowledged safe
        // interrupt must not begin a new external mutation.
        const interruptRequested = yield* Effect.orDie(
          store.checkpointState(claim.queue, claim.taskId, Reserved.interruptCheckpointName),
        );
        if (Option.isSome(interruptRequested)) {
          const instance = yield* WorkflowEngine.WorkflowInstance;
          instance.interrupted = true;
          instance.suspended = false;
          return yield* Effect.interrupt;
        }

        const activityInstance = WorkflowEngine.WorkflowInstance.initial(
          parent.workflow,
          parent.executionId,
        );
        const result = yield* activity.executeEncoded.pipe(
          Workflow.intoResult,
          Effect.provideService(WorkflowEngine.WorkflowInstance, activityInstance),
          Effect.provideService(Activity.CurrentAttempt, attempt),
        );
        if (result._tag === "Suspended") {
          yield* Scope.close(activityInstance.scope, Exit.void);
          return result;
        }

        yield* Effect.orDie(
          store.setCheckpointState(
            claim.queue,
            claim.taskId,
            checkpoint,
            encodeStructuralExit(result.exit),
            claim.runId,
          ),
        );
        return new Workflow.Complete({
          exit: exitWithNullishValues(result.exit),
        });
      }),

      deferredResult: Effect.fnUntraced(function* (deferred: DurableDeferred.Any) {
        const claim = yield* requireClaim();
        const instance = yield* WorkflowEngine.WorkflowInstance;
        const isWorkflowLevel = instance.awaitedDeferreds === claim.instance.awaitedDeferreds;
        const checkpoint = Reserved.deferredCheckpointName(deferred.name);
        const stored = yield* Effect.orDie(
          store.checkpointState(claim.queue, claim.taskId, checkpoint),
        );
        if (Option.isSome(stored)) {
          if (isWorkflowLevel) claim.pendingDeferreds.delete(deferred.name);
          return Option.some(decodeStructuralExit(stored.value));
        }

        const event = yield* Effect.orDie(
          store.eventPayload(
            claim.queue,
            Reserved.deferredEventName(instance.workflow._tag, instance.executionId, deferred.name),
          ),
        );
        if (Option.isNone(event)) {
          if (isWorkflowLevel) claim.pendingDeferreds.add(deferred.name);
          return Option.none();
        }

        yield* Effect.orDie(
          store.setCheckpointState(claim.queue, claim.taskId, checkpoint, event.value, claim.runId),
        );
        if (isWorkflowLevel) claim.pendingDeferreds.delete(deferred.name);
        return Option.some(decodeStructuralExit(event.value));
      }),

      deferredDone: Effect.fnUntraced(function* (opts) {
        const queue = yield* queueForDeferred(opts.workflowName, opts.executionId);
        // `emit_event` is first-write-wins and atomically wakes an existing
        // waiter (including committing its checkpoint). If no waiter exists
        // yet, the cached event closes the registration race on replay.
        yield* Effect.orDie(
          store.emitEvent(
            queue,
            Reserved.deferredEventName(opts.workflowName, opts.executionId, opts.deferredName),
            encodeStructuralExit(opts.exit as Exit.Exit<unknown, unknown>),
          ),
        );
        const task = yield* Effect.orDie(store.taskByExecutionId(queue, opts.executionId));
        if (Option.isSome(task) && task.value.last_attempt_run !== null) {
          yield* ringCurrentDoorbell(queue, task.value.last_attempt_run);
        }
      }),

      scheduleClock: Effect.fnUntraced(function* (workflow, opts) {
        const claim = yield* requireClaim();
        const deferredName = opts.clock.deferred.name;
        const deadlineCheckpoint = Reserved.clockDeadlineCheckpointName(deferredName);
        const existing = yield* Effect.orDie(
          store.checkpointState(claim.queue, claim.taskId, deadlineCheckpoint),
        );
        const now = yield* Clock.currentTimeMillis;
        const deadline =
          Option.isSome(existing) && typeof existing.value === "number"
            ? existing.value
            : now + Duration.toMillis(opts.clock.duration);

        if (Option.isNone(existing)) {
          yield* Effect.orDie(
            store.setCheckpointState(
              claim.queue,
              claim.taskId,
              deadlineCheckpoint,
              deadline,
              claim.runId,
            ),
          );
        }
        if (now < deadline) return;

        const completion = encodeStructuralExit(Exit.succeed(null));
        yield* Effect.orDie(
          store.setCheckpointState(
            claim.queue,
            claim.taskId,
            Reserved.deferredCheckpointName(deferredName),
            completion,
            claim.runId,
          ),
        );
        yield* Effect.orDie(
          store.emitEvent(
            claim.queue,
            Reserved.deferredEventName(workflow._tag, opts.executionId, deferredName),
            completion,
          ),
        );
      }),
    });

    for (const config of queueConfigs) {
      yield* queueReady
        .get(config.name)!
        .pipe(
          Deferred.await,
          Effect.andThen(
            Effect.forEach(
              Array.from({ length: config.concurrency }),
              () => Effect.forkScoped(workerLoop(config)),
              { concurrency: "unbounded", discard: true },
            ),
          ),
          Effect.forkScoped,
        );
    }

    return { engine, getExecutionStatus } as const;
  });

/** Creates a `WorkflowEngine` implementation backed by Absurd and PostgreSQL. */
const make = (options: LayerOptions) =>
  makeServices(options).pipe(Effect.map(({ engine }) => engine));

/** Layer that provides Effect's `WorkflowEngine` using the Absurd adapter. */
const layer = (
  options: LayerOptions,
): Layer.Layer<
  WorkflowEngine.WorkflowEngine | ExecutionStatusService,
  never,
  SqlClient.SqlClient
> =>
  Layer.effectContext(
    makeServices(options).pipe(
      Effect.map(({ engine, getExecutionStatus }) =>
        Context.make(WorkflowEngine.WorkflowEngine, engine).pipe(
          Context.add(
            ExecutionStatusService,
            ExecutionStatusService.of({ get: getExecutionStatus }),
          ),
        ),
      ),
    ),
  );

/** Effect-native durable workflow-engine adapter backed by Absurd. */
export const AbsurdWorkflowEngine = {
  Queue,
  inQueue,
  executionStatus,
  make,
  layer,
} as const;
